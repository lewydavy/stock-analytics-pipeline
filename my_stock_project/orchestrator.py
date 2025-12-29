import os
import subprocess
import sys
from pathlib import Path
from dagster import (
    AssetExecutionContext, 
    Definitions, 
    asset, 
    define_asset_job, 
    ScheduleDefinition,
    AssetKey
)
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

# --- 1. CONFIG ---
# Get path of the current file directory
PROJECT_DIR = Path(__file__).parent.resolve()

# Path to your Python script
PYTHON_SCRIPT_PATH = PROJECT_DIR / "python_scripts" / "fetch_stock_data.py"

# Path to dbt manifest
DBT_MANIFEST_PATH = PROJECT_DIR / "target" / "manifest.json"

# --- 2. PYTHON ASSET ---
# the link ID
PYTHON_ASSET_KEY = AssetKey(["raw_data", "stock_prices_batch"])

@asset(
    key=PYTHON_ASSET_KEY,
    description="Fetches Yahoo Finance data and loads it into Postgres",
    group_name="ingestion",
    compute_kind="python"
)
def raw_stock_data(context: AssetExecutionContext):
    context.log.info(f"Running script at: {PYTHON_SCRIPT_PATH}")
    
    # Run script using current Python executable
    result = subprocess.run(
        [sys.executable, str(PYTHON_SCRIPT_PATH)], 
        capture_output=True, 
        text=True
    )
    
    # Logs for UI
    if result.returncode != 0:
        raise Exception(f"Script Failed: {result.stderr}")
    
    context.log.info(f"Script Output: {result.stdout}")
    context.log.info("Finished loading raw data to Postgres.")

# --- 3. TRANSLATOR ---
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    # use *args and **kwargs to handle any version of Dagster
    def get_asset_spec(self, *args, **kwargs):
        # 1. Generate the default spec using the parent logic
        default_spec = super().get_asset_spec(*args, **kwargs)
        
        # 2. Look for "stg_stock_prices"
        # If found force to depend on Python Asset
        if default_spec.key.path[-1] == "stg_stock_prices":
            current_deps = list(default_spec.deps)
            return default_spec.replace_attributes(
                deps=current_deps + [PYTHON_ASSET_KEY]
            )
        
        return default_spec

# --- 4. DBT ASSETS---
if not DBT_MANIFEST_PATH.exists():
    raise Exception("Manifest not found. Run 'dbt parse'!")

@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# --- 5. JOB & DEFS ---
daily_pipeline_job = define_asset_job(
    name="daily_stock_update_job",
    selection="*"
)

# Schedule: Run daily at 6 AM
daily_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[raw_stock_data, my_dbt_assets],
    jobs=[daily_pipeline_job],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=os.fspath(PROJECT_DIR))},
)