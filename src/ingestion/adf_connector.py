"""
Azure Data Factory Ingestion — pulls raw policy, claims and telematics
data from source systems into ADLS Gen2 Bronze layer.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

import logging
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    PipelineRun,
    CreateRunResponse,
)
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUBSCRIPTION_ID = "your-subscription-id"
RESOURCE_GROUP  = "pramod-customer360-rg"
FACTORY_NAME    = "pramod-customer360-adf"
REGION          = "eastus"

# ADF pipeline names
PIPELINES = {
    "policy":     "pl_ingest_policy_bronze",
    "claims":     "pl_ingest_claims_bronze",
    "telematics": "pl_ingest_telematics_bronze",
}


class ADFIngestionClient:
    """
    Triggers Azure Data Factory pipelines for Bronze layer ingestion.
    Monitors run status and reports completion metrics.
    """

    def __init__(self):
        credential   = DefaultAzureCredential()
        self.client  = DataFactoryManagementClient(credential, SUBSCRIPTION_ID)
        self.rg      = RESOURCE_GROUP
        self.factory = FACTORY_NAME

    def trigger_pipeline(self, pipeline_name: str, parameters: dict = None) -> str:
        """Trigger an ADF pipeline and return the run ID."""
        params = parameters or {}
        params["ingestion_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        response: CreateRunResponse = self.client.pipelines.create_run(
            self.rg, self.factory, pipeline_name,
            parameters=params,
        )
        logger.info(f"Triggered pipeline '{pipeline_name}' — run_id: {response.run_id}")
        return response.run_id

    def get_run_status(self, run_id: str) -> str:
        """Poll pipeline run status."""
        run: PipelineRun = self.client.pipeline_runs.get(self.rg, self.factory, run_id)
        return run.status   # Queued | InProgress | Succeeded | Failed | Cancelled

    def wait_for_completion(self, run_id: str, poll_interval: int = 30, timeout: int = 3600) -> bool:
        """Wait for pipeline run to complete. Returns True on success."""
        import time
        start = time.time()
        while time.time() - start < timeout:
            status = self.get_run_status(run_id)
            logger.info(f"Run {run_id} status: {status}")
            if status == "Succeeded":
                return True
            if status in ("Failed", "Cancelled"):
                logger.error(f"Pipeline run {run_id} ended with status: {status}")
                return False
            time.sleep(poll_interval)
        raise TimeoutError(f"Pipeline run {run_id} timed out after {timeout}s")

    def trigger_all_bronze_pipelines(self) -> dict[str, bool]:
        """Trigger all three Bronze ingestion pipelines in parallel."""
        import concurrent.futures
        results = {}

        def run_pipeline(name: str, pipeline: str) -> tuple[str, bool]:
            run_id = self.trigger_pipeline(pipeline)
            success = self.wait_for_completion(run_id)
            return name, success

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(run_pipeline, name, pipeline): name
                for name, pipeline in PIPELINES.items()
            }
            for future in concurrent.futures.as_completed(futures):
                name, success = future.result()
                results[name] = success
                logger.info(f"Pipeline '{name}': {'✓ SUCCESS' if success else '✗ FAILED'}")

        return results


if __name__ == "__main__":
    client  = ADFIngestionClient()
    results = client.trigger_all_bronze_pipelines()
    failed  = [k for k, v in results.items() if not v]
    if failed:
        raise RuntimeError(f"Bronze ingestion failed for: {failed}")
    logger.info("All Bronze ingestion pipelines completed successfully.")
