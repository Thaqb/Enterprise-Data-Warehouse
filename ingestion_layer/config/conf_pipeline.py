"""Runtime environment configuration for the Hostfully dlt pipelines.

Resolves the active environment (dev/prod) from the APP_ENV variable and
exposes the resulting destination and dataset name via PipelineConfig,
keeping this environment-selection logic separate from HostfullyConfig.
"""

import os

from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """Runtime configuration for the Hostfully ingestion_layer."""

    environment: str
    destination: str
    dataset: str

    @staticmethod
    def from_environment() -> "PipelineConfig":
        """Load and validate runtime configuration from APP_ENV."""

        environment = os.getenv("APP_ENV")

        if environment not in {"dev", "prod"}:
            raise ValueError(
                "APP_ENV must be defined and set to either 'dev' or 'prod'."
            )

        destination = "bigquery" if environment == "prod" else "duckdb"
        dataset = f"{environment}_hostfully"

        return PipelineConfig(
            environment=environment,
            destination=destination,
            dataset=dataset,
        )