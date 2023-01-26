from dataclasses import dataclass


@dataclass(frozen=True)
class ZoomcampConfig:
    GCP_PROJECT_ID: str = "light-reality-344611"
    GCS_BUCKET: str = "zoomcamp"