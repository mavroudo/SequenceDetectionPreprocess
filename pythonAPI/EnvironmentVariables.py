from typing import Optional

from pydantic import BaseModel


class EnvironmentVariables(BaseModel):
    s3accessKeyAws: Optional[str] = "minioadmin"
    s3secretKeyAws: Optional[str] = "minioadmin"
    s3ConnectionTimeout: Optional[str] = "600000"
    s3endPointLoc: Optional[str] = "http://localhost:9000"
    kafkaBroker: Optional[str] = "localhost:29092"
    kafkaTopic: Optional[str] = "test"
    POSTGRES_ENDPOINT: Optional[str] = "localhost:5432/metrics"
    POSTGRES_PASSWORD: Optional[str] = "admin"
    POSTGRES_USERNAME: Optional[str] = "admin"
