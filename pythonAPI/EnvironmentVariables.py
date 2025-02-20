from typing import Optional

from pydantic import BaseModel


class EnvironmentVariables(BaseModel):
    cassandra_host: Optional[str] = "localhost"
    cassandra_port: Optional[str] = "9042"
    cassandra_user: Optional[str] = "cassandra"
    cassandra_pass: Optional[str] = "cassandra"
    cassandra_replication_factor: Optional[str] = "1"
    cassandra_gc_grace_seconds: Optional[str] = "864000"
    s3accessKeyAws: Optional[str] = "minioadmin"
    s3secretKeyAws: Optional[str] = "minioadmin"
    s3ConnectionTimeout: Optional[str] = "600000"
    s3endPointLoc: Optional[str] = "http://localhost:9000"
    kafkaBroker: Optional[str] = "localhost:29092"
    kafkaTopic: Optional[str] = "test"
    POSTGRES_ENDPOINT: Optional[str] = "localhost:5432/metrics"
    POSTGRES_PASSWORD: Optional[str] = "admin"
    POSTGRES_USERNAME: Optional[str] = "admin"
