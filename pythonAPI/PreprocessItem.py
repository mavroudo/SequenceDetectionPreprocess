from typing import Optional

from pydantic import BaseModel


class PreprocessItem(BaseModel):
    spark_master: Optional[str] = "master[*]"
    database: Optional[str] = "s3"
    mode: Optional[str] = "positions"
    compression: Optional[str] = "snappy"
    spark_parameters: Optional[str] = ""
    file: str = ""
    logname: str = ""
    delete_all: Optional[bool] = False
    delete_prev: Optional[bool] = False
    split_every_days: Optional[int] = 30
    lookback: Optional[int] = 30

    def getAttributes(self):
        s = f" --master {self.spark_master}"
        s += f" {self.spark_parameters}"
        s += f" preprocess.jar "
        s += f" -d {self.database}"
        s += f" -m {self.mode}"
        s += f" -c {self.compression}"
        s += f" -f uploadedfiles/{self.file}"
        s += f" --logname {self.logname}"
        if self.delete_all:
            s += f" --delete_all"
        if self.delete_prev:
            s += f" --delete_prev"
        s += f" -s {self.split_every_days}"
        s += f" --lookback {self.lookback}"
        return s
