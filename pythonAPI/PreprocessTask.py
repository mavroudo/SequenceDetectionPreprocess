import time

from sqlalchemy.orm import scoped_session
from dbSQL import crud, model
from dbSQL.database import SessionLocal
import threading
import os
import subprocess, sys


def task(spark_location: str, params: str, process_id, lock: threading.Lock):
    db = scoped_session(SessionLocal)
    processEntry: model.PreprocessEntry = crud.get(db, process_id)
    processEntry.status = "executing"
    crud.update(db, processEntry)
    x = spark_location + " " + params
    coms = [i for i in x.split(" ") if i != '']
    result = subprocess.run(coms, capture_output=True, text=True)
    processEntry.error = result.stderr
    processEntry.output = result.stdout
    processEntry.status = "finished"
    crud.update(db, processEntry)
    lock.release()
