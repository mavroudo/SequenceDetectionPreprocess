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
    process = os.popen(spark_location + " " + params)
    #  TODO: redirect standar output and error to capture them and send them to the use
    output = process.read()
    # result = subprocess.run([spark_location, params], capture_output=True, text=True)
    processEntry.output = output
    # processEntry.error = result.stderr
    processEntry.status = "finished"
    crud.update(db, processEntry)
    lock.release()
