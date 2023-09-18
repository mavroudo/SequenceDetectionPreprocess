import time

from fastapi import FastAPI, Depends
from fastapi import File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import MetaData
from sqlalchemy.orm import Session
from EnvironmentVariables import EnvironmentVariables
import uuid, os
from PreprocessItem import PreprocessItem
from threading import Lock
from fastapi.responses import JSONResponse

from dbSQL import crud, model, schema
from dbSQL.database import SessionLocal, engine

model.Base.metadata.create_all(bind=engine)

spark_location = "/opt/spark/bin/spark-submit"

ALLOWED_EXTENSIONS = {'withTimestamp', 'xes', 'jpg', 'pdf'}
env_vars = {"cassandra_host": "localhost", "cassandra_port": 9042, "cassandra_user": "cassandra",
            "cassandra_pass": "cassandra", "cassandra_replication_factor": 1, "cassandra_gc_grace_seconds": 864000,
            "s3accessKeyAws": "minioadmin", "s3ConnectionTimeout": 600000, "s3endPointLoc": "http://localhost:9000",
            "s3secretKeyAws": "minioadmin"}

app = FastAPI()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


origins = ["*"]
app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials=True,
                   allow_methods=["*"],
                   allow_headers=["*"]
                   )
lock: Lock = Lock()


@app.get("/isFree",
         responses={
             200: {
                 "content": {"available": {}},
                 "description": "Return if the preprocessing component is ready to accept a new job",
             }})
async def available_to_preprocess():
    x: bool = not lock.locked()
    return JSONResponse(content={"available": x}, status_code=200)


@app.get("/filelist",
         responses={
             200: {
                 "content": {"files": {}},
                 "description": "Return a list with all the uploaded files.",
             },
             500: {
                 "content": {"error": {}},
                 "description": "An error has occurred during setting the variables",
             }
         })
async def get_File_List():
    try:
        files = []
        for uidd in os.listdir("uploadedfiles"):
            if uidd != ".gitkeep":
                path = f"uploadedfiles/{uidd}/"
                for file in os.listdir(path):
                    files.append({"uidd": uidd, "filename": file})
        return JSONResponse(content={"files": files}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/setting_vars",
         responses={
             200: {
                 "description": "Successfully set the variables.",
             },
             500: {
                 "content": {"error": {}},
                 "description": "An error has occurred during reading the variables",
             }
         })
def get_Environmental_Variables():
    try:
        env = EnvironmentVariables()
        for key in env.dict():
            try:
                stored_value = os.environ[key]
                setattr(env, key, stored_value)
            except Exception as e:
                os.environ[key] = getattr(env, key)
        # pass the default unchangeable env here
        os.environ["cassandra_keyspace_name"] = "siesta"
        os.environ["cassandra_replication_class"] = "SimpleStrategy"
        os.environ["cassandra_replication_rack"] = "replication_factor"
        os.environ["cassandra_write_consistency_level"] = "ONE"
        return JSONResponse(content=env.dict(), status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/setting_vars",
          responses={
              200: {
                  "description": "Successfully set the variables.",
              },
              500: {
                  "content": {"error": {}},
                  "description": "An error has occurred during setting the variables",
              }
          }
          )
def set_Environmental_Variables(env: EnvironmentVariables):
    try:
        for key in env.dict():
            os.environ[key] = getattr(env, key)
        return JSONResponse(content={"message": "Variables set successfully!"}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/preprocess",
          responses={
              200: {
                  "content": {"output": {}},
                  "description": "Return the process output",
              },
              520: {
                  "content": {"message": {}},
                  "description": "Another preprocessing job is already running",
              },
              500: {
                  "content": {"error": {}},
                  "description": "An error has occurred during processing",
              }
          }
          )
async def preprocess_file(params: PreprocessItem, db: Session = Depends(get_db)):
    '''
    Executes the preprocessing step for a particular file (Already uploaded).
        The environmental parameters are set using  the **/setting_vars** endpoint.
         Additionally, make sure that the database are accessible from where the preprocessing component is running. \n
     **Configuration parameters**: Spark config (e.g. master, number of executors etc.)
     and preprocessing parameters (e.g. lookback, compression etc.)\n
    '''
    spark_command = spark_location + " " + params.getAttributes()
    process = crud.start_process(db)
    print(process)
    # time.sleep(10)
    process.message = "something is happening"
    crud.update(db, process)
    print(crud.get_all(db))

    # if not lock.locked():
    #     with lock:
    #         try:
    #             process = os.popen(spark_command)
    #             # TODO: redirect standar output and error to capture them and send them to the use
    #             output = process.read()
    #             return JSONResponse(content={"output": output}, status_code=200)
    #         except Exception as e:
    #             return JSONResponse(content={"error": str(e)}, status_code=500)
    # else:
    #     return JSONResponse(content={"message": "Already running a preprocessing job"}, status_code=520)


@app.post("/upload/",
          responses={
              200: {
                  "content": {"message": {}},
                  "description": "Return name of the uploaded file",
              },
              522: {
                  "content": {"message": {}},
                  "description": "Invalid file extension",
              },
              500: {
                  "content": {"error": {}},
                  "description": "An error occurred during uploading",
              }
          }
          )
async def upload_log_file(file: UploadFile = File(...)):
    '''
    Upload log file in order to be processed.
    '''
    file_extension = file.filename.split(".")[-1]
    if file_extension not in ALLOWED_EXTENSIONS:
        return JSONResponse(content={"error": "Only {} extensions are allowed.".format(
            ', '.join(ALLOWED_EXTENSIONS))}, status_code=522)
    try:
        id = uuid.uuid4()
        path = f"uploadedfiles/{id}/"
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}/{file.filename}", 'wb') as f:
            while contents := file.file.read(1024 * 1024):
                f.write(contents)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    finally:
        file.file.close()
    return JSONResponse(content={"message": f"Successfully uploaded {file.filename}", "uuid": str(id)}, status_code=200)
