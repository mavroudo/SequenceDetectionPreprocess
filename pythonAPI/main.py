import os
import uuid
from threading import Lock, Thread

import uvicorn
from fastapi import FastAPI, Depends
from fastapi import File, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy.orm import scoped_session

from EnvironmentVariables import EnvironmentVariables
from PreprocessItem import PreprocessItem
from PreprocessTask import task
from dbSQL import crud, model
from dbSQL.database import SessionLocal, engine

model.Base.metadata.create_all(bind=engine)

spark_location = "/opt/spark/bin/spark-submit"

ALLOWED_EXTENSIONS = {'withTimestamp', 'xes'}

s3accessKeyAws = "minioadmin" if os.environ.get("s3accessKeyAws") is None else os.environ["s3accessKeyAws"]
s3secretKeyAws = "minioadmin" if os.environ.get("s3secretKeyAws") is None else os.environ["s3secretKeyAws"]
s3ConnectionTimeout = 600000 if os.environ.get("s3ConnectionTimeout") is None else os.environ["s3ConnectionTimeout"]
s3endPointLoc = "http://localhost:9000" if os.environ.get("s3endPointLoc") is None else os.environ["s3endPointLoc"]

env_vars = {"s3accessKeyAws": s3accessKeyAws, "s3ConnectionTimeout": s3ConnectionTimeout,
            "s3endPointLoc": s3endPointLoc, "s3secretKeyAws": s3secretKeyAws}

app = FastAPI()


# Dependency
def get_db():
    db = scoped_session(SessionLocal)
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
                  "content": {"process_id": {}},
                  "description": "Return the process id",
              },
              520: {
                  "content": {"message": {}},
                  "description": "Another preprocessing job is already running",
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
    process = crud.start_process(db)
    if not lock.locked():
        lock.acquire()
        thread = Thread(target=task, args=(spark_location, params, process.id, lock))
        thread.start()
        return JSONResponse(content={"process_id": process.id}, status_code=200)
    else:
        return JSONResponse(content={"message": "Already running a preprocessing job"}, status_code=520)


@app.get("/process/{process_id}",
         responses={
             200: {
                 "content": {},
                 "description": "Information about the process",
             },
             404: {
                 "content": {"message": {}},
                 "description": "Process is not found",
             }
         }
         )
async def get_information_for_process(process_id: str, db: Session = Depends(get_db)):
    process = crud.get(db, process_id)
    if process is None:
        return JSONResponse(content={"message": "Process is not found"}, status_code=404)
    json = jsonable_encoder(process)
    return JSONResponse(content=json, status_code=200)


@app.get("/process/",
         responses={
             200: {
                 "content": {},
                 "description": "Information about the process",
             },
             404: {
                 "content": {"message": {}},
                 "description": "Process is not found",
             }
         }
         )
async def get_information_for_all(db: Session = Depends(get_db)):
    processes = crud.get_all(db)
    json = jsonable_encoder(processes)
    return JSONResponse(content=json, status_code=200)


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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
