from fastapi import FastAPI
from fastapi import File, UploadFile
from EnvironmentVariables import EnvironmentVariables
import uuid, os

from PreprocessItem import PreprocessItem

isFree: bool = True
ALLOWED_EXTENSIONS = {'withTimestamp', 'xes', 'jpg', 'pdf'}
env_vars = {"cassandra_host": "localhost", "cassandra_port": 9042, "cassandra_user": "cassandra",
            "cassandra_pass": "cassandra", "cassandra_replication_factor": 1, "cassandra_gc_grace_seconds": 864000,
            "s3accessKeyAws": "minioadmin", "s3ConnectionTimeout": 600000, "s3endPointLoc": "http://localhost:9000",
            "s3secretKeyAws": "minioadmin"}

app = FastAPI()


@app.get("/isFree")
async def isFreeToRun():
    return {"isFree": isFree}


@app.get("/filelist")
async def getFileList():
    files = []
    for uidd in os.listdir("uploadedfiles"):
        if uidd != ".gitkeep":
            path = f"uploadedfiles/{uidd}/"
            for file in os.listdir(path):
                files.append({"uidd": uidd, "filename": file})
    return {"files": files}


@app.get("/setting_vars")
def getEnvironmentalVariables():
    env = EnvironmentVariables()
    for key in env.dict():
        try:
            stored_value = os.environ[key]
            print(f"Stored for {key} -> {stored_value}")
            setattr(env, key, stored_value)
        except Exception as e:
            os.environ[key] = getattr(env, key)
    # pass the default unchangeable env here
    os.environ["cassandra_keyspace_name"] = "siesta"
    os.environ["cassandra_replication_class"] = "SimpleStrategy"
    os.environ["cassandra_replication_rack"] = "replication_factor"
    os.environ["cassandra_write_consistency_level"] = "ONE"
    return env.dict()


@app.post("/setting_vars")
def setEnvironmentalVariables(env: EnvironmentVariables):
    for key in env.dict():
        os.environ[key] = getattr(env, key)
    return {"status": "OK"}


@app.post("/preprocess")
async def preprocessfile(params: PreprocessItem):
    spark_command = params.getAttributes()
    print(spark_command)
    isFree = False
    return {"status": "OK"}


@app.post("/upload/")
async def upload(file: UploadFile = File(...)):
    file_extension = file.filename.split(".")[-1]
    if file_extension not in ALLOWED_EXTENSIONS:
        return {"error": "Invalid file extension. Only {} extensions are allowed.".format(
            ', '.join(ALLOWED_EXTENSIONS))}
    try:
        id = uuid.uuid4()
        path = f"uploadedfiles/{id}/"
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}/{file.filename}", 'wb') as f:
            while contents := file.file.read(1024 * 1024):
                f.write(contents)
    except Exception as e:
        print(str(e))
        return {"message": "There was an error uploading the file"}
    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}
