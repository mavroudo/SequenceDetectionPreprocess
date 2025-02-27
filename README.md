# SIESTA Preprocess Component

[//]: # (General idea for siesta -> image of the whole system - describe it  some interesting results)
#### SIESTA is a highly scalable infrastructure designed for performing pattern analysis in large volumes of event logs.

### Architecture overview
<img src='experiments/architecture.jpg' width='927'>

The architecture of SIESTA consists of two main components: the preprocessing component and the query processor.
The preprocessing component (implemented in this repo) is responsible for handling continuously arriving logs and 
computing the appropriate indices, while the [Query Processor](https://github.com/mavroudo/SequenceDetectionQueryExecutor)
utilizes the stored indices to perform efficient pattern analysis. Pattern analysis corresponds to various tasks, including pattern detection, pattern mining and pattern exploration. You can find detailed instructions of how to deploy the complete infrastracture, along with complete list of all our publications in [this](https://github.com/siesta-tool/siesta-demo) repository.

### Preprocess Component
This module processes the provided logfile using Apache Spark, a framework specifically designed for big data projects, 
and stores the indices into scalable databases such S3. The primary index, named IndexTable, is an inverted index where the key represents an event pair, and the value is a list of all traces that contain this pair, along with their corresponding timestamps or positions within the original trace. Additionally, there are other indices that contain useful information, such as statistics for each pair and the timestamp of the last completed pair, enabling different processes. 
A comprehensive list of all tables and their structures can be found in our published works.

Recently, we added support for incremental mining of declarative constraints. These constraints describe the underlying structure of the process that generated the event log and can be used in applications like predicting the outcome of a process, detecting outlying executions of the process and more.

Finally, it's important to note that although our case study uses log files from the Business Process Management field, 
the solution is generic and can be applied to any log file (provided a parser is implemented) 
as long as the events contain an event type, a timestamp, and correspond to a specific sequence or trace.

### Build and run with Intellij IDEA
Before configuring JetBrain's IDE to compile and run the component, ensure you're running an S3/minio instance 
(probably through a docker container). Assuming that an instance of minio is running at http://localhost:9000 (See instructions on how to run 
the S3 database in the next section's notes) we move to the next steps.
#### Requirements
- Intellij IDEA
- Java 11 JRE (can be downloaded from IDEA)
- Scala 11 SDK (can be downloaded from IDEA)
#### Steps for Batching
1. After cloning this repo, inside IDEA, create a new configuration file.
   1. Select `Application` 
   2. Select `Java 11` JRE and compilation component (`-cp sequencedetectionpreprocess`)
   3. Select `auth.datalab.siesta.siesta_main` as Main class
   4. Add as CLI arguments something like `--logname test --delete_prev`. Check [configuration options](#complete-list-of-parameters).
   5. Add as environmental variables the following (modify wherever necessary)
      ````
      s3accessKeyAws=minioadmin;
      s3ConnectionTimeout=600000;
      s3endPointLoc=http://localhost:9000;
      s3secretKeyAws=minioadmin
   6. Save the configuration file.
2. Open `Project Structure` settings, select `Scala 11` as SDK and language level `SDK default`. 
3. Modify `S3Connector.scala`, setting spark's master node as `local[*]` (for running locally).
4. Run the configuration file.

#### Steps for Streaming
1. After cloning this repo, inside IDEA, create a new configuration file.
   1. Select `Application`
   2. Select `Java 11` JRE and compilation component (`-cp sequencedetectionpreprocess`)
   3. Select `auth.datalab.siesta.siesta_main` as Main class
   4. Add as CLI arguments something like `--logname test --delete_prev --system streaming`. Check [configuration options](#complete-list-of-parameters).
   5. Add as environmental variables the following (modify wherever necessary)
      ````
      s3accessKeyAws=minioadmin;
      s3ConnectionTimeout=600000;
      s3endPointLoc=http://localhost:9000;
      s3secretKeyAws=minioadmin
      kafkaBroker=http://localhost:9092;
      kafkaTopic=test;
      POSTGRES_ENDPOINT=localhost:5432/metrics;
      POSTGRES_PASSWORD=admin;
      POSTGRES_USERNAME=admin;
   6. Save the configuration file.

### Getting Started with Docker
Using Docker makes it easy to deploy the preprocess component. The following steps will guide you on how to run the component 
for randomly generated event sequences using local Spark and a database (all Dockerfiles are provided). 
Once you have tested the successful build and execution, we will provide further instructions on how to execute 
the preprocess component for a provided logfile using an already running Spark cluster or a deployed database.
#### Requirements
- docker
- docker-compose

1. **Create network:** In order for all the components to communicate they have to belong to the same network. Create
a new docker network using the following command:
```bash
docker network create --driver=bridge  siesta-net
```

2. **Deploy the infrastructure:** From the root directory execute the following command:
```bash
docker-compose up -d 
```
This will build and run the preprocessing component (along with the Rest API, which is used to access preprocesses capabilities through API requests) and deploy the required infrastructure. This includes an open source version of the S3, i.e., Minio, a Postgre database and Apache Kafka. Finally, docker compose will execute scripts that create the required backet for the data to be stored and create a topic in kafka.

#### Notes
1. In some cases the composing command runs with a similar form (if the above is not working): `docker compose up -d`
2. Apache Kafka and Postgres are used for when the data are read as event stream. Ensure that the the ``OUTSIDE`` value of ``KAFKA_ADVERTISED_LISTENERS`` in docker-compose is set to the name of the address you wish to receive the messages (it should match the kafkaBroker environment variable in InteliJ). _Local mode:_ //localhost:your-port (e.g. 9092)
3. You can access the different services from the following endpoints:
   - FastAPI: http://localhost:8000/docs
   - S3: http://localhost:9000 (default username/password: minionadmin/minioadmin)
4. In case you want to run only the minio/S3 component (e.g., when developing on this codebase), you may start only 
   the corresponding service:
   ```bash
   docker-compose up -d minio
   ```
   You may also start the `createbuckets` service, if you're running it for the first time in your machine.

### Test the execution of the preprocess component
After the deployment of the entire infrastructure (and assuming that everything run correctly) lets test the execution of the preprocess mode. We will evaluate both batch and streaming mode using testing data. All commands will be submitting using the REST API.
1. **Batching**
```bash
curl -X 'POST' 'http://localhost:8000/preprocess' \
  -H 'Content-Type: application/json' \
  -d '{
    "spark_master": "local[*]",
    "file": "synthetic",
    "logname": "synthetic"
  }'
```
This curl command will start a preprocess task with artificial generated traces. By default, it will index 100 traces 
using 10 different event types, and lengths that vary from 10 to 90 events. These parameters can be modified by using
the CLI [parameters](#complete-list-of-parameters). The request will immediately return a unique id of this process, 
which can be used to monitor its process through http://localhost:8000/process/{process_id} request.

2. **Streaming**

Next lets test a streaming process. The curl request below will start a preprocess job that will monitor the events that 
comes to a specific topic in kafka. Once new events appear in the topic, it will process them and store them in minio.
The name of the log database will be equal to the logname that we provide in the curl request.
```bash
curl -X 'POST' 'http://localhost:8000/preprocess' \
  -H 'Content-Type: application/json' \
  -d '{
    "spark_master": "local[*]",
    "logname": "test_stream",
    "system": "streaming"
  }'
```
Once this is executed, we will see in the spark monitoring (http://localhost:4040/StreamingQuery/) that the queries are
running. Next we can try and send some demo data using the send_events docker file.
```bash
docker build -t send_events -f dockerbase/send_events.Dockerfile .
docker run --network siesta-net send_events
```
After that we will notice that the queries in the spark UI has kicked in and the events are processed and stored in the
corresponding indices.

### Connection preprocess component with preexisting resources
Connecting to already deployed databases or utilizing a spark cluster can be easily achieved with the use 
of parameters. The only thing that you should make sure is that their urls are accessible
by the docker container. this can be done by either making the url publicly available or by connecting the
docker container in the same network (as done above with the siesta-net).
- **Connect with spark cluster:** Change the value of the Spark master parameter before submitting the
preprocess job from "**local[*]**" to the resource manager's url. 
- **Connect with S3:** Change the environmental values  that start with **s3**. 
These parameters include the contact point and the credentials required to achieve connection.
If you have an S3 database deployed in AWS, you can change these parameters to store the build indices there. Note that
environmental parameters can be easily set with the use of the http://localhost:8000/setting_vars request,
that can both read and override the parameters inside the preprocess container. Complete list of all the supported
requests, the required formats and their descriptions can be found in the [docs](http://localhost:8000/docs/).



### Executing preprocess for a provided logfile
Till now the supported file extensions are "**.xes**", which are the default file for the Business Process
Management logfiles and "**.withTimestamp**", which is a generic file format generated for testing. A new
connector can be easily implemented in the _auth.datalab.siesta.BusinessLogic.IngestData.ReadLogFile_. 

You can either submit a file to be preprocessed through the User Interface (Preprocessing tab), through the FastAPI docs
or simply place it inside the _experiments/input_ folder. Since there is already a volume set between this folder and
the folder where preprocess stores the uploaded files. Assuming that the logfile is correctly placed, and it is
named "log.xes", and the indices should have the name "log" run the following curl command:
```bash
curl -X 'POST' 'http://localhost:8000/preprocess' \
  -H 'Content-Type: application/json' \
  -d '{
    "spark_master": "local[*]",
    "logname": "log",
    "file": "uploadedfiles/log.xes"
  }'
```
Since we currently only allow one execution of the preprocess per docker container, you can stop a streaming process by
sending a post request to the http://localhost:8000/stop_streaming/ endpoint. You can choose however, to run
multiple containers of the preprocess image as long as you choose different ports for the APIs (and also make 
sure that you have enough resources for spark).

### Complete list of parameters:
```
Usage: preprocess.jar [options]

  --system <system>        System refers to the system that will be used for indexing
  -d, --database <database>
                           Database refers to the database that will be used to store the index
  -m, --mode <mode>        Mode will determine if we use the timestamps or positions in indexing
  -c, --compression <compression>
                           Compression determined the algorithm that will be used to compress the indexes
  -f, --file <file>        If not set will generate artificially data
  --logname <logname>      Specify the name of the index to be created. This is used in case of incremental preprocessing
  --delete_all             cleans all tables in the keyspace
  --delete_prev            removes all the tables generated by a previous execution of this method
  --lookback <value>       How many days will look back for completions (default=30)
  --declare_incremental    run a post processing job in order to create the required state for incremental mining declare constraints

The parameters below are used if the file was not set and data will be randomly generated
  -t, --traces <#traces>
  -e, --event_types <#event_types>
  --lmin <min length>
  --lmax <max length>
  --duration_determination
                           Include activity duration calculation
  --help                   prints this usage text
```

### Documentation
The documentation for the latest version of the preprocess component can be accessed from 
[here](https://mavroudo.github.io/SequenceDetectionPreprocess/auth/datalab/siesta/index.html).
Otherwise, they are located in the **docs/** folder and you can access it by opening the index.html
in a browser.

# Change Log
### [3.0.0] - 2024-11-30
 - Removed the support for Cassandra, since it required specific spark version which limited the functionalities we could provide.
 - Optimized the preprocess pipeline and S3 structure, in order to provide efficient incremental indexing.
 - Added optional support for incremental declare mining, which can be easily set using the command argument **incremental_declare**.

### [2.1.1] - 2023-07-29
- Hotfix in indexing process for Cassandra and S3
- Introduce partitions for LastChecked to handle incremental indexing
- Simpler way to extract pairs and not n-tuples

### [2.1.0] - 2023-06-18
- Added FastAPI to submit preprocessing jobs using api calls
- Extensive documentation to the entire project

### [2.0.0] - 2023-05-24
- Implement efficient incremental indexing, utilizing the previously indexed traces/pairs
- Connection with S3, as an alternative to Cassandra
- Optimize storage space, utilizing compression algorithms and avoiding storing timestamps

### [1.0.0] - 2022-12-14
- Building the appropriate inverted indices to support efficient pattern detection
- Evaluate different indexing schemas
- Integration of Signature method
- Integration of Set-Containment method
- Connection with Cassandra, where indices are stored

