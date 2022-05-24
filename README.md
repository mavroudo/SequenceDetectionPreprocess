# Sequence-Detection

Build project
```sbt package```

Run project \
```spark-submit -master local[*] #add all cassandra configurations SequenceDetection-assembly-0.1.jar filename method drop_Tables_cassanra join_previous_logs drop_previous_records```