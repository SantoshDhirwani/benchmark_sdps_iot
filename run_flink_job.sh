#!/bin/bash
FLINK_HOME=/Users/santoshdhirwani/Downloads/flink-1.10.1
JOB_JAR=FlinkJobsMisc/target/masterthesis-jobs-1.0-SNAPSHOT.jar
JOB_CLASS=de.adrianbartnik.benchmarks.yahoo.YahooBenchmark

$FLINK_HOME/bin/flink run -c $JOB_CLASS $JOB_JAR -hostnames localhost -ports 0
