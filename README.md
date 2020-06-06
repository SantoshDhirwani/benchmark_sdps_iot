# Yahoo Streaming Benchmark for Apache Flink
Updated for Flink version 1.10

## Setup
* Flink 1.10
* Maven
* Gradle


## Steps to run tests

### Compile
#### Flink Job
```
cd FlinkJobsMisc
mvn clean package
```
#### Data generator
```
cd InputGenerator/InputGenerator
./gradlew clean jar
```

### Run Tests

#### Flink Job
```
export FLINK_HOME=<flink-home>
export JOB_JAR=<jar-path>
export JOB_CLASS=de.adrianbartnik.benchmarks.yahoo.YahooBenchmark
$FLINK_HOME/bin/flink run -c $JOB_CLASS $JOB_JAR -hostnames localhost -ports 0
```

#### Data Generator
```
java -cp InputGenerator/InputGenerator/build/libs/InputGenerator-1.0-SNAPSHOT.jar de.adrian.thesis.generator.benchmark.netty.NettyBenchmark ./build/resources/main/log4j2.xml -p 31000
```
