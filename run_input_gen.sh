#!/bin/bash
# RUN Flink Job before running this

java -cp InputGenerator/InputGenerator/build/libs/InputGenerator-1.0-SNAPSHOT.jar de.adrian.thesis.generator.benchmark.netty.NettyBenchmark ./build/resources/main/log4j2.xml -p 31000
