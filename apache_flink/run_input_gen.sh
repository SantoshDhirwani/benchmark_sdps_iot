#!/bin/bash

# RUN the script for Apache Flink job before running this script

# ------------------------------------------------------------------
# Santosh Dhirwani
# 
# 	This script is used to run the Input Generator.
# ------------------------------------------------------------------


java -cp InputGenerator/InputGenerator/build/libs/InputGenerator-1.0-SNAPSHOT.jar de.adrian.thesis.generator.benchmark.netty.NettyBenchmark ./build/resources/main/log4j2.xml -p 31000
