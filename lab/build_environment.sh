#!/bin/bash

# Build the base images from which are based the Dockerfiles
# then Startup all the containers at once

mkdir -p ./logs ./plugins

echo "AIRFLOW_UID=$(id -u)" > .env

cd plugins 
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar
wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.1.0/delta-core_2.12-2.1.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.1.0/delta-storage-2.1.0.jar
wget https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.8/antlr4-runtime-4.8.jar
wget https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar

cd ..

mkdir -p volumes
cd volumes 
mkdir -p ./metabase ./minio

docker compose up airflow-init && \
docker compose up -d
