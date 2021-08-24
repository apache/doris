#!/usr/bin/env bash
set -e
rm -rf output
mkdir output
mkdir -p output/
mvn clean install
mv manager-server/target/manager-server-1.0.0.jar output/doris-manager.jar
cp -r conf output/
cp manager-bin/* output/