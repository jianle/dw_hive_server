#!/bin/bash

git pull
sbt package
java -jar -Drun.mode=test ~/dwetl/test/jizhang/jetty-runner-8.1.7.v20120910.jar --port 8089 --path /hive-server /data/dwlogs/jizhang/dw_hive_server/target/scala-2.10/dw-hive-server_2.10-0.0.1.war

