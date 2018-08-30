#!/usr/bin/env bash

hdfs dfs -mkdir /data/frwiki
hdfs dfs -mkdir /data/frwiki/raw
hdfs dfs -mkdir /data/frwiki/master
hdfs dfs -mkdir /data/frwiki/master/full
hdfs dfs -mkdir /data/frwiki/master/test

hdfs dfs -ls -R /data/frwiki