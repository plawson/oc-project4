#!/usr/bin/env bash

hdfs dfs -mkdir /data/frwiki
hdfs dfs -mkdir /data/frwiki/raw
hdfs dfs -mkdir /data/frwiki/frwiki-20180501/master
hdfs dfs -mkdir /data/frwiki/frwiki-20180501/master/full
hdfs dfs -mkdir /data/frwiki/frwiki-20180501/master/test

hdfs dfs -ls -R /data/frwiki