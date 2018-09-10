#!/usr/bin/python3
# coding: utf-8

import sys
import json

import hdfs
import fastavro

src = '/home/kubernetes/code/frwiki_20180501/flat/frwiki-20180501-pagelinks.sql'
dst_test = '/data/frwiki/frwiki-20180501/master/test/'
dst_full = '/data/frwiki/frwiki-20180501/master/full/'
sch = '/data/frwiki/frwiki-20180501/master/pagelinks.avsc'

hdfs_client = hdfs.client.InsecureClient("http://kube-node07:50070")

with hdfs_client.read(sch, encoding='utf-8') as reader:
    schema = json.load(reader)

if sys.argv[2] == 'prod':
    dst = dst_full
else:
    dst = dst_test

limit = int(sys.argv[1])
begining = "INSERT INTO `pagelinks` VALUES "

with open(src) as f:
    index = 0
    pagelinkfileno = 0
    pagelinks = []
    for line in f:
        if line.startswith(begining):
            records = line[len(begining)+1:-3].split("),(")
            index += 1
            for record in records:
                pagelink = record.split(",")
                pagelinks.append({
                    "pl_from": int(pagelink[0]),
                    "pl_namespace": int(pagelink[1]),
                    "pl_title": pagelink[2][1:-1],
                    "pl_from_namespace": int(pagelink[3])
                })

            if index >= limit:
                with hdfs_client.write('{}pagelinks.{}.avro'.format(dst, pagelinkfileno)) as avro_file:
                    fastavro.writer(avro_file, schema, pagelinks)
                index = 0
                pagelinkfileno += 1

                if not sys.argv[2] == 'prod':
                    sys.exit(0)
