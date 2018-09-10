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
                pl_from = int(pagelink[0])
                pl_namespace = int(pagelink[1])
                pl_from_namespace = int(pagelink[len(pagelink)-1])
                pl_title = ''
                start = len(pagelink) - 2 - 1
                stop = len(pagelink) - 1
                index = start
                while index < stop:
                    if index == start:
                        pl_title = pagelink[index][1:]
                    elif index == stop - 1:
                        pl_title += "," + pagelink[index][:-1]
                    else:
                        pl_title += "," + pagelink[index]
                    index += 1
                pl_title = pl_title.replace('_', ' ')
                pl_title = pl_title.replace('\\"', '')
                pl_title = pl_title.replace("\\'", "'")
                pagelinks.append({
                    "pl_from": pl_from,
                    "pl_namespace": pl_namespace,
                    "pl_title": pl_title,
                    "pl_from_namespace": pl_from_namespace
                })

            if index >= limit:

                with hdfs_client.write('{}pagelinks.{}.avro'.format(dst, pagelinkfileno)) as avro_file:
                    fastavro.writer(avro_file, schema, pagelinks)
                index = 0
                pagelinkfileno += 1

                if not sys.argv[2] == 'prod':
                    sys.exit(0)
