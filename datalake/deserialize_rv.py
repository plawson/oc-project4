#!/usr/bin/python3
# coding: utf-8
import json
import sys

import hdfs
import fastavro

srcdir_test = '/data/frwiki/frwiki-20180501/master/test/'
srcdir_full = '/data/frwiki/frwiki-20180501/master/full/'
sch = '/data/frwiki/frwiki-20180501/master/history.avsc'

hdfs_client = hdfs.client.InsecureClient("http://kube-node07:50070")

with hdfs_client.read(sch, encoding='utf-8') as reader:
    schema = json.load(reader)

if sys.argv[2] == 'full':
    srcdir = srcdir_full
else:
    srcdir = srcdir_test

filename = sys.argv[1]

num = 0

with hdfs_client.read(srcdir + filename) as avro_file:
    reader = fastavro.reader(avro_file, reader_schema=schema)
    for rev in reader:
        # print(rev)
        if num < 10:
            print('Revision size for page {} =>{})<=: {}'.format(rev['h_title'], rev['h_namespace'],
                                                                 len(rev['h_revisions'])))
        num += 1
        # sys.exit(0)

print('Number of pages in file: {}'.format(num))
