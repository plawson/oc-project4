#!/usr/bin/python3
# coding: utf-8
import hdfs
import json
import sys
import fastavro
from xml.etree.ElementTree import iterparse
from datetime import datetime

src = '/home/kubernetes/code/frwiki_20180501/flat/frwiki-20180501-stub-meta-history.xml'
dst_test = '/data/frwiki/frwiki-20180501/master/test/'
dst_full = '/data/frwiki/frwiki-20180501/master/full/'
sch = '/data/frwiki/frwiki-20180501/master/history.avsc'

hdfs_client = hdfs.client.InsecureClient("http://kube-node07:50070")

with hdfs_client.read(sch, encoding='utf-8') as reader:
    schema = json.load(reader)

if sys.argv[2] == 'full':
    dst = dst_full
else:
    dst = dst_test


def save_avro_file(filename, histories):
    with hdfs_client.write(filename) as avro_file:
        fastavro.writer(avro_file, schema, histories)

with open(src) as f:
    histories = []
    tree = iterparse(f)
    for history in tree.iterfind("page"):
        h_revisions = []
        for revision in history.iterfind("revision"):
            r_id = int(revision.find("id").text)
            r_parent_id = int(revision.find("parentid").text)
            str_date = revision.find("timestamp").text
            r_timestamp = int(datetime.strptime(str_date, '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))

