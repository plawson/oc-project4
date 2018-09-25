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

if sys.argv[3] == 'full':
    dst = dst_full
else:
    dst = dst_test

limit = int(sys.argv[1])
file_limit = int(sys.argv[2])
begining = "INSERT INTO `pagelinks` VALUES "


def save_avro_file(filename, pagelinks):
    with hdfs_client.write(filename) as avro_file:
        fastavro.writer(avro_file, schema, pagelinks)


with open(src, 'rb') as f:
    index = 0
    pagelinkfileno = 0
    pagelinks = []
    for lineb in f:
        line = lineb.decode('utf-8', 'ignore')[:-1]
        if line.startswith(begining):
            records = line[len(begining)+1:-3].split("),(")
            index += 1
            for record in records:
                pagelink = record.split(",")
                pl_from = int(pagelink[0])
                pl_namespace = int(pagelink[1])
                pl_from_namespace = int(pagelink[len(pagelink)-1])
                pl_title = ''
                start = 2
                stop = len(pagelink) - 1
                idx = start
                while idx < stop:
                    if idx == start:
                        if idx == 2 and (idx == stop - 1):
                            pl_title = pagelink[idx][1:-1]
                        else:
                            pl_title = pagelink[idx][1:]
                    elif idx == stop - 1:
                        pl_title += "," + pagelink[idx][:-1]
                    else:
                        pl_title += "," + pagelink[idx]
                    idx += 1
                pl_title = pl_title.replace('_', ' ')
                pl_title = pl_title.replace('\\"', '')
                pl_title = pl_title.replace("\\'", "'")
                if len(pl_title) > 1 and pl_title[-1] == "\\" and pl_title[-2] == "\\":
                    pl_title = pl_title[:-1]
                pagelinks.append({
                    "pl_from": pl_from,
                    "pl_namespace": pl_namespace,
                    "pl_title": pl_title,
                    "pl_from_namespace": pl_from_namespace
                })

            if index >= limit:
                print('Saving {}pagelinks.{}.avro...'.format(dst, pagelinkfileno), end='', flush=True)
                save_avro_file('{}pagelinks.{}.avro'.format(dst, pagelinkfileno), pagelinks)
                print(' Done.')
                index = 0
                pagelinkfileno += 1
                pagelinks = []

                if not sys.argv[3] == 'full' and pagelinkfileno >= file_limit:
                    sys.exit(0)
    if len(pagelinks) > 0:
        print('Saving {}pagelinks.{}.avro...'.format(dst, pagelinkfileno), end='', flush=True)
        save_avro_file('{}pagelinks.{}.avro'.format(dst, pagelinkfileno), pagelinks)
        print(' Done.')
