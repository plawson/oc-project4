#!/usr/bin/python3
# coding: utf-8

import sys

#import hdfs

src = '/home/kubernetes/code/frwiki_20180501/flat/frwiki-20180501-pagelinks.sql'
dst_test = '/data/frwiki/frwiki-20180501/master/test'
dst_full = '/data/frwiki/frwiki-20180501/master/full'

#hdfs_client = hdfs.client.InsecureClient("http://kube-node07:50070")

limit = int(sys.argv[1])
begining = "INSERT INTO `pagelinks` VALUES "

with open(src) as f:
    index = 0
    for line in f:
        if line.startswith(begining):
            records = line[len(begining)+1:-3].split("),(")
            index += 1
            print(records[0])
            print(records[len(records)-1:][0])

        if index > limit:
            sys.exit(0)
