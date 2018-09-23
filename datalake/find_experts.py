#!/usr/bin/python3
# coding: utf-8
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from io import BytesIO

import fastavro
import hdfs
import json
import sys

# Setup context and session
sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

# Schema files
sch_pl = '/data/frwiki/frwiki-20180501/master/pagelinks.avsc'
sch_rv = '/data/frwiki/frwiki-20180501/master/history.avsc'

# HDFS cluster address
hdfs_address = 'hdfs://kube-node07:9000'

# Dataset directories
src_test = hdfs_address + '/data/frwiki/frwiki-20180501/master/test/'
src_full = hdfs_address + '/data/frwiki/frwiki-20180501/master/full/'

# Avro file patterns
avro_rv_pattern = 'revisions.*.avro'
avro_pl_pattern = 'pagelinks.*.avro'

# HDFS Client
hdfs_client = hdfs.client.InsecureClient("http://kube-node07:50070")

# Load frwiki revisions avro schema
with hdfs_client.read(sch_rv, encoding='utf-8') as reader:
    schema_rv = json.load(reader)

# Load frwiki pagelinks avro schema
with hdfs_client.read(sch_pl, encoding='utf-8') as reader:
    schema_pl = json.load(reader)

# Setup test or full mode
if sys.argv[2] == 'full':
    src = src_full
else:
    src = src_test

title = sys.argv[1]


def filter_contributors(page_revs):
    flatten_revs = []
    page_id = page_revs['h_id']
    page_title = page_revs['h_title']
    for revision in page_revs['h_revisions']:
        if not revision['r_contributor']['r_contributor_id'] == -911 \
                and not 'No name' == revision['r_contributor']['r_username']\
                and not 'script de conversion' == revision['r_contributor']['r_username']:
            flatten_revs.append({
                'page_id': page_id,
                'page_title': page_title,
                'contributor': revision['r_contributor']['r_username']
            })
    return flatten_revs


# Load revision files
rdd_avro_rv = sc.binaryFiles(src + avro_rv_pattern)  # (filename, content)
# Parse avro revision files
rdd_rv = rdd_avro_rv.flatMap(lambda rv: fastavro.reader(BytesIO(rv[1]), reader_schema=schema_rv))\
    .flatMap(filter_contributors)
# Convert to a RDD of revision rows
rdd_rows_rv = rdd_rv.map(lambda rev: Row(**rev))
# Convert to a dataframe of revisions
df_rv = spark.createDataFrame(rdd_rows_rv)
# Cache to avoid recomputation
df_rv.persist()


# Load pagelins files
rdd_avro_pl = sc.binaryFiles(src + avro_pl_pattern)  # (filename, content)
# Parse avro pagelinks files
rdd_pl = rdd_avro_pl.flatMap(lambda pl_bin: fastavro.reader(BytesIO(pl_bin[1]), reader_schema=schema_pl))\
    .map(lambda pl: {'pl_from': pl['pl_from'], 'pl_title': pl['pl_title']})
# Convert to a RDD of pagelinks rows
rdd_rows_pl = rdd_pl.map(lambda pl: Row(**pl))
# Convert to a dataframe of pagelinks
df_pl = spark.createDataFrame(rdd_rows_pl)
# Cache to avoid recomputation
df_pl.persist()

# df_rv.limit(10).show()
# df_pl.limit(10).show()

df_rv.createOrReplaceTempView("revision")
df_pl.createOrReplaceTempView("pagelink")

select_string = "SELECT rv.contributor contributeur, " \
                "COUNT(rv.contributor) quantite FROM revision rv, pagelink pl " \
                "WHERE rv.page_title = '{}' and (rv.page_id = pl.pl_from or " \
                "rv.page_title = pl.pl_title) group by contributeur order by quantite desc".format(title)

result = spark.sql(select_string)
result.limit(3).show()
