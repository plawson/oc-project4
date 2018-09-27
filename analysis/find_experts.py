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
sc = SparkContext(appName=sys.argv[1].replace(' ', '_'))
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
# if sys.argv[2] == 'full':
#     src = src_full
# else:
#     src = src_test

src = src_full

title = sys.argv[1]


def filter_contributors(page_revs):
    flatten_revs = []
    page_id = page_revs['h_id']
    page_title = page_revs['h_title']
    for revision in page_revs['h_revisions']:
        if not revision['r_contributor']['r_contributor_id'] == -911 \
                and not 'No name' == revision['r_contributor']['r_username']\
                and not 'script de conversion' == revision['r_contributor']['r_username']\
                and 'bot' not in revision['r_contributor']['r_username'].lower():
            flatten_revs.append({
                'page_id': page_id,
                'page_title': page_title,
                'contributor': revision['r_contributor']['r_username']
            })
    return flatten_revs


# Load revision files
rdd_avro_rv = sc.binaryFiles(src + avro_rv_pattern)  # (filename, content)

# Parse avro revision files and discard non 0 namespace
rdd_page = rdd_avro_rv.flatMap(lambda rv: fastavro.reader(BytesIO(rv[1]), reader_schema=schema_rv))\
    .filter(lambda rv: rv['h_namespace'] == 0).persist()

# Flatten the rdd creating a directed graph
rdd_rv = rdd_page.flatMap(filter_contributors)

# Convert to a RDD of revision rows
rdd_rows_rv = rdd_rv.map(lambda rev: Row(**rev))

# Convert to a dataframe of revisions
df_rv = spark.createDataFrame(rdd_rows_rv).persist()

# Create a criterion with the page info we are looking for
crit = rdd_page.filter(lambda page: page['h_title'] == title)\
    .map(lambda page: {'pid': page['h_id'], 'ptitle': page['h_title']}).collect()

# Load pagelins files
rdd_avro_pl = sc.binaryFiles(src + avro_pl_pattern)  # (filename, content)

# Parse avro pagelinks files and cache it discarding non 0 namespace pagelinks
rdd_pl = rdd_avro_pl.flatMap(lambda pl_bin: fastavro.reader(BytesIO(pl_bin[1]), reader_schema=schema_pl))\
    .filter(lambda pl: pl['pl_namespace'] == 0 and pl['pl_from_namespace'] == 0).persist()

# Create a rdd of pagelinks pointing to the criterion
rdd_pl_to_me = rdd_pl.filter(lambda pl: pl['pl_title'] == crit[0]['ptitle'])\
    .map(lambda pl: {'pl_from': pl['pl_from']})

# Create a rdd of pagelinks pointed by the criterion
rdd_pl_from_me = rdd_pl.filter(lambda pl: pl['pl_from'] == crit[0]['pid'])\
    .map(lambda pl: {'pl_title': pl['pl_title']})

# Convert to a RDD of pagelinks rows
rdd_rows_pl_to_me = rdd_pl_to_me.map(lambda pl: Row(**pl))
rdd_rows_pl_from_me = rdd_pl_from_me.map(lambda pl: Row(**pl))

# Convert to a dataframe of pagelinks
df_pl_to_me = spark.createDataFrame(rdd_rows_pl_to_me).persist()
df_pl_from_me = spark.createDataFrame(rdd_rows_pl_from_me).persist()

# Remove unused cache to free memory space on executors
# rdd_page.unpersist()

# Create temporary views for SQL usage
df_rv.createOrReplaceTempView("revision")
df_pl_to_me.createOrReplaceTempView("pagelink_to_me")
df_pl_from_me.createOrReplaceTempView("pagelink_from_me")

# Build the select statement
select_string = "SELECT rv.contributor contributeur, " \
                "COUNT(rv.contributor) quantite FROM revision rv " \
                "WHERE rv.page_title = '{}' or rv.page_id in (SELECT pl_from FROM pagelink_to_me) " \
                "or rv.page_title in (SELECT pl_title FROM pagelink_from_me) " \
                "group by contributeur order by quantite desc"\
    .format(crit[0]['ptitle'])

# Create rge resulting dataframe
result = spark.sql(select_string)
# Get the top three contributors
result.limit(3).show()
