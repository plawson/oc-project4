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

limit = int(sys.argv[1])


def save_avro_file(filename, contributions):
    with hdfs_client.write(filename) as avro_file:
        fastavro.writer(avro_file, schema, contributions)


with open(src) as f:
    index = 0
    historyfileno = 0
    histories = []
    tree = iterparse(f)
    for event, elem in tree:
        if "page" in elem.tag:
            print(elem)
            print(elem.find("title"))
            sys.exit(0)
    #    for item in page:
    #        print(item)
    #        sys.exit(0)
    #     index += 1
    #     h_revisions = []
    #     for revision in history.iterfind("revision"):
    #         r_id = int(revision.find("id").text)
    #         r_parent_id = int(revision.find("parentid").text)
    #         str_date = revision.find("timestamp").text
    #         r_timestamp = int(datetime.strptime(str_date, '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))
    #         contributor = revision.find("contributor")
    #         r_contributor = {
    #             "r_username": contributor.find("username").text if None is not contributor.find("username") else None,
    #             "r_contributor_id": int(contributor.find("id").text) if None is not contributor.find("id").text
    #             else None,
    #             "r_contributor_ip": contributor.find("ip").text if None is not contributor.find("ip") else None
    #         }
    #         r_minor = revision.find("minor").text if None is not revision.find("minor") else None
    #         r_comment = revision.find("minor").text if None is not revision.find("minor") else None
    #         r_model = revision.find("model").text
    #         r_format = revision.find("format").text
    #         text = revision.find("text")
    #         r_text = {
    #             "r_text_id": int(text.get("id")),
    #             "r_text_bytes": int(text.get("bytes"))
    #         }
    #         r_sha1 = revision.find("sha1").text
    #         h_revisions.append({
    #             "r_id": r_id,
    #             "r_parent_id": r_parent_id,
    #             "r_timestamp": r_timestamp,
    #             "r_contributor": r_contributor,
    #             "r_minor": r_minor,
    #             "r_comment": r_comment,
    #             "r_model": r_model,
    #             "r_format": r_format,
    #             "r_text": r_text,
    #             "r_sha1": r_sha1
    #         })
    #     h_title = history.find("title").text
    #     h_namespace = int(history.find("ns").text)
    #     h_id = int(history.find("id").text)
    #     histories.append({
    #         "h_title": h_title,
    #         "h_namespace": h_namespace,
    #         "h_id": h_id,
    #         "h_revisions": h_revisions
    #     })
    #
    #     if index >= limit:
    #         file_name = '{}histories.{}.avro'.format(dst, historyfileno)
    #         print('Saving avro file: {}'.format(file_name))
    #         save_avro_file(file_name, histories)
    #         index = 0
    #         historyfileno += 1
    #         histories = []
    #
    #     if not sys.argv[2] == 'prod':
    #         sys.exit(0)
    # if len(histories) > 0:
    #     file_name = '{}histories.{}.avro'.format(dst, historyfileno)
    #     print('Saving avro file: {}'.format(file_name))
    #     save_avro_file(file_name, histories)
