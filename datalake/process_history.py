#!/usr/bin/python3
# coding: utf-8
import hdfs
import json
import sys
import fastavro
from xml.etree.ElementTree import iterparse, QName
from datetime import datetime

xmlns = 'http://www.mediawiki.org/xml/export-0.10/'
src = '/home/kubernetes/code/frwiki_20180501/flat/frwiki-20180501-stub-meta-history.xml'
dst_test = '/data/frwiki/frwiki-20180501/master/test/'
dst_full = '/data/frwiki/frwiki-20180501/master/full/'
sch = '/data/frwiki/frwiki-20180501/master/history.avsc'

hdfs_client = hdfs.client.InsecureClient("http://kube-node07:50070")

with hdfs_client.read(sch, encoding='utf-8') as reader:
    schema = json.load(reader)

if sys.argv[3] == 'full':
    dst = dst_full
else:
    dst = dst_test

limit = int(sys.argv[1])
file_limit = int(sys.argv[2])


def save_avro_file(filename, contributions):
    with hdfs_client.write(filename) as avro_file:
        fastavro.writer(avro_file, schema, contributions)


with open(src) as xml_file:
    revisions = []
    index = 1
    revisionfileno = 0
    for event, elem in iterparse(xml_file):
        if elem.tag == QName(xmlns, 'page'):
            index += 1
            # print(elem.tag)
            h_title = None
            h_namespace = None
            h_id = None
            h_revisions = []
            for page_child in elem:
                # print(page_child.tag)
                if page_child.tag == QName(xmlns, 'title'):
                    h_title = page_child.text
                    # print('Title: {}'.format(h_title))
                elif page_child.tag == QName(xmlns, 'ns'):
                    h_namespace = int(page_child.text)
                    # print('Namespace: {}'.format(h_namespace))
                elif page_child.tag == QName(xmlns, 'id'):
                    h_id = int(page_child.text)
                    # print('Id: {}'.format(h_id))
                elif page_child.tag == QName(xmlns, 'revision'):
                    r_id = None
                    r_parent_id = None
                    r_timestamp = None
                    r_contributor = None
                    r_minor = None
                    r_comment = None
                    r_model = None
                    r_format = None
                    r_text = None
                    r_sha1 = None
                    for rev_child in page_child:
                        if rev_child.tag == QName(xmlns, 'id'):
                            r_id = int(rev_child.text)
                            # print('\tRev Id: {}'.format(r_id))
                        elif rev_child.tag == QName(xmlns, 'parentid'):
                            r_parent_id = int(rev_child.text)
                            # print('\tRev Parent Id: {}'.format(r_parent_id))
                        elif rev_child.tag == QName(xmlns, 'timestamp'):
                            r_timestamp = int(datetime.strptime(rev_child.text, '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))
                            # print('\tRev Timestamp: {}'.format(r_timestamp))
                        elif rev_child.tag == QName(xmlns, 'contributor'):
                            r_contributor = None
                            r_username = None
                            r_contributor_id = None
                            r_contributor_ip = None
                            for contrib_child in rev_child:
                                if contrib_child.tag == QName(xmlns, 'username'):
                                    r_username = contrib_child.text
                                    # print('Contrib User: {}'.format(r_username))
                                elif contrib_child.tag == QName(xmlns, 'id'):
                                    r_contributor_id = int(contrib_child.text)
                                    # print('Contrib Id: {}'.format(r_contributor_id))
                                elif contrib_child.tag == QName(xmlns, 'ip'):
                                    r_contributor_ip = contrib_child.text
                                    # print('Contrib IP: {}'.format(r_contributor_ip))
                            r_contributor = {
                                'r_contributor_ip': r_contributor_ip
                            }
                            if None is not r_username:
                                r_contributor['r_username'] = r_username
                            if None is not r_contributor_id:
                                r_contributor['r_contributor_id'] = r_contributor_id
                            # print('\tRev Contrib: {}'.format(r_contributor))
                        elif rev_child.tag == QName(xmlns, 'minor'):
                            r_minor = rev_child.text
                            # print('\tRev Minor: {}'.format(r_minor))
                        elif rev_child.tag == QName(xmlns, 'comment'):
                            r_comment = rev_child.text
                            # print('\tRev Comment: {}'.format(r_comment))
                        elif rev_child.tag == QName(xmlns, 'model'):
                            r_model = rev_child.text
                            # print('\tRev Model: {}'.format(r_model))
                        elif rev_child.tag == QName(xmlns, 'format'):
                            r_format = rev_child.text
                            # print('\tRev Format: {}'.format(r_format))
                        elif rev_child.tag == QName(xmlns, 'text'):
                            r_text = {'r_text_bytes': int(rev_child.get('bytes')) if None is not rev_child.get(
                                          'bytes') else -911,
                                      'r_text_id': int(rev_child.get('id')) if None is not rev_child.get(
                                          'id') else -911}
                            # print('\tRev Text: {}'.format(r_text))
                        elif rev_child.tag == QName(xmlns, 'sha1'):
                            r_sha1 = rev_child.text
                            # print('\tRev SHA1: {}'.format(r_sha1))
                    revision = {
                        'r_id': r_id,
                        'r_timestamp': r_timestamp,
                        'r_contributor': r_contributor,
                        'r_minor': r_minor,
                        'r_comment': r_comment,
                        'r_model': r_model,
                        'r_format': r_format,
                        'r_text': r_text,
                        'r_sha1': r_sha1
                    }
                    if None is not r_parent_id:
                        revision['r_parent_id'] = r_parent_id
                    h_revisions.append(revision)
            revisions.append({
                'h_title': h_title,
                'h_namespace': h_namespace,
                'h_id': h_id,
                'h_revisions': h_revisions
            })
        if index > limit:
            save_avro_file('{}revisions.{}.avro'.format(dst, revisionfileno), revisions)
            index = 0
            revisionfileno += 1
            revisions = []

            if not sys.argv[2] == 'prod' and revisionfileno >= file_limit:
                sys.exit(0)
    if len(revisions) > 0:
        save_avro_file('{}revisions.{}.avro'.format(dst, revisionfileno), revisions)
