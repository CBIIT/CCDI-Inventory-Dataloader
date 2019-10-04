#!/usr/bin/env python3
# This file is used to unzip/untar raw files, populate manifest then call data loader
# It will listen on SQS for incoming messages sent by S3, to process new files

import boto3
import uuid
import re
from urllib.parse import unquote_plus
import logging
import zipfile
import tarfile
import glob
import os, sys
from utils import *
from sqs import Queue
import json
import csv
import hashlib
import io
import argparse
from sqs import *
import re
import shutil

RAW_PREFIX = 'RAW'
FINAL_PREFIX = 'Final'
ICDC_FILE_UPLOADED = 'ICDC-file-uploaded'
FILE_NAME = 'file_name'
CASE_ID = 'case_id'
MD5 = 'md5sum'
FILE_SIZE = "file_size"
FILE_LOC = 'file_location'
FILE_FORMAT = 'file_format'
UUID = 'uuid'
FILE_STAT = 'file_status'
ACL = 'acl'
MANIFEST_FIELDS = [UUID, FILE_SIZE, MD5, FILE_STAT, FILE_LOC, FILE_FORMAT, ACL]
BLOCK_SIZE = 65536
DEFAULT_STAT = 'uploaded'
DEFAULT_ACL = 'open'
MANIFESTS = 'manifests'
FILES = 'files'
RECORDS = 'Records'

class FileProcessor:
    def __init__(self, queue_name):
        self.log = get_logger('File Processor')
        self.queue_name = queue_name
        self.s3_client = boto3.client('s3')

    @staticmethod
    def join_path(folder, file):
        clean_folder = re.sub(r'/+$', '', folder)
        clean_file = re.sub(r'^/+', '', file)
        return '{}/{}'.format(clean_folder, clean_file)

    # Download and extract files, return list of files with final location and md5
    def extract_file(self, bucket, key, final_path, local_folder):
        try:
            ext = key.split('.')[-1]
            if ext:
                if ext == 'tar':
                    self.log.info('Streaming tar file: {}'.format(key))
                    files = {}
                    obj = self.s3_client.get_object(Bucket=bucket, Key=key)
                    stream = io.BufferedReader(obj['Body']._raw_stream)
                    with tarfile.open(None, 'r|*', stream) as tar_ref:
                        self.log.info('Extracting tar file: {}'.format(key))
                        for item in tar_ref:
                            file_name = item.name
                            file_path = self.join_path(final_path, file_name)
                            if item.isfile() and '/' not in file_name:
                                self.log.info('Extracting {} to {}'.format(file_name, file_path))
                                tar_ref.extract(item, local_folder)
                                local_file = os.path.join(local_folder, file_name)
                                with open(local_file, 'rb') as lf:
                                    s3_obj = self.s3_client.put_object(Bucket=bucket, Key=file_path, Body=lf)
                                    files[file_name] = {FILE_NAME: file_name, FILE_LOC: self.get_s3_location(bucket, final_path, file_name), FILE_SIZE: os.stat( local_file).st_size, MD5: s3_obj['ETag'].replace('"', '')}
                                if not file_name.endswith('.txt'):
                                    os.remove(local_file)
                    return files
                else:
                    self.log.warning('{} file is not supported!'.format(ext))
                    return False
            else:
                return False
        except Exception as e:
            self.log.exception(e)
            return False

    def upload_files(self, bucket, upload_path, file_list):
        try:
            for file in file_list:
                if os.path.isfile(file):
                    self.log.info('Uploading file: {}'.format(file))
                    file_name = self.join_path(upload_path, os.path.basename(file))
                    self.s3_client.upload_file(file, bucket, file_name)
                else:
                    self.log.info('{} is not a file, and won\'t be uploaded to S3'.format(file))
            return True
        except Exception as e:
            self.log.exception(e)
            return False


    def send_sqs_message(self, queue, data_bucket, data_path):
        try:
            obj = {
                'type': ICDC_FILE_UPLOADED,
                'bucket': data_bucket,
                'path': data_path
            }
            queue.sendMsgToQueue(json.dumps(obj), data_path)
            self.log.info('Data path: {}'.format(data_path))
            return True
        except Exception as e:
            self.log.exception(e)
            return False

    @staticmethod
    def get_s3_location(bucket, folder, key):
        return "s3://{}/{}/{}".format(bucket, folder, key)

    def populate_record(self, record, file_info):
        file_name = file_info[FILE_NAME]
        record[FILE_SIZE] = file_info[FILE_SIZE]
        record[FILE_LOC] = file_info[FILE_LOC]
        record[MD5] = file_info[MD5]
        record[FILE_FORMAT] = (os.path.splitext(file_name)[1]).split('.')[1].lower()
        record[UUID] = get_uuid_for_node("file", record[FILE_LOC])
        record[FILE_STAT] = DEFAULT_STAT
        record[ACL] = DEFAULT_ACL
        return record

    # check the field file_name/case id in the manifest which should not be null/empty
    # check files included in the manifest exist or not
    def populate_manifest(self, manifest, extracted_files):
        self.log.info('Validating manifest: {}'.format(manifest))
        succeeded = True
        # check manifest
        if not os.path.isfile(manifest):
            self.log.error('Manifest: "{}" does not exists !'.format(manifest))
            succeeded = False
        else:
            try:
                self.log.info('Processing fields in manifest.')
                # check fields in the manifest, if missing fields stops
                with open(manifest) as inf:
                    temp_file = manifest + '_populated'
                    with open(temp_file, 'w') as outf:
                        tsv_reader = csv.DictReader(inf, delimiter='\t')
                        fieldnames = tsv_reader.fieldnames
                        fieldnames += MANIFEST_FIELDS
                        tsv_writer = csv.DictWriter(outf, delimiter='\t', fieldnames=fieldnames)
                        tsv_writer.writeheader()
                        line_count = 1
                        for record in tsv_reader:
                            line_count += 1
                            file_name = record.get(FILE_NAME, None)
                            if file_name:
                                if not file_name in extracted_files:
                                    self.log.error('Invalid data at line {} : File "{}" doesn\'t exist!'.format(line_count, file_name))
                                    succeeded = False
                                else:
                                    # Populate fields in record
                                    self.populate_record(record, extracted_files[file_name])
                            else:
                                self.log.error('Invalid data at line {} : Empty file name'.format(line_count))
                                succeeded = False

                            case_id = record.get(CASE_ID, None)
                            if not case_id:
                                self.log.error('Invalid data at line {} : Empty case_id'.format(line_count))
                                succeeded = False
                            tsv_writer.writerow(record)
            except Exception as e:
                self.log.exception(e)
                succeeded = False

        if succeeded:
            os.remove(manifest)
            os.rename(temp_file, manifest)
        else:
            os.remove(temp_file)
        return succeeded



    # Find and process manifest files, return list of updated manifest files and a list of files that included in manifest files
    def process_manifest(self, data_folder, extracted_files) -> [str]:
        results = []
        file_list = glob.glob('{}/*.txt'.format(data_folder))
        try:
            for manifest in file_list:
                if not self.populate_manifest(manifest, extracted_files):
                    self.log.warning('Populate manifest file "{}" failed!'.format(manifest))
                    continue
                else:
                    results.append(manifest)
                    self.log.info('Populated manifest file "{}"!'.format(manifest))

        except Exception as e:
            self.log.exception(e)

        return results

    # Get possible one level folder inside tar/zip file
    def get_true_data_folder(self, folder):
        folder = folder.replace(r'/$', '')
        first_level_file_list = glob.glob('{}/*'.format(folder))
        result = folder
        if len(first_level_file_list) == 1:
            inside_folder = first_level_file_list[0]
            if os.path.isdir(inside_folder):
                result = inside_folder
        return result

    def handler(self, event):
        try:
            for record in event['Records']:
                temp_folder = 'temp/{}'.format(uuid.uuid4())
                bucket = record['s3']['bucket']['name']
                key = unquote_plus(record['s3']['object']['key'])
                # Assume raw files will be in a sub-folder inside '/RAW'
                if not key.startswith(RAW_PREFIX):
                    self.log.error('File is not in {} folder'.format(RAW_PREFIX))
                    return False
                final_path = os.path.dirname(key).replace(RAW_PREFIX, FINAL_PREFIX)
                org_file_name = os.path.basename(key)
                file_list = self.extract_file(bucket, key, final_path, temp_folder)
                if not file_list:
                    self.log.error('Extract RAW file "{}" failed'.format(org_file_name))
                    return False
                manifests = self.process_manifest(temp_folder, file_list)
                if not manifests:
                    self.log.error('Process manifest failed!')
                    return False
                self.upload_files(bucket, final_path, manifests)
                # Todo: call data loader
                shutil.rmtree(temp_folder)
            return True

        except Exception as e:
            self.log.exception(e)
            return False

    def listen(self):
        self.queue = Queue(self.queue_name)
        self.log.info('PIMixture Processor service started!')
        while True:
            self.log.info("Receiving more messages...")
            for msg in self.queue.receiveMsgs(VISIBILITY_TIMEOUT):
                extender = None
                try:
                    data = json.loads(msg.body)
                    if data and RECORDS in data and isinstance(data[RECORDS], list):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        self.log.info('Start processing job ...')

                        if self.handler(data):
                            msg.delete()
                            self.log.info('Finish processing job!')

                except Exception as e:
                    self.log.exception(e)

                finally:
                    if extender:
                        extender.stop()
                        del(extender)

def main(args):
    log = get_logger('Raw file processor - main')

    if not args.queue:
        log.error('Please specify queue name with -q/--queue argument')
        sys.exit(1)

    try:
        processor = FileProcessor(args.queue)
        processor.listen()

    except KeyboardInterrupt:
        log.info("\nBye!")
        sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process incoming S3 files and call data loader to load into Neo4j')
    parser.add_argument('-q', '--queue', help='SQS queue name')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('-c', '--cheat-mode', help='Skip validations, aka. Cheat Mode', action='store_true')
    parser.add_argument('-d', '--dry-run', help='Validations only, skip loading', action='store_true')
    parser.add_argument('-m', '--max-violations', help='Max violations to display', nargs='?', type=int, default=10)
    parser.add_argument('-b', '--bucket', help='S3 bucket name')
    parser.add_argument('-f', '--s3-folder', help='S3 folder')
    # parser.add_argument('dir', help='Data directory')
    args = parser.parse_args()
    main(args)