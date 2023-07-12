#!/usr/bin/env python3
import argparse
import glob
import os
import sys

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from neo4j.exceptions import AuthError

from icdc_schema import ICDC_Schema
from props import Props
from bento.common.utils import get_logger, get_raw_logger, removeTrailingSlash, check_schema_files, UPSERT_MODE, NEW_MODE, DELETE_MODE, \
    get_log_file, LOG_PREFIX, APP_NAME, load_plugin, print_config

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Data_Loader'

os.environ[APP_NAME] = 'Data_Loader'

from config import BentoConfig
from data_loader import DataLoader, VALIDATION_ERROR, VALIDATION_DELIMITER
from bento.common.s3 import S3Bucket


def parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Load TSV(TXT) files (from Pentaho) into Neo4j')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('--data-model-version', help='Version for data model files')
    parser.add_argument('--prop-file', help='Property file, example is in config/props.example.yml')
    parser.add_argument('--backup-folder', help='Location to store database backup')
    parser.add_argument('config_file', help='Configuration file, example is in config/data-loader-config.example.yml',
                        nargs='?', default=None)
    parser.add_argument('-c', '--cheat-mode', help='Skip validations, aka. Cheat Mode', action='store_true')
    parser.add_argument('-d', '--dry-run', help='Validations only, skip loading', action='store_true')
    parser.add_argument('--wipe-db', help='Wipe out database before loading, you\'ll lose all data!',
                        action='store_true')
    parser.add_argument('--no-backup', help='Skip backup step', action='store_true')
    parser.add_argument('-y', '--yes', help='Automatically confirm deletion and database wiping', action='store_true')
    parser.add_argument('-M', '--max-violations', help='Max violations to display', nargs='?', type=int)
    parser.add_argument('-b', '--bucket', help='S3 bucket name')
    parser.add_argument('-f', '--s3-folder', help='S3 folder')
    parser.add_argument('--bucket-logs', help='S3 bucket name for logs')
    parser.add_argument('--s3-folder-logs', help='S3 folder for logs')
    parser.add_argument('--bucket-fail', help='S3 bucket name for data that failed to validate and load.')
    parser.add_argument('--s3-folder-fail', help='S3 folder for data that failed to validate and load.')
    parser.add_argument('--bucket-success', help='S3 bucket name for data that successfully loaded.')
    parser.add_argument('--s3-folder-success', help='S3 folder for data that successfully loaded.')
    parser.add_argument('-m', '--mode', help='Loading mode', choices=[UPSERT_MODE, NEW_MODE, DELETE_MODE],
                        default=UPSERT_MODE)
    parser.add_argument('--dataset', help='Dataset directory')
    parser.add_argument('--split-transactions', help='Creates a separate transaction for each file',
                        action='store_true')
    return parser.parse_args() if args is None else parser.parse_args(args)


def process_arguments(args, log):
    config_file = None
    if args.config_file:
        config_file = args.config_file
    config = BentoConfig(config_file)

    # Required Fields
    if args.dataset:
        config.dataset = args.dataset
    if not config.dataset:
        log.error('No dataset specified! Please specify a dataset in config file or with CLI argument --dataset')
        sys.exit(1)
    if not config.s3_folder and not os.path.isdir(config.dataset):
        log.error('{} is not a directory!'.format(config.dataset))
        sys.exit(1)

    if args.prop_file:
        config.prop_file = args.prop_file
    if not config.prop_file:
        log.error('No properties file specified! ' +
                  'Please specify a properties file in config file or with CLI argument --prop-file')
        sys.exit(1)

    if args.schema:
        config.schema_files = args.schema
    if not config.schema_files:
        log.error('No schema file specified! ' +
                  'Please specify at least one schema file in config file or with CLI argument --schema')
        sys.exit(1)

    if args.data_model_version:
        config.data_model_version = args.data_model_version
    if not config.data_model_version:
        log.error('No data model version suppplied. ' +
                  'Please specify the version for the supplied data model files in the config file ' +
                  'or with CLI argument --data-model-version')

    if config.PSWD_ENV in os.environ and not config.neo4j_password:
        config.neo4j_password = os.environ[config.PSWD_ENV]
    if args.password:
        config.neo4j_password = args.password
    if not config.neo4j_password:
        log.error('Password not specified! Please specify password with -p or --password argument,' +
                  ' or set {} env var'.format(config.PSWD_ENV))
        sys.exit(1)

    # Conditionally Required Fields
    if args.split_transactions:
        config.split_transactions = args.split_transactions
    if args.no_backup:
        config.no_backup = args.no_backup
    if args.backup_folder:
        config.backup_folder = args.backup_folder
    if config.split_transactions and config.no_backup:
        log.error('--split-transaction and --no-backup cannot both be enabled, a backup is required when running'
                  ' in split transactions mode')
        sys.exit(1)
    if not config.backup_folder and not config.no_backup:
        log.error('Backup folder not specified! A backup folder is required unless the --no-backup argument is used')
        sys.exit(1)

    if args.s3_folder:
        config.s3_folder = args.s3_folder
    if config.s3_folder:
        if not os.path.exists(config.dataset):
            os.makedirs(config.dataset)
        else:
            exist_files = glob.glob('{}/*.txt'.format(config.dataset))
            if len(exist_files) > 0:
                log.error('Folder: "{}" is not empty, please empty it first'.format(config.dataset))
                sys.exit(1)

        if args.bucket:
            config.s3_bucket = args.bucket
        if not config.s3_bucket:
            log.error('Please specify S3 bucket name with -b/--bucket argument!')
            sys.exit(1)
        bucket = S3Bucket(config.s3_bucket)
        if not os.path.isdir(config.dataset):
            log.error('{} is not a directory!'.format(config.dataset))
            sys.exit(1)
        log.info(f'Loading data from s3://{config.s3_bucket}/{config.s3_folder}')
        if not bucket.download_files_in_folder(config.s3_folder, config.dataset):
            log.error('Download files from S3 bucket "{}" failed!'.format(config.s3_bucket))
            sys.exit(1)

    if args.bucket_logs:
        config.s3_bucket_logs = args.bucket_logs
    if args.s3_folder_logs:
        config.s3_folder_logs = args.s3_folder_logs
    if (config.s3_bucket_logs and not config.s3_folder_logs) or (not config.s3_bucket_logs and config.s3_folder_logs):  # Python doesn't have an XOR for existence of value I don't think
        log.error("Must specify both bucket and folder for depositing logs, if specifying an S3 location for logs. Use CLI arguments --bucket-logs and --s3-folder-logs.")
        sys.exit(1)

    if args.bucket_fail:
        config.s3_bucket_fail = args.bucket_fail
    if args.s3_folder_fail:
        config.s3_folder_fail = args.s3_folder_fail
    if (config.s3_bucket_fail and not config.s3_folder_fail) or (not config.s3_bucket_fail and config.s3_folder_fail):  # Python doesn't have an XOR for existence of value I don't think
        log.error("Must specify both bucket and folder for depositing data that failed to validate and load, if specifying an S3 location for logs. Use CLI arguments --bucket-logs and --s3-folder-logs.")
        sys.exit(1)

    if args.bucket_success:
        config.s3_bucket_success = args.bucket_success
    if args.s3_folder_success:
        config.s3_folder_success = args.s3_folder_success
    if (config.s3_bucket_success and not config.s3_folder_success) or (not config.s3_bucket_success and config.s3_folder_success):  # Python doesn't have an XOR for existence of value I don't think
        log.error("Must specify both bucket and folder for depositing data that successfully validated and loaded, if specifying an S3 location for logs. Use CLI arguments --bucket-logs and --s3-folder-logs.")
        sys.exit(1)
    

    # Optional Fields
    if args.uri:
        config.neo4j_uri = args.uri
    if not config.neo4j_uri:
        config.neo4j_uri = 'bolt://localhost:7687'
    config.neo4j_uri = removeTrailingSlash(config.neo4j_uri)
    log.info(f"Loading into Neo4j at: {config.neo4j_uri}")

    if args.user:
        config.neo4j_user = args.user
    if not config.neo4j_user:
        config.neo4j_user = 'neo4j'

    if args.wipe_db:
        config.wipe_db = args.wipe_db

    if args.yes:
        config.yes = args.yes

    if args.dry_run:
        config.dry_run = args.dry_run

    if args.cheat_mode:
        config.cheat_mode = args.cheat_mode

    if args.mode:
        config.loading_mode = args.mode
    if not config.loading_mode:
        config.loading_mode = "UPSERT_MODE"

    if args.max_violations:
        config.max_violations = int(args.max_violations)
    if not config.max_violations:
        config.max_violations = 10

    return config


def upload_log_file(bucket_name, folder, file_path):
    base_name = os.path.basename(file_path)
    s3 = S3Bucket(bucket_name)
    key = f'{folder}/{base_name}'
    return s3.upload_file(key, file_path)

def prepare_plugin(config, schema):
    if not config.params:
        config.params = {}
    config.params['schema'] = schema
    return load_plugin(config.module_name, config.class_name, config.params)


# Data loader will try to load all TSV(.TXT) files from given directory into Neo4j
# optional arguments includes:
# -i or --uri followed by Neo4j server address and port in format like bolt://12.34.56.78:7687
def main(args=None):
    log = get_logger('Loader')
    log_file = get_log_file()
    validation_logger, validation_log_file = get_raw_logger('Data Loader Validation', log_level=VALIDATION_ERROR, log_folder='tmp_validation', log_prefix='inventory_validation')
    config = process_arguments(parse_arguments(args), log)
    print_config(log, config)

    if not check_schema_files(config.schema_files, log):
        return

    driver = None
    restore_cmd = ''
    try:
        txt_files = glob.glob('{}/*.txt'.format(config.dataset))
        tsv_files = glob.glob('{}/*.tsv'.format(config.dataset))
        file_list = txt_files + tsv_files
        if file_list:
            if config.wipe_db and not config.yes:
                if not confirm_deletion('Wipe out entire Neo4j database before loading?'):
                    sys.exit(1)

            if config.loading_mode == DELETE_MODE and not config.yes:
                if not confirm_deletion('Delete all nodes and child nodes from data file?'):
                    sys.exit(1)

            prop_path = os.path.join(config.dataset, config.prop_file)
            if os.path.isfile(prop_path):
                props = Props(prop_path)
            else:
                props = Props(config.prop_file)
            schema = ICDC_Schema(config.schema_files, props)
            if not config.dry_run:
                driver = GraphDatabase.driver(
                    config.neo4j_uri,
                    auth=(config.neo4j_user, config.neo4j_password),
                    encrypted=False
                )

            plugins = []
            if len(config.plugins) > 0:
                for plugin_config in config.plugins:
                    plugins.append(prepare_plugin(plugin_config, schema))

            validation_logger.log(VALIDATION_ERROR, ",".join([f"DataModelVersion: {config.data_model_version}"]))
            validation_logger.log(VALIDATION_ERROR, "BatchFilenames")
            validation_logger.log(VALIDATION_ERROR, "\n".join(file_list))  # have a column per file, I think this is easier to use in Excel            
            validation_logger.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join(["Filename","LineNumber","OffendingColumn","OffendingValue","OffendingReason"]))
            loader = DataLoader(driver, schema, validation_logger, plugins)

            load_result = loader.load(file_list, config.cheat_mode, config.dry_run, config.loading_mode, config.wipe_db,
                        config.max_violations, split=config.split_transactions,
                        no_backup=config.no_backup, neo4j_uri=config.neo4j_uri, backup_folder=config.backup_folder)
            if driver:
                driver.close()
            if restore_cmd:
                log.info(restore_cmd)
            if load_result == False:
                log.error('Data files database load failed')
                if config.s3_bucket_fail and config.s3_folder_fail:
                    bucket = S3Bucket(config.s3_bucket_fail)
                    key = config.s3_folder_fail
                    files = [x for x in os.listdir(config.dataset) if os.path.isfile(os.path.join(config.dataset,x))]
                    for file in files:
                        upload_log_file(config.s3_bucket_fail, config.dataset, file)  # it says 'upload_log_file' but it's really just uploading a file
                    log.info(f'Data files moved to {config.s3_bucket_fail}/{config.s3_folder_fail} due to failure.')
                else:
                    log.info(f'Data files not moved, still in directory {config.dataset} where this code ran, but was successful.')
                    
            else:
                log.info('Data files database load success')
                validation_logger.log(VALIDATION_ERROR, "Done.")
                if config.s3_bucket_success and config.s3_folder_success:
                    bucket = S3Bucket(config.s3_bucket_success)
                    key = config.s3_folder_success
                    files = [x for x in os.listdir(config.dataset) if os.path.isfile(os.path.join(config.dataset,x))]
                    for file in files:
                        upload_log_file(config.s3_bucket_success, config.dataset, file)  # it says 'upload_log_file' but it's really just uploading a file
                    log.info(f'Data files moved to {config.s3_bucket_success}/{config.s3_folder_success} due to success.')
                else:
                    log.info(f'Data files not moved, still in directory {config.dataset} where this code ran, but was successful.')
        else:
            log.info('No files to load.')


    except ServiceUnavailable:
        log.critical("Neo4j service not available at: \"{}\"".format(config.neo4j_uri))
        return
    except AuthError:
        log.error("Wrong Neo4j username or password!")
        return
    except KeyboardInterrupt:
        log.critical("User stopped the loading!")
        return
    finally:
        if driver:
            driver.close()
        if restore_cmd:
            log.info(restore_cmd)

    if config.s3_bucket_logs and config.s3_folder_logs:
        result = upload_log_file(config.s3_bucket_logs, f'{config.s3_folder_logs}/validation_logs', validation_log_file)
        if result:
            log.info(f'Uploading log file {validation_log_file} succeeded!')
            if os.path.isfile(os.path.abspath(validation_log_file)):
                os.remove(validation_log_file)
        else:
            log.error(f'Uploading log file {validation_log_file} failed!')

        result = upload_log_file(config.s3_bucket_logs, f'{config.s3_folder_logs}/logs', log_file)
        if result:
            log.info(f'Uploading log file {log_file} succeeded!')
            if os.path.isfile(os.path.abspath(log_file)):
                os.remove(log_file)
        else:
            log.error(f'Uploading log file {log_file} failed!')

    


def confirm_deletion(message):
    print(message)
    confirm = input('Type "yes" and press enter to proceed (You\'ll LOSE DATA!!!), press enter to cancel:')
    confirm = confirm.strip().lower()
    return confirm == 'yes'


if __name__ == '__main__':
    main()
