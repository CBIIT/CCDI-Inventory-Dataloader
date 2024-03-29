import os
import yaml

from bento.common.utils import get_logger, UPSERT_MODE

class PluginConfig:
    def __init__(self, config):
        self.module_name = config['module']
        self.class_name = config['class']
        self.params = config.get('params')


class BentoConfig:
    def __init__(self, config_file):
        self.log = get_logger('Bento Config')
        self.PSWD_ENV = 'NEO_PASSWORD'

        if config_file is None:
            # Metadata related
            self.data_model_version = None

            # File-Loader related
            self.temp_folder = None
            self.queue_long_pull_time = None
            self.visibility_timeout = None
            self.indexd_guid_prefix = None
            self.indexd_manifest_ext = None
            self.rel_prop_delimiter = None

            # Data-Loader Related
            self.backup_folder = None
            self.neo4j_uri = None
            self.neo4j_user = None
            self.neo4j_password = None
            self.schema_files = None
            self.prop_file = None
            self.cheat_mode = None
            self.dry_run = None
            self.wipe_db = None
            self.no_backup = None
            self.yes = None
            self.max_violations = None
            self.s3_bucket = None
            self.s3_folder = None
            self.s3_bucket_logs = None
            self.s3_folder_logs = None
            self.s3_bucket_fail = None
            self.s3_folder_fail = None
            self.s3_bucket_success = None
            self.s3_folder_success = None
            self.loading_mode = None
            self.dataset = None
            self.no_parents = None
            self.split_transactions = None
        else:
            if os.path.isfile(config_file):
                with open(config_file) as c_file:
                    config = yaml.safe_load(c_file)['Config']

                    #################################
                    # Metadata
                    self.data_model_version = config.get('data_model_version')

                    #################################
                    # Folders
                    self.temp_folder = config.get('temp_folder')
                    if self.temp_folder:
                        self._create_folder(self.temp_folder)

                    self.backup_folder = config.get('backup_folder')
                    if self.backup_folder:
                        self._create_folder(self.backup_folder)

                    #################################
                    # File-loader related
                    if 'sqs' in config:
                        sqs = config['sqs']
                        self.queue_long_pull_time = sqs.get('long_pull_time')
                        self.visibility_timeout = sqs.get('visibility_timeout')

                    if 'indexd' in config:
                        indexd = config['indexd']
                        self.indexd_guid_prefix = indexd.get('GUID_prefix')
                        self.indexd_manifest_ext = indexd.get('ext')
                        if self.indexd_manifest_ext and not self.indexd_manifest_ext.startswith('.'):
                            self.indexd_manifest_ext = '.' + self.indexd_manifest_ext
                    self.slack_url = config.get('url')

                    #################################
                    # Data-loader related
                    self.rel_prop_delimiter = config.get('rel_prop_delimiter')
                    if 'neo4j' in config:
                        neo4j = config['neo4j']
                        self.neo4j_uri = neo4j.get('uri')
                        self.neo4j_user = neo4j.get('user')
                        self.neo4j_password = neo4j.get('password')

                    self.plugins = []
                    if 'plugins' in config:
                        for plugin in config.get('plugins', []) or []:
                            self.plugins.append(PluginConfig(plugin))

                    self.schema_files = config.get('schema')
                    self.prop_file = config.get('prop_file')
                    self.cheat_mode = config.get('cheat_mode')
                    self.dry_run = config.get('dry_run')
                    self.wipe_db = config.get('wipe_db')
                    self.no_backup = config.get('no_backup')
                    self.yes = config.get('no_confirmation')
                    self.max_violations = config.get('max_violations', 10)
                    self.s3_bucket = config.get('s3_bucket')
                    self.s3_folder = config.get('s3_folder')
                    self.s3_bucket_logs = config.get('s3_bucket_logs')
                    self.s3_folder_logs = config.get('s3_folder_logs')
                    self.s3_bucket_fail = config.get('s3_bucket_fail')
                    self.s3_folder_fail = config.get('s3_folder_fail')
                    self.s3_bucket_success = config.get('s3_bucket_success')
                    self.s3_folder_success = config.get('s3_folder_success')
                    self.loading_mode = config.get('loading_mode', UPSERT_MODE)
                    self.dataset = config.get('dataset')
                    self.no_parents = config.get('no_parents')
                    self.split_transactions = config.get('split_transactions')
            else:
                msg = f'Can NOT open configuration file "{config_file}"!'
                self.log.error(msg)
                raise Exception(msg)

    def _create_folder(self, folder):
        os.makedirs(folder, exist_ok=True)
        if not os.path.isdir(folder):
            msg = f'{folder} is not a folder!'
            self.log.error(msg)
            raise Exception(msg)
