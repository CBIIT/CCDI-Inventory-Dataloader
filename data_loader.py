#!/usr/bin/env python3

import os
from collections import deque
import csv
import re
import datetime
import sys
import platform
import subprocess
import json
from timeit import default_timer as timer
from bento.common.utils import get_host, DATETIME_FORMAT, reformat_date

from neo4j import Driver

from icdc_schema import ICDC_Schema, is_parent_pointer, get_list_values
from bento.common.utils import get_logger, NODES_CREATED, RELATIONSHIP_CREATED, UUID, \
    RELATIONSHIP_TYPE, MULTIPLIER, ONE_TO_ONE, DEFAULT_MULTIPLIER, UPSERT_MODE, \
    NEW_MODE, DELETE_MODE, NODES_DELETED, RELATIONSHIP_DELETED, combined_dict_counters, \
    MISSING_PARENT, NODE_LOADED, get_string_md5

NODE_TYPE = 'type'
PROP_TYPE = 'Type'
PARENT_TYPE = 'parent_type'
PARENT_ID_FIELD = 'parent_id_field'
PARENT_ID = 'parent_id'
excluded_fields = {NODE_TYPE}
CASE_NODE = 'case'
CASE_ID = 'case_id'
CREATED = 'created'
UPDATED = 'updated'
RELATIONSHIPS = 'relationships'
INT_NODE_CREATED = 'int_node_created'
PROVIDED_PARENTS = 'provided_parents'
RELATIONSHIP_PROPS = 'relationship_properties'
BATCH_SIZE = 1000

# the format for a validation error is as follows:
#   [Filename, LineNumber(s), OffendingColumn, OffendingValue, OffendingReason]
#   LineNumber(s) may be plural for duplicate IDs or duplicate data instead of having two separate columns
#   this is supposed to be human readable
VALIDATION_ERROR = 51  # custom log level to capture validation errors only, https://docs.python.org/3/library/logging.html#logging-levels
VALIDATION_DELIMITER = "\t"
MISSING = "!MISSING!"
EMPTY = "!EMPTY!"
DUPLICATE_ID = "Duplicate ID."
MISSING_ID = "Missing ID."
MISSING_ID_FIELD = "Missing ID field."
NODE_EXISTS = "Node exists."
DUPLICATE_DATA = "Duplicate Data."
INVALID_DATA = "Invalid Value."
INVALID_RELATIONSHIP = "Invalid Relationship."
RELATIONSHIP_EXISTS = "Relationship already exists."
UNDEFINED_RELATIONSHIP = "Undefined relationship."


def get_btree_indexes(session):
    """
    Queries the database to get all existing indexes
    :param session: the current neo4j transaction session
    :return: A set of tuples representing all existing indexes in the database
    """
    command = "SHOW INDEXES"
    result = session.run(command)
    indexes = set()
    for r in result:
        if r["type"] == "BTREE":
            indexes.add(format_as_tuple(r["labelsOrTypes"][0], r["properties"]))
    return indexes

def format_as_tuple(node_name, properties):
    """
    Format index info as a tuple
    :param node_name: The name of the node type for the index
    :param properties: The list of node properties being used by the index
    :return: A tuple containing the index node_name followed by the index properties in alphabetical order
    """
    if isinstance(properties, str):
        properties = [properties]
    lst = [node_name] + sorted(properties)
    return tuple(lst)


def backup_neo4j(backup_dir, name, address, log):
    try:
        restore_cmd = 'To restore DB from backup (to remove any changes caused by current data loading, run ' \
                      'following commands:\n '
        restore_cmd += '#' * 160 + '\n'
        neo4j_cmd = 'neo4j-admin restore --from={}/{} --force'.format(backup_dir, name)
        mkdir_cmd = [
            'mkdir',
            '-p',
            backup_dir
        ]
        is_shell = False
        # settings for Windows platforms
        if platform.system() == "Windows":
            mkdir_cmd[2] = os.path.abspath(backup_dir)
            is_shell = True
        cmds = [
            mkdir_cmd,
            [
                'neo4j-admin',
                'backup',
                '--backup-dir={}'.format(backup_dir)
            ]
        ]
        if address in ['localhost', '127.0.0.1']:
            # On Windows, the Neo4j service cannot be accessed through the command line without an absolute path
            # or a custom installation location
            if platform.system() == "Windows":
                restore_cmd += '\tManually stop the Neo4j service\n\t$ {}\n\tManually start the Neo4j service\n'.format(
                    neo4j_cmd)
            else:
                restore_cmd += '\t$ neo4j stop && {} && neo4j start\n'.format(neo4j_cmd)
            for cmd in cmds:
                log.info(cmd)
                subprocess.call(cmd, shell=is_shell)
        else:
            second_cmd = 'sudo systemctl stop neo4j && {} && sudo systemctl start neo4j && exit'.format(neo4j_cmd)
            restore_cmd += '\t$ echo "{}" | ssh -t {} sudo su - neo4j\n'.format(second_cmd, address)
            for cmd in cmds:
                remote_cmd = ['ssh', address, '-o', 'StrictHostKeyChecking=no'] + cmd
                log.info(' '.join(remote_cmd))
                subprocess.call(remote_cmd)
        restore_cmd += '#' * 160
        return restore_cmd
    except Exception as e:
        log.exception(e)
        return False


def check_encoding(file_name):
    utf8 = 'utf-8'
    windows1252 = 'windows-1252'
    try:
        with open(file_name, encoding=utf8) as file:
            for _ in file.readlines():
                pass
        return utf8
    except UnicodeDecodeError:
        return windows1252


# Mask all relationship properties, so they won't participate in property comparison
def get_props_signature(props):
    clean_props = props
    for key in clean_props.keys():
        if '$' in key:
            clean_props[key] = ''
    signature = get_string_md5(str(clean_props))
    return signature


class DataLoader:
    def __init__(self, driver, schema, validation_logger, plugins=None):
        if plugins is None:
            plugins = []
        if not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        self.log = get_logger('Data Loader')
        self.validation_log = validation_logger
        self.driver = driver
        self.schema = schema
        self.rel_prop_delimiter = self.schema.rel_prop_delimiter

        if plugins:
            for plugin in plugins:
                if not hasattr(plugin, 'create_node'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'should_run'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'nodes_stat'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'relationships_stat'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'nodes_created'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'relationships_created'):
                    raise ValueError('Invalid Plugin!')
        self.plugins = plugins
        self.nodes_created = 0
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}

    def check_files(self, file_list):
        if not file_list:
            self.log.error('Invalid file list')
            return False
        elif file_list:
            for data_file in file_list:
                if not os.path.isfile(data_file):
                    self.log.error('File "{}" does not exist'.format(data_file))
                    return False
            return True

    def validate_files(self, cheat_mode, file_list, max_violations):
        if not cheat_mode:
            validation_failed = False
            for txt in file_list:
                if not self.validate_file(txt, max_violations):
                    self.log.error('Validating file "{}" failed!'.format(txt))
                    validation_failed = True
            return not validation_failed
        else:
            self.log.info('Cheat mode enabled, all validations skipped!')
            return True

    def load(self, file_list, cheat_mode, dry_run, loading_mode, wipe_db, max_violations,
             split=False, no_backup=True, backup_folder="/", neo4j_uri=None):
        if not self.check_files(file_list):
            return False
        start = timer()
        if not self.validate_files(cheat_mode, file_list, max_violations):
            return False
        self.validation_log.log(VALIDATION_ERROR, "################\n" +
                                "# No file validation errors. Loading validation errors below.\n" +
                                "################")
        if not no_backup and not dry_run:
            if not neo4j_uri:
                self.log.error('No Neo4j URI specified for backup, abort loading!')
                sys.exit(1)
            backup_name = datetime.datetime.today().strftime(DATETIME_FORMAT)
            host = get_host(neo4j_uri)
            restore_cmd = backup_neo4j(backup_folder, backup_name, host, self.log)
            if not restore_cmd:
                self.log.error('Backup Neo4j failed, abort loading!')
                sys.exit(1)
        if dry_run:
            end = timer()
            self.log.info('Dry run mode, no nodes or relationships loaded.')  # Time in seconds, e.g. 5.38091952400282
            self.log.info('Running time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
            return {NODES_CREATED: 0, RELATIONSHIP_CREATED: 0}

        self.nodes_created = 0
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        # Data updates and schema related updates cannot be performed in the same session so multiple will be created
        # Create new session for schema related updates (index creation)
        with self.driver.session() as session:
            tx = session.begin_transaction()
            try:
                self.create_indexes(tx)
                tx.commit()
            except Exception as e:
                tx.rollback()
                self.log.exception(e)
                return False
        # Create new session for data related updates
        with self.driver.session() as session:
            # Split Transactions enabled
            if split:
                self._load_all(session, file_list, loading_mode, split, wipe_db)

            # Split Transactions Disabled
            else:
                # Data updates transaction
                tx = session.begin_transaction()
                try:
                    self._load_all(tx, file_list, loading_mode, split, wipe_db)
                    tx.commit()
                except Exception as e:
                    tx.rollback()
                    self.log.exception(e)
                    return False

        # End the timer
        end = timer()

        # Print statistics
        for plugin in self.plugins:
            combined_dict_counters(self.nodes_stat, plugin.nodes_stat)
            combined_dict_counters(self.relationships_stat, plugin.relationships_stat)
            self.nodes_created += plugin.nodes_created
            self.relationships_created += plugin.relationships_created
        for node in sorted(self.nodes_stat.keys()):
            count = self.nodes_stat[node]
            self.log.info('Node: (:{}) loaded: {}'.format(node, count))
        for rel in sorted(self.relationships_stat.keys()):
            count = self.relationships_stat[rel]
            self.log.info('Relationship: [:{}] loaded: {}'.format(rel, count))
        self.log.info('{} new indexes created!'.format(self.indexes_created))
        self.log.info('{} nodes and {} relationships loaded!'.format(self.nodes_created, self.relationships_created))
        self.log.info('{} nodes and {} relationships deleted!'.format(self.nodes_deleted, self.relationships_deleted))
        self.log.info('Loading time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
        return {NODES_CREATED: self.nodes_created, RELATIONSHIP_CREATED: self.relationships_created,
                NODES_DELETED: self.nodes_deleted, RELATIONSHIP_DELETED: self.relationships_deleted}

    def _load_all(self, tx, file_list, loading_mode, split, wipe_db):
        if wipe_db:
            self.wipe_db(tx, split)
        for txt in file_list:
            self.load_nodes(tx, txt, loading_mode, split)
        if loading_mode != DELETE_MODE:
            for txt in file_list:
                self.load_relationships(tx, txt, loading_mode, split)

    # Remove extra spaces at beginning and end of the keys and values
    @staticmethod
    def cleanup_node(node):
        return {key if not key else key.strip(): value if not value else value.strip() for key, value in node.items()}

    # Cleanup values for Boolean, Int and Float types
    # Add uuid to nodes if one not exists
    # Add parent id(s)
    # Add extra properties for "value with unit" properties
    def prepare_node(self, node):
        obj = self.cleanup_node(node)

        node_type = obj.get(NODE_TYPE, None)
        # Cleanup values for Boolean, Int and Float types
        if node_type:
            for key, value in obj.items():
                search_node_type = node_type
                search_key = key
                if is_parent_pointer(key):
                    search_node_type, search_key = key.split('.')
                elif self.schema.is_relationship_property(key):
                    search_node_type, search_key = key.split(self.rel_prop_delimiter)

                key_type = self.schema.get_prop_type(search_node_type, search_key)
                if key_type == 'Boolean':
                    cleaned_value = None
                    if isinstance(value, str):
                        if re.search(r'yes|true', value, re.IGNORECASE):
                            cleaned_value = True
                        elif re.search(r'no|false', value, re.IGNORECASE):
                            cleaned_value = False
                        else:
                            self.log.debug('Unsupported Boolean value: "{}"'.format(value))
                            cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Int':
                    try:
                        if value is None:
                            cleaned_value = None
                        else:
                            cleaned_value = int(value)
                    except ValueError:
                        cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Float':
                    try:
                        if value is None:
                            cleaned_value = None
                        else:
                            cleaned_value = float(value)
                    except ValueError:
                        cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Array':
                    items = get_list_values(value)
                    # todo: need to transform items if item type is not string
                    obj[key] = json.dumps(items)
                elif key_type == 'DateTime' or key_type == 'Date':
                    if value is None:
                        cleaned_value = None
                    else:
                        cleaned_value = reformat_date(value)
                    obj[key] = cleaned_value

        obj2 = {}
        for key, value in obj.items():
            obj2[key] = value
            # Add parent id field(s) into node
            if obj[NODE_TYPE] in self.schema.props.save_parent_id and is_parent_pointer(key):
                header = key.split('.')
                if len(header) > 2:
                    self.log.warning('Column header "{}" has multiple periods!'.format(key))
                field_name = header[1]
                parent = header[0]
                combined = '{}_{}'.format(parent, field_name)
                if field_name in obj:
                    self.log.debug(
                        '"{}" field is in both current node and parent "{}", use {} instead !'.format(key, parent,
                                                                                                      combined))
                    field_name = combined
                # Add an value for parent id
                obj2[field_name] = value
            # Add extra properties if any
            for extra_prop_name, extra_value in self.schema.get_extra_props(node_type, key, value).items():
                obj2[extra_prop_name] = extra_value

        if UUID not in obj2:
            id_field = self.schema.get_id_field(obj2)
            id_value = self.schema.get_id(obj2)
            node_type = obj2.get(NODE_TYPE)
            if node_type:
                if not id_value:
                    obj2[UUID] = self.schema.get_uuid_for_node(node_type, self.get_signature(obj2))
                elif id_field != UUID:
                    obj2[UUID] = self.schema.get_uuid_for_node(node_type, id_value)
            else:
                raise Exception('No "type" property in node')

        return obj2

    def get_signature(self, node):
        result = []
        for key in sorted(node.keys()):
            value = node[key]
            if not is_parent_pointer(key):
                result.append('{}: {}'.format(key, value))
        return '{{ {} }}'.format(', '.join(result))

    # Validate all cases exist in a data (TSV/TXT) file
    def validate_cases_exist_in_file(self, file_name, max_violations):
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        with self.driver.session() as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    obj = self.prepare_node(org_obj)
                    line_num += 1
                    # Validate parent exist
                    if CASE_ID in obj:
                        case_id = obj[CASE_ID]
                        if not self.node_exists(session, CASE_NODE, CASE_ID, case_id):
                            self.log.error(
                                'Invalid data at line {}: Parent (:{} {{ {}: "{}" }}) does not exist!'.format(
                                    line_num, CASE_NODE, CASE_ID, case_id))
                            validation_failed = True
                            violations += 1
                            if max_violations and violations >= max_violations:
                                return False
                return not validation_failed

    # Validate all parents exist in a data (TSV/TXT) file
    def validate_parents_exist_in_file(self, file_name, max_violations):
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        with self.driver.session() as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    line_num += 1
                    obj = self.prepare_node(org_obj)
                    results = self.collect_relationships(obj, session, False, line_num, file_name)
                    relationships = results[RELATIONSHIPS]
                    provided_parents = results[PROVIDED_PARENTS]
                    if provided_parents > 0:
                        if len(relationships) == 0:
                            self.log.error('Invalid data at line {}: No parents found!'.format(line_num))
                            validation_failed = True
                            violations += 1
                            if max_violations and violations >= max_violations:
                                return False
                    else:
                        self.log.info('Line: {} - No parents found'.format(line_num))

        return not validation_failed

    def get_node_properties(self, obj):
        """
        Generate a node with only node properties from input data
        :param obj: input data object (dict), may contain parent pointers, relationship properties etc.
        :return: an object (dict) that only contains properties on this node
        """
        node = {}

        for key, value in obj.items():
            if is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue
            else:
                node[key] = value

        return node

    # Validate the field names
    def validate_field_name(self, file_name):
        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')

            row = next(reader)
            row = self.cleanup_node(row)
            row_prepare_node = self.prepare_node(row)
            parent_pointer = []
            for key in row_prepare_node.keys():
                if is_parent_pointer(key):
                    parent_pointer.append(key)
            error_list = []
            parent_error_list = []
            for key in row.keys():
                if key not in parent_pointer:
                    try:
                        if key not in self.schema.get_props_for_node(row['type']) and key != 'type':
                            error_list.append(key)
                    except:
                        error_list.append(key)
                else:
                    try:
                        if key.split('.')[1] not in self.schema.get_props_for_node(key.split('.')[0]):
                            parent_error_list.append(key)
                    except:
                        parent_error_list.append(key)
            if len(error_list) > 0:
                for error_field_name in error_list:
                    self.log.warning('Property: "{}" not found in data model'.format(error_field_name))
            if len(parent_error_list) > 0:
                for parent_error_field_name in parent_error_list:
                    self.log.error('Parent pointer: "{}" not found in data model'.format(parent_error_field_name))
                self.log.error('Parent pointer not found in the data model, abort loading!')
                return False
        return True

    # Validate file
    def validate_file(self, file_name, max_violations):
        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            self.log.info('Validating file "{}" ...'.format(file_name))
            reader = csv.DictReader(in_file, delimiter='\t')
            line_num = 1
            validation_failed = False
            violations = 0
            ids = {}
            if not self.validate_field_name(file_name):
                return False

            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                props = self.get_node_properties(obj)
                line_num += 1
                id_field = self.schema.get_id_field(obj)
                node_id = self.schema.get_id(obj)

                if node_id:
                    if node_id in ids:
                        if get_props_signature(props) != ids[node_id]['props']:
                            validation_failed = True
                            self.log.error(
                                f'Invalid data at line {line_num}: duplicate {id_field}: {node_id}, found in line: '
                                f'{", ".join(ids[node_id]["lines"])}')
                            
                            self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, ",".join(ids[node_id]["lines"]), id_field, node_id, DUPLICATE_ID]))
                            ids[node_id]['lines'].append(str(line_num))
                        else:
                            # Same ID exists in same file, but properties are also same, probably it's pointing same
                            # object to multiple parents
                            self.log.debug(
                                f'Duplicated data at line {line_num}: duplicate {id_field}: {node_id}, found in line: '
                                f'{", ".join(ids[node_id]["lines"])}')
                            self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, ",".join(ids[node_id]["lines"]), id_field, node_id, DUPLICATE_DATA]))
                    else:
                        ids[node_id] = {'props': get_props_signature(props), 'lines': [str(line_num)]}

                validate_result = self.schema.validate_node(obj[NODE_TYPE], obj)
                if not validate_result['result'] and not validate_result['warning']:
                    for msg in validate_result['messages']:
                        self.log.error('Invalid data at line {}: "{}"!'.format(line_num, msg))
                    for msg in validate_result['data_validation_messages']:
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), VALIDATION_DELIMITER.join(msg), INVALID_DATA]))
                    for msg in validate_result['relationship_validation_messages']:
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), VALIDATION_DELIMITER.join(msg), INVALID_RELATIONSHIP]))
                    validation_failed = True
                    violations += 1
                    if max_violations and violations >= max_violations:
                        return False
                elif not validate_result['result'] and validate_result['warning']:
                    for msg in validate_result['messages']:
                        self.log.warning('Invalid data at line {}: "{}"!'.format(line_num, msg))
                    for msg in validate_result['data_validation_messages']:
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), VALIDATION_DELIMITER.join(msg), INVALID_DATA]))
                    for msg in validate_result['relationship_validation_messages']:
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), VALIDATION_DELIMITER.join(msg), INVALID_RELATIONSHIP]))
            return not validation_failed

    def get_new_statement(self, node_type, obj):
        # statement is used to create current node
        prop_stmts = []

        for key in obj.keys():
            if key in excluded_fields:
                continue
            elif is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue

            prop_stmts.append('{0}: ${0}'.format(key))

        statement = 'CREATE (:{0} {{ {1} }})'.format(node_type, ' ,'.join(prop_stmts))
        return statement

    def get_upsert_statement(self, node_type, id_field, obj):
        # statement is used to create current node
        statement = ''
        prop_stmts = []

        for key in obj.keys():
            if key in excluded_fields:
                continue
            elif key == id_field:
                continue
            elif is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue

            prop_stmts.append('n.{0} = ${0}'.format(key))

        statement += 'MERGE (n:{0} {{ {1}: ${1} }})'.format(node_type, id_field)
        statement += ' ON CREATE ' + 'SET n.{} = datetime(), '.format(CREATED) + ' ,'.join(prop_stmts)
        statement += ' ON MATCH ' + 'SET n.{} = datetime(), '.format(UPDATED) + ' ,'.join(prop_stmts)
        return statement

    # Delete a node and children with no other parents recursively
    def delete_node(self, session, node):
        delete_queue = deque([node])
        node_deleted = 0
        relationship_deleted = 0
        while len(delete_queue) > 0:
            root = delete_queue.popleft()
            delete_queue.extend(self.get_children_with_single_parent(session, root))
            n_deleted, r_deleted = self.delete_single_node(session, root)
            node_deleted += n_deleted
            relationship_deleted += r_deleted
        return node_deleted, relationship_deleted

    # Return children of node without other parents
    def get_children_with_single_parent(self, session, node):
        node_type = node[NODE_TYPE]
        statement = 'MATCH (n:{0} {{ {1}: ${1} }})<--(m)'.format(node_type, self.schema.get_id_field(node))
        statement += ' WHERE NOT (n)<--(m)-->() RETURN m'
        result = session.run(statement, node)
        children = []
        for obj in result:
            children.append(self.get_node_from_result(obj, 'm'))
        return children

    @staticmethod
    def get_node_from_result(record, name):
        node = record.data()[name]
        result = dict(node.items())
        for label in record[0].labels:
            result[NODE_TYPE] = label
            break
        return result

    # Simple delete given node, and it's relationships
    def delete_single_node(self, session, node):
        node_type = node[NODE_TYPE]
        statement = 'MATCH (n:{0} {{ {1}: ${1} }}) detach delete n'.format(node_type, self.schema.get_id_field(node))
        result = session.run(statement, node)
        nodes_deleted = result.consume().counters.nodes_deleted
        self.nodes_deleted += nodes_deleted
        self.nodes_deleted_stat[node_type] = self.nodes_deleted_stat.get(node_type, 0) + nodes_deleted
        relationship_deleted = result.consume().counters.relationships_deleted
        self.relationships_deleted += relationship_deleted
        return nodes_deleted, relationship_deleted

    # load file
    def load_nodes(self, session, file_name, loading_mode, split=False):
        if loading_mode == NEW_MODE:
            action_word = 'Loading new'
        elif loading_mode == UPSERT_MODE:
            action_word = 'Loading'
        elif loading_mode == DELETE_MODE:
            action_word = 'Deleting'
        else:
            raise Exception('Wrong loading_mode: {}'.format(loading_mode))
        self.log.info('{} nodes from file: {}'.format(action_word, file_name))

        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            nodes_created = 0
            nodes_deleted = 0
            node_type = 'UNKNOWN'
            relationship_deleted = 0
            line_num = 1
            transaction_counter = 0

            # Use session in one transaction mode
            tx = session
            # Use transactions in split-transactions mode
            if split:
                tx = session.begin_transaction()

            for org_obj in reader:
                line_num += 1
                transaction_counter += 1
                obj = self.prepare_node(org_obj)
                node_type = obj[NODE_TYPE]

                node_id = self.schema.get_id(obj)
                if not node_id:
                    id_field = self.schema.get_id_field(obj)  # this is specifically for validation logging
                    if not id_field:
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), MISSING, MISSING, MISSING_ID_FIELD]))
                    else:
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), id_field, MISSING, MISSING_ID]))
                    raise Exception('Line:{}: No ids found!'.format(line_num))
                id_field = self.schema.get_id_field(obj)
                if loading_mode == UPSERT_MODE:
                    statement = self.get_upsert_statement(node_type, id_field, obj)
                elif loading_mode == NEW_MODE:
                    if self.node_exists(tx, node_type, id_field, node_id):
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), id_field, node_id, NODE_EXISTS]))
                        raise Exception(
                            'Line: {}: Node (:{} {{ {}: {} }}) exists! Abort loading!'.format(line_num, node_type,
                                                                                              id_field, node_id))
                    else:
                        statement = self.get_new_statement(node_type, obj)
                elif loading_mode == DELETE_MODE:
                    n_deleted, r_deleted = self.delete_node(tx, obj)
                    nodes_deleted += n_deleted
                    relationship_deleted += r_deleted
                else:
                    raise Exception('Wrong loading_mode: {}'.format(loading_mode))

                if loading_mode != DELETE_MODE:
                    result = tx.run(statement, obj)
                    count = result.consume().counters.nodes_created
                    self.nodes_created += count
                    nodes_created += count
                    self.nodes_stat[node_type] = self.nodes_stat.get(node_type, 0) + count
                # commit and restart a transaction when batch size reached
                if split and transaction_counter >= BATCH_SIZE:
                    tx.commit()
                    tx = session.begin_transaction()
                    self.log.info(f'{line_num - 1} rows loaded ...')
                    transaction_counter = 0
            # commit last transaction
            if split:
                tx.commit()

            if loading_mode == DELETE_MODE:
                self.log.info('{} node(s) deleted'.format(nodes_deleted))
                self.log.info('{} relationship(s) deleted'.format(relationship_deleted))
            else:
                self.log.info('{} (:{}) node(s) loaded'.format(nodes_created, node_type))

    def node_exists(self, session, label, prop, value):
        statement = 'MATCH (m:{0} {{ {1}: ${1} }}) return m'.format(label, prop)
        result = session.run(statement, {prop: value})
        count = len(result.data())
        if count > 1:
            self.log.warning('More than one nodes found! ')
        return count >= 1

    def collect_relationships(self, obj, session, create_intermediate_node, line_num, file_name):
        node_type = obj[NODE_TYPE]
        relationships = []
        int_node_created = 0
        provided_parents = 0
        relationship_properties = {}
        
        for key, value in obj.items():
            if is_parent_pointer(key):
                provided_parents += 1
                other_node, other_id = key.split('.')
                relationship = self.schema.get_relationship(node_type, other_node)
                if not isinstance(relationship, dict):
                    self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), key, MISSING, UNDEFINED_RELATIONSHIP]))
                    self.log.error('Line: {}: Relationship not found!'.format(line_num))
                    raise Exception('Undefined relationship, abort loading!')
                relationship_name = relationship[RELATIONSHIP_TYPE]
                multiplier = relationship[MULTIPLIER]
                if not relationship_name:
                    self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), key, MISSING, UNDEFINED_RELATIONSHIP]))
                    self.log.error('Line: {}: Relationship not found!'.format(line_num))
                    raise Exception('Undefined relationship, abort loading!')
                if not self.node_exists(session, other_node, other_id, value):
                    create_parent = False
                    if create_intermediate_node:
                        for plugin in self.plugins:
                            if plugin.should_run(other_node, MISSING_PARENT):
                                create_parent = True
                                if plugin.create_node(session, line_num, other_node, value, obj):
                                    int_node_created += 1
                                    relationships.append(
                                        {PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                         RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
                                else:
                                    self.log.error(
                                        'Line: {}: Could not create {} node automatically!'.format(line_num,
                                                                                                   other_node))
                    else:
                        self.log.warning(
                            'Line: {}: Parent node (:{} {{{}: "{}"}} not found in DB!'.format(line_num, other_node,
                                                                                              other_id,
                                                                                              value))
                    if not create_parent:
                        self.log.warning(
                            'Line: {}: Parent node (:{} {{{}: "{}"}} not found in DB!'.format(line_num, other_node,
                                                                                              other_id,
                                                                                              value))
                else:
                    if multiplier == ONE_TO_ONE and self.parent_already_has_child(session, node_type, obj,
                                                                                  relationship_name, other_node,
                                                                                  other_id, value):
                        self.log.error(
                            'Line: {}: one_to_one relationship failed, parent already has a child!'.format(line_num))
                    else:
                        relationships.append({PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                              RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
            elif self.schema.is_relationship_property(key):
                rel_name, prop_name = key.split(self.rel_prop_delimiter)
                if rel_name not in relationship_properties:
                    relationship_properties[rel_name] = {}
                relationship_properties[rel_name][prop_name] = value
        return {RELATIONSHIPS: relationships, INT_NODE_CREATED: int_node_created, PROVIDED_PARENTS: provided_parents,
                RELATIONSHIP_PROPS: relationship_properties}

    def parent_already_has_child(self, session, node_type, node, relationship_name, parent_type, parent_id_field,
                                 parent_id):
        statement = 'MATCH (n:{})-[r:{}]->(m:{} {{ {}: $parent_id }}) return n'.format(node_type, relationship_name,
                                                                                       parent_type, parent_id_field)
        result = session.run(statement, {"parent_id": parent_id})
        if result:
            child = result.single()
            if child:
                find_current_node_statement = 'MATCH (n:{0} {{ {1}: ${1} }}) return n'.format(node_type,
                                                                                              self.schema.get_id_field(
                                                                                                  node))
                current_node_result = session.run(find_current_node_statement, node)
                if current_node_result:
                    current_node = current_node_result.single()
                    return child[0].id != current_node[0].id
                else:
                    self.log.error('Could NOT find current node!')

        return False

    # Check if a relationship of same type exists, if so, return a statement which can delete it, otherwise return False
    def has_existing_relationship(self, session, node_type, node, relationship, count_same_parent=False):
        relationship_name = relationship[RELATIONSHIP_TYPE]
        parent_type = relationship[PARENT_TYPE]
        parent_id_field = relationship[PARENT_ID_FIELD]

        base_statement = 'MATCH (n:{0} {{ {1}: ${1} }})-[r:{2}]->(m:{3})'.format(node_type,
                                                                                 self.schema.get_id_field(node),
                                                                                 relationship_name, parent_type)
        statement = base_statement + ' return m.{} AS {}'.format(parent_id_field, PARENT_ID)
        result = session.run(statement, node)
        if result:
            old_parent = result.single()
            if old_parent:
                if count_same_parent:
                    del_statement = base_statement + ' delete r'
                    return del_statement
                else:
                    old_parent_id = old_parent[PARENT_ID]
                    if old_parent_id != relationship[PARENT_ID]:
                        self.log.warning('Old parent is different from new parent, delete relationship to old parent:'
                                         + ' (:{} {{ {}: "{}" }})!'.format(parent_type, parent_id_field, old_parent_id))
                        del_statement = base_statement + ' delete r'
                        return del_statement
        else:
            self.log.error('Remove old relationship failed: Query old relationship failed!')

        return False

    def remove_old_relationship(self, session, node_type, node, relationship):
        del_statement = self.has_existing_relationship(session, node_type, node, relationship)
        if del_statement:
            del_result = session.run(del_statement, node)
            if not del_result:
                self.log.error('Delete old relationship failed!')

    def load_relationships(self, session, file_name, loading_mode, split=False):
        if loading_mode == NEW_MODE:
            action_word = 'Loading new'
        elif loading_mode == UPSERT_MODE:
            action_word = 'Loading'
        else:
            raise Exception('Wrong loading_mode: {}'.format(loading_mode))
        self.log.info('{} relationships from file: {}'.format(action_word, file_name))

        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            relationships_created = {}
            int_nodes_created = 0
            line_num = 1
            transaction_counter = 0

            # Use session in one transaction mode
            tx = session
            # Use transactions in split-transactions mode
            if split:
                tx = session.begin_transaction()
            for org_obj in reader:
                line_num += 1
                transaction_counter += 1
                obj = self.prepare_node(org_obj)
                node_type = obj[NODE_TYPE]
                results = self.collect_relationships(obj, tx, True, line_num, file_name)
                relationships = results[RELATIONSHIPS]
                int_nodes_created += results[INT_NODE_CREATED]
                provided_parents = results[PROVIDED_PARENTS]
                relationship_props = results[RELATIONSHIP_PROPS]
                if provided_parents > 0:
                    if len(relationships) == 0:
                        # why would there be defined relationships without concrete ones; parent probably doesn't exist in the database
                        self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), "!PARENT RELATIONSHIPS!", ",".join([obj[x] for x in obj.keys() if is_parent_pointer(x)]), f"{provided_parents} parent relationships should exist, none do."]))
                        raise Exception('Line: {}: No parents found, abort loading!'.format(line_num))
                    for relationship in relationships:
                        relationship_name = relationship[RELATIONSHIP_TYPE]
                        multiplier = relationship[MULTIPLIER]
                        parent_node = relationship[PARENT_TYPE]
                        parent_id_field = relationship[PARENT_ID_FIELD]
                        parent_id = relationship[PARENT_ID]
                        properties = relationship_props.get(relationship_name, {})
                        if multiplier in [DEFAULT_MULTIPLIER, ONE_TO_ONE]:
                            if loading_mode == UPSERT_MODE:
                                self.remove_old_relationship(tx, node_type, obj, relationship)
                            elif loading_mode == NEW_MODE:
                                if self.has_existing_relationship(tx, node_type, obj, relationship, True):
                                    self.validation_log.log(VALIDATION_ERROR, VALIDATION_DELIMITER.join([file_name, str(line_num), parent_id_field, parent_id, RELATIONSHIP_EXISTS]))
                                    raise Exception(
                                        'Line: {}: Relationship already exists, abort loading!'.format(line_num))
                            else:
                                raise Exception('Wrong loading_mode: {}'.format(loading_mode))
                        else:
                            self.log.debug('Multiplier: {}, no action needed!'.format(multiplier))
                        prop_statement = ', '.join(self.get_relationship_prop_statements(properties))
                        statement = 'MATCH (m:{0} {{ {1}: $__parentID__ }})'.format(parent_node, parent_id_field)
                        statement += ' MATCH (n:{0} {{ {1}: ${1} }})'.format(node_type,
                                                                             self.schema.get_id_field(obj))
                        statement += ' MERGE (n)-[r:{}]->(m)'.format(relationship_name)
                        statement += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
                        statement += ', {}'.format(prop_statement) if prop_statement else ''
                        statement += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)
                        statement += ', {}'.format(prop_statement) if prop_statement else ''

                        result = tx.run(statement, {**obj, "__parentID__": parent_id, **properties})
                        count = result.consume().counters.relationships_created
                        self.relationships_created += count
                        relationship_pattern = '(:{})->[:{}]->(:{})'.format(node_type, relationship_name, parent_node)
                        relationships_created[relationship_pattern] = relationships_created.get(relationship_pattern,
                                                                                                0) + count
                        self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name,
                                                                                                 0) + count
                    for plugin in self.plugins:
                        if plugin.should_run(node_type, NODE_LOADED):
                            if plugin.create_node(session=tx, line_num=line_num, src=obj):
                                int_nodes_created += 1
                # commit and restart a transaction when batch size reached
                if split and transaction_counter >= BATCH_SIZE:
                    tx.commit()
                    tx = session.begin_transaction()
                    self.log.info(f'{line_num - 1} rows loaded ...')
                    transaction_counter = 0

            # commit last transaction
            if split:
                tx.commit()
            if provided_parents == 0:
                    self.log.warning('there is no parent mapping columns in the node {}'.format(node_type))
            for rel, count in relationships_created.items():
                self.log.info('{} {} relationship(s) loaded'.format(count, rel))
            if int_nodes_created > 0:
                self.log.info('{} intermediate node(s) loaded'.format(int_nodes_created))

        return True

    @staticmethod
    def get_relationship_prop_statements(props):
        prop_stmts = []

        for key in props:
            prop_stmts.append('r.{0} = ${0}'.format(key))
        return prop_stmts

    def wipe_db(self, session, split=False):
        if split:
            return self.wipe_db_split(session)
        else:
            cleanup_db = 'MATCH (n) DETACH DELETE n'
            result = session.run(cleanup_db).consume()
            self.nodes_deleted = result.counters.nodes_deleted
            self.relationships_deleted = result.counters.relationships_deleted
            self.log.info('{} nodes deleted!'.format(self.nodes_deleted))
            self.log.info('{} relationships deleted!'.format(self.relationships_deleted))

    def wipe_db_split(self, session):
        while True:
            tx = session.begin_transaction()
            try:
                cleanup_db = f'MATCH (n) WITH n LIMIT {BATCH_SIZE} DETACH DELETE n'
                result = tx.run(cleanup_db).consume()
                tx.commit()
                deleted_nodes = result.counters.nodes_deleted
                self.nodes_deleted += deleted_nodes
                deleted_relationships = result.counters.relationships_deleted
                self.relationships_deleted += deleted_relationships
                self.log.info(f'{deleted_nodes} nodes deleted...')
                self.log.info(f'{deleted_relationships} relationships deleted...')
                if deleted_nodes == 0 and deleted_relationships == 0:
                    break
            except Exception as e:
                tx.rollback()
                self.log.exception(e)
                raise e
        self.log.info('{} nodes deleted!'.format(self.nodes_deleted))
        self.log.info('{} relationships deleted!'.format(self.relationships_deleted))

    def create_indexes(self, session):
        """
        Creates indexes, if they do not already exist, for all entries in the "id_fields" and "indexes" sections of the
        properties file
        :param session: the current neo4j transaction session
        """
        existing = get_btree_indexes(session)
        # Create indexes from "id_fields" section of the properties file
        ids = self.schema.props.id_fields
        for node_name in ids:
            self.create_index(node_name, ids[node_name], existing, session)
        # Create indexes from "indexes" section of the properties file
        indexes = self.schema.props.indexes
        # each index is a dictionary, indexes is a list of these dictionaries
        # for each dictionary in list
        for node_dict in indexes:
            node_name = list(node_dict.keys())[0]
            self.create_index(node_name, node_dict[node_name], existing, session)

    def create_index(self, node_name, node_property, existing, session):
        index_tuple = format_as_tuple(node_name, node_property)
        # If node_property is a list of properties, convert to a comma delimited string
        if isinstance(node_property, list):
            node_property = ",".join(node_property)
        if index_tuple not in existing:
            command = "CREATE INDEX ON :{}({});".format(node_name, node_property)
            session.run(command)
            self.indexes_created += 1
            self.log.info("Index created for \"{}\" on property \"{}\"".format(node_name, node_property))