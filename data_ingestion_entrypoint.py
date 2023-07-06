import sys
import argparse

from loader import main as main_neo4j_loader
from es_loader import main as main_opensearch_loader

ENTRYPOINT_OPTIONS = ["both", "opensearch", "neo4j"]

BEGIN_ENTRYPOINT_ARGS = 1
BEGIN_NEO4J_ARGS = BEGIN_ENTRYPOINT_ARGS + 1
BEGIN_OPENSEARCH_ARGS = -2

if __name__ == "__main__":
    # data_ingestion_entrypoint.py [args for the entrypoint script=1total] [args for neo4j=?total] [args for opensearch=2total]
    args = sys.argv

    entrypoint_args = args[BEGIN_ENTRYPOINT_ARGS:BEGIN_NEO4J_ARGS]
    parser = argparse.ArgumentParser(description='Invoke the Neo4j and/or OpenSearch data loaders.')
    parser.add_argument('option',
                        choices=ENTRYPOINT_OPTIONS,
                        help="Please specify whether to load into Neo4j('neo4j'), OpenSearch('opensearch'), or 'both'. ")
    optional = parser.parse_args(entrypoint_args)
    option = optional.option

    if option == "neo4j" or option == "both":
        neo4j_args = args[BEGIN_NEO4J_ARGS:BEGIN_OPENSEARCH_ARGS]
        main_neo4j_loader(args=neo4j_args)
    if option == "opensearch" or option == "both":
        opensearch_args = args[BEGIN_OPENSEARCH_ARGS:]
        main_opensearch_loader(args=opensearch_args)
