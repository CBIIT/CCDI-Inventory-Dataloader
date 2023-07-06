from loader import main as main_neo4j_loader
from es_loader import main as main_opensearch_loader

if __name__ == "__main__":
    main_neo4j_loader()
    main_opensearch_loader()
