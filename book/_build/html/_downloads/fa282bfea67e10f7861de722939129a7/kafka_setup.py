"""Task 3: Kafka Topic Design.

Creates the four topics that carry the CPG Parser Service's event
categories. Partition counts are chosen for the expected write volume:
node/edge events dominate (one message per AST node/edge across 543 files),
so they get more partitions than the lower-volume metadata/error topics.
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = "localhost:29092"

TOPIC_SPECS = [
    {"name": "cpg.nodes", "partitions": 3, "replication_factor": 1,
     "description": "AST node events emitted by the Parser Service"},
    {"name": "cpg.edges", "partitions": 3, "replication_factor": 1,
     "description": "CFG / DFG / CALL edge events emitted by the Parser Service"},
    {"name": "cpg.metadata", "partitions": 1, "replication_factor": 1,
     "description": "Per-file source metadata events (consumed by the Spark job)"},
    {"name": "cpg.errors", "partitions": 1, "replication_factor": 1,
     "description": "Parser error events (syntax errors, unreadable files)"},
]


def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="cpg-topic-admin")
    new_topics = [
        NewTopic(name=t["name"], num_partitions=t["partitions"], replication_factor=t["replication_factor"])
        for t in TOPIC_SPECS
    ]
    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        print("Created topics:", [t.name for t in new_topics])
    except TopicAlreadyExistsError:
        print("Some topics already exist; skipping creation for those.")
        for t in new_topics:
            try:
                admin.create_topics(new_topics=[t], validate_only=False)
                print(f"  created {t.name}")
            except TopicAlreadyExistsError:
                print(f"  already exists: {t.name}")
    finally:
        admin.close()


def list_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="cpg-topic-admin")
    topics = admin.list_topics()
    admin.close()
    return topics


if __name__ == "__main__":
    create_topics()
    print("\nCurrent topics on broker:")
    for name in sorted(list_topics()):
        print(" ", name)
