"""
Stub out heavy runtime deps (confluent_kafka, psycopg2) before job modules are
imported. Tests only exercise pure computation functions — no Kafka, no DB.
"""

import sys
from unittest.mock import MagicMock

# Stub confluent_kafka so job modules can be imported without librdkafka installed
_kafka_mod = MagicMock()
_kafka_mod.Consumer = MagicMock
_kafka_mod.KafkaError = MagicMock
_kafka_mod.KafkaError._PARTITION_EOF = -191
sys.modules.setdefault("confluent_kafka", _kafka_mod)

# Stub psycopg2 (used by timescale_sink)
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
