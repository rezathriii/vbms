from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.resolve()

IDF_PATH = PROJECT_ROOT / 'src/input/opt_bldg.idf'
EPW_PATH = PROJECT_ROOT / 'src/input/paris_2005.epw'
EPLUSOUT_CSV_PATH = PROJECT_ROOT / 'output/eplusout.csv'
FILTERED_DATA_CSV = PROJECT_ROOT / 'output/filtered_eplus_results.csv'

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
BUCKET_TOKEN = "AfXeKdMKMZUK1QFbkf283YLQDAghSS5LYblxxHJyAJm2cNeoYOYqr0AdjO-qgZZsNv8Jqoj-4qeBTNRpm33-4Q=="
INFLUXDB_ORG = "gp2"
INFLUXDB_URL = "http://localhost:8086"
