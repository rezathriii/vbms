import time
from src.constants import FILTERED_DATA_CSV, MQTT_BROKER, MQTT_PORT
from src.subscriber import InfluxDBStorage
from src.publisher import PublishMetrics
from threading import Thread


def run_subscriber():
    """Run the subscriber in a separate thread"""
    storage = InfluxDBStorage()
    try:
        storage.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSubscriber shutdown requested...")
    except Exception as e:
        print(f"Subscriber unexpected error: {e}")
    finally:
        storage.stop()


def run_publisher():
    """Run the publisher after a short delay"""
    time.sleep(1)

    processor = PublishMetrics(FILTERED_DATA_CSV, MQTT_BROKER, MQTT_PORT)
    try:
        processor.process_csv()
    except KeyboardInterrupt:
        print("\nPublisher shutting down...")
    finally:
        processor.shutdown()


if __name__ == "__main__":
    subscriber_thread = Thread(target=run_subscriber)
    subscriber_thread.daemon = True
    subscriber_thread.start()

    run_publisher()

    subscriber_thread.join()
