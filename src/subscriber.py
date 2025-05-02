import time
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from src.constants import BUCKET_TOKEN, INFLUXDB_ORG, MQTT_BROKER, MQTT_PORT, INFLUXDB_URL
from src.mqtt_classes.mqtt_subscriber import MQTTSubscriber


class InfluxDBStorage:
    def __init__(self):
        self.influx_client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=BUCKET_TOKEN,
            org=INFLUXDB_ORG,
            timeout=30_000
            # debug=True
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

        self.mqtt_subscriber = MQTTSubscriber(
            broker_address=MQTT_BROKER,
            broker_port=MQTT_PORT,
            client_id='influxdb_writer'
        )

        self.mqtt_subscriber.set_on_message_callback(self.on_message_received)
        self.mqtt_subscriber.set_on_connect_callback(self.on_connect)

    def on_connect(self, client, userdata, flags, rc):
        """Subscribe to topics when connected"""
        self.mqtt_subscriber.subscribe("building/thermal_zones_metrics")
        self.mqtt_subscriber.subscribe("building/site_metrics")

    def on_message_received(self, client, userdata, message):
        """Handle incoming MQTT messages and write to InfluxDB"""

        try:
            payload = json.loads(message.payload.decode("utf-8"))

            if message.topic == "building/thermal_zones_metrics":
                self.write_thermal_zone_data(payload)
            elif message.topic == "building/site_metrics":
                self.write_site_metrics_data(payload)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")

    def write_thermal_zone_data(self, data):
        """Write thermal zone data to InfluxDB"""
        try:
            point = Point(data["measurement"]) \
                .tag("zone_id", data["tags"]["zone_id"]) \
                .time(data["time"], WritePrecision.NS)

            for field, value in data["fields"].items():
                point.field(field, round(float(value), 4))

            response = self.write_api.write(bucket="gp2", record=point)
            print(f"Written thermal zone data!")
        except Exception as e:
            print(f"Error writing thermal zone data: {e}")

    def write_site_metrics_data(self, data):
        """Write site metrics data to InfluxDB"""
        try:
            point = Point(data["measurement"]) \
                .time(data["time"], WritePrecision.NS)

            for field, value in data["fields"].items():
                point.field(field, round(float(value), 4))

            self.write_api.write(bucket="gp2", record=point)
            print("Written site metrics data!")
        except Exception as e:
            print(f"Error writing site metrics: {e}")

    def start(self):
        """Start the MQTT subscriber and flush bucket"""
        self.mqtt_subscriber.connect()

    def stop(self):
        """Clean up resources"""
        self.mqtt_subscriber.disconnect()
        self.write_api.close()
        self.influx_client.close()
        print("Clean shutdown complete")


if __name__ == "__main__":
    storage = InfluxDBStorage()
    try:
        storage.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        storage.stop()
