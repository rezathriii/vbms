import json
import csv
from src.mqtt_classes.mqtt_publisher import MQTTPublisher
from src.constants import FILTERED_DATA_CSV, MQTT_PORT, MQTT_BROKER
from src.utils import parse_datetime


class PublishMetrics:
    def __init__(self, csv_path, mqtt_broker='localhost', mqtt_port=1883):
        """
        Initialize CSV to MQTT converter
        :param csv_path: Path to the CSV file
        :param mqtt_broker: MQTT broker address
        :param mqtt_port: MQTT broker port
        """
        self.csv_path = csv_path
        self.mqtt_publisher = MQTTPublisher(broker_address=mqtt_broker, broker_port=mqtt_port)
        self.mqtt_publisher.connect()

    def process_csv(self):
        """
        Process the CSV file and publish data to MQTT
        """
        with open(self.csv_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                try:
                    dt_obj = parse_datetime(row['DateTime'])
                    timestamp = dt_obj.isoformat() + 'Z'
                except ValueError as e:
                    print(f"Error parsing DateTime '{row['DateTime']}': {e}")
                    continue

                for block in ['BLOCK1', 'BLOCK2']:
                    for zone in ['OFFICEXSW', 'OFFICEXSE', 'OFFICEXNW', 'OFFICEXNE', 'CORRIDOR']:
                        for floor in ['X1F', 'X2F']:
                            zone_prefix = f"{block}:{zone}{floor}"

                            temp_key = f"{zone_prefix}:Zone Mean Air Temperature"
                            if temp_key not in row:
                                continue

                            payload = {
                                "measurement": "thermal_zone",
                                "tags": {
                                    "zone_id": f"{block}:{zone}:{floor}"
                                },
                                "time": timestamp,
                                "fields": {
                                    "mean_air_temperature": float(row[f"{zone_prefix}:Zone Mean Air Temperature"]),
                                    "operative_temperature": float(row[f"{zone_prefix}:Zone Operative Temperature"]),
                                    "air_relative_humidity": float(row[f"{zone_prefix}:Zone Air Relative Humidity"]),
                                    "air_co2_concentration": float(row[f"{zone_prefix}:Zone Air CO2 Concentration"]),
                                    "infiltration_air_change_rate": float(
                                        row[f"{zone_prefix}:Zone Infiltration Air Change Rate"]),
                                    "mech_ventilation_air_changes": float(
                                        row[f"{zone_prefix}:Zone Mechanical Ventilation Air Changes per Hour"]),
                                    "internal_latent_gain": float(
                                        row[f"{zone_prefix}:Zone Total Internal Latent Gain Energy"]),
                                    "cooling_rate": float(row[
                                                              f"{zone_prefix} IDEAL LOADS AIR:Zone Ideal Loads Supply Air Total Cooling Rate"]),
                                    "heating_rate": float(row[
                                                              f"{zone_prefix} IDEAL LOADS AIR:Zone Ideal Loads Supply Air Total Heating Rate"]),
                                    "people_sensible_heat": float(
                                        row[f"{zone_prefix}:Zone People Sensible Heating Rate"]),
                                    "thermal_comfort_pmv": float(
                                        row[f"PEOPLE {zone_prefix}:Zone Thermal Comfort Fanger Model PMV"]),
                                    "thermal_comfort_ppd": float(
                                        row[f"PEOPLE {zone_prefix}:Zone Thermal Comfort Fanger Model PPD"])
                                }
                            }

                            topic = "building/thermal_zones_metrics"
                            self.mqtt_publisher.publish(topic, json.dumps(payload))

                site_payload = {
                    "measurement": "site_metrics",
                    "time": timestamp,
                    "fields": {
                        "interior_lights_electricity": float(row["InteriorLights:Electricity"]),
                        "facility_electricity": float(row["Electricity:Facility"]),
                        "outdoor_air_temp": float(row["Site Site Outdoor Air Drybulb Temperature"]),
                        "diffuse_solar_radiation": float(row["Site Site Diffuse Solar Radiation Rate per Area"]),
                        "direct_solar_radiation": float(row["Site Site Direct Solar Radiation Rate per Area"])
                    }
                }
                self.mqtt_publisher.publish("building/site_metrics", json.dumps(site_payload))

            print("Simulation Complete!!!")

    def shutdown(self):
        """Clean up resources"""
        self.mqtt_publisher.disconnect()


if __name__ == "__main__":
    processor = PublishMetrics(FILTERED_DATA_CSV, MQTT_BROKER, MQTT_PORT)
    try:
        processor.process_csv()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        processor.shutdown()
