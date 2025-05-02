from fastapi import FastAPI, HTTPException, Query
from influxdb_client import InfluxDBClient
from datetime import datetime
from typing import Optional, List, Union, Dict
from pydantic import BaseModel
import statistics

INFLUXDB_URL = "http://localhost:8086"
BUCKET_TOKEN = "AfXeKdMKMZUK1QFbkf283YLQDAghSS5LYblxxHJyAJm2cNeoYOYqr0AdjO-qgZZsNv8Jqoj-4qeBTNRpm33-4Q=="
INFLUXDB_ORG = "gp2"
INFLUXDB_BUCKET = "gp2"

app = FastAPI()

client = InfluxDBClient(url=INFLUXDB_URL, token=BUCKET_TOKEN, org=INFLUXDB_ORG)


class ThermalZoneData(BaseModel):
    zone_id: str
    time: datetime
    mean_air_temperature: Optional[float] = None
    operative_temperature: Optional[float] = None
    air_relative_humidity: Optional[float] = None
    air_co2_concentration: Optional[float] = None
    infiltration_air_change_rate: Optional[float] = None
    mech_ventilation_air_changes: Optional[float] = None
    internal_latent_gain: Optional[float] = None
    cooling_rate: Optional[float] = None
    heating_rate: Optional[float] = None
    people_sensible_heat: Optional[float] = None
    thermal_comfort_pmv: Optional[float] = None
    thermal_comfort_ppd: Optional[float] = None


class SiteMetricsData(BaseModel):
    time: datetime
    interior_lights_electricity: Optional[float] = None
    facility_electricity: Optional[float] = None
    outdoor_air_temp: Optional[float] = None
    diffuse_solar_radiation: Optional[float] = None
    direct_solar_radiation: Optional[float] = None


class TemperatureReading(BaseModel):
    zone_id: Optional[str] = None
    temperature: Optional[float] = None


class TimestepTemperature(BaseModel):
    time: datetime
    outdoor_temp: Optional[float] = None
    indoor_temps: Union[Dict[str, float], float, None]


def format_time(dt: Optional[Union[datetime, str]]) -> str:
    """Format datetime for InfluxDB query"""
    if dt is None:
        return ""

    if isinstance(dt, str):
        # Handle both 'Z' and '+00:00' timezone formats
        if dt.endswith('Z'):
            dt = dt[:-1] + '+00:00'
        dt = datetime.fromisoformat(dt)

    return dt.isoformat().replace('+00:00', 'Z')


def build_thermal_zone_query(zone_id: Optional[str], start_time: str, end_time: str) -> str:
    zone_filter = f'r.zone_id == "{zone_id}"' if zone_id else 'true'
    return f"""
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: {start_time}, stop: {end_time})
      |> filter(fn: (r) => r._measurement == "thermal_zone" and {zone_filter})
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> keep(columns: ["_time", "zone_id", "mean_air_temperature", "operative_temperature", 
                        "air_relative_humidity", "air_co2_concentration", 
                        "infiltration_air_change_rate", "mech_ventilation_air_changes", 
                        "internal_latent_gain", "cooling_rate", "heating_rate", 
                        "people_sensible_heat", "thermal_comfort_pmv", "thermal_comfort_ppd"])
    """


def build_site_metrics_query(start_time: str, end_time: str) -> str:
    return f"""
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: {start_time}, stop: {end_time})
      |> filter(fn: (r) => r._measurement == "site_metrics")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> keep(columns: ["_time", "interior_lights_electricity", "facility_electricity", 
                        "outdoor_air_temp", "diffuse_solar_radiation", "direct_solar_radiation"])
    """


def process_thermal_zone_data(tables) -> List[ThermalZoneData]:
    data = []
    for table in tables:
        for row in table.records:
            data.append(ThermalZoneData(
                zone_id=row.values["zone_id"],
                time=row.values["_time"],
                mean_air_temperature=row.values.get("mean_air_temperature"),
                operative_temperature=row.values.get("operative_temperature"),
                air_relative_humidity=row.values.get("air_relative_humidity"),
                air_co2_concentration=row.values.get("air_co2_concentration"),
                infiltration_air_change_rate=row.values.get("infiltration_air_change_rate"),
                mech_ventilation_air_changes=row.values.get("mech_ventilation_air_changes"),
                internal_latent_gain=row.values.get("internal_latent_gain"),
                cooling_rate=row.values.get("cooling_rate"),
                heating_rate=row.values.get("heating_rate"),
                people_sensible_heat=row.values.get("people_sensible_heat"),
                thermal_comfort_pmv=row.values.get("thermal_comfort_pmv"),
                thermal_comfort_ppd=row.values.get("thermal_comfort_ppd")
            ))
    return data


def process_site_metrics_data(tables) -> List[SiteMetricsData]:
    data = []
    for table in tables:
        for row in table.records:
            data.append(SiteMetricsData(
                time=row.values["_time"],
                interior_lights_electricity=row.values.get("interior_lights_electricity"),
                facility_electricity=row.values.get("facility_electricity"),
                outdoor_air_temp=row.values.get("outdoor_air_temp"),
                diffuse_solar_radiation=row.values.get("diffuse_solar_radiation"),
                direct_solar_radiation=row.values.get("direct_solar_radiation")
            ))
    return data


@app.get("/data/", response_model=Union[
    List[ThermalZoneData], List[SiteMetricsData], List[Union[ThermalZoneData, SiteMetricsData]]])
async def get_data(
        data_type: Optional[str] = None,
        zone_id: Optional[str] = None,
        start_time: Optional[Union[datetime, str]] = None,
        end_time: Optional[Union[datetime, str]] = None
):
    """
    Retrieve data from InfluxDB with optional filters.
    """
    query_api = client.query_api()

    try:
        # Handle time range
        range_start = format_time(start_time) if start_time else "0"
        range_stop = format_time(end_time) if end_time else "now()"

        thermal_zone_data = []
        site_metrics_data = []

        if data_type == "thermal_zone" or data_type is None:
            thermal_zone_query = build_thermal_zone_query(zone_id, range_start, range_stop)
            thermal_zone_tables = query_api.query(thermal_zone_query)
            thermal_zone_data = process_thermal_zone_data(thermal_zone_tables)

            if data_type == "thermal_zone":
                return thermal_zone_data

        if data_type == "site_metrics" or data_type is None:
            site_metrics_query = build_site_metrics_query(range_start, range_stop)
            site_metrics_tables = query_api.query(site_metrics_query)
            site_metrics_data = process_site_metrics_data(site_metrics_tables)

            if data_type == "site_metrics":
                return site_metrics_data

        return thermal_zone_data + site_metrics_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/temperatures/", response_model=List[TimestepTemperature])
async def get_temperatures(
        zone_id: Optional[str] = None,
        start_time: Optional[Union[datetime, str]] = None,
        end_time: Optional[Union[datetime, str]] = None,
        aggregate: Optional[bool] = Query(
            False,
            description="If True, returns mean of all thermal zones. If False, returns individual zone temperatures"
        )
):
    """
    Retrieve organized temperature data with clear timestep structure.
    """
    query_api = client.query_api()

    try:
        # Handle time range
        range_start = format_time(start_time) if start_time else "0"
        range_stop = format_time(end_time) if end_time else "now()"

        # Query outdoor temperatures
        outdoor_query = build_site_metrics_query(range_start, range_stop)
        outdoor_results = query_api.query(outdoor_query)
        outdoor_temps = {record.values["_time"]: record.values["_value"]
                         for table in outdoor_results
                         for record in table.records
                         if record.get_field() == "outdoor_air_temp"}

        # Query indoor temperatures
        zone_filter = f'r.zone_id == "{zone_id}"' if zone_id else 'true'
        indoor_query = f"""
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: {range_start}, stop: {range_stop})
          |> filter(fn: (r) => r._measurement == "thermal_zone" and {zone_filter})
          |> filter(fn: (r) => r._field == "mean_air_temperature")
          |> keep(columns: ["_time", "_value", "zone_id"])
        """
        indoor_results = query_api.query(indoor_query)

        # Process results
        indoor_temps_by_time = {}
        for table in indoor_results:
            for record in table.records:
                timestamp = record.values["_time"]
                if timestamp not in indoor_temps_by_time:
                    indoor_temps_by_time[timestamp] = {}
                indoor_temps_by_time[timestamp][record.values.get("zone_id")] = record.values["_value"]

        # Build response
        response_data = []
        all_timestamps = set(outdoor_temps.keys()).union(set(indoor_temps_by_time.keys()))

        for timestamp in sorted(all_timestamps):
            outdoor_temp = outdoor_temps.get(timestamp)
            indoor_data = None

            if timestamp in indoor_temps_by_time:
                if aggregate:
                    temps = list(indoor_temps_by_time[timestamp].values())
                    indoor_data = float(statistics.mean(temps)) if temps else None
                else:
                    indoor_data = indoor_temps_by_time[timestamp]

            response_data.append(TimestepTemperature(
                time=timestamp,
                outdoor_temp=outdoor_temp,
                indoor_temps=indoor_data
            ))

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/data/")
async def delete_all_data():
    """
    Delete all data from the InfluxDB bucket.
    """
    try:
        delete_api = client.delete_api()
        delete_api.delete(
            start="1970-01-01T00:00:00Z",
            stop="2100-01-01T00:00:00Z",
            predicate='_measurement="thermal_zone" or _measurement="site_metrics"',
            bucket=INFLUXDB_BUCKET,
            org=INFLUXDB_ORG
        )
        return {"message": "All data has been deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
