import re
import json
import statistics
import pandas as pd
from typing import Optional, List
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from src.constants import EPLUSOUT_CSV_PATH


def extract_zone_temperatures(
        input_path: str = EPLUSOUT_CSV_PATH,
        output_path: Optional[str] = "output/zone_temperatures.csv",
        temperature_keyword: str = "Zone Mean Air Temperature",
        time_column: str = "Date/Time",
        rename_zones: bool = True,
        verbose: bool = True
) -> pd.DataFrame:
    """
    Extracts zone temperature data from EnergyPlus output CSV.

    Parameters:
    -----------
    input_path : str
        Path to EnergyPlus output CSV (e.g., 'eplusout.csv')
    output_path : Optional[str]
        Path to save extracted temperatures (None to skip saving)
    temperature_keyword : str
        Keyword to identify temperature columns
    time_column : str
        Name of the time column in the input CSV
    rename_zones : bool
        Whether to simplify zone names (removes ':Zone...')
    verbose : bool
        Whether to print the head of the resulting DataFrame

    Returns:
    --------
    pd.DataFrame
        DataFrame containing DateTime and zone temperatures
    """
    df = pd.read_csv(input_path)

    temp_cols = [col for col in df.columns if temperature_keyword in col]

    if not temp_cols:
        raise ValueError(f"No columns found containing '{temperature_keyword}'")

    zone_temps = df[[time_column] + temp_cols].copy()

    if rename_zones:
        zone_temps.columns = [time_column] + [col.split(":Zone")[0] for col in temp_cols]

    if output_path:
        zone_temps.to_csv(output_path, index=False)
        if verbose:
            print(f"Zone temperatures are extracted successfully and saved to {output_path}")

    return zone_temps


def clean_column_name(col: str) -> str:
    """Clean and standardize column names by removing units and reporting frequency"""
    col = re.sub(r'\[.*]', '', col)
    col = re.sub(r'\(.*\)', '', col)
    return col.strip().strip(':')


def extract_specific_outputs(
        input_path: str = EPLUSOUT_CSV_PATH,
        output_path: str = "output/filtered_eplus_results.csv",
        target_variables: List[str] = None,
        verbose: bool = True
) -> pd.DataFrame:
    """
    Extracts specific outputs from EnergyPlus CSV and creates a structured output.

    Parameters:
        input_path (str): Path to EnergyPlus output CSV
        output_path (str): Path to save filtered output
        target_variables (List[str]): List of variables to extract
        verbose (bool): Whether to print progress messages

    Returns:
        pd.DataFrame: The filtered DataFrame with only requested variables
    """
    if target_variables is None:
        target_variables = [
            "Zone Mean Air Temperature",
            "Zone Operative Temperature",
            "Zone Air Relative Humidity",
            "Zone Air CO2 Concentration",
            "Zone Infiltration Air Change Rate",
            "Zone Mechanical Ventilation Air Changes per Hour",
            "Zone Total Internal Latent Gain Energy",
            "Zone Ideal Loads Supply Air Total Cooling Rate",
            "Zone Ideal Loads Supply Air Total Heating Rate",
            "Zone People Sensible Heating Rate",
            "InteriorLights:Electricity",
            "Electricity:Facility",
            "Site Outdoor Air Drybulb Temperature",
            "Site Diffuse Solar Radiation Rate per Area",
            "Site Direct Solar Radiation Rate per Area",
            "Zone Thermal Comfort Fanger Model PMV",
            "Zone Thermal Comfort Fanger Model PPD"
        ]

    if verbose:
        print(f"Loading data from {input_path}...")

    df = pd.read_csv(input_path)

    if verbose:
        print(f"Original data has {len(df.columns)} columns and {len(df)} timesteps")

    # =====================================================================
    # 1. Find matching columns
    # =====================================================================
    if verbose:
        print("Finding matching columns for requested variables...")

    selected_columns = ["Date/Time"]

    for var in target_variables:
        if var == "InteriorLights:Electricity":
            pattern = r"InteriorLights:Electricity.*"
        elif var == "Electricity:Facility":
            pattern = r"Electricity:Facility.*"
        else:
            pattern = r".*" + re.escape(var) + r".*"

        matches = [col for col in df.columns if re.fullmatch(pattern, col, re.IGNORECASE)]

        if not matches:
            print(f"Warning: No matches found for variable: {var}")
        else:
            selected_columns.extend(matches)

    # =====================================================================
    # 2. Filter the DataFrame
    # =====================================================================
    if verbose:
        print(f"Filtering data to {len(selected_columns)} selected columns...")

    filtered_df = df[selected_columns]

    # =====================================================================
    # 3. Clean column names
    # =====================================================================
    if verbose:
        print("Cleaning column names...")

    column_mapping = {}
    for col in filtered_df.columns:
        if col == "Date/Time":
            column_mapping[col] = "DateTime"
        else:
            clean_name = clean_column_name(col)

            if "Environment:" in clean_name:
                clean_name = clean_name.replace("Environment:", "Site ")

            column_mapping[col] = clean_name

    filtered_df = filtered_df.rename(columns=column_mapping)

    # =====================================================================
    # 4. Save to CSV
    # =====================================================================
    if output_path:
        filtered_df.to_csv(output_path, index=False)
        if verbose:
            print(f"Saved filtered data to {output_path}")
            print(f"Final dimensions: {len(filtered_df.columns)} columns, {len(filtered_df)} timesteps")

    return filtered_df


def parse_datetime(dt_str):
    """Parse datetime string handling 24:00:00 special case"""
    dt_str = dt_str.strip()
    try:
        dt_obj = datetime.strptime(dt_str, '%m/%d %H:%M:%S')
    except ValueError as e:
        if dt_str.endswith('24:00:00'):
            date_part = dt_str.split()[0]
            dt_obj = datetime.strptime(date_part, '%m/%d') + timedelta(days=1)
        else:
            raise e
    return dt_obj.replace(year=2005)  # Set the fixed year


def export_temperature_data_to_json():
    """
    Export all temperature data from InfluxDB to two JSON files:
    - output/temperatures_aggregated.json (mean temperatures)
    - output/temperatures_by_zone.json (individual zone temperatures)
    """

    INFLUXDB_URL = "http://localhost:8086"
    BUCKET_TOKEN = "AfXeKdMKMZUK1QFbkf283YLQDAghSS5LYblxxHJyAJm2cNeoYOYqr0AdjO-qgZZsNv8Jqoj-4qeBTNRpm33-4Q=="
    INFLUXDB_ORG = "gp2"
    INFLUXDB_BUCKET = "gp2"

    client = InfluxDBClient(url=INFLUXDB_URL, token=BUCKET_TOKEN, org=INFLUXDB_ORG)

    query_api = client.query_api()

    try:
        # Query all outdoor temperatures
        outdoor_query = f"""
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: 0, stop: now())
          |> filter(fn: (r) => r._measurement == "site_metrics")
          |> filter(fn: (r) => r._field == "outdoor_air_temp")
          |> keep(columns: ["_time", "_value"])
        """
        outdoor_results = query_api.query(outdoor_query)
        outdoor_temps = {record.values["_time"]: record.values["_value"]
                         for table in outdoor_results
                         for record in table.records}

        # Query all indoor temperatures
        indoor_query = f"""
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: 0, stop: now())
          |> filter(fn: (r) => r._measurement == "thermal_zone")
          |> filter(fn: (r) => r._field == "mean_air_temperature")
          |> keep(columns: ["_time", "_value", "zone_id"])
        """
        indoor_results = query_api.query(indoor_query)

        indoor_temps_by_time = {}
        for table in indoor_results:
            for record in table.records:
                timestamp = record.values["_time"]
                if timestamp not in indoor_temps_by_time:
                    indoor_temps_by_time[timestamp] = {}
                indoor_temps_by_time[timestamp][record.values.get("zone_id")] = record.values["_value"]

        all_timestamps = set(outdoor_temps.keys()).union(set(indoor_temps_by_time.keys()))
        sorted_timestamps = sorted(all_timestamps)

        aggregated_data = []
        by_zone_data = []

        for timestamp in sorted_timestamps:
            entry = {
                "time": timestamp.isoformat(),
                "outdoor_temp": outdoor_temps.get(timestamp)
            }

            if timestamp in indoor_temps_by_time:
                by_zone_entry = entry.copy()
                by_zone_entry["indoor_temps"] = indoor_temps_by_time[timestamp]
                by_zone_data.append(by_zone_entry)
            else:
                by_zone_entry = entry.copy()
                by_zone_entry["indoor_temps"] = None
                by_zone_data.append(by_zone_entry)

            if timestamp in indoor_temps_by_time:
                temps = list(indoor_temps_by_time[timestamp].values())
                aggregated_entry = entry.copy()
                aggregated_entry["indoor_temps"] = float(statistics.mean(temps)) if temps else None
                aggregated_data.append(aggregated_entry)
            else:
                aggregated_entry = entry.copy()
                aggregated_entry["indoor_temps"] = None
                aggregated_data.append(aggregated_entry)

        with open("output/temperatures_aggregated.json", "w") as f:
            json.dump(aggregated_data, f, indent=4)

        with open("output/temperatures_by_zone.json", "w") as f:
            json.dump(by_zone_data, f, indent=4)

        print("Successfully exported temperature data to:")
        print("- output/temperatures_aggregated.json")
        print("- output/temperatures_by_zone.json")

    except Exception as e:
        print(f"Error exporting temperature data: {str(e)}")
