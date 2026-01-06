from pathlib import Path

BASE_DIR = Path("/opt/airflow")

RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

RAW_FILE = RAW_DIR / "India Agriculture Crop Production.csv"
BRONZE_FILE = BRONZE_DIR / "crop_production_bronze.csv"
SILVER_FILE = SILVER_DIR / "crop_production_silver.csv"
GOLD_FILE = GOLD_DIR / "crop_production_gold.csv"


CSV_ENCODING = "utf-8"