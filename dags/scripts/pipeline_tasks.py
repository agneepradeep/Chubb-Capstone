import pandas as pd
from pathlib import Path

from scripts.pipeline_config import (
    RAW_FILE,
    BRONZE_FILE,
    SILVER_FILE,
    RAW_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    CSV_ENCODING
)

# --------------------------------------------------
# Utility: ensure directory exists
# --------------------------------------------------

def ensure_directory(path: Path) -> None:
    """
    Create Directory if it does not exist.
    """
    path.mkdir(parents=True,exist_ok=True)

# --------------------------------------------------
# Task 1: Ingest raw data -> Bronze layer
# --------------------------------------------------

def ingest_raw_to_bronze() -> None:
    """
    Reads raw crop production CSV and writes it to Bronze layer with minimal transformation.
    """

    print("Starting raw to bronze ingestion...")

    # 1. Validate raw file exists
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw file not found at {RAW_FILE}")
    
    # 2. Ensure bronze directory exists
    ensure_directory(BRONZE_DIR)

    # 3. Read raw CSV
    df_raw = pd.read_csv(RAW_FILE, encoding = CSV_ENCODING)

    print(f"Raw records count: {len(df_raw)}")

    # 4. Column Name Standardization
    df_raw.columns = (
        df_raw.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # 5. Write to bronze
    df_raw.to_csv(BRONZE_FILE, index=False, encoding=CSV_ENCODING)

    print(f"Bronze data written to {BRONZE_FILE}")
    print("Raw to bronze ingestion completed successfully.")

# --------------------------------------------------
# Task 2: Bronze -> Silver transformation
# --------------------------------------------------

def transform_bronze_to_silver() -> None:
    """
    Cleans and standardizes bronze data to create silver layer.
    """

    print("Starting bronze to silver transfomration...")

    # 1. Validate bronze file exists
    if not BRONZE_FILE.exists():
        raise FileNotFoundError(f"Bronze file not found at {BRONZE_FILE}")
    
    # 2. Ensure silver directory exists
    ensure_directory(SILVER_DIR)

    # 3. Read bronze data
    df = pd.read_csv(BRONZE_FILE, encoding= CSV_ENCODING)

    print(f"Bronze records count: {len(df)}")

    # --------------------------------------------------
    # 4. Data quality filters (Spark-equivalent filters)
    # --------------------------------------------------

    df = df[
        df["crop"].notna() &
        (df["crop"].str.strip().str.len() > 0) &
        df["area"].notna() &
        (df["area"] > 0) &
        df["production"].notna() &
        (df["production"] >= 0) &
        df["yield"].notna() &
        (df["yield"] >= 0)
    ]

    print(f"Records after quality filtering: {len(df)}")

    # --------------------------------------------------
    # 5. Year parsing (year_start, year_end)
    # --------------------------------------------------

    df["year_start"] = df["year"].str.split("-").str[0].astype(int)
    df["year_end"] = df["year_start"] + 1

    df= df.rename(columns={"year":"year_label"})

    # --------------------------------------------------
    # 6. Standardize string columns (Title Case)
    # --------------------------------------------------

    string_cols = ["state", "district", "crop", "season"]

    for col in string_cols:
        df[col] = df[col].str.strip().str.title()

    # --------------------------------------------------
    # 7. Recalculate yield (consistent precision)
    # --------------------------------------------------

    df["yield"] = (df["production"] / df["area"]).round(4)

    # --------------------------------------------------
    # 8. Write Silver data
    # --------------------------------------------------

    df.to_csv(SILVER_FILE, index=False, encoding=CSV_ENCODING)

    print(f"Silver data written to {SILVER_FILE}")
    print("Bronze to silver transformation completed successfully.")

# --------------------------------------------------
# Task 3: Prepare Silver -> Gold Layer
# --------------------------------------------------
def transform_silver_to_gold() -> None:
    """
    Builds Gold layer star schema (dimensions + fact)
    from Silver layer data.
    """

    print("Starting silver to gold transformation...")

    # --------------------------------------------------
    # 1. Validate silver file exists
    # --------------------------------------------------
    if not SILVER_FILE.exists():
        raise FileNotFoundError(f"Silver file not found at {SILVER_FILE}")

    ensure_directory(GOLD_DIR)

    # --------------------------------------------------
    # 2. Read silver data
    # --------------------------------------------------
    df_silver = pd.read_csv(SILVER_FILE, encoding=CSV_ENCODING)

    print(f"Silver records count: {len(df_silver)}")

    # --------------------------------------------------
    # 3. Dimension tables
    # --------------------------------------------------

    # dim_crop
    dim_crop = (
        df_silver[["crop"]]
        .drop_duplicates()
        .rename(columns={"crop": "crop_name"})
        .reset_index(drop=True)
    )
    dim_crop["crop_id"] = dim_crop.index.astype(int)
    dim_crop = dim_crop[["crop_id", "crop_name"]]

    # dim_region
    dim_region = (
        df_silver[["state", "district"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_region["region_id"] = dim_region.index.astype(int)
    dim_region = dim_region[["region_id", "state", "district"]]

    # dim_season
    dim_season = (
        df_silver[["season"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_season["season_id"] = dim_season.index.astype(int)
    dim_season = dim_season[["season_id", "season"]]

    # dim_time
    dim_time = (
        df_silver[["year_start", "year_end", "year_label"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_time["time_id"] = dim_time.index.astype(int)
    dim_time = dim_time[["time_id", "year_start", "year_end", "year_label"]]

    # --------------------------------------------------
    # 4. Fact table
    # --------------------------------------------------
    df_fact = df_silver.merge(
        dim_crop,
        left_on="crop",
        right_on="crop_name",
        how="inner"
    )

    df_fact = df_fact.merge(
        dim_region,
        on=["state", "district"],
        how="inner"
    )

    df_fact = df_fact.merge(
        dim_season,
        on="season",
        how="inner"
    )

    df_fact = df_fact.merge(
        dim_time,
        on=["year_start", "year_end"],
        how="inner"
    )

    fact_crop_production = df_fact[
        [
            "crop_id",
            "region_id",
            "season_id",
            "time_id",
            "area",
            "area_units",
            "production",
            "production_units",
            "yield"
        ]
    ]

    # --------------------------------------------------
    # 5. Write Gold outputs
    # --------------------------------------------------
    dim_crop.to_csv(GOLD_DIR / "dim_crop.csv", index=False)
    dim_region.to_csv(GOLD_DIR / "dim_region.csv", index=False)
    dim_season.to_csv(GOLD_DIR / "dim_season.csv", index=False)
    dim_time.to_csv(GOLD_DIR / "dim_time.csv", index=False)
    fact_crop_production.to_csv(
        GOLD_DIR / "fact_crop_production.csv",
        index=False
    )

    print("Gold layer tables written successfully.")
    print("Silver to gold transformation completed.")
