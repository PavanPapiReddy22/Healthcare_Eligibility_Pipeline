# Healthcare Eligibility Data Pipeline

## Overview
This project implements a scalable, configuration-driven data pipeline designed to ingest healthcare member eligibility files from various partners. The pipeline handles multiple file formats (CSV, Pipe-delimited, JSON, Parquet, ORC) and transforms them into a unified, standardized schema for downstream consumption.

## Project Structure
```text
healthcare_pipeline/
├── config/             
│   └── partners.json   # Central configuration for all partner mappings
├── data/               # Source data directory
├── src/                
│   ├── pipeline.py     # Core ETL logic (PySpark)
│   └── create_sample_data.py # Utility to generate test datasets
├── .gitignore          # Git exclusion rules
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation
```

## Prerequisites
- **Python 3.8+**
- **Java 8 or 11** (Required for PySpark)
- **PySpark**: Install via requirements file:
  ```bash
  pip install -r requirements.txt
  ```

## How to Run
1. **Generate Sample Data**:
   The assessment requires specific data files. You can generate the sample datasets (including Acme Health, Better Care, and additional binary formats) by running:
   ```bash
   python3 src/create_sample_data.py
   ```
   *Note: Ensure you are in the project root directory or have the correct `PYTHONPATH`.*

2. **Execute the Pipeline**:
   Run the main pipeline script to process and unify all configured partners:
   ```bash
   python3 src/pipeline.py
   ```

## Standardized Output Schema
The pipeline applies the following transformations to ensure data quality and consistency:
- `external_id`: Mapped from partner-specific unique ID field.
- `first_name` & `last_name`: Converted to **Title Case**.
- `dob`: Formatted as **ISO-8601 (YYYY-MM-DD)**.
- `email`: Converted to **lowercase**.
- `phone`: Standardized to **XXX-XXX-XXXX**.
- `partner_code`: Hardcoded identifier based on the configuration key.

## Partner Onboarding Process
The pipeline is architected to be **zero-code** for onboarding. Adding a new partner requires only a configuration update.

### Onboarding Steps:
1. **Prepare the Data**: Place the new partner's source file into the `data/` directory.
2. **Configure Mappings**: Open `config/partners.json` and add a new JSON object for the partner.
3. **Run Pipeline**: Execute `python3 src/pipeline.py`. The new partner will be automatically picked up, transformed, and unified.

### Configuration Example:
To onboard a new partner named "CarePlus" providing a Tab-delimited file:
```json
"careplus": {
    "file_name": "careplus_data.tsv",
    "file_type": "delimited",
    "delimiter": "\t",
    "has_header": true,
    "input_date_format": "dd-MMM-yyyy",
    "mappings": {
        "Member_Number": "external_id",
        "Given_Name": "first_name",
        "Surname": "last_name",
        "Birthday": "dob",
        "Email_Primary": "email",
        "Cell": "phone"
    }
}
```

## Technical Design Decisions
- **PySpark**: Chosen for scalability and its native support for complex transformations and varied file formats (Parquet/ORC).
- **Centralized Configuration**: All partner-specific details (delimiters, date formats, column mappings) are abstracted into a JSON file, separating business logic from code.
- **Robustness**: 
    - **Validation**: Rows missing an `external_id` are automatically dropped and logged as warnings.
    - **Logging**: Uses the standard Python `logging` module for better observability in production environments.
    - **Standardization**: Uses Regex and Spark built-in functions to handle messy phone numbers and varied date formats.
