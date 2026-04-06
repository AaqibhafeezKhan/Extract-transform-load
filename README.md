# Extract-Transform-Load (ETL) Flow Studio

A high-performance suite of automated data extraction and transformation pipelines. This repository features a professional, light-themed engineering workstation for bridging the gap between raw data streams and high-fidelity reports.

## Project Objective

To demonstrate modern data engineering practices across varied ETL workflows. The ETL Flow Studio focuses on three core principles:

-   **Dynamic Extraction**: Real-time fetching from external APIs (e.g., Open-Meteo) and user-supplied CSV files.
-   **Intelligent Transformation**: Data cleaning, geocoding, categorical mapping, and mean imputation.
-   **Reliable Loading**: Immediate generation and download of structured, high-fidelity Excel (.xlsx) reports via SheetJS.

## Features

### 1. Data Workstation
A single-screen, non-scrolling dashboard (100vh) that allows users to select, configure, and execute data pipelines in real-time.

### 2. Live Data Snapshot
Integrated preview panel that renders a real-time table of extracted and transformed data before the final load phase, providing immediate visibility into the processing logic.

### 3. Integrated Engine Logs
A terminal-style console that streams live status updates from the ETL engine, tracking extraction success, transformation steps, and file generation.

## Featured Pipelines

### Weather Bridge (weather_mumbai.py)
-   **Extract**: Fetches coordinates for any city globally (default: Mumbai) and extracts 7-day hourly forecast indices from the Open-Meteo API.
-   **Transform**: Categorizes temperatures into Cold, Mild, and Hot states while normalizing timestamps.
-   **Load**: Generates a high-fidelity Excel report for local analysis.

### Data Segmenter (customer_segmenter.py)
-   **Extract**: Prioritizes user-uploaded CSV source streams with a local dataset fallback.
-   **Transform**: Executes mean age imputation and customer tier segmentation (e.g., VIP) based on transaction volume.
-   **Load**: Produces a structured analytics report.

### Finance Link (finance_link.py)
-   **Extract**: Aggregates temporal transaction records from log files.
-   **Transform**: Normalizes currency formats and calculates totalizers for revenue reporting.
-   **Load**: Generates an audit-ready financial summary.

## Repository Architecture

```
etls/
├── api-to-excel-weather/                # Weather Forecast Pipeline (API -> Excel)
│   └── weather_etl.py                   # Python implementation for weather processing
│
├── csv-to-excel-sales/                  # Sales & Customer Pipelines (CSV -> Excel)
│   ├── data/
│   │   ├── raw/                         # Local raw input fallbacks
│   │   │   ├── customers.csv
│   │   │   └── sales.csv
│   │   └── processed/                   # Output storage
│   ├── customer_etl.py                  # Python implementation for customer records
│   └── sales_etl.py                     # Python implementation for sales logs
│
├── index.html                           # Flow Studio Workstation UI
├── style.css                            # Professional Light-Theme Design System
└── README.md                            # Documentation
```

## Technology Stack

-   **Engine**: JavaScript (ES6+), SheetJS (XLSX Processing)
-   **Architecture**: Single Page Application (SPA), No-Scroll Viewport
-   **External Data**: Open-Meteo API (Meteorological Streams)
-   **Design**: Bespoke CSS Design System (Light Theme, Outfit & JetBrains Mono Typography)

## License

This project is licensed under the MIT License.
