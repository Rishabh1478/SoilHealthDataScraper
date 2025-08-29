# ScrapeSoilData

This repository contains a Python scraper class `ScrapeSoilData` for extracting **macro** and **micro nutrient soil data** from the [Soil Health Dashboard](https://soilhealth4.dac.gov.in/).  
It queries the portal’s GraphQL API, processes the responses into structured data, and saves them as CSV files organized by **year → state → district → block**.

---

## Features

- Scrapes soil health data for multiple years (default: `2023-24`, `2024-25`, `2025-26`).
- Extracts both **macro** (N, P, K, S, etc.) and **micro** (Fe, Mn, Zn, Cu, etc.) nutrient tables.
- Converts API JSON responses into structured **Pandas DataFrames**.
- Saves data into CSV files per block:
  - `*_macro.csv` for macro nutrients
  - `*_micro.csv` for micro nutrients
- Handles cases with no available data by creating placeholder `.txt` and empty `.csv` files.
- Runs scraping in **parallel** using `ThreadPoolExecutor` for faster execution.
- Gracefully skips errors while continuing with the rest of the dataset.

---

## Directory Structure

After running the scraper, data is saved under:

```
data/
 └── raw/
      ├── 2023-24/
      │    └── <state>/<district>/<block>_macro.csv
      │    └── <state>/<district>/<block>_micro.csv
      ├── 2024-25/
      │    └── ...
      └── 2025-26/
           └── ...
```

If a block has **no data**, files like this will be created instead:

```
no_rows_for_<block>_macro.txt
no_rows_for_<block>_micro.txt
```

---

## Requirements

Install dependencies using:

```bash
pip install -r requirements.txt
```

### `requirements.txt`
```txt
beautifulsoup4
pandas
requests
```

*(The script also uses Python’s built-in `os`, `concurrent.futures`.)*

---

## Usage

Run the scraper with:

```bash
python scrape_soil_data.py
```

This will:

1. Fetch all states → districts → blocks from the Soil Health Dashboard.
2. For each block in each year, request nutrient data (macro & micro).
3. Save the results into CSVs inside the `data/raw/` directory.

---

## Example

Running the script may log messages like:

```
Starting Scraper Task!

{'info': 'Working on 2023-24-Uttar Pradesh-Agra-Bah'}
{'status': 'Done with 2023-24-Uttar Pradesh-Agra-Bah'}
{'status': 'Done with 2023-24-Uttar Pradesh-Agra'}
{'status': 'Done with 2023-24-Uttar Pradesh'}
```

---

## Data Columns

The CSVs will contain columns such as:

- `village`
- `Nitrogen High`, `Nitrogen Medium`, `Nitrogen Low`
- `Phosphorus High`, `Phosphorus Medium`, `Phosphorus Low`
- `Iron Sufficient`, `Iron Deficient`
- `pH Neutral`, `pH Acidic`, `pH Alkaline`
- And other nutrients from the script’s `NUTRIENT_MAP`.

---

## Error Handling

- If any API request fails or a parsing issue occurs, the scraper logs the error and moves to the next state/district/block.
- If files already exist, they are skipped to avoid redundant downloads.

---

## Notes

- Data is directly fetched from the **GraphQL API** at `https://soilhealth4.dac.gov.in/`.
- The scraper uses a **fake browser-like User-Agent** header to mimic normal requests.
- Make sure you have a stable internet connection since the dataset is large.

---

## License

This project is for educational and research purposes.  
Please ensure compliance with the [Soil Health Dashboard’s](https://soilhealth4.dac.gov.in/) terms of use before scraping large datasets.
