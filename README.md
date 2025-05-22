# Data Engineering Challenge – Spark Purchase Aggregator

##  Overview

The goal is to process transactional data from CSV, clean and filter it, perform aggregations, and write the result as Parquet to cloud storage.


---

## Challenge Requirements (Summary)

- **Read** CSV input containing: `user_id`, `user_name`, `email`, `purchase_datetime`, `purchase_amount`
- **Clean**: Drop rows with nulls in critical fields
- **Transform**: Convert date to timestamp, filter to 2023 range
- **Aggregate**: Sum purchase amount per user
- **Write** output in Parquet format
- **Test**: Unit tests for valid, invalid, and edge cases




## Project Structure

```bash
.
├── jobs/
│   └── purchase_transform.py     # Entry point script using CLI args
├── src/
│   ├── __init__.py
│   └── data_processing.py        # Core processing logic (clean, filter, aggregate)
├── tests/
│   ├── __init__.py
│   └── test_process.py           # PySpark unit tests using mock data
├── README.md                     # Project documentation
└── requirements.txt              # Python + PySpark dependencies
