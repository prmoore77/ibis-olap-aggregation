# ibis-olap-aggregation
Demo of Ibis for OLAP-style hierarchical aggregation.

This is an attempt to use Ibis for the SQL aggregation approach detailed [here](https://medium.com/@philipmoore_53699/olap-hierarchical-aggregation-with-sql-6c45ebc206d7).

# Setup

## Install requirements
Create a new Python 3.8+ virtual environment - and install the requirements with:
```shell
pip install -r requirements.txt
```

## Source Data
To create the sample DuckDB database - run this from the root of the repo:

```shell
python -m create_database
```

# Running the aggregation
```shell
python -m main
```
