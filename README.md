# ibis-olap-aggregation
Demo of Ibis for OLAP-style hierarchical aggregation

# Setup

## Install requirements
Create a new Python 3.8+ virtual environment - and install the requirements with:
```shell
pip install -r requirements.txt
```

## Source Data
To get the source DuckDB database - run this from the root of the repo:

```shell
python -m get_data
```

# Running the aggregation
```shell
python -m main
```
