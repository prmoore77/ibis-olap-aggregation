import os
import sys
import tempfile
import ibis
import logging
import pandas as pd

# Setup logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

# Setup ibis
ibis.options.interactive = False
ibis.options.repr.query_text_length = 255

# Setup pandas
pd.set_option("display.width", 0)
pd.set_option("display.max_columns", 99)
pd.set_option("display.max_colwidth", -1)


def main():
    connection = ibis.duckdb.connect('./data/grocery_store.duckdb')
    logger.info(msg=connection.list_tables())

    facts = connection.table('sales_facts')
    products = connection.table('product_aggregation_dim')

    level_indent_pad = ibis.literal('-').lpad(((products.ancestor_level_number - 1) * 5), '-')
    product_level_name = level_indent_pad.concat(products.ancestor_level_name)
    product_node_name = level_indent_pad.concat(products.ancestor_node_name)

    products = products.mutate(product_level_name=product_level_name,
                               product_node_name=product_node_name
                               )
    sales_fact_aggregation = \
    (facts.join(products, predicates=(facts.product_id == products.descendant_node_natural_key))
     .group_by([products.product_node_name,
                products.product_level_name,
                products.ancestor_level_number,
                products.ancestor_node_sort_order
                ]
               )
     .aggregate(sum_sales_amount=facts.sales_amount.sum(),
                sum_unit_quantity=facts.unit_quantity.sum(),
                distinct_customer_count=facts.customer_id.nunique(),
                count_of_fact_records=facts.count()
                )
     .sort_by(products.ancestor_node_sort_order)
     )[
        'product_node_name', 'product_level_name', 'sum_sales_amount', 'sum_unit_quantity', 'distinct_customer_count', 'count_of_fact_records']
    print(sales_fact_aggregation.compile())
    df = sales_fact_aggregation.execute()
    print(df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
