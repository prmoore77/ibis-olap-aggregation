import ibis
from ibis import _
import pandas as pd
import sqlparse
from classes.hierarchy_dimension_table import HierarchyDimension
from config import logger

# Setup ibis
ibis.options.interactive = False
ibis.options.repr.query_text_length = 255

# Setup pandas
pd.set_option("display.width", 0)
pd.set_option("display.max_columns", 99)
pd.set_option("display.max_colwidth", None)


def main():
    connection = ibis.duckdb.connect('./data/grocery_store.duckdb')
    logger.info(msg=connection.list_tables())

    products = HierarchyDimension(dimension_name="product",
                                  ibis_expr=connection.table('product_nodes'),
                                  node_id_column_name="node_id",
                                  parent_node_id_column_name="parent_node_id"
                                  ).aggregation_dim_ibis_expr
    facts = connection.table('sales_facts')

    level_indent_pad = ibis.literal('-').lpad(((products.ancestor_level_number - 1) * 7), '-')
    product_level_name = level_indent_pad.concat(products.ancestor_level_name)
    product_node_name = level_indent_pad.concat(products.ancestor_node_name)

    products = products.mutate(product_level_name=product_level_name,
                               product_node_name=product_node_name
                               )
    sales_fact_aggregation = \
        (facts.join(products, predicates=(facts.product_id == products.descendant_node_natural_key))
         .group_by([_.product_node_name,
                    _.product_level_name,
                    _.ancestor_node_sort_order
                    ]
                   )
         .aggregate(sum_sales_amount=_.sales_amount.sum(),
                    sum_unit_quantity=_.unit_quantity.sum(),
                    distinct_customer_count=_.customer_id.nunique(),
                    count_of_fact_records=_.count()
                    )
         .order_by(_.ancestor_node_sort_order)
         )[
            'product_node_name',
            'product_level_name',
            'sum_sales_amount',
            'sum_unit_quantity',
            'distinct_customer_count',
            'count_of_fact_records'
        ]
    # Format/Print out the SQL - and replace binds with literals (for easy copy/paste)
    print(sqlparse.format(sql=str(sales_fact_aggregation.compile().compile(compile_kwargs={"literal_binds": True})),
                          reindent=True,
                          keyword_case='upper',
                          identifier_case='lower',
                          use_space_around_operators=True,
                          comma_first=True
                          )
          )

    df = sales_fact_aggregation.execute()
    print(df)

    # Try decompiling the expression to Ibis code...
    code = ibis.decompile(sales_fact_aggregation, format=True)
    print(code)


if __name__ == '__main__':
    main()
