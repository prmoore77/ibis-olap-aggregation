import ibis
import pandas as pd

# Manually added code section
# -----------------------------------

# Setup ibis
ibis.options.interactive = False
ibis.options.repr.query_text_length = 255

# Setup pandas
pd.set_option("display.width", 0)
pd.set_option("display.max_columns", 99)
pd.set_option("display.max_colwidth", None)

connection = ibis.duckdb.connect('./data/grocery_store.duckdb')

sales_facts = connection.table('sales_facts')
product_aggregation_dim = connection.table('product_aggregation_dim')
lpad = ibis.literal('-').lpad(((product_aggregation_dim.ancestor_level_number - 1) * 7), '-')


# -----------------------------------


decimal = "!decimal(18, 3)"
lit = ibis.literal("-")
# sales_facts = ibis.table(
#     name="sales_facts",
#     schema={
#         "product_id": "!int32",
#         "customer_id": "!string",
#         "date_id": "!date",
#         "unit_quantity": "!decimal(18, 3)",
#         "sales_amount": "!decimal(18, 3)",
#     },
# )
# product_aggregation_dim = ibis.table(
#     name="product_aggregation_dim",
#     schema={
#         "ancestor_node_id": "string",
#         "ancestor_node_natural_key": "int32",
#         "ancestor_node_name": "string",
#         "ancestor_level_name": "string",
#         "ancestor_is_root": "boolean",
#         "ancestor_is_leaf": "boolean",
#         "ancestor_level_number": "int32",
#         "ancestor_node_sort_order": "int64",
#         "descendant_node_id": "string",
#         "descendant_node_natural_key": "int32",
#         "descendant_node_name": "string",
#         "descendant_level_name": "string",
#         "descendant_is_root": "boolean",
#         "descendant_is_leaf": "boolean",
#         "descendant_level_number": "int32",
#         "descendant_node_sort_order": "int64",
#         "net_level": "int32",
#     },
# )

# Removed this line b/c of error:
# lpad = lit.l_pad(
#     length=(product_aggregation_dim.ancestor_level_number - 1) * 7, pad=lit
# )

# Changed: "lpad.string_concat" to: "lpad.concat" in this code...
proj = product_aggregation_dim.select(
    [
        product_aggregation_dim,
        lpad.concat(product_aggregation_dim.ancestor_level_name).name(
            "product_level_name"
        ),
        lpad.concat(product_aggregation_dim.ancestor_node_name).name(
            "product_node_name"
        ),
    ]
)
innerjoin = sales_facts.inner_join(
    proj, sales_facts.product_id == proj.descendant_node_natural_key
)
agg = innerjoin.group_by(
    [
        innerjoin.product_node_name,
        innerjoin.product_level_name,
        innerjoin.ancestor_node_sort_order,
    ]
).aggregate(
    [
        innerjoin.sales_amount.sum().name("sum_sales_amount"),
        innerjoin.unit_quantity.sum().name("sum_unit_quantity"),
        innerjoin.customer_id.nunique().name("distinct_customer_count"),
        innerjoin.count().name("count_of_fact_records"),
    ]
)
proj1 = agg.order_by(agg.ancestor_node_sort_order.asc())

result = proj1.select(
    [
        proj1.product_node_name,
        proj1.product_level_name,
        proj1.sum_sales_amount,
        proj1.sum_unit_quantity,
        proj1.distinct_customer_count,
        proj1.count_of_fact_records,
    ]
)

# Added a print of the output
print(result.execute())
