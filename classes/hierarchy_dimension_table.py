import sqlalchemy.util
from ibis.expr.types import Expr
from config import logger
from sqlalchemy import (Table, Column, select, case, cast, func, literal_column, literal, subquery, JSON, bindparam,
                        sql, String, text)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable
import re


class CreateTableAs(Executable, ClauseElement):

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs, "duckdb")
def _create_table_as(element, compiler, **kw):
    sql = f"CREATE OR REPLACE TABLE {element.name} AS {compiler.process(element.query)}"
    logger.info(msg=sql)
    return sql


def _create_struct_literal(query, table_alias: str = None, override_dict: dict = dict()) -> str:
    return_str = "{"
    if table_alias:
        alias_str = f"{table_alias}."
    else:
        alias_str = ""

    for column in query.columns:
        if override_dict.get(column.name):
            value_str = override_dict.pop(column.name)
        else:
            value_str = alias_str + column.name

        return_str += f"{column.name}: {value_str}, "

    for key, value in override_dict.items():
        return_str += f"{key}: {value}, "

    return return_str.rstrip(' ,') + "}"


class HierarchyDimension(object):
    def __init__(self,
                 dimension_name: str,
                 ibis_expr: Expr,
                 node_id_column_name: str,
                 parent_node_id_column_name: str
                 ):
        self.dimension_name = dimension_name
        self.ibis_expr: Expr = ibis_expr
        # TODO: Try to find another way to get the backend without using a protected method
        self.ibis_connection = ibis_expr._find_backend()
        self.metadata = self.ibis_connection.meta
        self.sql_connection = self.metadata.bind.engine
        # TODO: try to find an unprotected attribute for this...
        self.source_table_name = self.ibis_expr._key[2].name
        self.source_table: Table = self.metadata.tables[self.source_table_name]
        self.node_id_column: Column = self.source_table.columns[node_id_column_name]
        self.parent_node_id_column: Column = self.source_table.columns[parent_node_id_column_name]

        self._nodes_query = self._get_nodes_query()
        self._reporting_dim_table_name = self._create_reporting_dim_table()
        self._reporting_dim_table = Table(self._reporting_dim_table_name, self.metadata,
                                          autoload_with=self.sql_connection)
        self._aggregation_dim_table_name = self._create_aggregation_dim_table()
        self._aggregation_dim_table = Table(self._aggregation_dim_table_name, self.metadata,
                                            autoload_with=self.sql_connection)

        # Ibis doesn't seem to like the struct type from DuckDB, so we can't use the reporting_dim_table yet...
        # self.reporting_dim_ibis_expr = self.ibis_connection.table(self._reporting_dim_table_name)

        self.aggregation_dim_ibis_expr = self.ibis_connection.table(self._aggregation_dim_table_name)

    def execute_ibis_sql(self, sql_text: str):
        logger.info(msg=f"Executing SQL: {sql_text}")
        return self.ibis_connection.raw_sql(query=sql_text)

    def _validate_columns(self):
        if self.node_id_column not in self.ibis_expr.columns:
            raise ValueError

        if self.parent_node_id_column not in self.ibis_expr.columns:
            raise ValueError

    def _get_nodes_query(self):
        nodes_query = \
            select(self.source_table,
                   case((self.parent_node_id_column.is_(None), True),
                        else_=False).label("is_root"),
                   case((self.node_id_column.in_(select(self.parent_node_id_column)), False),
                        else_=True
                        ).label("is_leaf")
                   )

        return nodes_query

    def _create_reporting_dim_table(self) -> str:
        if not hasattr(self, "_nodes_query"):
            raise RuntimeError("The nodes_query MUST be created to run this method!")

        anchor_node_json_literal_column_expr: str = _create_struct_literal(query=self._nodes_query, table_alias="nodes",
                                                                           override_dict=dict(level_number=1))

        recursive_node_json_literal_column_expr: str = _create_struct_literal(query=self._nodes_query,
                                                                              table_alias="nodes", override_dict=dict(
                level_number="(parent_nodes.level_number + 1)"))

        nodes_alias = self._nodes_query.alias("nodes")
        parent_nodes = select([column for column in nodes_alias.columns] +
                              [literal_column("1").label("level_number"),
                               literal_column(anchor_node_json_literal_column_expr).label("node_json"),
                               literal_column(f"[{anchor_node_json_literal_column_expr}]").label("node_json_path")
                               ]
                              ). \
            where(nodes_alias.c.is_root == True). \
            cte("parent_nodes", recursive=True)

        recursive_cte_query = parent_nodes.union_all(
            select(
                [column for column in nodes_alias.columns] +
                [(parent_nodes.c.level_number + literal_column("1")).label("level_number"),
                 literal_column(recursive_node_json_literal_column_expr).label("node_json"),
                 literal_column(
                     f"array_append(parent_nodes.node_json_path, {recursive_node_json_literal_column_expr})").label(
                     "node_json_path")
                 ]
            ).where(nodes_alias.c.parent_node_id == parent_nodes.c.node_id)
        )

        node_sort_order_query = select([column for column in recursive_cte_query.columns] +
                                       [func.row_number().over(
                                           order_by=func.replace(cast(recursive_cte_query.c.node_json_path, String),
                                                                 ']',
                                                                 '').asc()).label("node_sort_order")
                                        ]
                                       ).cte("node_sort_order_query")

        node_json_expr: str = _create_struct_literal(query=self._nodes_query, table_alias=None,
                                                     override_dict=dict(level_number="level_number",
                                                                        node_sort_order="node_sort_order"))

        # Add our level* columns
        level_columns = []
        for i in range(10):
            for column in node_sort_order_query.columns:
                if column.name not in ["node_json", "node_json_path", "is_root", "is_leaf", "node_sort_order"]:
                    level_columns.append(
                        func.struct_extract(func.list_extract(node_sort_order_query.c.node_json_path, (i + 1)),
                                            literal_column(f"'{column.name}'")).label(
                            f"level_{i + 1}_{column.name}")
                    )

        reporting_dim_query = select(
            [column for column in node_sort_order_query.columns if column.name not in ["node_json", "node_json_path"]] +
            [literal_column(node_json_expr).label("node_json")] +
            level_columns
            )

        reporting_dim_table_name = f"{self.dimension_name}_reporting_dim"
        self.sql_connection.execute(CreateTableAs(reporting_dim_table_name, reporting_dim_query))

        return reporting_dim_table_name

    def _create_aggregation_dim_table(self) -> str:
        if not self._reporting_dim_table_name:
            raise RuntimeError("The reporting_dim_table MUST be created to run this method!")

        reporting_dim_query = select(self._reporting_dim_table)

        nodes_alias = reporting_dim_query.alias("nodes")

        # Subset our column list...
        column_list = []
        for column in nodes_alias.columns:
            if column not in ['node_json', 'node_json_path'] and not re.search('^level_\d+_\S+$', column.name):
                column_list.append(column)

        # No Anchor filter here (we want every node to be an anchor in the aggregation dim)!
        parent_nodes = select(column_list +
                              [literal_column(f"[nodes.node_json]").label("node_json_path")]
                              ). \
            cte("parent_nodes", recursive=True)

        recursive_cte_query = parent_nodes.union_all(
            select(
                column_list +
                [literal_column(
                    f"array_append(parent_nodes.node_json_path, nodes.node_json)").label(
                    "node_json_path")
                ]
            ).where(nodes_alias.c.parent_node_id == parent_nodes.c.node_id)
        )

        ancestor_columns = []
        for column in recursive_cte_query.columns:
            if column.name not in ['node_json', 'node_json_path']:
                ancestor_columns.append(func.struct_extract(func.list_extract(recursive_cte_query.c.node_json_path, 1),
                                                            literal_column(f"'{column.name}'")
                                                            ).label(f"ancestor_{column.name}")
                                        )

        descendant_columns = []
        for column in recursive_cte_query.columns:
            if column.name not in ['node_json', 'node_json_path']:
                descendant_columns.append(column.label(f"descendant_{column.name}"))

        ancestor_descendant_query = select(ancestor_columns +
                                           descendant_columns
                                           ).cte()

        aggregation_dim_query = select(ancestor_descendant_query,
                                       (
                                                   ancestor_descendant_query.c.descendant_level_number - ancestor_descendant_query.c.ancestor_level_number).label(
                                           "net_level")
                                       )

        aggregation_dim_table_name = f"{self.dimension_name}_aggregation_dim"

        self.sql_connection.execute(CreateTableAs(aggregation_dim_table_name, aggregation_dim_query))
        return aggregation_dim_table_name
