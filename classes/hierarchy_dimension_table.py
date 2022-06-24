import sqlalchemy.util
from ibis.expr.types import Expr
from config import logger
from sqlalchemy import (Table, Column, select, case, cast, func, literal_column, literal, subquery, JSON, bindparam,
                        sql, String, text)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable


class CreateTableAs(Executable, ClauseElement):

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs, "duckdb")
def _create_table_as(element, compiler, **kw):
    sql = f"CREATE OR REPLACE TABLE {element.name} AS {compiler.process(element.query)}"
    logger.info(msg=sql)
    return sql


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
        self._aggregation_dim_table_name = self._create_aggregation_dim_table()

        # Ibis doesn't seem to like the struct type from DuckDB, so we can't use the reporting_dim_table yet...
        # self.reporting_dim_ibis_expr = self.connection.table(self._reporting_dim_table_name)

        self.aggregation_dim_ibis_expr = self.ibis_connection.table(self._aggregation_dim_table_name)

    def execute_ibis_sql(self, sql_text: str):
        logger.info(msg=f"Executing SQL: {sql_text}")
        return self.ibis_connection.raw_sql(query=sql_text)

    def _validate_columns(self):
        if self.node_id_column not in self.ibis_expr.columns:
            raise ValueError

        if self.parent_node_id_column not in self.ibis_expr.columns:
            raise ValueError

    def _get_nodes_query(self) -> str:
        nodes_query = \
            select(self.source_table,
                   case((self.parent_node_id_column.is_(None), True),
                        else_=False).label("is_root"),
                   case((self.node_id_column.in_(select(self.parent_node_id_column)), False),
                        else_=True
                        ).label("is_leaf")
                   )

        return nodes_query

    def _generate_level_column_sql(self) -> str:
        level_column_sql = ""
        for i in range(1, 11):
            level_column_sql += \
                (f"-- Level {i} columns\n"
                 f", node_json_path[{i}].{self.node_id_column}        AS level_{i}_{self.node_id_column}\n"
                 f", {', '.join(f'node_json_path[{i}].{column_name}   AS level_{i}_{column_name}' for column_name in self._attribute_column_names)}\n"
                 )
        return level_column_sql

    def _create_reporting_dim_table(self) -> str:
        if not hasattr(self, "_nodes_query"):
            raise RuntimeError("The nodes_query MUST be created to run this method!")

        anchor_node_json_literal_column_expr: str = ("{node_id: node_id,"
                                                     " node_natural_key: node_natural_key,"
                                                     " node_name: node_name,"
                                                     " level_name: level_name,"
                                                     " parent_node_id: parent_node_id,"
                                                     " is_root: is_root,"
                                                     " is_leaf: is_leaf,"
                                                     " level_number: 1}"
                                                     )
        recursive_node_json_literal_column_expr: str = ("{node_id: nodes.node_id,"
                                                        " node_natural_key: nodes.node_natural_key,"
                                                        " node_name: nodes.node_name,"
                                                        " level_name: nodes.level_name,"
                                                        " parent_node_id: nodes.parent_node_id,"
                                                        " is_root: nodes.is_root,"
                                                        " is_leaf: nodes.is_leaf,"
                                                        " level_number: (parent_nodes.level_number + 1)}"
                                                        )

        parent_nodes = select([column for column in self._nodes_query.columns] +
                              [literal_column("1").label("level_number"),
                               literal_column(anchor_node_json_literal_column_expr).label("node_json"),
                               literal_column(f"[{anchor_node_json_literal_column_expr}]").label("node_json_path")
                               ]
                              ). \
            where(self._nodes_query.c.is_root == True). \
            cte("parent_nodes", recursive=True)

        nodes_alias = self._nodes_query.alias("nodes")
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

        # Add our level* columns
        level_columns = []
        for i in range(10):
            for column in recursive_cte_query.columns:
                if column.name not in ["node_json", "node_json_path", "is_root", "is_leaf"]:
                    level_columns.append(
                        func.struct_extract(func.list_extract(recursive_cte_query.c.node_json_path, (i + 1)),
                                            literal_column(f"'{column.name}'")).label(
                            f"level_{i + 1}_{column.name}")
                    )

        reporting_dim_query = select([column for column in recursive_cte_query.columns] +
                                     [func.row_number().over(
                                         order_by=func.replace(cast(recursive_cte_query.c.node_json_path, String), ']',
                                                               '').asc()).label("node_sort_order")
                                      ] +
                                     level_columns
                                     )

        self.sql_connection.execute(CreateTableAs("larry", select(recursive_cte_query)))
        self.sql_connection.execute(CreateTableAs("larry_bird", reporting_dim_query))

        sql_text = (f"CREATE OR REPLACE TABLE {reporting_dim_table_name}\n"
                    f"AS\n"
                    f"WITH RECURSIVE parent_nodes (\n"
                    f"   {self.node_id_column}\n"
                    f" , {', '.join(column_name for column_name in self._attribute_columns)}\n"
                    f" , {self.parent_node_id_column}\n"
                    f" , is_root\n"
                    f" , is_leaf\n"
                    f" , level_number\n"
                    f" , node_json\n"
                    f" , node_json_path\n"
                    f")\n"
                    f"AS (--Anchor Clause\n"
                    f"    SELECT\n"
                    f"        {self.node_id_column}\n"
                    f"      , {', '.join(column_name for column_name in self._attribute_columns)}\n"
                    f"      , {self.parent_node_id_column}\n"
                    f"      , is_root\n"
                    f"      , is_leaf\n"
                    f"      , 1 AS level_number\n"
                    f"      , {{  {self.node_id_column}: {self.node_id_column}\n"
                    f"          , {', '.join(f'{column_name}: {column_name}' for column_name in self._attribute_columns)}\n"
                    f"          , is_root: is_root\n"
                    f"          , is_leaf: is_leaf\n"
                    f"          , level_number: 1 }} AS node_json\n"
                    f"      , [{{  {self.node_id_column}: {self.node_id_column}\n"
                    f"          , {', '.join(f'{column_name}: {column_name}' for column_name in self._attribute_columns)}\n"
                    f"          , is_root: is_root\n"
                    f"          , is_leaf: is_leaf\n"
                    f"          , level_number: 1 }}] AS node_json_path\n"
                    f"       FROM {self._nodes_temp_table_name}\n"
                    f"      WHERE is_root = TRUE\n"
                    f"    --\n"
                    f"    UNION ALL\n"
                    f"    -- Recursive Clause\n"
                    f"    SELECT\n"
                    f"        nodes.{self.node_id_column}\n"
                    f"      , {', '.join(f'nodes.{column_name}' for column_name in self._attribute_columns)}\n"
                    f"      , nodes.{self.parent_node_id_column}\n"
                    f"      , nodes.is_root\n"
                    f"      , nodes.is_leaf\n"
                    f"      , parent_nodes.level_number + 1 AS level_number\n"
                    f"      , {{  {self.node_id_column}: nodes.{self.node_id_column}\n"
                    f"          , {', '.join(f'{column_name}: nodes.{column_name}' for column_name in self._attribute_columns)}\n"
                    f"          , is_root: nodes.is_root\n"
                    f"          , is_leaf: nodes.is_leaf\n"
                    f"          , level_number: parent_nodes.level_number + 1 }} AS node_json\n"
                    f"      , array_append (parent_nodes.node_json_path\n"
                    f"          , {{  {self.node_id_column}: nodes.{self.node_id_column}\n"
                    f"            , {', '.join(f'{column_name}: nodes.{column_name}' for column_name in self._attribute_columns)}\n"
                    f"            , is_root: nodes.is_root\n"
                    f"            , is_leaf: nodes.is_leaf\n"
                    f"            , level_number: parent_nodes.level_number + 1 }}) AS node_json_path\n"
                    f"       FROM {self._nodes_temp_table_name} AS nodes\n"
                    f"          JOIN\n"
                    f"            parent_nodes\n"
                    f"          ON nodes.{self.parent_node_id_column} = parent_nodes.{self.node_id_column}\n"
                    f")\n"
                    f"SELECT {self.node_id_column}\n"
                    f"     , {', '.join(f'{column_name}' for column_name in self._attribute_columns)}\n"
                    f"     , {self.parent_node_id_column}\n"
                    f"     , is_root\n"
                    f"     , is_leaf\n"
                    f"     , level_number\n"
                    f"     , {{node_id: {self.node_id_column},\n"
                    f"        {', '.join(f'{column_name}: {column_name}' for column_name in self._attribute_columns)}\n,"
                    f"        is_root: is_root,\n"
                    f"        is_leaf: is_leaf,\n"
                    f"        level_number: level_number,\n"
                    f"        node_sort_order: {node_sort_order_expression}}} AS node_json\n"
                    f"      , node_json_path\n"
                    f"      , {node_sort_order_expression} AS node_sort_order\n"
                    f"      {self._generate_level_column_sql()}"
                    f"  FROM parent_nodes\n"
                    f" ORDER BY node_sort_order ASC\n"
                    )

        self.execute_sql(sql_text=sql_text)
        return reporting_dim_table_name

    def _create_aggregation_dim_table(self) -> str:
        if not self._reporting_dim_table_name:
            raise RuntimeError("The reporting_dim_table MUST be created to run this method!")

        aggregation_dim_table_name = f"{self.dimension_name}_aggregation_dim"
        sql_text = (f"CREATE OR REPLACE TABLE {aggregation_dim_table_name}\n"
                    f"AS\n"
                    f"WITH RECURSIVE parent_nodes (\n"
                    f"   {self.node_id_column}\n"
                    f" , {', '.join(column_name for column_name in self._attribute_columns)}\n"
                    f" , {self.parent_node_id_column}\n"
                    f" , is_root\n"
                    f" , is_leaf\n"
                    f" , level_number\n"
                    f" , node_sort_order\n"
                    f" , node_json\n"
                    f" , node_json_path\n"
                    f")\n"
                    f"AS (--Anchor Clause\n"
                    f"    SELECT\n"
                    f"        {self.node_id_column}\n"
                    f"      , {', '.join(column_name for column_name in self._attribute_columns)}\n"
                    f"      , {self.parent_node_id_column}\n"
                    f"      , is_root\n"
                    f"      , is_leaf\n"
                    f"      , level_number\n"
                    f"      , node_sort_order"
                    f"      , node_json\n"
                    f"      -- We must start a new NODE_JSON array b/c each node will be represented as a root node...\n"
                    f"      , [node_json] AS node_json_path\n"
                    f"       FROM {self._reporting_dim_table_name}\n"
                    f"      -- We do NOT filter the anchor, because we want EVERY node in the hierarchy to be a root node...\n"
                    f"      --\n"
                    f"    UNION ALL\n"
                    f"    -- Recursive Clause\n"
                    f"    SELECT\n"
                    f"        nodes.{self.node_id_column}\n"
                    f"      , {', '.join(f'nodes.{column_name}' for column_name in self._attribute_columns)}\n"
                    f"      , nodes.{self.parent_node_id_column}\n"
                    f"      , nodes.is_root\n"
                    f"      , nodes.is_leaf\n"
                    f"      , nodes.level_number\n"
                    f"      , nodes.node_sort_order\n"
                    f"      , nodes.node_json\n"
                    f"      , array_append (parent_nodes.node_json_path\n"
                    f"                    , nodes.node_json) AS node_json_path"
                    f"       FROM {self._reporting_dim_table_name} AS nodes\n"
                    f"          JOIN\n"
                    f"            parent_nodes\n"
                    f"          ON nodes.{self.parent_node_id_column} = parent_nodes.{self.node_id_column}\n"
                    f")\n"
                    f"SELECT -- Ancestor columns (we take the first array element to get the anchor root)\n"
                    f"       node_json_path[1].{self.node_id_column}              AS ancestor_{self.node_id_column}\n"
                    f"     , {', '.join(f'node_json_path[1].{column_name}         AS ancestor_{column_name}' for column_name in self._attribute_columns)}\n"
                    f"     , node_json_path[1].level_number                       AS ancestor_level_number\n"
                    f"     , node_json_path[1].is_root                            AS ancestor_is_root\n"
                    f"     , node_json_path[1].is_leaf                            AS ancestor_is_leaf\n"
                    f"     , node_json_path[1].node_sort_order                    AS ancestor_node_sort_order\n"
                    f"     -- Descendant columns\n"
                    f"     , {self.node_id_column}                                AS descendant_{self.node_id_column}\n"
                    f"     , {', '.join(f'{column_name}                           AS descendant_{column_name}' for column_name in self._attribute_columns)}\n"
                    f"     , level_number                                         AS descendant_level_number\n"
                    f"     , is_root                                              AS descendant_is_root\n"
                    f"     , is_leaf                                              AS descendant_is_leaf\n"
                    f"     , node_sort_order                                      AS descendant_node_sort_order\n"
                    f"     --\n"
                    f"     , (level_number - node_json_path[1].level_number)        AS net_level\n"
                    f"  FROM parent_nodes\n"
                    f" ORDER BY node_sort_order ASC\n"
                    )

        self.execute_sql(sql_text=sql_text)
        return aggregation_dim_table_name
