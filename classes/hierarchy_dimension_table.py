from ibis.expr.types import Expr
from config import logger


class HierarchyDimension(object):
    def __init__(self,
                 dimension_name: str,
                 ibis_expr: Expr,
                 node_id_column: str,
                 parent_node_id_column: str
                 ):
        self.dimension_name = dimension_name
        self.ibis_expr: Expr = ibis_expr
        # TODO: Try to find another way to get the backend without using a protected method
        self.connection = ibis_expr._find_backend()
        # TODO: try to find an unprotected attribute for this...
        self.source_table_name = self.ibis_expr._key[2].name
        self.node_id_column = node_id_column
        self.parent_node_id_column = parent_node_id_column
        self._validate_columns()

        self._nodes_temp_table_name = self._create_nodes_temp_table()
        self._reporting_dim_table_name = self._create_reporting_dim_table()
        self._aggregation_dim_table_name = self._create_aggregation_dim_table()

        # Ibis doesn't seem to like the struct type from DuckDB, so we can't use the reporting_dim_table yet...
        # self.reporting_dim_ibis_expr = self.connection.table(self._reporting_dim_table_name)

        self.aggregation_dim_ibis_expr = self.connection.table(self._aggregation_dim_table_name)

    def execute_sql(self, sql_text: str):
        logger.info(msg=f"Executing SQL: {sql_text}")
        return self.connection.raw_sql(query=sql_text)

    def _validate_columns(self):
        if self.node_id_column not in self.ibis_expr.columns:
            raise ValueError

        if self.parent_node_id_column not in self.ibis_expr.columns:
            raise ValueError

    @property
    def _attribute_columns(self) -> list:
        return [item for item in self.ibis_expr.columns if
                item not in [self.node_id_column, self.parent_node_id_column]]

    def _create_nodes_temp_table(self) -> str:
        nodes_temp_table_name = f"{self.source_table_name}_temp"
        sql_text = (f"CREATE OR REPLACE TEMPORARY TABLE {nodes_temp_table_name}\n"
                    f"AS\n"
                    f"SELECT {self.node_id_column}\n"
                    f"     , {', '.join(column_name for column_name in self._attribute_columns)}\n"
                    f"     , {self.parent_node_id_column}"
                    f"     , CASE WHEN {self.parent_node_id_column} IS NULL\n"
                    f"               THEN TRUE\n"
                    f"               ELSE FALSE\n"
                    f"       END AS is_root\n"
                    f"     , CASE WHEN {self.node_id_column} IN (SELECT {self.parent_node_id_column}\n"
                    f"                                             FROM {self.source_table_name}\n"
                    f"                                          )\n"
                    f"               THEN FALSE\n"
                    f"               ELSE TRUE\n"
                    f"       END AS is_leaf\n"
                    f"  FROM {self.source_table_name}"
                    )
        self.execute_sql(sql_text=sql_text)
        return nodes_temp_table_name

    def _generate_level_column_sql(self) -> str:
        level_column_sql = ""
        for i in range(1, 11):
            level_column_sql += \
                (f"-- Level {i} columns\n"
                 f", node_json_path[{i}].{self.node_id_column}        AS level_{i}_{self.node_id_column}\n"
                 f", {', '.join(f'node_json_path[{i}].{column_name}   AS level_{i}_{column_name}' for column_name in self._attribute_columns)}\n"
                 )
        return level_column_sql

    def _create_reporting_dim_table(self) -> str:
        if not self._nodes_temp_table_name:
            raise RuntimeError("The nodes_temp_table MUST be created to run this method!")

        reporting_dim_table_name = f"{self.dimension_name}_reporting_dim"

        node_sort_order_expression = "ROW_NUMBER() OVER (ORDER BY REPLACE (node_json_path::VARCHAR, ']', '') ASC NULLS LAST)"
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
