from ibis.expr.types import Expr
from psycopg2 import sql


class HierarchyDimensionTable(object):
    def __init__(self,
                 ibis_expr: Expr,
                 node_id_column: str,
                 parent_node_id_column: str
                 ):
        self.ibis_expr: Expr = ibis_expr
        # TODO: try to find an unprotected attribute for this...
        self.source_table_name = self.ibis_expr._key[2].name
        self.node_id_column = node_id_column
        self.parent_node_id_column = parent_node_id_column
        self._validate_columns()

        self._nodes_temp_table_name = self._create_nodes_temp_table()
        self.max_hierarchy_depth = self._get_max_hierarchy_depth()

    def execute_sql(self, sql_text: str):
        # TODO: Try to find another way to get the backend without using an unprotected method
        return self.ibis_expr._find_backend().raw_sql(query=sql_text)

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

    def _get_recursive_cte_sql(self,
                               cte_name: str = "parent_nodes",
                               filter_anchor_for_root: bool=True
                               ) -> str:
        if not self._nodes_temp_table_name:
            raise RuntimeError("The nodes_temp_table MUST be created to run this method!")

        return (f"WITH RECURSIVE {cte_name} (\n"
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
                f"      WHERE {('is_root' if filter_anchor_for_root else 'TRUE')} = TRUE\n"
                f"      --\n"
                f"      UNION ALL"
                f"      --\n"
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
                )

    def _get_max_hierarchy_depth(self) -> int:
        sql_text = (f"{self._get_recursive_cte_sql(cte_name='parent_nodes', filter_anchor_for_root=True)}"
                    f"SELECT MAX (level_number) FROM parent_nodes"
                    )
        return self.execute_sql(sql_text=sql_text).first()[0]

    def _create_reporting_dim_table(self) -> str:
        reporting_dim_table_name = f"{self.source_table_name}_reporting_dim"
        sql_text = (f"CREATE OR REPLACE TABLE {reporting_dim_table_name}\n"
                    f"AS\n"
                    f"{self._get_recursive_cte_sql(filter_anchor_for_root=True)}"
                    )
