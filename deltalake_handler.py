import mindsdb
from delta import DeltaTable
from typing import List, Optional

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)


class DeltaLakeHandler(VectorStoreHandler):
    name = "deltalake"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self._connection_data = kwargs.get("connection_data")
        self.delta_table_path = self._connection_data.get("delta_table_path")
        self.mdb = mindsdb.Predictor(name='deltalake_predictor')

    def connect(self):
        try:
            delta_data = DeltaTable.forPath(spark, self.delta_table_path)
            self.delta_data = delta_data
            return True
        except Exception as e:
            return False

    def disconnect(self):
        self.delta_data = None

    def check_connection(self):
        return self.connect()

    def select(
            self,
            table_name: str,
            columns: List[str] = None,
            conditions: List[FilterCondition] = None,
            offset: int = None,
            limit: int = None,
    ) -> HandlerResponse:
        delta_data = self.delta_data.toDF()

        if conditions:
            for condition in conditions:
                delta_data = delta_data.filter(
                    f"{condition.column} {self._get_delta_operator(condition.op)} '{condition.value}'"
                )

        if columns:
            delta_data = delta_data.select(columns)

        if offset:
            delta_data = delta_data.dropDuplicates().limit(limit)

        result_df = delta_data.toPandas()
        return HandlerResponse(RESPONSE_TYPE.TABLE, data_frame=result_df)

    def insert(
            self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        # Insert data into Delta Lake
        delta_data = DeltaTable.forPath(spark, self.delta_table_path)

        if columns:
            data = data[columns]

        delta_data.write.format("delta").mode("append").saveAsTable(table_name)

        return HandlerResponse(RESPONSE_TYPE.OK)

    # Implement other methods like update and delete as needed

    def _get_delta_operator(self, operator: FilterOperator) -> str:
        # Map MindsDB filter operators to Delta Lake filter operators
        mapping = {
            FilterOperator.EQUAL: "=",
            FilterOperator.NOT_EQUAL: "!=",
            FilterOperator.LESS_THAN: "<",
            FilterOperator.LESS_THAN_OR_EQUAL: "<=",
            FilterOperator.GREATER_THAN: ">",
            FilterOperator.GREATER_THAN_OR_EQUAL: ">=",
        }

        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by Delta Lake!")

        return mapping[operator]


connection_args = {
    "delta_table_path": {
        "type": ARG_TYPE.STR,
        "description": "Path to the Delta Lake table",
        "required": True,
    }
}

connection_args_example = {
    "delta_table_path": "/path/to/delta/lake/table",
}
