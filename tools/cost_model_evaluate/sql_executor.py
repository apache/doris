# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from unittest import result
import mysql.connector
from typing import List, Tuple


class SQLExecutor:
    def __init__(self, user: str, password: str, host: str, port: int, database: str) -> None:
        self.connection = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.cursor = self.connection.cursor()
        self.wait_fetch_time_index = 16

    def execute_query(self, query: str, parameters: Tuple | None) -> List[Tuple]:
        if parameters:
            self.cursor.execute(query, parameters)
        else:
            self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def get_execute_time(self, query: str) -> float:
        self.execute_query(query, None)
        profile = self.execute_query("show query profile\"\"", None)
        return float(profile[0][self.wait_fetch_time_index].replace("ms", ""))

    def execute_many_queries(self, queries: List[Tuple[str, Tuple]]) -> List[List[Tuple]]:
        results = []
        for query, parameters in queries:
            result = self.execute_query(query, parameters)
            results.append(result)
        return results

    def get_plan_with_cost(self, query: str):
        result = self.execute_query(f"explain optimized plan {query}", None)
        cost = float(result[0][0].replace("cost = ", ""))
        plan = "".join([s[0] for s in result[1:]])
        return plan, cost

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.cursor.close()
        self.connection.close()
