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

from distutils.command.config import config
from config import Config
from index_calculator import IndexCalculator
from sql_executor import SQLExecutor
import matplotlib.pyplot as plt


class Evaluator:
    def __init__(self, config: Config, query: str) -> None:
        self.config = config
        self.query = query.lower()
        self.setup_queries = [
            "set enable_nereids_planner=true;",
            "set enable_fallback_to_original_planner=false;",
            "set enable_profile=true;"
        ]
        self.sql_executor = SQLExecutor(
            config.user,
            config.password,
            config.host,
            config.port,
            config.database)

    def cold_run(self):
        for _ in range(self.config.cold_run):
            self.sql_executor.execute_query(self.query, None)

    def evaluate(self):
        self.setup()
        self.cold_run()
        plans = self.extract_all_plans()
        res: list[tuple[float, float]] = []
        for n, (plan, cost) in plans.items():
            print(f"run {n}-th plan")
            time = self.sql_executor.get_execute_time(plan)
            res.append((cost, time))
        if self.config.plot:
            self.plot(res)
        print(res)
        index_calculator = IndexCalculator(res)
        return index_calculator.calculate()

    def plot(self, data):
        x_values = [t[0] for t in data]
        y_values = [t[1] for t in data]
        fig, ax = plt.subplots()
        ax.scatter(x_values[:1], y_values[:1], c='r')
        ax.scatter(x_values[1:], y_values[1:])
        ax.set_xlabel('Cost')
        ax.set_ylabel('Time')
        plt.show()

    def setup(self):
        for q in self.setup_queries:
            self.sql_executor.execute_query(q, None)

    def extract_all_plans(self):
        plan_set = set()
        plan_map: dict[int, tuple[str, float]] = {}
        n = 0
        while len(plan_set) < self.config.plan_number:
            n += 1
            query = self.inject_nth_optimized_hint(n)
            plan, cost = self.sql_executor.get_plan_with_cost(query)
            if plan in plan_set:
                continue
            plan_set.add(plan)
            plan_map[n] = (query, cost)
        return plan_map

    def inject_nth_optimized_hint(self, n: int):
        if ("set_var(" in self.query):
            query = self.query.replace(
                "/*+set_var(", f"/*+set_var(nth_optimized_plan={n}, ")
        else:
            query = self.query.replace(
                "select", f"select /*+set_var(nth_optimized_plan={n})*/")
        return query
