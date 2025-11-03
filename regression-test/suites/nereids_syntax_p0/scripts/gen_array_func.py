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

import random
from typing import List

DATA_TYPES = {"INT", "DOUBLE", "STR"}

def generate_lambda_sql(array_func, lambda_func: str, data_type, nullable) -> str:
    n = len(lambda_func.split(","))
    sql = "SELECT {array_func}({lambda_func}, {arrays})".format(
        array_func=array_func,
        lambda_func=lambda_func,
        arrays=", ".join(generate_array_values(n, data_type, nullable))
    )
    return sql

def generate_normal_sql(array_func, data_type, n, nullable) -> str:
    sql = "SELECT {array_func}({arrays})".format(
        array_func=array_func,
        arrays=", ".join(generate_array_values(n, data_type, nullable))
    )
    return sql

def generate_array_values(n, data_type, nullable) -> List[str]:
    array_values = []
    array_len = random.randint(3, 5)
    for array in range(n):
        array_values.append(generate_array_value(array_len, data_type, nullable))
    return array_values

def generate_array_value(array_len: int, data_type: str, nullable: bool) -> str:
    array_values = [generate_value(data_type, nullable) for _ in range(array_len)]
    return "ARRAY({values})".format(
        values=", ".join(array_values)
    )

def generate_value(value_type: str, nullable: bool):
    if generate_null(nullable):
        return "NULL"
    if value_type == "INT":
        return str(random.randint(-100, 100))
    elif value_type == "DOUBLE":
        return str(round(random.uniform(-100, 100), 2))
    elif value_type == "STR":
        return "'{}'".format(generate_random_string(5)) 
    else:
        raise ValueError("Unsupported data type")

def generate_null(nullable):
    return nullable and random.random() > 0.9

def generate_random_string(length: int) -> str:
    characters = "abcdefghijklmnopqrstuvwxyz"
    return ''.join(random.choice(characters) for _ in range(length))

lambda_func_set = {"x->x+1", "(x,y)->x+y"}
id = 0
for dt in DATA_TYPES:
    for lambda_func in lambda_func_set: 
        sql = generate_lambda_sql("ARRAY_MAP", lambda_func, dt, True)
        print(f"sql_{id} \"{sql}\"")
        id += 1