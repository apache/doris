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

from dataclasses import dataclass

@dataclass
class Config:
    # user for mysql client
    user: str
    # password for mysql client
    password: str
    # host of mysql client
    host: str
    # post of mysql client
    port: int
    # database of query that used to evaluated
    database: str
    # execute times for one plan of the query. Note a query can generate multiple plans
    execute_times: int
    # the number of generate plans for one query. Note if the number > the possible plans, 
    # we will only use the valid plans.   
    plan_number: int
    # Does plot the relation of cost and time
    plot: bool
    # run the query before really evaluate, just for avoiding cold running
    cold_run: int