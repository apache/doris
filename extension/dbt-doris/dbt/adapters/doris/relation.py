#!/usr/bin/env python
# encoding: utf-8

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

from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import DbtRuntimeError


@dataclass
class DorisQuotePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass
class DorisIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DorisRelation(BaseRelation):
    quote_policy: DorisQuotePolicy = field(default_factory=lambda: DorisQuotePolicy())
    include_policy: DorisIncludePolicy = field(default_factory=lambda: DorisIncludePolicy())
    quote_character: str = "`"

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise DbtRuntimeError(f"Cannot set database {self.database} in Doris!")

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                "Got a Doris relation with schema and database set to include, but only one can be set"
            )
        return super().render()
