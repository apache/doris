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

from dataclasses import dataclass
import re

from dbt.adapters.base.column import Column


@dataclass
class DorisColumn(Column):
    @classmethod
    def from_description(cls, name: str, raw_data_type: str) -> "DorisColumn":
        """Parse only Doris types whose parameters dbt must reason about.

        The generic dbt parser truncates nested types such as
        ``ARRAY<VARCHAR(20)>`` and parameterized non-numeric types such as
        ``DATETIMEV2(6)``. Preserve those verbatim while structuring VARCHAR and
        DECIMAL so schema comparison and safe string widening retain their
        sizes.
        """
        raw_data_type = raw_data_type.strip()
        varchar = re.fullmatch(
            r"varchar\s*\(\s*(\d+)\s*\)",
            raw_data_type,
            flags=re.IGNORECASE,
        )
        if varchar is not None:
            return cls(name, "varchar", char_size=int(varchar.group(1)))

        decimal = re.fullmatch(
            r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
            raw_data_type,
            flags=re.IGNORECASE,
        )
        if decimal is not None:
            return cls(
                name,
                "decimal",
                numeric_precision=int(decimal.group(1)),
                numeric_scale=int(decimal.group(2)),
            )

        return cls(name, raw_data_type)

    @classmethod
    def string_type(cls, size: int) -> str:
        """Render Doris syntax rather than dbt's unsupported CHARACTER VARYING."""
        return "varchar({})".format(size)

    @property
    def quoted(self) -> str:
        return "`{}`".format(self.column)

    def __repr__(self) -> str:
        return f"<DorisColumn {self.name} ({self.data_type})>"
