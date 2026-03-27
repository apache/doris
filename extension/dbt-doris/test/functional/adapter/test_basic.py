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

import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    check_relation_types,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class TestSimpleMaterializationsDoris(BaseSimpleMaterializations):
    def test_base(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt()
        assert len(results) == 3

        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])


class TestSingularTestsDoris(BaseSingularTests):
    pass


class TestSingularTestsEphemeralDoris(BaseSingularTestsEphemeral):
    pass


class TestEmptyDoris(BaseEmpty):
    pass


class TestEphemeralDoris(BaseEphemeral):
    def test_ephemeral(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])
        results = run_dbt(["run"])
        assert len(results) == 2
        check_result_nodes_by_name(results, ["view_model", "table_model"])
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10
        check_relations_equal(project.adapter, ["base", "view_model", "table_model"])


class TestGenericTestsDoris(BaseGenericTests):
    pass


class TestBaseAdapterMethodDoris(BaseAdapterMethod):
    def test_adapter_methods(self, project, equal_tables):
        result = run_dbt()
        assert len(result) == 3
        check_relations_equal(project.adapter, equal_tables)
