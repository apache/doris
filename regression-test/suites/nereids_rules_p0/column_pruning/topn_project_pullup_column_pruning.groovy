// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("topn_project_pullup_column_pruning") {
    sql """ DROP TABLE IF EXISTS tppcp_tbl """
    sql """
        CREATE TABLE tppcp_tbl (
            id          INT,
            str_col     STRING,
            struct_col  STRUCT<city: STRING, zip: INT>,
            arr_col     ARRAY<INT>,
            map_col     MAP<STRING, INT>,
            int_col     INT
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO tppcp_tbl VALUES
            (3, 's3', named_struct('city', 'beijing', 'zip', 10003), [3, 30], {'k3': 3}, 30),
            (1, 's1', named_struct('city', 'shanghai', 'zip', 10001), [1, 10], {'k1': 1}, 10),
            (4, 's4', named_struct('city', 'shenzhen', 'zip', 10004), [4, 40], {'k4': 4}, 40),
            (2, 's2', named_struct('city', 'hangzhou', 'zip', 10002), [2, 20], {'k2': 2}, 20)
    """

    def pullupPlan = sql """
        explain physical plan
        select id, substring(struct_element(struct_col, 'city'), 1) as city
        from tppcp_tbl
        order by id
        limit 3
    """
    def pullupPlanString = pullupPlan.collect { it[0] }.join("\n")
    assertTrue((pullupPlanString =~ /operativeSlots=\[id#\d+, struct_col#/).find())

    def topNPos = pullupPlanString.indexOf("PhysicalTopN")
    def substringPos = pullupPlanString.indexOf("substring(struct_element")
    assertTrue(substringPos >= 0)
    assertTrue(topNPos >= 0)
    assertTrue(substringPos < topNPos)

    def lowerStructElementPos = pullupPlanString.indexOf("struct_element(struct_col#")
    assertTrue(lowerStructElementPos > topNPos)

    def nestedPruningPlan = sql """
        explain
        select id, substring(struct_element(struct_col, 'city'), 1) as city
        from tppcp_tbl
        order by id
        limit 3
    """
    def nestedPruningPlanString = nestedPruningPlan.collect { it[0] }.join("\n")
    assertTrue(nestedPruningPlanString.contains("pruned type: struct<city:text>"))
    assertTrue(nestedPruningPlanString.contains("all access paths: [struct_col.city]"))
    assertFalse(nestedPruningPlanString.contains("all access paths: [struct_col]"))

    qt_project_after_topn """
        select id, substring(struct_element(struct_col, 'city'), 1) as city
        from tppcp_tbl
        order by id
        limit 3
    """

    def orderByAliasPlan = sql """
        explain physical plan
        select id, substring(struct_element(struct_col, 'city'), 1) as city
        from tppcp_tbl
        order by city
        limit 3
    """
    def orderByAliasPlanString = orderByAliasPlan.collect { it[0] }.join("\n")
    def orderByAliasTopNPos = orderByAliasPlanString.indexOf("PhysicalTopN")
    def orderByAliasSubstringPos = orderByAliasPlanString.indexOf("substring(struct_element")
    assertTrue(orderByAliasTopNPos >= 0)
    assertTrue(orderByAliasSubstringPos >= 0)
    assertTrue(orderByAliasSubstringPos > orderByAliasTopNPos)

    qt_project_used_by_topn """
        select id, substring(struct_element(struct_col, 'city'), 1) as city
        from tppcp_tbl
        order by city
        limit 3
    """
}
