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

suite("test_max_compute_in_predicate_pushdown", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("ak")
        String sk = context.config.otherConfigs.get("sk")
        String mcCatalogName = "test_max_compute_in_predicate_pushdown"
        String defaultProject = "mc_datalake"
        String tableName = "mc_all_types"

        sql """drop catalog if exists ${mcCatalogName} """
        sql """
            create catalog if not exists ${mcCatalogName} properties (
                "type" = "max_compute",
                "mc.default.project" = "${defaultProject}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.quota" = "pay-as-you-go"
            );
        """

        sql """switch ${mcCatalogName};"""
        sql """use ${defaultProject};"""

        def pushdownInSingle = sql """
            select id, string_col from ${tableName}
            where string_col in ("str")
            order by id
        """
        def localInSingle = sql """
            select id, string_col from ${tableName}
            where concat(string_col, "") in ("str")
            order by id
        """
        assertEquals(localInSingle, pushdownInSingle)

        def pushdownNotInSingle = sql """
            select id, string_col from ${tableName}
            where string_col not in ("str")
            order by id
        """
        def localNotInSingle = sql """
            select id, string_col from ${tableName}
            where concat(string_col, "") not in ("str")
            order by id
        """
        assertEquals(localNotInSingle, pushdownNotInSingle)

        order_qt_in_single """
            select id, string_col from ${tableName}
            where string_col in ("str")
            order by id
        """

        order_qt_in_multi """
            select id, string_col from ${tableName}
            where string_col in ("str", "string_value")
            order by id
        """

        order_qt_not_in_single """
            select id, string_col from ${tableName}
            where string_col not in ("str")
            order by id
        """

        order_qt_not_in_multi """
            select id, string_col from ${tableName}
            where string_col not in ("str", "string_value")
            order by id
        """

        order_qt_in_non_pushdown """
            select id, string_col from ${tableName}
            where concat(string_col, "") in ("str")
            order by id
        """

        order_qt_not_in_non_pushdown """
            select id, string_col from ${tableName}
            where concat(string_col, "") not in ("str")
            order by id
        """
    }
}
