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

suite("bind_view_alias_star_agg") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"

    def viewName = "bind_view_alias_star_agg"

    try {
        sql "DROP VIEW IF EXISTS ${viewName}"

        sql """
            create view ${viewName} as
            with sale2 as (
                select 1 as pack_factory, 2 as area, 3 as purchase_month, 4 as label_model, 50000 as mile_range
            ),
            sale3 as (
                select c.*, s.id, s.id * 10000 as mile_range
                from (
                    select pack_factory, area, purchase_month, label_model, max(mile_range) / 10000 as max_range
                    from sale2
                    group by pack_factory, area, purchase_month, label_model
                ) c
                join (select 1 as id) s
                where s.id <= max_range
            )
            select * from sale3
        """

        order_qt_select_view """
            select * from ${viewName}
        """
    } finally {
        sql "DROP VIEW IF EXISTS ${viewName}"
    }
}
