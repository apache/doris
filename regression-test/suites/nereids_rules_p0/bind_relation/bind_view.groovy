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

suite("test_bind_view") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"

    def table_name = "base_table"
    def view_name = "decimal_view"

    sql """
        DROP TABLE IF EXISTS ${table_name};
    """
    sql """
        DROP VIEW IF EXISTS ${view_name};
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${table_name}
        (
            `c1` varchar(255),
            `c2` TINYINT,
            `c3` varchar(65533),
            `c4` varchar(255),
            `c5` varchar(255),
            `c6` DECIMAL(26,8),
            `c7` varchar(255),
            `c8` DECIMAL(35,8)
        )
        DUPLICATE KEY(`c1`)
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    test {
        sql """
              select ggg.* from ( select * from ${table_name} ) l;
        """
        exception "unknown qualifier:"
    }

    try {

        sql """
            create view ${view_name} AS
            SELECT
                substr(c1,1,8) c1
                ,c2 c2
                ,c3 c3
                ,count(c4) c4
                ,count(CASE WHEN c5='PA' THEN c4 end) c5
                ,sum(c6 / 100) c6
                ,sum(CASE WHEN c5='PA' THEN c6/100 end) c7
                ,sum(CASE WHEN c5 = 'PA' THEN c6 * c7 END) / sum(CASE WHEN c5 = 'PA' THEN c6 end) c8
                ,sum(CASE WHEN c5 = 'PA' THEN c6 * c8 END) / sum(CASE WHEN c5 = 'PA' THEN c6 END) c9
            FROM ${table_name}
            GROUP BY 1,2,3
            ORDER BY 1,2,3;
        """

        sql """
            INSERT INTO ${table_name} values
                ('20231214142039',1,'B','12312872384723','PA',3000,'12',11.111)
                ,('20231214132039',1,'B','12312872384723','PA',4000,'12',12.222)
                ,('20231214142239',1,'B','12312872384723','PA',3000,'12',13.333)
                ,('20231214162339',1,'B','12312872384723','PA',4000,'3',11.111)
                ,('20231214152139',1,'B','12312872384723','RJ',3000,'12',11.111)
                ,('20231214152339',1,'A','12312872384723','RJ',5000,'3',11.111)
                ,('20231214162139',1,'B','12312872384723','PA',5000,'12',13.444)
                ,('20231214132439',1,'B','12312872384723','PA',3000,'12',12.111)
                ,('20231214162039',1,'A','12312872384723','PA',7000,'3',11.111)
                ,('20231214142039',1,'B','12312872384723','PA',1000,'12',11.111)
                ,('20231214142039',1,'B','12312872384723','PA',3000,'12',15.555);
        """

        order_qt_select_view """
            select * from ${view_name}
        """
    } finally {
        sql """
            DROP VIEW IF EXISTS ${view_name};
        """
        sql """
            DROP TABLE IF EXISTS ${table_name};
        """

    }
}
