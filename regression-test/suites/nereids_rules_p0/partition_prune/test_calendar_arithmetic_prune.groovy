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

suite("test_calendar_arithmetic_prune") {
    sql "set time_zone = '+00:00'"
    // Each type carries a time part that can go backwards when month-end clamping merges dates.
    ["datetimev2(6)", "timestamp_ns", "timestamptz(6)"].eachWithIndex { dateType, typeIndex ->
        sql "drop table if exists calendar_add_sub_prune"
        sql """
            create table calendar_add_sub_prune(d ${dateType})
            duplicate key(d)
            partition by range(d) (
                partition p_month values [('2023-01-28 10:00:00'), ('2023-01-31 23:59:59')),
                partition p_sub values [('2023-03-28 10:00:00'), ('2023-03-31 23:59:59')),
                partition p_quarter values [('2023-05-30 10:00:00'), ('2023-05-31 23:59:59')),
                partition p_leap values [('2024-02-28 10:00:00'), ('2024-02-29 23:59:59'))
            )
            distributed by hash(d) buckets 1
            properties('replication_num' = '1')
        """
        sql """insert into calendar_add_sub_prune values
            ('2023-01-29 00:00:00'), ('2023-03-29 00:00:00'),
            ('2023-05-31 00:00:00'), ('2024-02-29 00:00:00')"""

        "order_qt_${typeIndex}_months_add" """
            select d, months_add(d, 1) from calendar_add_sub_prune
            where months_add(d, 1) < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_months_add_equal" """
            select d from calendar_add_sub_prune where months_add(d, 1) = '2023-02-28 00:00:00'
        """
        "order_qt_${typeIndex}_months_add_in" """
            select d from calendar_add_sub_prune
            where months_add(d, 1) in ('2023-02-28 00:00:00', '2023-02-28 01:00:00')
        """
        "order_qt_${typeIndex}_months_sub" """
            select d, months_sub(d, 1) from calendar_add_sub_prune
            where d >= '2023-03-01' and d < '2023-04-01'
                and months_sub(d, 1) < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_quarters_add" """
            select d, quarters_add(d, 3) from calendar_add_sub_prune
            where d >= '2023-05-01' and d < '2023-06-01'
                and quarters_add(d, 3) < '2024-02-29 10:00:00'
        """
        "order_qt_${typeIndex}_quarters_sub" """
            select d, quarters_sub(d, 1) from calendar_add_sub_prune
            where d >= '2023-05-01' and d < '2023-06-01'
                and quarters_sub(d, 1) < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_years_add" """
            select d, years_add(d, 1) from calendar_add_sub_prune
            where d >= '2024-02-01' and years_add(d, 1) < '2025-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_years_sub" """
            select d, years_sub(d, 1) from calendar_add_sub_prune
            where d >= '2024-02-01' and years_sub(d, 1) < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_year_month_add" """
            select d from calendar_add_sub_prune
            where d + interval '0-1' year_month < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_year_month_sub" """
            select d from calendar_add_sub_prune where d >= '2023-03-01' and d < '2023-04-01'
                and d - interval '0-1' year_month < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_date_add" """
            select d from calendar_add_sub_prune where date_add(d, interval 1 month) < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_date_sub" """
            select d from calendar_add_sub_prune where d >= '2023-03-01' and d < '2023-04-01'
                and date_sub(d, interval 1 month) < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_timestampadd" """
            select d from calendar_add_sub_prune where timestampadd(month, 1, d) < '2023-02-28 10:00:00'
        """
        "order_qt_${typeIndex}_nested" """
            select d from calendar_add_sub_prune where date_trunc(months_add(d, 1), 'hour') < '2023-02-28 10:00:00'
        """

        sql "drop table if exists calendar_add_upper_prune"
        sql """
            create table calendar_add_upper_prune(d ${dateType})
            duplicate key(d)
            partition by range(d) (partition p1 values [('2023-01-27 00:00:00'), ('2023-01-31 00:00:00')))
            distributed by hash(d) buckets 1 properties('replication_num' = '1')
        """
        sql "insert into calendar_add_upper_prune values ('2023-01-29 23:00:00')"
        "order_qt_${typeIndex}_upper_bound" """
            select d from calendar_add_upper_prune where months_add(d, 1) > '2023-02-28 12:00:00'
        """

        // LocalDateTime.until used to fold both partition endpoints to zero instead of one.
        sql "drop table if exists calendar_diff_prune"
        sql """
            create table calendar_diff_prune(d ${dateType})
            duplicate key(d)
            partition by range(d) (partition p1 values [('2021-03-01 00:00:00'), ('2021-03-01 11:00:00')))
            distributed by hash(d) buckets 1 properties('replication_num' = '1')
        """
        sql "insert into calendar_diff_prune values ('2021-03-01 10:00:00')"
        ["months": "2021-01-31 12:00:00", "quarters": "2020-11-30 12:00:00",
                "years": "2020-02-29 12:00:00"].each { unit, origin ->
            "order_qt_${typeIndex}_${unit}_diff" """
                select d, ${unit}_diff(d, '${origin}') from calendar_diff_prune
                where ${unit}_diff(d, '${origin}') = 1
            """
            "order_qt_${typeIndex}_${unit}_diff_reverse" """
                select d, ${unit}_diff('${origin}', d) from calendar_diff_prune
                where ${unit}_diff('${origin}', d) = -1
            """
            "order_qt_${typeIndex}_${unit}_diff_constant" """
                select ${unit}_diff(cast('2021-03-01 10:00:00' as ${dateType}), cast('${origin}' as ${dateType}))
            """
        }
    }

    // DATE has no time reset and must keep its existing monotonic partition pruning.
    sql "drop table if exists calendar_date_prune"
    sql """
        create table calendar_date_prune(d datev2) duplicate key(d)
        partition by range(d) (
            partition p1 values [('2023-01-28'), ('2023-02-01')),
            partition p2 values [('2023-03-01'), ('2023-04-01'))
        ) distributed by hash(d) buckets 1 properties('replication_num' = '1')
    """
    sql "insert into calendar_date_prune values ('2023-01-29'), ('2023-03-29')"
    order_qt_date_only "select d from calendar_date_prune where months_add(d, 1) = '2023-02-28'"
    explain {
        sql "select d from calendar_date_prune where months_add(d, 1) = '2023-02-28'"
        contains "partitions=1/2 (p1)"
    }
}
