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

suite("test_low_bucket") {
    sql "set enable_spill = false" // spill will cause rf not_ready
    sql "set enable_force_spill = false"
    sql "set runtime_filter_wait_infinitely = true"
    sql "set parallel_pipeline_task_num = 4"
    sql "set runtime_filter_type='MIN_MAX'"

    sql """ DROP TABLE IF EXISTS ads_income_statistics; """
    sql """ DROP TABLE IF EXISTS eq_group; """
    sql """ DROP TABLE IF EXISTS eq_label_setting_relation; """

    // ads_income_statistics must be set to 1 buckets to reproduce the problem
    sql """
        CREATE TABLE ads_income_statistics (
            statistics_date DATE,
            distributor_id INT,
            eq_group_id INT,
            eq_id INT,
            other_column VARCHAR(100) -- 示例其他列
        ) duplicate key (statistics_date)
        PARTITION BY RANGE(`statistics_date`)
        (PARTITION p20210117 VALUES [('2025-01-01'), ('2025-01-10')),
        PARTITION p20210118 VALUES [('2025-01-10'), ('2025-01-19')))
        distributed BY hash(statistics_date) buckets 1
        properties("replication_num" = "1");
        """

    sql """
        CREATE TABLE eq_group (
            eq_group_id INT,
            group_name VARCHAR(100) -- 示例列
        ) duplicate key (eq_group_id)
        properties("replication_num" = "1");
    """

    sql """
    CREATE TABLE eq_label_setting_relation (
        eq_id INT,
        eq_label_setting_id INT
    ) duplicate key (eq_id)
    properties("replication_num" = "1");
    """

    sql """
    INSERT INTO ads_income_statistics 
    (statistics_date, distributor_id, eq_group_id, eq_id, other_column)
    VALUES
    ('2025-01-01', 1537918, 2000, 3000, 'test1'),
    ('2025-01-10', 1537918, 2001, 3001, 'test2'),
    ('2025-01-05', 1537918, 1000, 3002, 'test3'), -- 被排除的记录
    ('2025-01-12', 1537918, 2000, 3003, 'test4'),
    ('2025-01-01', 1537918, 2000, 3000, 'test1'),
    ('2025-01-10', 1537918, 2001, 3001, 'test2'),
    ('2025-01-05', 1537918, 1000, 3002, 'test3'), -- 被排除的记录
    ('2025-01-12', 1537918, 2000, 3003, 'test4'),
    ('2025-01-01', 1537918, 2000, 3000, 'test1'),
    ('2025-01-10', 1537918, 2001, 3001, 'test2'),
    ('2025-01-05', 1537918, 1000, 3002, 'test3'), -- 被排除的记录
    ('2025-01-12', 1537918, 2000, 3003, 'test4'),
    ('2025-01-01', 1537918, 2000, 3000, 'test1'),
    ('2025-01-10', 1537918, 2001, 3001, 'test2'),
    ('2025-01-05', 1537918, 1000, 3002, 'test3'), -- 被排除的记录
    ('2025-01-12', 1537918, 2000, 3003, 'test4');
    """
    
    sql """
    INSERT INTO eq_group (eq_group_id, group_name)
    VALUES
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'),
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'),
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'),
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'),
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'),
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'),
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'),
    (2000, 'Group A'),
    (2001, 'Group B'),
    (1000, 'Excluded Group'); -- 被排除的组
    """

    sql """
    INSERT INTO eq_label_setting_relation (eq_id, eq_label_setting_id)
    VALUES
    (3000, 740960), -- 满足条件的记录
    (3001, 740960), -- 满足条件的记录
    (3002, 123456), -- 不满足条件
    (3003, 740960),
    (3000, 740960), -- 满足条件的记录
    (3001, 740960), -- 满足条件的记录
    (3002, 123456), -- 不满足条件
    (3003, 740960),
    (3000, 740960), -- 满足条件的记录
    (3001, 740960), -- 满足条件的记录
    (3002, 123456), -- 不满足条件
    (3003, 740960),
    (3000, 740960), -- 满足条件的记录
    (3001, 740960), -- 满足条件的记录
    (3002, 123456), -- 不满足条件
    (3003, 740960),
    (3000, 740960), -- 满足条件的记录
    (3001, 740960), -- 满足条件的记录
    (3002, 123456), -- 不满足条件
    (3003, 740960); -- 满足条件的记录
    """

    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
    qt_test """
    WITH tb_a AS ( SELECT * FROM ads_income_statistics ic WHERE ic.statistics_date >= '2025-01-01' AND ic.statistics_date < '2025-01-15' AND ic.distributor_id = 1537918 AND ic.eq_group_id != 1000 ) SELECT count(*) FROM eq_group eg JOIN tb_a ic ON eg.eq_group_id = ic.eq_group_id JOIN[shuffle] eq_label_setting_relation er ON ic.eq_id = er.eq_id WHERE ic.distributor_id = 1537918 AND er.eq_label_setting_id IN (740960) AND ic.eq_group_id != 1000;
    """
}
