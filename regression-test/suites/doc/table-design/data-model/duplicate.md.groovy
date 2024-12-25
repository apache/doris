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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/data-model/duplicate.md") {
    try {
        multi_sql """
        CREATE TABLE IF NOT EXISTS example_tbl_by_default
        (
            `timestamp` DATETIME NOT NULL COMMENT "日志时间",
            `type` INT NOT NULL COMMENT "日志类型",
            `error_code` INT COMMENT "错误码",
            `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
            `op_id` BIGINT COMMENT "负责人id",
            `op_time` DATETIME COMMENT "处理时间"
        )
        DISTRIBUTED BY HASH(`type`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 3"
        );
        desc example_tbl_by_default; 
        """

        multi_sql """
        CREATE TABLE IF NOT EXISTS example_tbl_duplicate
        (
            `timestamp` DATETIME NOT NULL COMMENT "日志时间",
            `type` INT NOT NULL COMMENT "日志类型",
            `error_code` INT COMMENT "错误码",
            `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
            `op_id` BIGINT COMMENT "负责人id",
            `op_time` DATETIME COMMENT "处理时间"
        )
        DUPLICATE KEY(`timestamp`, `type`, `error_code`)
        DISTRIBUTED BY HASH(`type`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 3"
        );
        desc example_tbl_duplicate; 
        """

        multi_sql """
        CREATE TABLE IF NOT EXISTS example_tbl_duplicate_without_keys_by_default
        (
            `timestamp` DATETIME NOT NULL COMMENT "日志时间",
            `type` INT NOT NULL COMMENT "日志类型",
            `error_code` INT COMMENT "错误码",
            `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
            `op_id` BIGINT COMMENT "负责人id",
            `op_time` DATETIME COMMENT "处理时间"
        )
        DISTRIBUTED BY HASH(`type`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "enable_duplicate_without_keys_by_default" = "true"
        );
        desc example_tbl_duplicate_without_keys_by_default;
        """

    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/data-model/duplicate.md failed to exec, please fix it", t)
    }
}
