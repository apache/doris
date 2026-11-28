-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Generate the file first and put it at hivevar:orc_merge_location.
-- Register a fresh external table; do not replace existing fixtures implicitly.
CREATE DATABASE IF NOT EXISTS regression;
CREATE EXTERNAL TABLE regression.test_orc_merge_io_input_streams_table (
    dt STRING,
    type STRING,
    risk_flow_id STRING,
    trace_id STRING,
    created_time STRING,
    created_ts BIGINT,
    subflow_decision STRING
) STORED AS ORC
LOCATION '${hivevar:orc_merge_location}';
