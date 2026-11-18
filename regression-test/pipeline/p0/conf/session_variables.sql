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

-- Preserve the existing CI visibility wait for asynchronous ingestion.
SET GLOBAL insert_visible_timeout_ms=60000;
-- Keep background automatic analysis from changing fixtures during unrelated tests.
-- Suites covering automatic analysis must explicitly enable it.
SET GLOBAL enable_auto_analyze=false;
-- Audit suites need the audit plugin enabled before cases start.
SET GLOBAL enable_audit_plugin=true;
-- The rootless SQL converter is deployed beside Doris on this local endpoint.
SET GLOBAL sql_converter_service_url='http://127.0.0.1:5001/api/v1/convert';
-- Preserve the connection budget used by framework/client pools.
SET PROPERTY FOR 'root' 'max_user_connections'='300';
