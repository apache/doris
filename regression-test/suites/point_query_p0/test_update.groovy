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

suite("test_update", "p0") {
    sql """
        create table if not exists test (
        workspace_id int not null comment "Workspace id",
        user_id varchar(64) comment "External user id",
        tenant_id int not null comment "Tenant id",
        created_at datetime not null default current_timestamp(0) comment "Created at",
        updated_at datetime comment "Updated at"
        )
        engine=olap
        unique key (workspace_id, user_id)
        distributed by hash(workspace_id, user_id)
        properties (
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "store_row_column" = "true"
        );
    """
    sql """insert into test (workspace_id, user_id, tenant_id) values (1, 'asdfadfa', 1);"""
    sql """update test set tenant_id = 5 where workspace_id = 1 and user_id = 'asdfadfa';"""
    sql """insert into test select * from test where workspace_id = 1 and user_id = 'asdfadfa';"""
}