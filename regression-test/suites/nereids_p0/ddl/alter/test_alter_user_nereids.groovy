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

suite("test_alter_user_nereids") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
	
    sql """DROP USER if exists 'fjytf';"""
    sql """CREATE USER 'fjytf';"""
	
    sql """ALTER USER fjytf@'%' IDENTIFIED BY "12345";"""
    sql """ALTER USER fjytf@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"""
    sql """ALTER USER fjytf@'%' ACCOUNT_UNLOCK;"""
	sql """ALTER USER fjytf@'%' COMMENT "this is my first user";"""
}