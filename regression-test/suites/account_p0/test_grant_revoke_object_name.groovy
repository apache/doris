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

suite("test_grant_revoke_object_name") {
    sql """DROP USER IF EXISTS 'test_grant_revoke_object_name_user'"""
    sql """DROP ROLE IF EXISTS test_grant_revoke_object_name_role"""
    sql """DROP DATABASE IF EXISTS test_grant_revoke_object_name_db"""
    sql """CREATE DATABASE test_grant_revoke_object_name_db"""
    sql """CREATE USER 'test_grant_revoke_object_name_user'"""
    sql """CREATE ROLE test_grant_revoke_object_name_role"""

    // db, db.tbl and ctl.db.tbl are the only object name shapes a table privilege takes
    sql """GRANT SELECT_PRIV ON test_grant_revoke_object_name_db TO 'test_grant_revoke_object_name_user'"""
    sql """REVOKE SELECT_PRIV ON test_grant_revoke_object_name_db FROM 'test_grant_revoke_object_name_user'"""
    sql """GRANT SELECT_PRIV ON test_grant_revoke_object_name_db.* TO 'test_grant_revoke_object_name_user'"""
    sql """REVOKE SELECT_PRIV ON test_grant_revoke_object_name_db.* FROM 'test_grant_revoke_object_name_user'"""
    sql """GRANT SELECT_PRIV ON internal.test_grant_revoke_object_name_db.* TO 'test_grant_revoke_object_name_user'"""
    sql """REVOKE SELECT_PRIV ON internal.test_grant_revoke_object_name_db.* FROM 'test_grant_revoke_object_name_user'"""

    // four or more parts are rejected while parsing, for both a user and a role
    test {
        sql """GRANT SELECT_PRIV ON a.b.c.d TO 'test_grant_revoke_object_name_user'"""
        exception "Privilege object name should be db, db.tbl or ctl.db.tbl, but got: a.b.c.d"
    }
    test {
        sql """REVOKE SELECT_PRIV ON a.b.c.d FROM 'test_grant_revoke_object_name_user'"""
        exception "Privilege object name should be db, db.tbl or ctl.db.tbl, but got: a.b.c.d"
    }
    test {
        sql """GRANT SELECT_PRIV ON internal.test_grant_revoke_object_name_db.t.c TO ROLE test_grant_revoke_object_name_role"""
        exception "Privilege object name should be db, db.tbl or ctl.db.tbl, but got: internal.test_grant_revoke_object_name_db.t.c"
    }
    test {
        sql """REVOKE SELECT_PRIV ON *.*.*.* FROM ROLE test_grant_revoke_object_name_role"""
        exception "Privilege object name should be db, db.tbl or ctl.db.tbl, but got: *.*.*.*"
    }
}
