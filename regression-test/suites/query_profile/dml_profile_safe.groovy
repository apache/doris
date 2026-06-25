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

import groovy.json.JsonSlurper

def getProfileList = { masterHTTPAddr ->
    def dst = 'http://' + masterHTTPAddr
    def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
    conn.setRequestMethod("GET")
    def password = context.config.feHttpPassword == null ? "" : context.config.feHttpPassword
    def encoding = Base64.getEncoder()
            .encodeToString((context.config.feHttpUser + ":" + password).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

def getMasterHttpAddress = { allFrontends ->
    for (def frontend : allFrontends) {
        if (frontend[8] == "true") {
            return frontend[1] + ":" + frontend[3]
        }
    }
    throw new IllegalStateException("master frontend not found")
}

def hasProfile = { masterHTTPAddr, taskType, sqlToken ->
    def wholeString = getProfileList(masterHTTPAddr)
    def profileListData = new JsonSlurper().parseText(wholeString).data.rows
    for (def profileList : profileListData) {
        def profileTaskType = profileList["Task Type"].toString()
        def stmt = profileList["Sql Statement"].toString().toLowerCase()
        if (profileTaskType == taskType && stmt.contains(sqlToken.toLowerCase())) {
            return true
        }
    }
    return false
}

def waitForProfile = { masterHTTPAddr, taskType, sqlToken ->
    for (int i = 0; i < 20; i++) {
        if (hasProfile(masterHTTPAddr, taskType, sqlToken)) {
            return true
        }
        Thread.sleep(500)
    }
    return false
}

suite("dml_profile_safe") {
    sql """set enable_nereids_planner = true;"""
    sql """set enable_fallback_to_original_planner = false;"""

    sql """drop table if exists dml_profile_update_target;"""
    sql """drop table if exists dml_profile_merge_target;"""
    sql """drop table if exists dml_profile_merge_source;"""
    sql """drop table if exists dml_profile_delete_using_target;"""
    sql """drop table if exists dml_profile_delete_using_source;"""
    sql """drop table if exists dml_profile_delete_command_target;"""

    sql """
        create table dml_profile_update_target (
            k int,
            v int
        )
        unique key(k)
        distributed by hash(k) buckets 1
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        create table dml_profile_merge_target (
            k int,
            v int
        )
        unique key(k)
        distributed by hash(k) buckets 1
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        create table dml_profile_merge_source (
            k int,
            v int
        )
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties("replication_num" = "1");
    """

    sql """
        create table dml_profile_delete_using_target (
            k int,
            v int
        )
        unique key(k)
        distributed by hash(k) buckets 1
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        create table dml_profile_delete_using_source (
            k int
        )
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties("replication_num" = "1");
    """

    sql """
        create table dml_profile_delete_command_target (
            k int,
            v int
        )
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties("replication_num" = "1");
    """

    sql """insert into dml_profile_update_target values (1, 10), (2, 20);"""
    sql """insert into dml_profile_merge_target values (1, 10);"""
    sql """insert into dml_profile_merge_source values (1, 11), (2, 22);"""
    sql """insert into dml_profile_delete_using_target values (1, 10), (2, 20);"""
    sql """insert into dml_profile_delete_using_source values (1);"""
    sql """insert into dml_profile_delete_command_target values (1, 10), (2, 20);"""
    sql """sync;"""

    def masterHTTPAddr = getMasterHttpAddress(sql """show frontends;""")

    sql """clean all profile;"""
    sql """set enable_profile = true;"""
    sql """set profile_level = 2;"""

    sql """update dml_profile_update_target set v = v + 1 where k = 1;"""
    assertTrue(waitForProfile(masterHTTPAddr, "LOAD",
            "update dml_profile_update_target set v = v + 1 where k = 1"))

    sql """
        merge into dml_profile_merge_target t
        using dml_profile_merge_source s
        on t.k = s.k
        when matched then update set v = s.v
        when not matched then insert values(s.k, s.v);
    """
    assertTrue(waitForProfile(masterHTTPAddr, "LOAD", "merge into dml_profile_merge_target"))

    sql """
        delete from dml_profile_delete_using_target t
        using dml_profile_delete_using_source s
        where t.k = s.k;
    """
    assertTrue(waitForProfile(masterHTTPAddr, "LOAD", "delete from dml_profile_delete_using_target"))

    sql """clean all profile;"""
    sql """delete from dml_profile_delete_command_target where k = 1;"""
    Thread.sleep(1000)
    assertFalse(hasProfile(masterHTTPAddr, "LOAD", "delete from dml_profile_delete_command_target"))
    assertFalse(hasProfile(masterHTTPAddr, "QUERY", "delete from dml_profile_delete_command_target"))
}
