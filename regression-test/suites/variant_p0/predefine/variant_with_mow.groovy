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

suite("variant_predefine_with_mow") {
    sql "DROP TABLE IF EXISTS var_mow"
    sql """
        CREATE TABLE `var_mow` (
        `PORTALID` int NOT NULL,
        `OBJECTTYPEID` varchar(65533) NOT NULL,
        `OBJECTIDHASH` tinyint NOT NULL,
        `OBJECTID` bigint NOT NULL,
        `DELETED` boolean NULL DEFAULT "FALSE",
        `INGESTIONTIMESTAMP` bigint NOT NULL,
        `PROCESSEDTIMESTAMP` bigint NOT NULL,
        `VERSION` bigint NULL DEFAULT "0",
        `OVERFLOWPROPERTIES` variant<'a' : int, 'b' : string, 'c' : largeint,
        properties("variant_max_subcolumns_count" = "100")
        > NULL,
        INDEX objects_properties_idx (`OVERFLOWPROPERTIES`) USING INVERTED COMMENT 'This is an inverted index on all properties of the object'
        ) ENGINE=OLAP
        UNIQUE KEY(`PORTALID`, `OBJECTTYPEID`, `OBJECTIDHASH`, `OBJECTID`)
        DISTRIBUTED BY HASH(`PORTALID`, `OBJECTTYPEID`, `OBJECTIDHASH`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true",
        "function_column.sequence_col" = "VERSION",
        "disable_auto_compaction" = "true"
        );
    """

    sql """ insert into var_mow values(944935233, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 30, "b": 40, "c": 50, "d": 60, "e": 70, "f": 80, "g": 90, "h": 100, "i": 110, "j": 120}'); """
    sql """ insert into var_mow values(944935234, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 31, "b": 41, "c": 51, "d": 61, "e": 71, "f": 81, "g": 91, "h": 101, "i": 111, "j": 121}'); """
    sql """ insert into var_mow values(944935235, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 32, "b": 42, "c": 52, "d": 62, "e": 72, "f": 82, "g": 92, "h": 102, "i": 112, "j": 122}'); """
    sql """ insert into var_mow values(944935236, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 33, "b": 43, "c": 53, "d": 63, "e": 73, "f": 83, "g": 93, "h": 103, "i": 113, "j": 123}'); """
    sql """ insert into var_mow values(944935237, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 34, "b": 44, "c": 54, "d": 64, "e": 74, "f": 84, "g": 94, "h": 104, "i": 114, "j": 124}'); """
    sql """ insert into var_mow values(944935238, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 35, "b": 45, "c": 55, "d": 65, "e": 75, "f": 85, "g": 95, "h": 105, "i": 115, "j": 125}'); """
    sql """ insert into var_mow values(944935239, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 36, "b": 46, "c": 56, "d": 66, "e": 76, "f": 86, "g": 96, "h": 106, "i": 116, "j": 126}'); """
    sql """ insert into var_mow values(944935240, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 37, "b": 47, "c": 57, "d": 67, "e": 77, "f": 87, "g": 97, "h": 107, "i": 117, "j": 127}'); """
    sql """ insert into var_mow values(944935241, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 38, "b": 48, "c": 58, "d": 68, "e": 78, "f": 88, "g": 98, "h": 108, "i": 118, "j": 128}'); """
    sql """ insert into var_mow values(944935242, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 39, "b": 49, "c": 59, "d": 69, "e": 79, "f": 89, "g": 99, "h": 109, "i": 119, "j": 129}'); """
    sql """ insert into var_mow values(944935243, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 40, "b": 50, "c": 60, "d": 70, "e": 80, "f": 90, "g": 100, "h": 110, "i": 120, "j": 130}'); """
    sql """ insert into var_mow values(944935244, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 41, "b": 51, "c": 61, "d": 71, "e": 81, "f": 91, "g": 101, "h": 111, "i": 121, "j": 131}'); """
    sql """ insert into var_mow values(944935245, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 42, "b": 52, "c": 62, "d": 72, "e": 82, "f": 92, "g": 102, "h": 112, "i": 122, "j": 132}'); """
    sql """ insert into var_mow values(944935246, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 43, "b": 53, "c": 63, "d": 73, "e": 83, "f": 93, "g": 103, "h": 113, "i": 123, "j": 133}'); """
    sql """ insert into var_mow values(944935247, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 44, "b": 54, "c": 64, "d": 74, "e": 84, "f": 94, "g": 104, "h": 114, "i": 124, "j": 134}'); """
    sql """ insert into var_mow values(944935248, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 45, "b": 55, "c": 65, "d": 75, "e": 85, "f": 95, "g": 105, "h": 115, "i": 125, "j": 135}'); """
    sql """ insert into var_mow values(944935249, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 46, "b": 56, "c": 66, "d": 76, "e": 86, "f": 96, "g": 106, "h": 116, "i": 126, "j": 136}'); """
    sql """ insert into var_mow values(944935250, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 47, "b": 57, "c": 67, "d": 77, "e": 87, "f": 97, "g": 107, "h": 117, "i": 127, "j": 137}'); """
    sql """ insert into var_mow values(944935251, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 48, "b": 58, "c": 68, "d": 78, "e": 88, "f": 98, "g": 108, "h": 118, "i": 128, "j": 138}'); """
    sql """ insert into var_mow values(944935252, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 49, "b": 59, "c": 69, "d": 79, "e": 89, "f": 99, "g": 109, "h": 119, "i": 129, "j": 139}'); """
    sql """ insert into var_mow values(944935253, '2', 1, 1, 'TRUE', 1741682404960657985, 1741682404960657985, 0, '{"a": 50, "b": 60, "c": 70, "d": 80, "e": 90, "f": 100, "g": 110, "h": 120, "i": 130, "j": 140}'); """

    trigger_and_wait_compaction("var_mow", "cumulative")

    qt_sql """ select objectId from var_mow objects_alias where objects_alias.portalid = 944935233 and objects_alias.objectTypeId = '2' limit 100 """
    // topn two phase enabled
    qt_sql """select * from var_mow order by portalid  limit 5"""
    // topn two phase disabled
    qt_sql """select * from var_mow order by portalid + OBJECTIDHASH limit 5"""
    // qt_sql """select variant_type(OVERFLOWPROPERTIES) from var_mow limit 1"""
}