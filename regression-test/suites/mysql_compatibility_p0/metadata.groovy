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

import org.apache.doris.regression.util.JdbcUtils

suite ("metadata") {

    sql """
        drop table if exists metadata
    """

    sql """
        drop view if exists view_metadata
    """

    sql """
        create table metadata (
            k1 int null,
            k2 int not null,
            k3 bigint null
        )
        distributed BY hash(k1)
        properties("replication_num" = "1");
    """

    def (tableResult, tableMeta) = JdbcUtils.executeToList(context.getConnection(), "select k1 + 1 as c1, k2 c2, abs(k3) as c3 from metadata ta ")

    println tableMeta

    assertEquals("c1", tableMeta.getColumnName(1))
    assertEquals("k2", tableMeta.getColumnName(2))
    assertEquals("c3", tableMeta.getColumnName(3))

    assertEquals("c1", tableMeta.getColumnLabel(1))
    assertEquals("c2", tableMeta.getColumnLabel(2))
    assertEquals("c3", tableMeta.getColumnLabel(3))

    assertEquals("", tableMeta.getTableName(1))
    assertEquals("metadata", tableMeta.getTableName(2))
    assertEquals("", tableMeta.getTableName(3))

    sql """
        create view view_metadata as
        select k1 as vk1, k2 from metadata
    """

    def (viewResult, viewMeta) = JdbcUtils.executeToList(context.getConnection(), "select vk1 + 1 as c1, vk1 c2, vk1, k2 c3, k2, abs(k2) as c4 from view_metadata ta")

    println viewMeta

    assertEquals("c1", viewMeta.getColumnName(1))
    assertEquals("vk1", viewMeta.getColumnName(2))
    assertEquals("vk1", viewMeta.getColumnName(3))
    assertEquals("k2", viewMeta.getColumnName(4))
    assertEquals("k2", viewMeta.getColumnName(5))
    assertEquals("c4", viewMeta.getColumnName(6))

    assertEquals("c1", viewMeta.getColumnLabel(1))
    assertEquals("c2", viewMeta.getColumnLabel(2))
    assertEquals("vk1", viewMeta.getColumnLabel(3))
    assertEquals("c3", viewMeta.getColumnLabel(4))
    assertEquals("k2", viewMeta.getColumnLabel(5))
    assertEquals("c4", viewMeta.getColumnLabel(6))

    assertEquals("", viewMeta.getTableName(1))
    assertEquals("view_metadata", viewMeta.getTableName(2))
    assertEquals("view_metadata", viewMeta.getTableName(3))
    assertEquals("view_metadata", viewMeta.getTableName(4))
    assertEquals("view_metadata", viewMeta.getTableName(5))
    assertEquals("", viewMeta.getTableName(6))

    sql """
        drop table if exists metadata
    """

    sql """
        drop view if exists view_metadata
    """
}
