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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import com.google.common.collect.ImmutableList;

public class PlanConstructor {
    public static Table student = new Table(0L, "student", Table.TableType.OLAP,
            ImmutableList.<Column>of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                    new Column("gender", Type.INT, false, AggregateType.NONE, "0", ""),
                    new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                    new Column("age", Type.INT, true, AggregateType.NONE, "", "")));

    public static Table score = new Table(0L, "score", Table.TableType.OLAP,
            ImmutableList.<Column>of(new Column("sid", Type.INT, true, AggregateType.NONE, "0", ""),
                    new Column("cid", Type.INT, true, AggregateType.NONE, "", ""),
                    new Column("grade", Type.DOUBLE, true, AggregateType.NONE, "", "")));

    public static Table course = new Table(0L, "course", Table.TableType.OLAP,
            ImmutableList.<Column>of(new Column("cid", Type.INT, true, AggregateType.NONE, "0", ""),
                    new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                    new Column("teacher", Type.STRING, true, AggregateType.NONE, "", "")));

    public static OlapTable newOlapTable(long tableId, String tableName) {
        return new OlapTable(0L, tableName,
                ImmutableList.of(
                        new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", "")),
                KeysType.PRIMARY_KEYS, null, null);
    }

    public static Table newTable(long tableId, String tableName) {
        return new Table(tableId, tableName, Table.TableType.OLAP,
                ImmutableList.<Column>of(
                        new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", "")
                ));
    }

    // With OlapTable
    public static LogicalOlapScan newLogicalOlapScan(String tableName) {
        return new LogicalOlapScan(newOlapTable(0L, tableName), ImmutableList.of("db"));
    }

    // With Table
    public static LogicalOlapScan newLogicalOlapScanWithTable(String tableName) {
        return new LogicalOlapScan(newTable(0L, tableName), ImmutableList.of("db"));
    }
}
