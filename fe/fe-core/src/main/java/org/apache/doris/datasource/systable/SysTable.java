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

package org.apache.doris.datasource.systable;

import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;

public abstract class SysTable {
    // eg. table$partitions
    //  sysTableName => partitions
    //  tvfName => partition_values;
    //  suffix => $partitions
    protected final String sysTableName;
    protected final String tvfName;
    protected final String suffix;

    protected SysTable(String sysTableName, String tvfName) {
        this.sysTableName = sysTableName;
        this.suffix = "$" + sysTableName.toLowerCase();
        this.tvfName = tvfName;
    }

    public String getSysTableName() {
        return sysTableName;
    }

    public boolean containsMetaTable(String tableName) {
        return tableName.endsWith(suffix) && (tableName.length() > suffix.length());
    }

    public String getSourceTableName(String tableName) {
        return tableName.substring(0, tableName.length() - suffix.length());
    }

    public abstract TableValuedFunction createFunction(String ctlName, String dbName, String sourceNameWithMetaName);

    public abstract TableValuedFunctionRef createFunctionRef(String ctlName, String dbName,
            String sourceNameWithMetaName);

    // table$partition => <table, partition>
    // table$xx$partition => <table$xx, partition>
    public static Pair<String, String> getTableNameWithSysTableName(String input) {
        int lastDollarIndex = input.lastIndexOf('$');
        if (lastDollarIndex == -1 || lastDollarIndex == input.length() - 1) {
            return Pair.of(input, "");
        } else {
            String before = input.substring(0, lastDollarIndex);
            String after = input.substring(lastDollarIndex + 1);
            return Pair.of(before, after);
        }
    }
}
