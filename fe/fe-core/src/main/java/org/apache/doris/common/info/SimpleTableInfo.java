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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AnalysisException.java
// and modified by Doris

package org.apache.doris.common.info;

import java.util.Objects;

public class SimpleTableInfo {

    private final String dbName;
    private final String tbName;

    public SimpleTableInfo(String dbName, String tbName) {
        this.dbName = dbName;
        this.tbName = tbName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTbName() {
        return tbName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tbName);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        SimpleTableInfo that = (SimpleTableInfo) other;
        return Objects.equals(dbName, that.dbName) && Objects.equals(tbName, that.tbName);
    }

    @Override
    public String toString() {
        return String.format("%s.%s", dbName, tbName);
    }
}
