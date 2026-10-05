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

package org.apache.doris.qe;

// Meta data to describe result set of show statement.
// Because ResultSetMetaData is complicated, redefine it.

import org.apache.doris.catalog.Column;

import com.google.common.collect.Lists;

import java.util.List;

public class ShowResultSetMetaData extends AbstractResultSetMetaData {

    public ShowResultSetMetaData(List<Column> columns) {
        super(columns);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public ShowResultSetMetaData build() {
            return new ShowResultSetMetaData(columns);
        }

        public Builder addColumn(Column col) {
            columns.add(col);
            return this;
        }
    }
}
