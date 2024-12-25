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

package org.apache.doris.datasource.es;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.SchemaCacheValue;

import java.util.List;
import java.util.Map;

public class EsSchemaCacheValue extends SchemaCacheValue {
    public Map<String, String> column2typeMap;

    public EsSchemaCacheValue(List<Column> columns, Map<String, String> column2typeMap) {
        super(columns);
        this.column2typeMap = column2typeMap;
    }

    public Map<String, String> getColumn2typeMap() {
        return column2typeMap;
    }
}
