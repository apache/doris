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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.SchemaCacheValue;

import com.aliyun.odps.Table;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class MaxComputeSchemaCacheValue extends SchemaCacheValue {
    private Table odpsTable;
    private List<String> partitionSpecs;
    private Map<String, Column> partitionNameToColumns;
    private List<Type> partitionTypes;

    public MaxComputeSchemaCacheValue(List<Column> schema, Table odpsTable, List<String> partitionSpecs,
            Map<String, Column> partitionNameToColumns, List<Type> partitionTypes) {
        super(schema);
        this.odpsTable = odpsTable;
        this.partitionSpecs = partitionSpecs;
        this.partitionNameToColumns = partitionNameToColumns;
        this.partitionTypes = partitionTypes;
    }

    public Set<String> getPartitionColNames() {
        return partitionNameToColumns.keySet();
    }

    public List<Column> getPartitionColumns() {
        return Lists.newArrayList(partitionNameToColumns.values());
    }
}
