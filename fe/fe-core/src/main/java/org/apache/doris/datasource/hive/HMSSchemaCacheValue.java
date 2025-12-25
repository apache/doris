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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.SchemaCacheValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class HMSSchemaCacheValue extends SchemaCacheValue {

    protected final Map<Long, List<Column>> partitionColumnsById;

    public HMSSchemaCacheValue(List<Column> schema, List<Column> partitionColumns) {
        super(schema);
        this.partitionColumnsById = new ConcurrentHashMap<>();
        this.partitionColumnsById.put(primaryVersionId, partitionColumns);
    }

    public HMSSchemaCacheValue(Map<Long, List<Column>> schemas, long primaryVersionId,
            Map<Long, List<Column>> partitionColumns) {
        super(schemas, primaryVersionId);
        this.partitionColumnsById = new ConcurrentHashMap<>(partitionColumns);
    }

    public List<Column> getPartitionColumns() {
        return getPartitionColumns(primaryVersionId);
    }

    public List<Column> getPartitionColumns(long versionId) {
        return partitionColumnsById.get(versionId);
    }

    public List<Type> getPartitionColTypes() {
        return getPartitionColumns().stream().map(Column::getType).collect(Collectors.toList());
    }
}
