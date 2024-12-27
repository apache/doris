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

package org.apache.doris.datasource.paimon;

import org.apache.paimon.types.DataField;

import java.util.List;

public class PaimonSchema {
    private final long schemaId;
    private final List<DataField> fields;
    private final List<String> partitionKeys;

    public PaimonSchema(long schemaId, List<DataField> fields, List<String> partitionKeys) {
        this.schemaId = schemaId;
        this.fields = fields;
        this.partitionKeys = partitionKeys;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public List<DataField> getFields() {
        return fields;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }
}
