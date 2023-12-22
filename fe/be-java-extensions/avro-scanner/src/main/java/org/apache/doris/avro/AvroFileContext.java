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

package org.apache.doris.avro;

import org.apache.avro.Schema;

import java.util.Set;

public class AvroFileContext {
    private Schema schema;
    private Set<String> requiredFields;
    private Long splitStartOffset;
    private Long splitSize;

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setRequiredFields(Set<String> requiredFields) {
        this.requiredFields = requiredFields;
    }

    public void setSplitStartOffset(Long splitStartOffset) {
        this.splitStartOffset = splitStartOffset;
    }

    public void setSplitSize(Long splitSize) {
        this.splitSize = splitSize;
    }

    public Long getSplitStartOffset() {
        return this.splitStartOffset;
    }

    public Long getSplitSize() {
        return this.splitSize;
    }

    public Set<String> getRequiredFields() {
        return requiredFields;
    }
}
