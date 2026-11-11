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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;

import java.util.Objects;

/** Logical BSON tags and paths survive planning even when several BSON types share a Doris type. */
final class MongoColumnHandle implements ConnectorColumnHandle {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final String schemaVersion;
    private final String bindingJson;

    MongoColumnHandle(MongoSchema.Binding binding, String schemaVersion) {
        this.name = binding.getName();
        this.schemaVersion = schemaVersion;
        this.bindingJson = binding.toDocument().toJson();
    }

    String getSchemaVersion() {
        return schemaVersion;
    }

    String getBindingJson() {
        return bindingJson;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schemaVersion, bindingJson);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MongoColumnHandle)) {
            return false;
        }
        MongoColumnHandle column = (MongoColumnHandle) other;
        return name.equals(column.name) && schemaVersion.equals(column.schemaVersion)
                && bindingJson.equals(column.bindingJson);
    }
}
