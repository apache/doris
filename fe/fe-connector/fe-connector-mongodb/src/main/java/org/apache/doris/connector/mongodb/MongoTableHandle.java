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

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Binary;

import java.util.HexFormat;

/** Namespace and frozen metadata values only; no MongoDB clients, sessions, or cursors. */
final class MongoTableHandle implements ConnectorTableHandle {
    private static final long serialVersionUID = 1L;
    private final String database;
    private final String name;
    private final String objectKind;
    private final String metadataJson;
    private final String uuid;
    private final String schemaJson;
    private transient volatile MongoSchema schema;

    MongoTableHandle(String database, Document metadata, MongoCatalogProperties properties) {
        this.database = database;
        metadataJson = metadata.toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
        Document pinned = Document.parse(metadataJson);
        name = pinned.getString("name");
        objectKind = pinned.getString("type");
        Object identity = pinned.get("info", new Document()).get("uuid");
        uuid = identity instanceof Binary ? HexFormat.of().formatHex(((Binary) identity).getData()) : "";
        schema = MongoSchema.resolve(database + "." + name, pinned, properties);
        schemaJson = schema.toPinnedJson();
    }

    String getDatabase() {
        return database;
    }

    String getName() {
        return name;
    }

    String getObjectKind() {
        return objectKind;
    }

    String getUuid() {
        return uuid;
    }

    Document getOptions() {
        return Document.parse(metadataJson).get("options", new Document());
    }

    MongoSchema getSchema() {
        if (schema == null) {
            schema = MongoSchema.fromPinnedJson(schemaJson);
        }
        return schema;
    }
}
