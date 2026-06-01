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

package org.apache.doris.catalog;

import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Minimal persistence stub for Elasticsearch table.
 *
 * <p>This class exists solely for backward compatibility with older metadata images
 * that contain serialized EsTable entries. It preserves only the persistence fields
 * needed for Gson deserialization. All functional ES logic has been moved to
 * {@link org.apache.doris.datasource.es.EsExternalTable}.</p>
 *
 * <p>Internal ES tables are no longer supported. Users should use ES Catalog instead.</p>
 */
@Deprecated
public class EsTable extends Table implements GsonPostProcessable {

    @SerializedName("pi")
    private PartitionInfo partitionInfo;

    @SerializedName("tc")
    private Map<String, String> tableContext = new HashMap<>();

    public EsTable() {
        super(TableType.ELASTICSEARCH);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // Only restore basic fields from tableContext for metadata display.
        // No ES client is created — internal ES tables are deprecated.
    }
}
