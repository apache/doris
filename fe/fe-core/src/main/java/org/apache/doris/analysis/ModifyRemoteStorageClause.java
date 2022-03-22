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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.base.Strings;

import java.util.Map;

import static org.apache.doris.analysis.AddRemoteStorageClause.TYPE;

/**
 * Modify remote storage properties by name.
 * Syntax:
 *     ALTER SYSTEM MODIFY REMOTE STORAGE `remote_storage_name`
 *     PROPERTIES
 *     (
 *         "key" = "value",
 *         ...
 *     )
 */
public class ModifyRemoteStorageClause extends RemoteStorageClause {

    private Map<String, String> properties;

    public ModifyRemoteStorageClause(String storageName, Map<String, String> properties) {
        super(storageName);
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (properties == null || properties.size() == 0) {
            throw new AnalysisException("Empty remote storage properties.");
        }
        // Do not support modify remote storage type
        String storageType = properties.get(TYPE);
        if (!Strings.isNullOrEmpty(storageType)) {
            throw new AnalysisException("Can not modify remote storage type.");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY REMOTE STORAGE ")
                .append(getStorageName());
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
            sb.append(")");
        }
        return sb.toString();
    }
}
