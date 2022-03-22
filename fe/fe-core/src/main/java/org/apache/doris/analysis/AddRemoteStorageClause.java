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

import org.apache.doris.catalog.RemoteStorageProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;

import static org.apache.doris.catalog.S3Property.S3_ACCESS_KEY;
import static org.apache.doris.catalog.S3Property.S3_CONNECTION_TIMEOUT_MS;
import static org.apache.doris.catalog.S3Property.S3_ENDPOINT;
import static org.apache.doris.catalog.S3Property.S3_MAX_CONNECTIONS;
import static org.apache.doris.catalog.S3Property.S3_REGION;
import static org.apache.doris.catalog.S3Property.S3_REQUEST_TIMEOUT_MS;
import static org.apache.doris.catalog.S3Property.S3_ROOT_PATH;
import static org.apache.doris.catalog.S3Property.S3_SECRET_KEY;

/**
 * Add remote storage clause
 * Syntax:
 *     ALTER SYSTEM ADD REMOTE STORAGE `remote_storage_name`
 *     PROPERTIES
 *     (
 *         "key" = "value",
 *         ...
 *     )
 */
public class AddRemoteStorageClause extends RemoteStorageClause {
    private static final String PROPERTY_MISSING_MSG = "Remote storage %s is null. " +
            "Please add properties('%s'='xxx') when create remote storage.";
    protected static final String TYPE = "type";

    private Map<String, String> properties;
    private RemoteStorageProperty.RemoteStorageType remoteStorageType;

    public AddRemoteStorageClause(String name, Map<String, String> properties) {
        super(name);
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public RemoteStorageProperty.RemoteStorageType getRemoteStorageType() {
        return remoteStorageType;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // analyze properties
        if (properties == null || properties.size() == 0) {
            throw new AnalysisException("Please add remote storage properties first. " +
                    "You can find examples in `HELP ADD REMOTE STORAGE`.");
        }
        String storageType = properties.get(TYPE);
        if (Strings.isNullOrEmpty(storageType)) {
            throw new AnalysisException("Remote storage type is empty.");
        }
        if (storageType.equalsIgnoreCase("s3")) {
            this.remoteStorageType = RemoteStorageProperty.RemoteStorageType.S3;
            analyzeS3Properties();
        } else {
            throw new AnalysisException("Not supported storage type: " + storageType);
        }
    }

    private void analyzeS3Properties() throws AnalysisException {
        Map<String, String> s3Properties = Maps.newHashMap(properties);
        s3Properties.remove(TYPE);

        if (Strings.isNullOrEmpty(s3Properties.get(S3_ENDPOINT))) {
            throw new AnalysisException(String.format(PROPERTY_MISSING_MSG, S3_ENDPOINT, S3_ENDPOINT));
        }
        s3Properties.remove(S3_ENDPOINT);

        if (Strings.isNullOrEmpty(s3Properties.get(S3_REGION))) {
            throw new AnalysisException(String.format(PROPERTY_MISSING_MSG, S3_REGION, S3_REGION));
        }
        s3Properties.remove(S3_REGION);

        if (Strings.isNullOrEmpty(s3Properties.get(S3_ROOT_PATH))) {
            throw new AnalysisException(String.format(PROPERTY_MISSING_MSG, S3_ROOT_PATH, S3_ROOT_PATH));
        }
        s3Properties.remove(S3_ROOT_PATH);

        if (Strings.isNullOrEmpty(s3Properties.get(S3_ACCESS_KEY))) {
            throw new AnalysisException(String.format(PROPERTY_MISSING_MSG, S3_ACCESS_KEY, S3_ACCESS_KEY));
        }
        s3Properties.remove(S3_ACCESS_KEY);

        if (Strings.isNullOrEmpty(s3Properties.get(S3_SECRET_KEY))) {
            throw new AnalysisException(String.format(PROPERTY_MISSING_MSG, S3_SECRET_KEY, S3_SECRET_KEY));
        }
        s3Properties.remove(S3_SECRET_KEY);
        s3Properties.remove(S3_MAX_CONNECTIONS);
        s3Properties.remove(S3_REQUEST_TIMEOUT_MS);
        s3Properties.remove(S3_CONNECTION_TIMEOUT_MS);

        if (!s3Properties.isEmpty()) {
            throw new AnalysisException("Unknown s3 remote storage properties: " + s3Properties);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD REMOTE STORAGE ")
                .append(getStorageName());
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
            sb.append(")");
        }
        return sb.toString();
    }
}
