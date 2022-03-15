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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

import static org.apache.doris.catalog.S3Property.S3_ACCESS_KEY;
import static org.apache.doris.catalog.S3Property.S3_CONNECTION_TIMEOUT_MS;
import static org.apache.doris.catalog.S3Property.S3_ENDPOINT;
import static org.apache.doris.catalog.S3Property.S3_MAX_CONNECTIONS;
import static org.apache.doris.catalog.S3Property.S3_REGION;
import static org.apache.doris.catalog.S3Property.S3_REQUEST_TIMEOUT_MS;
import static org.apache.doris.catalog.S3Property.S3_ROOT_PATH;
import static org.apache.doris.catalog.S3Property.S3_SECRET_KEY;

public class RemoteStorageTest {
    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() throws Exception {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    public RemoteStorageClause createStmt(int type, String name, Map<String, String> properties) {
        RemoteStorageClause stmt = null;
        switch (type) {
            case 1:
                stmt = new AddRemoteStorageClause(name, properties);
                break;
            case 2:
                stmt = new DropRemoteStorageClause(name);
                break;
            default:
                break;
        }
        return stmt;
    }

    private Map<String, String> initPropertyMap(String type) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", type);
        properties.put(S3_ENDPOINT, "s3.amazonaws.com");
        properties.put(S3_REGION, "us");
        properties.put(S3_ROOT_PATH, "/doris");
        properties.put(S3_ACCESS_KEY, "xxxxxxx");
        properties.put(S3_SECRET_KEY, "yyyyyyyy");
        properties.put(S3_MAX_CONNECTIONS, "50");
        properties.put(S3_REQUEST_TIMEOUT_MS, "3000");
        properties.put(S3_CONNECTION_TIMEOUT_MS, "1000");
        return properties;
    }

    @Test(expected = AnalysisException.class)
    public void addRemoteStorageNullNameTest() throws Exception {
        RemoteStorageClause stmt = createStmt(1, null, initPropertyMap("s3"));
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void addRemoteStorageEmptyNameTest() throws Exception {
        RemoteStorageClause stmt = createStmt(1, "", initPropertyMap("s3"));
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void addRemoteStorageMissingPropertyTest() throws Exception {
        Map<String, String> properties = initPropertyMap("s3");
        properties.remove(S3_ENDPOINT);
        RemoteStorageClause stmt = createStmt(1, "remote_s3", properties);
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void addRemoteStorageUnknownTypeTest() throws Exception {
        RemoteStorageClause stmt = createStmt(1, "remote_s3", initPropertyMap("hdfs"));
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void addRemoteStorageEmptyTypeTest() throws Exception {
        RemoteStorageClause stmt = createStmt(1, "remote_s3", initPropertyMap("hdfs"));
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void addRemoteStorageEmptyPropertiesTest() throws Exception {
        RemoteStorageClause stmt = createStmt(1, "remote_s3", Maps.newHashMap());
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void addRemoteStorageUnknownPropertyTest() throws Exception {
        Map<String, String> properties = initPropertyMap("s3");
        properties.put("s3_unknown", "xxx");
        RemoteStorageClause stmt = createStmt(1, "remote_s3", properties);
        stmt.analyze(analyzer);
    }

    @Test
    public void addRemoteStorageNormalTest() throws Exception {
        RemoteStorageClause stmt = createStmt(1, "remote_s3", initPropertyMap("s3"));
        stmt.analyze(analyzer);
        Assert.assertEquals("ADD REMOTE STORAGE remote_s3\n" +
                "PROPERTIES (\"s3_secret_key\"  =  \"*XXX\",\n" +
                "\"s3_region\"  =  \"us\",\n" +
                "\"s3_access_key\"  =  \"xxxxxxx\",\n" +
                "\"s3_max_connections\"  =  \"50\",\n" +
                "\"s3_connection_timeout_ms\"  =  \"1000\",\n" +
                "\"type\"  =  \"s3\",\n" +
                "\"s3_root_path\"  =  \"/doris\",\n" +
                "\"s3_endpoint\"  =  \"s3.amazonaws.com\",\n" +
                "\"s3_request_timeout_ms\"  =  \"3000\")", stmt.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void dropRemoteStorageEmptyNameTest() throws Exception {
        RemoteStorageClause stmt = createStmt(2, "", Maps.newHashMap());
        stmt.analyze(analyzer);
    }

    @Test
    public void dropRemoteStorageNormalTest() throws Exception {
        RemoteStorageClause stmt = createStmt(2, "remote_s3", Maps.newHashMap());
        stmt.analyze(analyzer);
        Assert.assertEquals("DROP REMOTE STORAGE remote_s3", stmt.toSql());
    }
}
