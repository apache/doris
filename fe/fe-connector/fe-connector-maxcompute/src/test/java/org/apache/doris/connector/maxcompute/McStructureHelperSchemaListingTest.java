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

package org.apache.doris.connector.maxcompute;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class McStructureHelperSchemaListingTest {
    private final McStructureHelper helper = new McStructureHelper.ProjectSchemaTableHelper("project");

    @Test
    void skipsSchemaDeletedAfterListingAndContinues() {
        TestOdps odps = new TestOdps(new NoSuchObjectException("schema was deleted"), null);
        RecordingClient client = odps.getRestClient();
        Assertions.assertEquals(Arrays.asList("first", "last"), helper.listDatabaseNames(odps, "project"));
        Assertions.assertEquals(Arrays.asList("/projects/project/schemas", "/projects/project/schemas/first",
                "/projects/project/schemas/deleted", "/projects/project/schemas/last"), client.requests);
    }

    @Test
    void propagatesSchemaReloadFailureOtherThanDeletion() {
        OdpsException denied = new OdpsException("access denied");
        TestOdps odps = new TestOdps(denied, null);
        ReloadException failure = Assertions.assertThrows(ReloadException.class,
                () -> helper.listDatabaseNames(odps, "project"));
        Assertions.assertSame(denied, failure.getCause());
    }

    @Test
    void propagatesMissingProjectDuringListing() {
        TestOdps odps = new TestOdps(null, new NoSuchObjectException("project was deleted"));
        RecordingClient client = odps.getRestClient();
        Assertions.assertThrows(RuntimeException.class,
                () -> helper.listDatabaseNames(odps, "project"));
        Assertions.assertEquals(Arrays.asList("/projects/project/schemas"), client.requests);
    }

    private static final class TestOdps extends Odps {
        private RecordingClient recordingClient;

        private TestOdps(OdpsException schemaFailure, OdpsException listFailure) {
            super(new AliyunAccount("test-access-key", "test-secret"));
            getRestClient().schemaFailure = schemaFailure;
            getRestClient().listFailure = listFailure;
            options().setSkipCheckIfEpv2(true);
        }

        @Override
        public RecordingClient getRestClient() {
            // The SDK constructs its schema collection during the Odps constructor.
            if (recordingClient == null) {
                recordingClient = new RecordingClient();
            }
            return recordingClient;
        }
    }

    private static final class RecordingClient extends RestClient {
        private OdpsException schemaFailure;
        private OdpsException listFailure;
        private final List<String> requests = new ArrayList<>();

        private RecordingClient() {
            super(null);
        }

        @Override
        public Response request(String resource, String method, Map<String, String> params,
                Map<String, String> headers, byte[] body) throws OdpsException {
            requests.add(resource);
            Assertions.assertEquals("GET", method);
            if (resource.equals("/projects/project/schemas")) {
                if (listFailure != null) {
                    throw listFailure;
                }
                return new XmlResponse("<Schemas><Schema><Name>first</Name></Schema>"
                        + "<Schema><Name>deleted</Name></Schema><Schema><Name>last</Name></Schema></Schemas>");
            }
            if (resource.endsWith("/deleted")) {
                throw schemaFailure;
            }
            String name = resource.substring(resource.lastIndexOf('/') + 1);
            return new XmlResponse("<Schema><Name>" + name + "</Name></Schema>");
        }
    }

    private static final class XmlResponse extends Response {
        private XmlResponse(String xml) {
            status = 200;
            body = xml.getBytes(StandardCharsets.UTF_8);
            headers = new HashMap<>();
            headers.put("x-odps-creation-time", "Wed, 01 Jan 2020 00:00:00 GMT");
            headers.put("Last-Modified", "Wed, 01 Jan 2020 00:00:00 GMT");
        }
    }
}
