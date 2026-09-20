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

package org.apache.doris.datasource.lance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.RenameTableRequest;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class LanceNamespaceClientMutationTest {
    @Test
    public void testDatabaseMutationRequestsUseConfiguredParentAndModes() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceNamespaceClient client = client(namespace);

        client.createDatabase("analytics", Collections.singletonMap("owner", "doris"));
        client.dropDatabase("analytics", true, false);
        client.dropDatabase("analytics", false, true);

        ArgumentCaptor<CreateNamespaceRequest> create =
                ArgumentCaptor.forClass(CreateNamespaceRequest.class);
        Mockito.verify(namespace).createNamespace(create.capture());
        Assertions.assertEquals(Arrays.asList("tenant", "analytics"), create.getValue().getId());
        Assertions.assertEquals("Create", create.getValue().getMode());
        Assertions.assertEquals("doris", create.getValue().getProperties().get("owner"));

        ArgumentCaptor<DropNamespaceRequest> drop =
                ArgumentCaptor.forClass(DropNamespaceRequest.class);
        Mockito.verify(namespace, Mockito.times(2)).dropNamespace(drop.capture());
        Assertions.assertEquals("Skip", drop.getAllValues().get(0).getMode());
        Assertions.assertEquals("Restrict", drop.getAllValues().get(0).getBehavior());
        Assertions.assertEquals("Fail", drop.getAllValues().get(1).getMode());
        Assertions.assertEquals("Cascade", drop.getAllValues().get(1).getBehavior());
    }

    @Test
    public void testRootDatabaseExistsWithoutRemoteRequest() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceNamespaceClient client = client(namespace);

        Assertions.assertTrue(client.databaseExists("default"));

        Mockito.verify(namespace, Mockito.never()).namespaceExists(Mockito.any());
    }

    @Test
    public void testTableMutationRequestsUseFullIdentifierAndStorageOptions() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceNamespaceClient client = client(namespace);
        byte[] arrowStream = new byte[] {1, 2, 3};

        client.createTable("analytics", "events",
                Collections.singletonMap("purpose", "test"), arrowStream);
        client.renameTable("analytics", "events", "renamed_events");
        client.dropTable("analytics", "renamed_events");

        ArgumentCaptor<CreateTableRequest> create = ArgumentCaptor.forClass(CreateTableRequest.class);
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        Mockito.verify(namespace).createTable(create.capture(), payload.capture());
        Assertions.assertEquals(Arrays.asList("tenant", "analytics", "events"),
                create.getValue().getId());
        Assertions.assertEquals("Create", create.getValue().getMode());
        Assertions.assertEquals("secret",
                create.getValue().getStorageOptions().get("aws_secret_access_key"));
        Assertions.assertArrayEquals(arrowStream, payload.getValue());

        ArgumentCaptor<RenameTableRequest> rename = ArgumentCaptor.forClass(RenameTableRequest.class);
        Mockito.verify(namespace).renameTable(rename.capture());
        Assertions.assertEquals(Arrays.asList("tenant", "analytics", "events"),
                rename.getValue().getId());
        Assertions.assertEquals(Arrays.asList("tenant", "analytics"),
                rename.getValue().getNewNamespaceId());
        Assertions.assertEquals("renamed_events", rename.getValue().getNewTableName());

        ArgumentCaptor<DropTableRequest> drop = ArgumentCaptor.forClass(DropTableRequest.class);
        Mockito.verify(namespace).dropTable(drop.capture());
        Assertions.assertEquals(Arrays.asList("tenant", "analytics", "renamed_events"),
                drop.getValue().getId());
    }

    @Test
    public void testDescribeForDisplayDoesNotVendCredentials() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any()))
                .thenReturn(new DescribeTableResponse().properties(Collections.singletonMap("owner", "doris")));
        LanceNamespaceClient client = client(namespace);

        Map<String, String> properties = client.describeTable("analytics", "events", false).getProperties();

        Assertions.assertEquals("doris", properties.get("owner"));
        ArgumentCaptor<DescribeTableRequest> request =
                ArgumentCaptor.forClass(DescribeTableRequest.class);
        Mockito.verify(namespace).describeTable(request.capture());
        Assertions.assertFalse(request.getValue().getVendCredentials());
    }

    private static LanceNamespaceClient client(LanceNamespace namespace) {
        return new LanceNamespaceClient(namespace, "rest", "default",
                Collections.singletonList("tenant"), Collections.emptyList(),
                Collections.singletonMap("aws_secret_access_key", "secret"));
    }
}
