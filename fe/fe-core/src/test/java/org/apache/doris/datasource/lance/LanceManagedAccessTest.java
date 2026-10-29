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

import org.apache.doris.datasource.lance.metadata.LanceTableAccess;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListTableVersionsRequest;
import org.lance.namespace.model.ListTableVersionsResponse;
import org.lance.namespace.model.TableVersion;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** How a managed table's access, newest version and branch directory are resolved, without JNI. */
public class LanceManagedAccessTest {
    private static final String LOCATION = "s3://example-bucket/items.lance";

    /** A table_uri may add a query, such as presigned credentials, to the location; Doris opens it. */
    @Test
    public void testTableUriMayAddAQueryToTheLocation() {
        LanceNamespace namespace = describing(LOCATION, LOCATION + "?sig=abc");
        LanceTableAccess access = client(namespace).resolveTableAccess("default", "items");
        Assertions.assertTrue(access.isManagedVersioning());
        Assertions.assertEquals(LOCATION + "?sig=abc", access.getDatasetUri());

        RuntimeException elsewhere = Assertions.assertThrows(RuntimeException.class,
                () -> client(describing(LOCATION, "s3://example-bucket/other.lance?sig=abc"))
                        .resolveTableAccess("default", "items"));
        Assertions.assertTrue(elsewhere.getMessage().contains("differs from location"), elsewhere.getMessage());

        // A fragment is not a query: Lance would join a branch directory after it.
        RuntimeException fragment = Assertions.assertThrows(RuntimeException.class,
                () -> client(describing(LOCATION, LOCATION + "#f")).resolveTableAccess("default", "items"));
        Assertions.assertTrue(fragment.getMessage().contains("differs from location"), fragment.getMessage());
    }

    /**
     * Managed tables are described on every read, so their describes must not queue behind each
     * other: each describe below waits until the other one has started.
     */
    @Test
    public void testManagedDescribesRunConcurrently() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any())).thenAnswer(invocation -> {
            bothStarted.countDown();
            if (!bothStarted.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the other describe never started");
            }
            return new DescribeTableResponse().location(LOCATION).tableUri(LOCATION).managedVersioning(true);
        });
        LanceNamespaceClient client = client(namespace);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<LanceTableAccess> first = executor.submit(() -> client.resolveTableAccess("default", "a"));
            Future<LanceTableAccess> second = executor.submit(() -> client.resolveTableAccess("default", "b"));
            Assertions.assertTrue(first.get(20, TimeUnit.SECONDS).isManagedVersioning());
            Assertions.assertTrue(second.get(20, TimeUnit.SECONDS).isManagedVersioning());
        } finally {
            executor.shutdownNow();
        }
    }

    /** An s3+ddb URI commits through DynamoDB, which would record and finalize versions on read. */
    @Test
    public void testManagedTableRejectsADynamoDbCommitUri() {
        String ddb = "s3+ddb://example-bucket/items.lance";
        RuntimeException rejected = Assertions.assertThrows(RuntimeException.class,
                () -> client(describing(ddb, ddb + "?ddbTableName=commits")).resolveTableAccess("default", "items"));
        Assertions.assertTrue(rejected.getMessage().contains("s3+ddb"), rejected.getMessage());
    }

    /** A limit is an upper bound: an empty page with a token does not mean an empty history. */
    @Test
    public void testNewestVersionFollowsPageTokens() {
        LanceNamespace namespace = describing(LOCATION, LOCATION);
        Mockito.when(namespace.listTableVersions(Mockito.any())).thenReturn(
                new ListTableVersionsResponse().versions(Collections.emptyList()).pageToken("p2"),
                new ListTableVersionsResponse().versions(Collections.singletonList(
                        new TableVersion().version(3L).manifestPath("items.lance/_versions/3.manifest"))));
        LanceNamespaceClient client = client(namespace);
        LanceTableAccess access = client.resolveTableAccess("default", "items");
        Optional<TableVersion> newest = client.latestManagedVersion(access, Optional.of("dev"));
        Assertions.assertEquals(Long.valueOf(3), newest.map(TableVersion::getVersion).orElse(null));
        ArgumentCaptor<ListTableVersionsRequest> requests = ArgumentCaptor.forClass(ListTableVersionsRequest.class);
        Mockito.verify(namespace, Mockito.times(2)).listTableVersions(requests.capture());
        List<ListTableVersionsRequest> sent = requests.getAllValues();
        Assertions.assertNull(sent.get(0).getPageToken());
        Assertions.assertEquals("p2", sent.get(1).getPageToken());
        Assertions.assertEquals("dev", sent.get(1).getBranch());
        Assertions.assertEquals(Boolean.TRUE, sent.get(1).getDescending());

        // Every page empty: the chain records no version.
        LanceNamespace empty = describing(LOCATION, LOCATION);
        Mockito.when(empty.listTableVersions(Mockito.any())).thenReturn(
                new ListTableVersionsResponse().versions(Collections.emptyList()).pageToken("p2"),
                new ListTableVersionsResponse().versions(Collections.emptyList()));
        LanceNamespaceClient emptyClient = client(empty);
        Assertions.assertEquals(Optional.empty(), emptyClient.latestManagedVersion(
                emptyClient.resolveTableAccess("default", "items"), Optional.empty()));

        // A token the namespace hands out twice would loop forever.
        LanceNamespace looping = describing(LOCATION, LOCATION);
        Mockito.when(looping.listTableVersions(Mockito.any())).thenReturn(
                new ListTableVersionsResponse().versions(Collections.emptyList()).pageToken("p2"));
        LanceNamespaceClient loopingClient = client(looping);
        LanceTableAccess loopingAccess = loopingClient.resolveTableAccess("default", "items");
        Assertions.assertThrows(IllegalStateException.class,
                () -> loopingClient.latestManagedVersion(loopingAccess, Optional.empty()));
    }

    /** A branch's directory is joined as Lance joins it, and branchOf reads the branch back. */
    @Test
    public void testBranchUriFollowsLance() {
        Assertions.assertEquals("s3://b/t.lance/tree/dev", LanceCatalogClient.branchUri("s3://b/t.lance", "dev"));
        Assertions.assertEquals("s3://b/t.lance/tree/dev", LanceCatalogClient.branchUri("s3://b/t.lance/", "dev"));
        Assertions.assertEquals("s3://b/t.lance/tree/feature/x",
                LanceCatalogClient.branchUri("s3://b/t.lance", "feature/x"));
        // Lance appends path segments before a query string.
        Assertions.assertEquals("s3+ddb://b/t.lance/tree/dev?ddbTableName=t",
                LanceCatalogClient.branchUri("s3+ddb://b/t.lance?ddbTableName=t", "dev"));
        for (String root : new String[] {"s3://b/t.lance", "s3://b/t.lance?sig=abc", "file:///tmp/t.lance"}) {
            for (String branch : new String[] {"dev", "feature/x"}) {
                Assertions.assertEquals(Optional.of(branch),
                        LanceCatalogClient.branchOf(LanceCatalogClient.branchUri(root, branch), root), root);
            }
        }
    }

    private static LanceNamespace describing(String location, String tableUri) {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(new DescribeTableResponse()
                .location(location).tableUri(tableUri).managedVersioning(true));
        return namespace;
    }

    private static LanceNamespaceClient client(LanceNamespace namespace) {
        return new LanceNamespaceClient(namespace, "rest", "default", Collections.emptyList(),
                Collections.emptyList(), 60, () -> TimeUnit.MILLISECONDS.toNanos(1_000_000));
    }
}
