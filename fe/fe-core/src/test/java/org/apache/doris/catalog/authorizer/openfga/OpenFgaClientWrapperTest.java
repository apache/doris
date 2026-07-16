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

package org.apache.doris.catalog.authorizer.openfga;

import dev.openfga.sdk.api.client.OpenFgaClient;
import dev.openfga.sdk.api.client.model.ClientCheckRequest;
import dev.openfga.sdk.api.client.model.ClientCheckResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;

public class OpenFgaClientWrapperTest {

    private final OpenFgaCheckRequest request =
            new OpenFgaCheckRequest("user:analyst", "can_select", "table:internal/db1/tbl1");

    @Test
    public void testCheckReturnsTrueWhenOpenFgaAllows() throws Exception {
        OpenFgaClient client = Mockito.mock(OpenFgaClient.class);
        ClientCheckResponse response = Mockito.mock(ClientCheckResponse.class);
        Mockito.when(response.getAllowed()).thenReturn(true);
        Mockito.when(client.check(Mockito.any(ClientCheckRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OpenFgaClientWrapper wrapper = new OpenFgaClientWrapper(client, 5000L);

        Assert.assertTrue(wrapper.check(request));
    }

    @Test
    public void testCheckReturnsFalseWhenOpenFgaDenies() throws Exception {
        OpenFgaClient client = Mockito.mock(OpenFgaClient.class);
        ClientCheckResponse response = Mockito.mock(ClientCheckResponse.class);
        Mockito.when(response.getAllowed()).thenReturn(false);
        Mockito.when(client.check(Mockito.any(ClientCheckRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OpenFgaClientWrapper wrapper = new OpenFgaClientWrapper(client, 5000L);

        Assert.assertFalse(wrapper.check(request));
    }

    @Test
    public void testCheckFailsClosedWhenClientThrows() throws Exception {
        OpenFgaClient client = Mockito.mock(OpenFgaClient.class);
        Mockito.when(client.check(Mockito.any(ClientCheckRequest.class)))
                .thenThrow(new RuntimeException("openfga unreachable"));

        OpenFgaClientWrapper wrapper = new OpenFgaClientWrapper(client, 5000L);

        // A transport or protocol error must deny, never allow.
        Assert.assertFalse(wrapper.check(request));
    }

    @Test
    public void testCheckFailsClosedWhenFutureCompletesExceptionally() throws Exception {
        OpenFgaClient client = Mockito.mock(OpenFgaClient.class);
        CompletableFuture<ClientCheckResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("boom"));
        Mockito.when(client.check(Mockito.any(ClientCheckRequest.class))).thenReturn(failed);

        OpenFgaClientWrapper wrapper = new OpenFgaClientWrapper(client, 5000L);

        Assert.assertFalse(wrapper.check(request));
    }

    @Test
    public void testCheckReturnsFalseForNullRelationOrObject() {
        OpenFgaClient client = Mockito.mock(OpenFgaClient.class);
        OpenFgaClientWrapper wrapper = new OpenFgaClientWrapper(client, 5000L);

        Assert.assertFalse(wrapper.check(new OpenFgaCheckRequest("user:analyst", null, "table:internal/db1/tbl1")));
        Assert.assertFalse(wrapper.check(new OpenFgaCheckRequest("user:analyst", "can_select", null)));
        Mockito.verifyNoInteractions(client);
    }
}
