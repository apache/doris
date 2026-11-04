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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.rpc.RpcException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;

public class VersionHelperTest {
    @Test
    public void testGetVisibleVersionUsesSpecifiedMaxAttempts() throws RpcException {
        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder().build();
        Cloud.GetVersionResponse failedResponse = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.KV_TXN_GET_ERR))
                .build();
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(proxy.getVisibleVersionAsync(request))
                .thenReturn(CompletableFuture.completedFuture(failedResponse));

        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);

            Assert.assertThrows(RpcException.class, () -> VersionHelper.getVisibleVersion(request, 3));
        }

        Mockito.verify(proxy, Mockito.times(3)).getVisibleVersionAsync(request);
    }

    @Test
    public void testGetVisibleVersionStopsOnVersionNotFound() throws RpcException {
        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder().build();
        Cloud.GetVersionResponse notFoundResponse = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.VERSION_NOT_FOUND))
                .build();
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(proxy.getVisibleVersionAsync(request))
                .thenReturn(CompletableFuture.completedFuture(notFoundResponse));

        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);

            Assert.assertSame(notFoundResponse, VersionHelper.getVisibleVersion(request, 3));
        }

        Mockito.verify(proxy).getVisibleVersionAsync(request);
    }
}
