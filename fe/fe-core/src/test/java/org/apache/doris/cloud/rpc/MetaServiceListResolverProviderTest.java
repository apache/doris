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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class MetaServiceListResolverProviderTest {

    @Test
    public void testIpv6AddressListTarget() throws Exception {
        String address = "[2001:db8::35]:5000,[2001:db8:1::48]:5000";
        URI targetUri = new URI(MetaServiceListResolverProvider.toTarget(address));

        Assertions.assertNull(targetUri.getAuthority());
        Assertions.assertEquals(address, MetaServiceListResolverProvider.getAddressList(targetUri));
    }

    @Test
    public void testIpv4AddressListTarget() throws Exception {
        String address = "192.168.1.1:5000,192.168.1.2:5000";
        URI targetUri = new URI(MetaServiceListResolverProvider.toTarget(address));

        Assertions.assertEquals(address, MetaServiceListResolverProvider.getAddressList(targetUri));
    }

    @Test
    public void testLegacyAuthorityTarget() throws Exception {
        String address = "192.168.1.1:5000,192.168.1.2:5000";
        URI targetUri = new URI("ms-list://" + address);

        Assertions.assertEquals(address, MetaServiceListResolverProvider.getAddressList(targetUri));
    }
}
