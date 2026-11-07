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

package org.apache.doris.mysql.privilege;

import org.apache.doris.authentication.Principal;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class UserPropertyMetadataTimeoutTest {
    @Test
    void regularUserTimeoutDoesNotInheritTemporaryCallerDefaults() {
        String previousAuthType = Config.authentication_type;
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext caller = Mockito.mock(ConnectContext.class);
        Mockito.when(caller.getQualifiedUser()).thenReturn("metadata_user");
        Mockito.when(caller.getIsTempUser()).thenReturn(true);
        Mockito.when(caller.getAuthenticatedPrincipal()).thenReturn(Mockito.mock(Principal.class));
        Mockito.doCallRealMethod().when(caller).setThreadLocalInfo();
        caller.setThreadLocalInfo();
        try {
            Config.authentication_type = "DEFAULT";
            UserPropertyMgr properties = new UserPropertyMgr();
            Assertions.assertEquals(-1, properties.getQueryTimeout("metadata_user"));
            Assertions.assertEquals(0, properties.getQueryTimeoutForRegularUser("metadata_user"));
            Assertions.assertSame(caller, ConnectContext.get());

            UserProperty property = Mockito.mock(UserProperty.class);
            Mockito.when(property.getQueryTimeout()).thenReturn(7);
            properties.propertyMap.put("metadata_user", property);
            Assertions.assertEquals(7, properties.getQueryTimeout("metadata_user"));
            Assertions.assertEquals(7, properties.getQueryTimeoutForRegularUser("metadata_user"));

            // Configured external authentication defaults still apply without borrowing a SQL caller.
            Config.authentication_type = "custom";
            Assertions.assertEquals(-1, properties.getQueryTimeoutForRegularUser("other_user"));
        } finally {
            Config.authentication_type = previousAuthType;
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }
}
