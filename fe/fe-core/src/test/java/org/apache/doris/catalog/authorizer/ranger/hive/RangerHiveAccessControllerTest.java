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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Set;

public class RangerHiveAccessControllerTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("user1", "%");

    /**
     * A Ranger policy may be written against a role, and which roles a Doris account holds is the engine's to
     * know: the source has no user directory of its own to look them up in.
     */
    @Test
    public void testRequestCarriesTheRolesTheEngineKnows() {
        Set<String> roles = ImmutableSet.of("analyst");
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Mockito.when(context.rolesOf(SUBJECT)).thenReturn(roles);

        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                RangerAccessRequestImpl request = controller.createRequest(SUBJECT);

                Assert.assertEquals("user1", request.getUser());
                Assert.assertEquals(roles, request.getUserRoles());
                Assert.assertEquals("%", request.getClientIPAddress());
            } finally {
                controller.close();
            }
        }
    }

    /**
     * Only the checks the engine asks by name map onto a Hive access type. Anything else - a requirement put
     * together for one statement, for instance - maps to one no Hive policy grants, rather than to a
     * neighbouring access type that some policy might.
     */
    @Test
    public void testAccessTypeIsRecognisedByWhatIsAsked() {
        Assert.assertEquals(HiveAccessType.USE,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.VISIBILITY));
        Assert.assertEquals(HiveAccessType.SELECT,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.SELECT));
        Assert.assertEquals(HiveAccessType.UPDATE,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.LOAD));
        Assert.assertEquals(HiveAccessType.ALTER,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.ALTER));
        Assert.assertEquals(HiveAccessType.CREATE,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.CREATE));
        Assert.assertEquals(HiveAccessType.DROP,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.DROP));
        Assert.assertEquals(HiveAccessType.ALL,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.ADMINISTRATION));
        Assert.assertEquals(HiveAccessType.ALL,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.ANY_PRIVILEGE));

        Assert.assertEquals(HiveAccessType.NONE, RangerHiveAccessController.accessTypeOf(
                AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT)));
        Assert.assertEquals(HiveAccessType.NONE,
                RangerHiveAccessController.accessTypeOf(AccessRequirement.of(AccessAction.USAGE)));
    }
}
