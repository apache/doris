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

package org.apache.doris.catalog.authorizer;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AuthorizationException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerHiveAuthorizerTest {

    private RangerHiveAccessController authorizer;

    @Before
    public void testCreatePlugin() {
        Map<String, String> properties = new HashMap<>();
        properties.put("ranger.service.name", "hive");
        authorizer = new RangerHiveAccessController(properties);
    }

    @Test
    public void testCheckPrivileges() throws AuthorizationException {
        String user = "test";
        String userIP = "172.16.18.9";
        HiveAccessType accessType = HiveAccessType.SELECT;

        RangerHiveResource resource = new RangerHiveResource(HiveObjectType.TABLE, "tpch_orc", "nation");
        List<RangerHiveResource> hiveResources = new ArrayList<>();
        hiveResources.add(resource);

        UserIdentity currentUser = new UserIdentity(user, userIP);
        authorizer.checkPrivileges(currentUser, accessType, hiveResources);
    }

    @Test
    public void testGetFilterExpr() throws HiveAccessControlException {
        String user = "test";
        String userIP = "172.16.18.9";
        HiveAccessType accessType = HiveAccessType.SELECT;
        RangerHiveResource resource = new RangerHiveResource(HiveObjectType.TABLE, "tpch_orc", "nation");

        UserIdentity currentUser = new UserIdentity(user, userIP);
        authorizer.getFilterExpr(currentUser, accessType, resource);
    }

    @Test
    public void testGetColumnMask() throws HiveAccessControlException {
        String user = "test";
        String userIP = "172.16.18.9";
        HiveAccessType accessType = HiveAccessType.SELECT;
        RangerHiveResource resource = new RangerHiveResource(HiveObjectType.TABLE, "tpch_orc", "nation", "n_nationkey");

        UserIdentity currentUser = new UserIdentity(user, userIP);
        authorizer.getColumnMask(currentUser, accessType, resource);
    }
}
