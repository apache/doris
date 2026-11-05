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

package org.apache.doris.catalog.authorizer.ranger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

class CompactedServicePoliciesTest {
    @AfterEach
    void restoreLibraryDefault() {
        RangerBaseModelObject.setNullSafeSupplier((String) null);
    }

    @Test
    void sharesResourceDescriptorsWithoutCombiningOrBlocksOrDroppingFakeColumns() {
        RangerBaseModelObject.setNullSafeSupplier(RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2);
        RangerPolicy first = policy("one", resource("db1", "t1", "c1", "ad_one"));
        first.setAdditionalResources(Collections.singletonList(resource("db2", "t2", "c2", "ad_one")));
        RangerPolicy second = policy("two", resource("db1", "t1", "c1", "ad_two"));
        ServicePolicies source = policies(first, second);
        ServicePolicies compacted = CompactedServicePolicies.wrap(source, true);
        compacted.dedupStrings();

        Assertions.assertSame(first.getResources().get("catalog"), second.getResources().get("catalog"));
        Assertions.assertSame(first.getResources().get("database"), second.getResources().get("database"));
        Assertions.assertSame(first.getResources().get("table"), second.getResources().get("table"));
        Assertions.assertNotSame(first.getResources().get("column"), second.getResources().get("column"));
        Assertions.assertEquals(Arrays.asList("c1", "ad_one"), first.getResources().get("column").getValues());
        Assertions.assertEquals(Arrays.asList("c1", "ad_two"), second.getResources().get("column").getValues());
        Assertions.assertEquals("db2", first.getAdditionalResources().get(0).get("database").getValues().get(0));
        Assertions.assertEquals("t2", first.getAdditionalResources().get(0).get("table").getValues().get(0));
        Assertions.assertEquals(1, first.getAdditionalResources().size());
    }

    @Test
    void resourceFlagsAndOrderedValuesRemainDistinct() {
        RangerBaseModelObject.setNullSafeSupplier(RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2);
        RangerPolicy first = policy("one", resource("db", "t", "c", "ad"));
        RangerPolicy excluded = policy("two", resource("db", "t", "c", "ad"));
        excluded.getResources().get("table").setIsExcludes(true);
        RangerPolicy recursive = policy("three", resource("db", "t", "c", "ad"));
        recursive.getResources().get("table").setIsRecursive(true);
        ServicePolicies compacted = CompactedServicePolicies.wrap(policies(first, excluded, recursive), true);
        compacted.dedupStrings();
        Assertions.assertFalse(first.getResources().get("table").getIsExcludes());
        Assertions.assertTrue(excluded.getResources().get("table").getIsExcludes());
        Assertions.assertTrue(recursive.getResources().get("table").getIsRecursive());
        Assertions.assertNotSame(first.getResources().get("table"), excluded.getResources().get("table"));
        Assertions.assertNotSame(first.getResources().get("table"), recursive.getResources().get("table"));
    }

    @Test
    void independentlyLoadedSnapshotsDoNotShareTheirPool() {
        RangerBaseModelObject.setNullSafeSupplier(RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2);
        ServicePolicies first = CompactedServicePolicies.wrap(policies(policy("one", resource("db", "t", "c"))), true);
        ServicePolicies second = CompactedServicePolicies.wrap(policies(policy("one", resource("db", "t", "c"))), true);
        first.dedupStrings();
        second.dedupStrings();
        Assertions.assertNotSame(first.getPolicies().get(0).getResources().get("table"),
                second.getPolicies().get(0).getResources().get("table"));
    }

    @Test
    void diskCacheRoundTripPreservesFullSnapshotAndDeltaHeaders() throws Exception {
        for (String supplier : Arrays.asList("v1", "v2")) {
            RangerBaseModelObject.setNullSafeSupplier(supplier);
            RangerPolicy first = policy("one", resource("db", "t", "c", "ad"));
            first.setId(7L);
            first.setVersion(8L);
            ServicePolicies source = policies(first);
            source.setServiceId(9L);
            source.setPolicyVersion(10L);
            source.setPolicyUpdateTime(new Date(1234L));
            source.setServiceConfig(Collections.singletonMap("key", "value"));
            RangerPolicyDelta delta = new RangerPolicyDelta();
            delta.setPolicy(policy("delta", resource("db", "t2", "c2")));
            source.setPolicyDeltas(Collections.singletonList(delta));
            ObjectMapper mapper = new ObjectMapper();
            String original = mapper.writeValueAsString(source);
            StringWriter originalCache = new StringWriter();
            JsonUtils.objectToWriter(originalCache, source);
            ServicePolicies compacted = CompactedServicePolicies.wrap(source, true);
            compacted.dedupStrings();
            String serialized = mapper.writeValueAsString(compacted);
            Assertions.assertEquals(mapper.readTree(original), mapper.readTree(serialized));
            StringWriter compactedCache = new StringWriter();
            JsonUtils.objectToWriter(compactedCache, compacted);
            Assertions.assertEquals(mapper.readTree(originalCache.toString()),
                    mapper.readTree(compactedCache.toString()));
            ServicePolicies rangerRestored = JsonUtils.jsonToObject(new StringReader(compactedCache.toString()),
                    ServicePolicies.class);
            Assertions.assertNotNull(rangerRestored);
            Assertions.assertEquals(source.getPolicyVersion(), rangerRestored.getPolicyVersion());
            ServicePolicies restored = mapper.readValue(serialized, ServicePolicies.class);
            Assertions.assertEquals(source.getServiceId(), restored.getServiceId());
            Assertions.assertEquals(source.getPolicyVersion(), restored.getPolicyVersion());
            Assertions.assertEquals(source.getPolicyUpdateTime(), restored.getPolicyUpdateTime());
            Assertions.assertEquals(source.getPolicies().get(0).getResources(),
                    restored.getPolicies().get(0).getResources());
            Assertions.assertEquals(source.getPolicyDeltas().get(0).getPolicy().getResources(),
                    restored.getPolicyDeltas().get(0).getPolicy().getResources());
        }
    }

    @Test
    void compactsWhenStringDeduplicationIsDisabledAndLeavesMalformedValuesToRanger() {
        RangerBaseModelObject.setNullSafeSupplier(RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2);
        RangerPolicy first = policy("one", resource("db", "t", "c"));
        RangerPolicy second = policy("two", resource("db", "t", "c"));
        first.getResources().get("column").setValues(new ArrayList<>(Arrays.asList("c", null)));
        CompactedServicePolicies.wrap(policies(first, second), false);
        Assertions.assertSame(first.getResources().get("table"), second.getResources().get("table"));
        Assertions.assertEquals(Arrays.asList("c", null), first.getResources().get("column").getValues());
    }

    private static ServicePolicies policies(RangerPolicy... policies) {
        ServicePolicies result = new ServicePolicies();
        result.setServiceName("doris");
        result.setPolicies(new ArrayList<>(Arrays.asList(policies)));
        return result;
    }

    private static RangerPolicy policy(String name, Map<String, RangerPolicyResource> resources) {
        RangerPolicy result = new RangerPolicy();
        result.setName(name);
        result.setResources(resources);
        return result;
    }

    private static Map<String, RangerPolicyResource> resource(String database, String table, String... columns) {
        Map<String, RangerPolicyResource> result = new HashMap<>();
        result.put("catalog", new RangerPolicyResource("internal"));
        result.put("database", new RangerPolicyResource(database));
        result.put("table", new RangerPolicyResource(table));
        result.put("column", new RangerPolicyResource(new ArrayList<>(Arrays.asList(columns)), false, false));
        return result;
    }
}
