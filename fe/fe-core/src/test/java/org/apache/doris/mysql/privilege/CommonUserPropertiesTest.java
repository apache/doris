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

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class CommonUserPropertiesTest {
    private static final String RESOURCE_TAG_JSON = "[{\"type\":\"location\",\"value\":\"group_a\"}]";

    @Test
    public void testDeserializeResourceTagsFromAllFieldNames() {
        for (String fieldName : new String[] {"rt", "resourceTag", "resourceTags"}) {
            String json = String.format("{\"%s\":%s}", fieldName, RESOURCE_TAG_JSON);
            CommonUserProperties properties = GsonUtils.GSON.fromJson(json, CommonUserProperties.class);

            Assertions.assertEquals(Collections.singleton(Tag.createNotCheck(Tag.TYPE_LOCATION, "group_a")),
                    properties.getResourceTags());
        }
    }

    @Test
    public void testDeserializeSqlBlockRulesFromAllFieldNames() {
        for (String fieldName : new String[] {"sbr", "sqlBlockRule", "sqlBlockRules"}) {
            String json = String.format("{\"%s\":\"rule_a, rule_b\"}", fieldName);
            CommonUserProperties properties = GsonUtils.GSON.fromJson(json, CommonUserProperties.class);

            Assertions.assertEquals("rule_a, rule_b", properties.getSqlBlockRules());
            Assertions.assertArrayEquals(new String[] {"rule_a", "rule_b"}, properties.getSqlBlockRulesSplit());
        }
    }

    @Test
    public void testDeserializeLegacyCommonUserProperties() {
        String json = "{"
                + "\"maxConn\":101,"
                + "\"maxQueryInstances\":102,"
                + "\"parallelFragmentExecInstanceNum\":103,"
                + "\"sqlBlockRules\":\"rule_a, rule_b\","
                + "\"cpuResourceLimit\":104,"
                + "\"resourceTags\":" + RESOURCE_TAG_JSON + ","
                + "\"execMemLimit\":105,"
                + "\"queryTimeout\":106,"
                + "\"insertTimeout\":107,"
                + "\"workloadGroup\":\"legacy_group\","
                + "\"enablePreferCachedRowset\":true,"
                + "\"queryFreshnessTolerance\":108"
                + "}";
        CommonUserProperties properties = GsonUtils.GSON.fromJson(json, CommonUserProperties.class);

        Assertions.assertEquals(101L, properties.getMaxConn());
        Assertions.assertEquals(102L, properties.getMaxQueryInstances());
        Assertions.assertEquals(103, properties.getParallelFragmentExecInstanceNum());
        Assertions.assertEquals("rule_a, rule_b", properties.getSqlBlockRules());
        Assertions.assertArrayEquals(new String[] {"rule_a", "rule_b"}, properties.getSqlBlockRulesSplit());
        Assertions.assertEquals(104, properties.getCpuResourceLimit());
        Assertions.assertEquals(Collections.singleton(Tag.createNotCheck(Tag.TYPE_LOCATION, "group_a")),
                properties.getResourceTags());
        Assertions.assertEquals(105L, properties.getExecMemLimit());
        Assertions.assertEquals(106, properties.getQueryTimeout());
        Assertions.assertEquals(107, properties.getInsertTimeout());
        Assertions.assertEquals("legacy_group", properties.getWorkloadGroup());
        Assertions.assertTrue(properties.getEnablePreferCachedRowset());
        Assertions.assertEquals(108L, properties.getQueryFreshnessToleranceMs());
    }
}
