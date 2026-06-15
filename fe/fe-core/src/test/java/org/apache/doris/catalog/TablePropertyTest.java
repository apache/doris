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

package org.apache.doris.catalog;

import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TablePropertyTest {

    @Test
    public void testMediumAllocationModeAdaptiveWhenUnspecified() {
        Map<String, String> properties = Maps.newHashMap();
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildMediumAllocationMode();
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testMediumAllocationModeStrictWhenSsdSpecified() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildMediumAllocationMode();
        Assert.assertEquals(MediumAllocationMode.STRICT, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testMediumAllocationModeStrictWhenHddSpecified() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildMediumAllocationMode();
        Assert.assertEquals(MediumAllocationMode.STRICT, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testMediumAllocationModeAdaptiveWhenEmptyString() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildMediumAllocationMode();
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, tableProperty.getMediumAllocationMode());
    }
}
