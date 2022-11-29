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

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.Assert;
import org.junit.Test;

public class DataPropertyTest {

    @Test
    public void testCooldownTimeMs() throws Exception {
        Config.default_storage_medium = "ssd";
        DataProperty dataProperty = DataProperty.DEFAULT_DATA_PROPERTY;
        Assert.assertNotEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());

        dataProperty = new DataProperty(TStorageMedium.SSD);
        Assert.assertNotEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());

        long storageCooldownTimeMs = System.currentTimeMillis() + 24 * 3600 * 1000L;
        dataProperty = new DataProperty(TStorageMedium.SSD, storageCooldownTimeMs, "");
        Assert.assertEquals(storageCooldownTimeMs, dataProperty.getCooldownTimeMs());

        dataProperty = new DataProperty(TStorageMedium.HDD);
        Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());
    }
}
