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

package org.apache.doris.enterprise;

import org.apache.doris.common.Config;

import org.junit.Assert;
import org.junit.Test;

public class KeyManagerTest {
    @Test
    public void testSetRootKeyByConfig() {
        Config.doris_tde_key_endpoint = "xxx";
        Config.doris_tde_key_region = "xxx";
        Config.doris_tde_key_provider = "aws_kms";
        Config.doris_tde_key_id = "";
        KeyManager manager = new KeyManager();
        IllegalArgumentException exception1 = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    manager.setRootKeyByConfig();
                }
        );

        Assert.assertTrue(exception1.getMessage().contains("some of the doris_tde-related configurations are empty"));

        Config.doris_tde_key_endpoint = "xxx";
        Config.doris_tde_key_region = "xxx";
        Config.doris_tde_key_provider = "xxx_kms";
        Config.doris_tde_key_id = "key id";
        IllegalArgumentException exception2 = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    manager.setRootKeyByConfig();
                }
        );

        Assert.assertTrue(exception2.getMessage().contains("doris_tde_key_provider"));
    }
}
