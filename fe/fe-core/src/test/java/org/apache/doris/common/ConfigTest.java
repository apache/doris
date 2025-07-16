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

package org.apache.doris.common;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class ConfigTest {
    @Test
    public void test() {
        System.out.println(Config.max_be_exec_version);
        System.out.println(Config.min_be_exec_version);
        System.out.println(Config.be_exec_version);
        // YOU MUST NOT CHANGE THIS TEST !
        // if you want to change be_exec_version, you should know what you are doing
        Assertions.assertEquals(7, Config.max_be_exec_version);
        Assertions.assertEquals(0, Config.min_be_exec_version);
        Assertions.assertEquals(6, Config.be_exec_version);
    }
}
