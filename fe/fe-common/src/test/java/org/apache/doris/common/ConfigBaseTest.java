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

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

public class ConfigBaseTest {

    @Test
    public void testReplacedByEnv() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ConfigBase configBase = new ConfigBase();
        Method replacedByEnvMethod = ConfigBase.class.getDeclaredMethod("replacedByEnv", Properties.class);
        replacedByEnvMethod.setAccessible(true);

        final String confKeyName = "LOG_DIRS";
        Properties properties = new Properties();

        String envName = "DORIS_HOME_key_123";
        System.setProperty(envName, "test_dir");

        // format: $env
        properties.setProperty(confKeyName, "$" + envName + "/conf");
        replacedByEnvMethod.invoke(configBase, properties);
        Assert.assertEquals("test_dir/conf", properties.getProperty(confKeyName));

        // format: ${env}
        properties.setProperty(confKeyName, "${" + envName + "}/conf");
        replacedByEnvMethod.invoke(configBase, properties);
        Assert.assertEquals("test_dir/conf", properties.getProperty(confKeyName));

        // format: ${env/}
        properties.setProperty(confKeyName, "${" + envName + "/}/conf");
        Assert.assertThrows("no such env variable", Exception.class,
                () -> replacedByEnvMethod.invoke(configBase, properties));

        // format: $env, get value from system env
        envName = "DORIS_HOME_key_123_env";
        properties.setProperty(confKeyName, "$" + envName + "/conf");
        replacedByEnvMethod.invoke(configBase, properties);
        Assert.assertEquals("test_dir_env/conf", properties.getProperty(confKeyName));

        properties.setProperty(confKeyName, "$" + envName + "/conf");
        replacedByEnvMethod.invoke(configBase, properties);
        Assert.assertEquals("test_dir_env/conf", properties.getProperty(confKeyName));
    }

}
