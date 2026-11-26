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

package org.apache.doris.jni.testplugin;

import org.apache.doris.jni.spi.JniScanner;

import java.util.HashMap;
import java.util.Map;

/**
 * Reports, through the SPI's own statistics channel, facts a test cannot otherwise observe: which
 * classloader defined this class and which one was current when the factory ran.
 */
public class TestScanner extends JniScanner {

    private final Map<String, String> params;
    private final String contextClassLoaderAtCreate;

    public TestScanner(int batchSize, Map<String, String> params, String contextClassLoaderAtCreate) {
        this.params = params;
        this.contextClassLoaderAtCreate = contextClassLoaderAtCreate;
        this.batchSize = batchSize;
    }

    @Override
    protected void openInternal() {
    }

    @Override
    protected void closeInternal() {
    }

    @Override
    protected int getNext() {
        return 0;
    }

    @Override
    protected Map<String, String> collectStatistics() {
        Map<String, String> statistics = new HashMap<>();
        statistics.put("contextClassLoaderAtCreate", contextClassLoaderAtCreate);
        statistics.put("definingClassLoader", String.valueOf(getClass().getClassLoader()));
        statistics.put("batchSize", String.valueOf(batchSize));
        statistics.put("params", String.valueOf(params));
        return statistics;
    }
}
