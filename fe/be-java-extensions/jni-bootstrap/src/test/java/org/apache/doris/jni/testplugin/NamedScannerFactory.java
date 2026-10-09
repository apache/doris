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
import org.apache.doris.jni.spi.JniScannerFactory;

import java.util.Map;

/** A scanner factory whose name is chosen by the plugin, used to build malformed plugins. */
public class NamedScannerFactory implements JniScannerFactory {

    private final String name;

    public NamedScannerFactory(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public JniScanner create(int batchSize, Map<String, String> params) {
        return new TestScanner(batchSize, params,
                String.valueOf(Thread.currentThread().getContextClassLoader()));
    }
}
