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

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.UdfExecutorFactory;

import java.util.Collections;

/**
 * A plugin whose classes are packed into a jar at test time and then loaded through a real
 * {@code DorisPluginClassLoader}. It is compiled with the tests but never on the classpath the
 * loader can see, so the copy the loader produces is a genuinely separate class - which is what
 * makes the isolation assertions meaningful rather than tautological.
 */
public class TestPlugin implements DorisPlugin {

    @Override
    public Iterable<JniScannerFactory> getScannerFactories() {
        return Collections.singletonList(new TestScannerFactory());
    }

    @Override
    public Iterable<UdfExecutorFactory> getUdfExecutorFactories() {
        return Collections.singletonList(new TestUdfExecutorFactory());
    }
}
