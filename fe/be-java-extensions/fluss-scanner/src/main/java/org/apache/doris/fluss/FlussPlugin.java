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

package org.apache.doris.fluss;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScannerFactory;

import java.util.Collections;

/**
 * Entry point of the {@code fluss} plugin, found through
 * {@code META-INF/services/org.apache.doris.jni.spi.DorisPlugin}.
 *
 * <p>Reading only: nothing writes to fluss through JNI. The lake half of a fluss table is not read
 * here either - BE hands a lake split to the paimon plugin, and this one sees only the ranges that
 * are still in fluss.
 */
public class FlussPlugin implements DorisPlugin {

    @Override
    public Iterable<JniScannerFactory> getScannerFactories() {
        return Collections.singletonList(new FlussScannerFactory());
    }
}
