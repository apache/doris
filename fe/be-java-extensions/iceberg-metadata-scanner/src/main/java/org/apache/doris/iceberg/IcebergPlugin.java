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

package org.apache.doris.iceberg;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScannerFactory;

import java.util.Collections;

/**
 * Entry point of the {@code iceberg} plugin, found through
 * {@code META-INF/services/org.apache.doris.jni.spi.DorisPlugin}.
 *
 * <p>The plugin has exactly one factory because Java reads only iceberg's <em>metadata</em> tables
 * here - data files are read by BE's native readers. That is also why its factory is named
 * {@code sys-table} rather than {@code reader}: it is not the iceberg connector's reader.
 */
public class IcebergPlugin implements DorisPlugin {

    @Override
    public Iterable<JniScannerFactory> getScannerFactories() {
        return Collections.singletonList(new IcebergSysTableScannerFactory());
    }
}
