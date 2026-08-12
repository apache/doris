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

package org.apache.doris.jdbc;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.JniWriterFactory;

import java.util.Arrays;
import java.util.Collections;

/**
 * Entry point of the {@code jdbc} plugin, found through
 * {@code META-INF/services/org.apache.doris.jni.spi.DorisPlugin}.
 *
 * <p>Everything a JDBC catalog needs of BE lives here, which is why the plugin owns three
 * factories rather than one: reading a table, writing to one, and testing a connection are
 * separate entry points that BE calls at unrelated times. Testing a connection is a scan of zero
 * rows, so it is a scanner factory under a name of its own rather than a fourth kind of thing.
 */
public class JdbcPlugin implements DorisPlugin {

    @Override
    public Iterable<JniScannerFactory> getScannerFactories() {
        return Arrays.asList(new JdbcScannerFactory(), new JdbcConnectionTesterFactory());
    }

    @Override
    public Iterable<JniWriterFactory> getWriterFactories() {
        return Collections.singletonList(new JdbcWriterFactory());
    }
}
