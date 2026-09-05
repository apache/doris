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

package org.apache.doris.jni.spi;

import java.util.Map;

/**
 * Creates one kind of {@link JniScanner}.
 *
 * <p>A factory replaces the old convention where BE reflected on a concrete scanner class and
 * invoked its {@code (int, Map)} constructor. The constructor shape was an unwritten contract that
 * every plugin had to reproduce and that nothing checked; a factory makes it a compiled interface,
 * and lets a plugin decide per request which implementation to hand back.
 */
public interface JniScannerFactory {

    /**
     * Key BE addresses this factory by, unique within the plugin. Together with the plugin
     * directory name it forms the pair BE sends down, for example {@code (paimon, paimon)}.
     */
    String getName();

    /**
     * @param batchSize rows per batch the scanner should produce
     * @param params    scan parameters; the reserved keys are listed in PROTOCOL.md, everything
     *                  else is private between the FE side of this connector and this plugin
     */
    JniScanner create(int batchSize, Map<String, String> params);
}
