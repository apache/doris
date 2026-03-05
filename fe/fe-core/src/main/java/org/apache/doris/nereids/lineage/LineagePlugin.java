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

package org.apache.doris.nereids.lineage;

import org.apache.doris.extension.spi.Plugin;

/**
 * SPI interface for lineage plugins.
 * <p>
 * Extends the generic {@link Plugin} from {@code fe-extension-spi}.
 * Implementations receive {@link LineageInfo} events from the {@link LineageEventProcessor}
 * and can filter or process them as needed.
 * </p>
 */
public interface LineagePlugin extends Plugin {

    /**
     * Returns the unique name of this plugin, used for activation via
     * {@code Config.activate_lineage_plugin}.
     */
    String name();

    /**
     * Returns {@code true} if this plugin should receive lineage events
     * under the current configuration. Called before each event dispatch.
     *
     * <p><b>Thread-safety:</b> This method is called from both the single
     * lineage worker thread and arbitrary query threads (via
     * {@code hasActivePlugins()}). Implementations must be thread-safe.
     */
    boolean eventFilter();

    /**
     * Processes a lineage event.
     *
     * <p><b>Thread-safety:</b> This method is called from a single worker
     * thread only, but may execute concurrently with {@link #eventFilter()}
     * calls from query threads.
     *
     * @param lineageInfo the extracted lineage information
     * @return {@code true} if processing succeeded
     */
    boolean exec(LineageInfo lineageInfo);
}
