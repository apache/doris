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

package org.apache.doris.plugin.lineage;

import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.plugin.Plugin;

/**
 * An abstract base class for lineage plugins in Apache Doris.
 *
 * <p>Lineage plugins are used to capture and process data lineage information
 * during query planning. Each plugin implementation can decide whether it
 * should participate in handling a given query plan and, if so, perform
 * custom logic (e.g., recording lineage metadata, sending it to an external system, etc.).
 *
 * <p>Subclasses must implement both {@link #eventFilter()} and {@link #exec(LineageInfo)}
 * to define their activation condition and processing behavior.
 */
public abstract class AbstractLineagePlugin extends Plugin {

    /** Return the only plugin name, this is used in fe.conf. activate_lineage_plugin conf */
    public abstract String getName();

    /**
     * Determines whether this plugin should handle the current query context.
     *
     * <p>This method is typically called before plugin exec.
     * It allows the plugin to enable or disable itself based on runtime conditions
     * (e.g., session variables, configuration flags, or workload type).
     *
     * @return {@code true} if this plugin should process the upcoming plan;
     *         {@code false} otherwise.
     */
    public abstract boolean eventFilter();

    /**
     * Processes the given logical query plan to extract or act upon data lineage information.
     *
     * <p>This method is invoked only if {@link #eventFilter()} returned {@code true}.
     * The provided {@code plan} represents the optimized logical plan from the Nereids planner.
     * Implementations may traverse the plan tree to collect table/column dependencies,
     * generate lineage graphs, or integrate with external metadata systems.
     *
     * @param lineageInfo the lineageInfo used by plugin
     * @return {@code true} if handling was successful; {@code false} if an error occurred
     *         (maybe used for logging or fallback logic)
     */
    public abstract boolean exec(LineageInfo lineageInfo);
}
