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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Runs a call into the lake sibling connector with the context classloader pinned to the sibling's own.
 *
 * <p>The engine pins the context classloader to the plugin whose object it is about to call
 * ({@code PluginDrivenScanNode.onPluginClassLoader}), so everything this connector runs sees the FLUSS
 * plugin's loader. That is right until this connector calls the sibling itself: the sibling's SDK is
 * loaded child-first by ITS plugin, and the parts of it that discover implementations by
 * {@code ServiceLoader} (catalog factories, file IO, file formats) look them up through the context
 * classloader — which would be this plugin, where none of them exist. The failure is a
 * {@code NoClassDefFoundError} or an empty factory list at the first lake table read, not at wiring time.
 *
 * <p>Every call that crosses into the sibling goes through here. The pin target is derived from an object
 * the SIBLING created (its connector or its metadata), never named as a class: the sibling's types are
 * invisible from this loader.
 */
final class LakeSibling {

    private LakeSibling() {
    }

    /**
     * Runs {@code body} with the context classloader set to the one that loaded {@code sibling}.
     *
     * @param sibling an object created by the sibling plugin (its {@code Connector} or its metadata)
     * @param body    the call to make into the sibling
     */
    static <T> T call(Object sibling, Supplier<T> body) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(sibling.getClass().getClassLoader());
        try {
            return body.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * Calls the sibling's metadata, pinned, through the per-statement funnel: the first call in a statement
     * builds the metadata and every later one reuses that instance, mirroring what fe-core's own metadata
     * funnel does for a plain connector. Building it is inside the pin too — that call already runs the
     * sibling's code (it opens its catalog).
     *
     * <p>This is the only route to the sibling's metadata, so the pin and the memo cannot be forgotten by a
     * new caller, and the metadata gateway and the scan planner cannot end up on two different instances
     * (and therefore two different views of the lake table) within one statement.
     *
     * <p>The memo key carries the catalog id AND a role, because a fluss catalog runs two connectors
     * (itself and the paimon sibling) under ONE catalog id: keying on the id alone would collapse them onto
     * one metadata and misroute every call.
     */
    static <T> T forward(ConnectorSession session, Connector sibling,
            Function<ConnectorMetadata, T> call) {
        String key = "metadata:" + session.getCatalogId() + ":lake";
        return call(sibling, () -> call.apply(session.getStatementScope()
                .getOrCreateMetadata(key, () -> sibling.getMetadata(session))));
    }
}
