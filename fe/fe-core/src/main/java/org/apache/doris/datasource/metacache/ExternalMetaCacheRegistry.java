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

package org.apache.doris.datasource.metacache;

import org.apache.doris.connector.metacache.MetaCacheRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * FE logging adapter over the data-source-neutral metadata cache registry.
 *
 * <p>Engine and alias indexing live in the shared runtime. FE keeps only diagnostics
 * for duplicate built-in/plugin registrations.
 */
public class ExternalMetaCacheRegistry extends MetaCacheRegistry<ExternalMetaCache> {
    private static final Logger LOG = LogManager.getLogger(ExternalMetaCacheRegistry.class);

    @Override
    protected void onRegistered(String engineName, ExternalMetaCache cache) {
        LOG.debug("registered external meta cache engine '{}'", engineName);
    }

    @Override
    protected void onDuplicatedEngine(
            String engineName, ExternalMetaCache existing, ExternalMetaCache duplicate) {
        LOG.warn("skip duplicated external meta cache engine '{}', existing class: {}, new class: {}",
                engineName, existing.getClass().getName(), duplicate.getClass().getName());
    }

    @Override
    protected void onDuplicatedAlias(String alias, String existingEngine, String duplicateEngine) {
        LOG.warn("skip duplicated external meta cache alias '{}', existing engine: {}, new engine: {}",
                alias, existingEngine, duplicateEngine);
    }
}
