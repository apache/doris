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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Catalog;

/*
 * MasterDaemon is a kind of thread only master FE will start.
 * And it will wait master FE to be ready before running.
 */
public class MasterDaemon extends Daemon {

    public MasterDaemon() {

    }

    public MasterDaemon(String name) {
        super(name);
    }

    public MasterDaemon(String name, long intervalMs) {
        super(name, intervalMs);
    }

    @Override
    protected final void runOneCycle() {
        if (!Catalog.getInstance().isReady()) {
            // here we use getInstance(), not getCurrentCatalog() because we truly want the Catalog instance,
            // not the Checkpoint catalog instance.
            // and if catalog is not ready, do not run
            return;
        }

        runAfterCatalogReady();
    }

    // override by derived classes
    protected void runAfterCatalogReady() {

    }
}
