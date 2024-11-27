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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.catalog.authorizer.ranger.cache.CatalogCacheAccessController;
import org.apache.doris.catalog.authorizer.ranger.cache.RangerCache;
import org.apache.doris.catalog.authorizer.ranger.cache.RangerCacheInvalidateListener;
import org.apache.doris.mysql.privilege.CatalogAccessController;

import java.util.Map;

public class RangerCacheHiveAccessController extends CatalogCacheAccessController {

    private CatalogAccessController proxyController;
    private RangerCache cache;

    public RangerCacheHiveAccessController(Map<String, String> properties) {
        this.cache = new RangerCache();
        this.proxyController = new RangerHiveAccessController(properties, new RangerCacheInvalidateListener(cache));
        this.cache.init(proxyController);
    }

    @Override
    public CatalogAccessController getProxyController() {
        return proxyController;
    }

    @Override
    public RangerCache getCache() {
        return cache;
    }
}
