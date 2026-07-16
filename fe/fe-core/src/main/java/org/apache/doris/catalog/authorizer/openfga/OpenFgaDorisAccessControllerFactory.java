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

package org.apache.doris.catalog.authorizer.openfga;

import org.apache.doris.mysql.privilege.AccessControllerFactory;
import org.apache.doris.mysql.privilege.CatalogAccessController;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Registers the OpenFGA access controller under the identifier {@code openfga-doris}.
 *
 * <p>Like {@code RangerDorisAccessControllerFactory}, this returns a shared singleton so there is
 * one OpenFGA client (and its HTTP connection pool) per JVM. Unlike the Ranger factory, which uses
 * a fixed service name, the OpenFGA client needs runtime configuration (api_url, store_id, ...), so
 * the singleton is built lazily from the first property map rather than in a static holder. The
 * first catalog that activates this controller supplies the connection configuration for the JVM,
 * which mirrors Ranger having a single shared controller.
 */
public class OpenFgaDorisAccessControllerFactory implements AccessControllerFactory {
    private static final Logger LOG = LogManager.getLogger(OpenFgaDorisAccessControllerFactory.class);

    private static volatile OpenFgaDorisAccessController instance;

    @Override
    public String factoryIdentifier() {
        return "openfga-doris";
    }

    @Override
    public CatalogAccessController createAccessController(Map<String, String> prop) {
        if (instance == null) {
            synchronized (OpenFgaDorisAccessControllerFactory.class) {
                if (instance == null) {
                    OpenFgaConfig config = OpenFgaConfig.fromProps(prop);
                    OpenFgaClientWrapper client = new OpenFgaClientWrapper(config);
                    instance = new OpenFgaDorisAccessController(client, config);
                    LOG.info("initialized openfga-doris access controller against {}", config.getApiUrl());
                }
            }
        }
        return instance;
    }
}
