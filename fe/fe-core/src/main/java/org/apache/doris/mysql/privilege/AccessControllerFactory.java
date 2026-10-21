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

package org.apache.doris.mysql.privilege;

import java.util.Map;

/**
 * Publishes a {@link CatalogAccessController}, the older shape of an authorization source.
 *
 * @deprecated implement {@link org.apache.doris.authorization.spi.AuthorizationPluginFactory} instead, and
 *         ship the plugin as a subdirectory of {@code authorization_plugins_dir} with the authorization
 *         plugin API version declared in its jar manifest. A factory found through this interface is still
 *         loaded, and a name published both ways resolves to the newer one; but this channel carries no
 *         declared API version, so a plugin built against an older Doris is admitted with no diagnosis.
 */
@Deprecated
public interface AccessControllerFactory {
    /**
     * Returns the identifier for the factory, such as "range-doris".
     *
     * @return the factory identifier
     */
    default String factoryIdentifier() {
        return this.getClass().getSimpleName();
    }

    CatalogAccessController createAccessController(Map<String, String> prop);
}
