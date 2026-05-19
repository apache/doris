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

package org.apache.doris.connector;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.spi.ConnectorContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Default implementation of {@link ConnectorContext}.
 *
 * <p>Provides the minimal catalog-level context that connector providers need
 * during creation. Additional context fields can be added here as the SPI evolves.
 */
public class DefaultConnectorContext implements ConnectorContext {

    private static final ExecutionAuthenticator NOOP_AUTH = new ExecutionAuthenticator() {};

    private final String catalogName;
    private final long catalogId;
    private final Map<String, String> environment;
    private final Supplier<ExecutionAuthenticator> authSupplier;

    private final ConnectorHttpSecurityHook httpSecurityHook = new ConnectorHttpSecurityHook() {
        @Override
        public void beforeRequest(String url) throws Exception {
            SecurityChecker.getInstance().startSSRFChecking(url);
        }

        @Override
        public void afterRequest() {
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    };

    public DefaultConnectorContext(String catalogName, long catalogId) {
        this(catalogName, catalogId, () -> NOOP_AUTH);
    }

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.catalogId = catalogId;
        this.authSupplier = Objects.requireNonNull(authSupplier, "authSupplier");
        this.environment = buildEnvironment();
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public Map<String, String> getEnvironment() {
        return environment;
    }

    @Override
    public ConnectorHttpSecurityHook getHttpSecurityHook() {
        return httpSecurityHook;
    }

    @Override
    public String sanitizeJdbcUrl(String jdbcUrl) {
        try {
            return SecurityChecker.getInstance().getSafeJdbcUrl(jdbcUrl);
        } catch (Exception e) {
            throw new RuntimeException("JDBC URL security check failed: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return authSupplier.get().execute(task);
    }

    private static Map<String, String> buildEnvironment() {
        Map<String, String> env = new HashMap<>();
        String dorisHome = EnvUtils.getDorisHome();
        if (dorisHome != null) {
            env.put("doris_home", dorisHome);
        }
        env.put("jdbc_drivers_dir", Config.jdbc_drivers_dir);
        env.put("force_sqlserver_jdbc_encrypt_false",
                String.valueOf(Config.force_sqlserver_jdbc_encrypt_false));
        env.put("jdbc_driver_secure_path", Config.jdbc_driver_secure_path);
        return Collections.unmodifiableMap(env);
    }
}
