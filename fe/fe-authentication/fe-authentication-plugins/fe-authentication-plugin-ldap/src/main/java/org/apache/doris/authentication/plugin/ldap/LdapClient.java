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

package org.apache.doris.authentication.plugin.ldap;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.AbstractContextMapper;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.pool2.factory.PooledContextSource;
import org.springframework.ldap.pool2.factory.PoolConfig;
import org.springframework.ldap.pool2.validation.DefaultDirContextValidator;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.support.LdapEncoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LDAP client for authentication plugin.
 *
 * <p>This class is aligned with fe-core's LdapClient but adapted for plugin architecture.
 *
 * <p>Key differences from fe-core version:
 * <ul>
 *   <li>Configuration from Map instead of global LdapConfig</li>
 *   <li>Per-integration instance instead of singleton</li>
 *   <li>added a client-side connection pool</li>
 * </ul>
 */
public class LdapClient {

    private static final Logger LOG = LogManager.getLogger(LdapClient.class);
    // pool param
    private static final String POOL_MAX_SIZE = "pool_max_size";
    private static final String POOL_MAX_SIZE_DEFAULT = "4";

    private static final String POOL_MAX_WAIT_MILLIS = "pool_max_wait_millis";
    private static final String POOL_MAX_WAIT_MILLIS_DEFAULT = "5000";

    private static final String POOL_MAX_IDLE_PER_KEY = "pool_max_idle_per_key";
    private static final String POOL_MAX_IDLE_PER_KEY_DEFAULT = "2";

    private static final String POOL_MIN_IDLE_PER_KEY = "pool_min_idle_per_key";
    private static final String POOL_MIN_IDLE_PER_KEY_DEFAULT = "0";

    private static final String POOL_TEST_ON_BORROW = "pool_test_on_borrow";
    private static final String POOL_TEST_ON_BORROW_DEFAULT = "false";

    private static final String POOL_TEST_WHILE_IDLE = "pool_test_while_idle";
    private static final String POOL_TEST_WHILE_IDLE_DEFAULT = "false";

    private static final String POOL_TIME_BETWEEN_EVICTION_RUNS = "pool_time_between_eviction_runs_millis";
    private static final String POOL_TIME_BETWEEN_EVICTION_RUNS_DEFAULT = "30000";

    private static final String POOL_VALIDATOR_BASE = "pool_validator_base";
    private static final String POOL_VALIDATOR_BASE_DEFAULT = "";

    private static final String POOL_VALIDATOR_FILTER = "pool_validator_filter";
    private static final String POOL_VALIDATOR_FILTER_DEFAULT = "(objectClass=*)";


    // LDAP configuration
    private final String server;
    private final String baseDn;
    private final String userBaseDn;
    private final String userFilter;
    private final String groupBaseDn;
    private final String groupFilter;
    private final String bindDn;
    private final String bindPassword;

    // LDAP template for operations
    private final LdapTemplate searchLdapTemplate; // pooled for searching
    private final LdapTemplate authLdapTemplate;   // without pool
    private final PooledContextSource pooledContextSource; // pooled context


    /**
     * Creates an LDAP client from integration configuration.
     *
     * @param config integration configuration map
     * @throws IllegalArgumentException if required config is missing
     */
    public LdapClient(Map<String, String> config) {
        // Required config
        this.server = requireConfig(config, "server", "LDAP server");
        this.baseDn = requireConfig(config, "base_dn", "LDAP base DN");

        // Optional config with defaults
        // Use RELATIVE paths (without base_dn) since we set base in LdapContextSource
        this.userBaseDn = config.getOrDefault("user_base_dn", "ou=users");
        this.userFilter = config.getOrDefault("user_filter", "(uid={login})");
        this.groupBaseDn = config.getOrDefault("group_base_dn", "ou=groups");
        this.groupFilter = config.getOrDefault("group_filter", "");
        this.bindDn = config.get("bind_dn");
        this.bindPassword = config.get("bind_password");


        // Initialize LDAP template
        LdapContextSource baseContextSource = createBaseContextSource(config);
        this.pooledContextSource = createPooledContextSource(baseContextSource, config);
        this.searchLdapTemplate = new LdapTemplate(pooledContextSource);
        this.searchLdapTemplate.setIgnorePartialResultException(true);
        this.authLdapTemplate = new LdapTemplate(baseContextSource);

        LOG.info("LDAP client created: server={}, baseDn={}", server, baseDn);
    }

    /**
     * Gets the user's Distinguished Name (DN) from LDAP.
     *
     * @param username the username
     * @return user DN, or null if user not found
     */
    public String getUserDn(String username) {
        if (Strings.isNullOrEmpty(username)) {
            return null;
        }

        try {
            String filter = getUserFilter(userFilter, username);
            List<String> userDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                    .base(userBaseDn)
                    .filter(filter));

            if (userDns == null || userDns.isEmpty()) {
                LOG.debug("User not found in LDAP: {}", username);
                return null;
            }

            if (userDns.size() > 1) {
                LOG.error("Multiple users found for username: {}, DNs: {}", username, userDns);
                throw new IllegalStateException("User not unique in LDAP: " + username);
            }

            return userDns.get(0);

        } catch (Exception e) {
            LOG.error("Failed to get user DN for: {}", username, e);
            throw new RuntimeException("Failed to query LDAP for user: " + username, e);
        }
    }

    /**
     * Validates user password against LDAP server.
     *
     * <p>This is aligned with fe-core's LdapClient.checkPassword()
     *
     * @param username the username
     * @param password the password
     * @return true if password is valid
     */
    public boolean checkPassword(String username, String password) {
        if (Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)) {
            return false;
        }

        try {
            // Use Spring LDAP's authenticate method
            // This creates a new connection with user credentials
            String filter = getUserFilter(userFilter, username);
            authLdapTemplate.authenticate(
                    org.springframework.ldap.query.LdapQueryBuilder.query()
                            .base(userBaseDn)
                            .filter(filter),
                    password);
            return true;
        } catch (Exception e) {
            LOG.info("LDAP password validation failed for user: {}", username);
            LOG.debug("Password validation error details", e);
            return false;
        }
    }

    /**
     * Gets LDAP groups for a user.
     *
     * <p>This is aligned with fe-core's LdapClient.getGroups()
     *
     * @param username the username
     * @return list of group names (not DNs)
     */
    public List<String> getGroups(String username) {
        List<String> groups = Lists.newArrayList();

        if (Strings.isNullOrEmpty(groupBaseDn)) {
            return groups;
        }

        String userDn = getUserDn(username);
        if (userDn == null) {
            return groups;
        }

        try {
            List<String> groupDns;

            if (!Strings.isNullOrEmpty(groupFilter)) {
                // Support Open Directory implementations with custom filter
                String filter = groupFilter.replace("{login}", LdapEncoder.filterEncode(username));
                groupDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                        .attributes("dn")
                        .base(groupBaseDn)
                        .filter(filter));
            } else {
                // Standard LDAP using member attribute
                groupDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                        .base(groupBaseDn)
                        .where("member").is(userDn));
            }

            if (groupDns == null) {
                return groups;
            }

            // Extract group names from DNs
            // e.g., "cn=developers,ou=groups,dc=example,dc=com" -> "developers"
            for (String dn : groupDns) {
                String[] parts = dn.split("[,=]", 3);
                if (parts.length > 2) {
                    groups.add(parts[1]);
                }
            }

            LOG.debug("Retrieved {} LDAP groups for user {}: {}", groups.size(), username, groups);
            return groups;

        } catch (Exception e) {
            LOG.error("Failed to retrieve LDAP groups for user: {}", username, e);
            return groups;
        }
    }

    /**
     * Closes the LDAP client and releases resources.
     */
    public void close() {
        // Spring LdapTemplate doesn't require explicit cleanup
        // Connection pooling resources are managed by context source
        if (pooledContextSource != null) {
            try {
                pooledContextSource.destroy();
                LOG.info("LDAP connection pool closed successfully");
            } catch (Exception e) {
                LOG.error("failed to close pooledContextSource:", e);
                LOG.debug("LDAP client closed failed, failed to destroy pooledContextSource");
            }
        }

    }

    // ==================== Private Helper Methods ====================
    private PooledContextSource createPooledContextSource(LdapContextSource baseSource, Map<String, String> param) {
        PoolConfig config = new PoolConfig();
        // pool param
        config.setMaxTotal(Integer.parseInt(param.getOrDefault(POOL_MAX_SIZE, POOL_MAX_SIZE_DEFAULT)));
        config.setMaxIdlePerKey(Integer.parseInt(
                param.getOrDefault(POOL_MAX_IDLE_PER_KEY, POOL_MAX_IDLE_PER_KEY_DEFAULT)));
        config.setMaxIdlePerKey(Integer.parseInt(
            param.getOrDefault(POOL_MIN_IDLE_PER_KEY, POOL_MIN_IDLE_PER_KEY_DEFAULT)));
        config.setMaxWaitMillis(Long.parseLong(
            param.getOrDefault(POOL_MAX_WAIT_MILLIS, POOL_MAX_WAIT_MILLIS_DEFAULT)));
        // setup test for connections
        config.setTestOnBorrow(Boolean.parseBoolean(
            param.getOrDefault(POOL_TEST_ON_BORROW, POOL_TEST_ON_BORROW_DEFAULT)));
        config.setTestWhileIdle(Boolean.parseBoolean(
            param.getOrDefault(POOL_TEST_WHILE_IDLE, POOL_TEST_WHILE_IDLE_DEFAULT)));
        config.setTimeBetweenEvictionRunsMillis(Long.parseLong(
            param.getOrDefault(POOL_TIME_BETWEEN_EVICTION_RUNS, POOL_TIME_BETWEEN_EVICTION_RUNS_DEFAULT)));
        PooledContextSource pooledSource = new PooledContextSource(config);
        // validate connections
        if (config.isTestOnBorrow() || config.isTestWhileIdle()) {
            DefaultDirContextValidator validator = new DefaultDirContextValidator();
            validator.setBase(param.getOrDefault(POOL_VALIDATOR_BASE, POOL_VALIDATOR_BASE_DEFAULT));
            validator.setFilter(param.getOrDefault(POOL_VALIDATOR_FILTER, POOL_VALIDATOR_FILTER_DEFAULT));
            pooledSource.setDirContextValidator(validator);
        }
        pooledSource.setContextSource(baseSource);
        return pooledSource;
    }

    private LdapContextSource createBaseContextSource(Map<String, String> config) {
        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(server);
        contextSource.setBase(baseDn);
        if (!Strings.isNullOrEmpty(bindDn)) {
            contextSource.setUserDn(bindDn);
            if (!Strings.isNullOrEmpty(bindPassword)) {
                contextSource.setPassword(bindPassword);
            }
        }
        Map<String, Object> envProps = new HashMap<>();
        //  envProps.put("com.sun.jndi.ldap.connect.timeout",
        //      config.getOrDefault("com.sun.jndi.ldap.connect.timeout","5000"));
        //  envProps.put("com.sun.jndi.ldap.read.timeout",
        //      config.getOrDefault("com.sun.jndi.ldap.read.timeout","5000"));
        // load ldap setting if exists
        for (String key: config.keySet()) {
            if (null!=key && key.startsWith("com.sun.jndi.ldap")) {
                envProps.put(key, config.get(key));
                LOG.info("set {} = {}", key, config.get(key));
            }
        }
        contextSource.setBaseEnvironmentProperties(envProps);
        contextSource.afterPropertiesSet();
        return contextSource;
    }

    private List<String> getDn(LdapQuery query) {
        try {
            return searchLdapTemplate.search(query, new AbstractContextMapper<>() {
              @Override
              protected String doMapFromContext(DirContextOperations ctx) {
                return ctx.getNameInNamespace();
              }
            });
        } catch (Exception e) {
            LOG.error("LDAP search failed", e);
            throw new RuntimeException("LDAP query failed: " + e.getMessage(), e);
        }
    }

    private String getUserFilter(String filterTemplate, String username) {
        // Replace {login} with escaped username to prevent LDAP filter injection (RFC 4515)
        return filterTemplate.replace("{login}", LdapEncoder.filterEncode(username));
    }

    private String requireConfig(Map<String, String> config, String key, String description) {
        String value = config.get(key);
        if (Strings.isNullOrEmpty(value)) {
            throw new IllegalArgumentException(description + " (" + key + ") is required");
        }
        return value;
    }
}
