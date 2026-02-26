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
import org.springframework.ldap.query.LdapQuery;

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
 *   <li>Simplified - no connection pooling (can be added later)</li>
 * </ul>
 */
public class LdapClient {

    private static final Logger LOG = LogManager.getLogger(LdapClient.class);

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
    private final LdapTemplate ldapTemplate;

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
        this.ldapTemplate = createLdapTemplate();

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
            ldapTemplate.authenticate(
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
                String filter = groupFilter.replace("{login}", username);
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
        LOG.debug("LDAP client closed");
    }

    // ==================== Private Helper Methods ====================

    private LdapTemplate createLdapTemplate() {
        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(server);
        contextSource.setBase(baseDn);

        // Set bind credentials if provided (for group lookup)
        if (!Strings.isNullOrEmpty(bindDn)) {
            contextSource.setUserDn(bindDn);
            if (!Strings.isNullOrEmpty(bindPassword)) {
                contextSource.setPassword(bindPassword);
            }
        }

        contextSource.afterPropertiesSet();

        LdapTemplate template = new LdapTemplate(contextSource);
        template.setIgnorePartialResultException(true);
        return template;
    }

    private List<String> getDn(LdapQuery query) {
        try {
            return ldapTemplate.search(query, new AbstractContextMapper<String>() {
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
        // Replace {login} with actual username
        return filterTemplate.replace("{login}", username);
    }

    private String requireConfig(Map<String, String> config, String key, String description) {
        String value = config.get(key);
        if (Strings.isNullOrEmpty(value)) {
            throw new IllegalArgumentException(description + " (" + key + ") is required");
        }
        return value;
    }
}
