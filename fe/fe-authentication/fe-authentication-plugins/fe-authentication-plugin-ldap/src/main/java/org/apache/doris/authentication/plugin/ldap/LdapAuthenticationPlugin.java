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

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authentication.spi.AuthenticationPlugin;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LDAP authentication plugin.
 *
 * <p>This plugin implements LDAP authentication aligned with fe-core's LdapAuthenticator.
 *
 * <p>Responsibilities (Authentication Layer):
 * <ul>
 *   <li>Validate LDAP password against LDAP server</li>
 *   <li>Extract LDAP groups from LDAP server</li>
 *   <li>Return Principal{username, externalGroups}</li>
 * </ul>
 *
 * <p>NOT responsibilities (delegated to Authorization Layer / fe-core):
 * <ul>
 *   <li>ROLE_MAPPING: groups → roles (handled by fe-core/Auth)</li>
 *   <li>Permission checking (handled by fe-core/Auth)</li>
 *   <li>JIT user creation (handled by fe-core/Auth)</li>
 * </ul>
 *
 * <p>Configuration example:
 * <pre>
 * CREATE AUTHENTICATION INTEGRATION corp_ldap
 *   TYPE = 'ldap'
 *   WITH (
 *     'server' = 'ldap://ldap.example.com:389',
 *     'base_dn' = 'dc=example,dc=com',
 *     'user_base_dn' = 'ou=users,dc=example,dc=com',
 *     'user_filter' = '(uid={login})',
 *     'group_base_dn' = 'ou=groups,dc=example,dc=com',
 *     'bind_dn' = 'cn=admin,dc=example,dc=com',
 *     'bind_password' = 'admin_password'
 *   );
 * </pre>
 */
public class LdapAuthenticationPlugin implements AuthenticationPlugin {

    private static final Logger LOG = LogManager.getLogger(LdapAuthenticationPlugin.class);

    public static final String PLUGIN_NAME = "ldap";

    // One plugin instance serves one AuthenticationIntegration.
    private volatile LdapClient ldapClient;

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public String description() {
        return "LDAP authentication plugin - validates credentials against LDAP server";
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        // LDAP requires clear text password
        return CredentialType.CLEAR_TEXT_PASSWORD.equalsIgnoreCase(request.getCredentialType());
    }

    @Override
    public boolean requiresClearPassword() {
        return true;
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request,
                                             AuthenticationIntegration integration)
            throws AuthenticationException {

        String username = request.getUsername();
        if (Strings.isNullOrEmpty(username)) {
            return AuthenticationResult.failure("Username is required");
        }

        // Get clear text password
        byte[] credentialBytes = request.getCredential();
        if (credentialBytes == null || credentialBytes.length == 0) {
            return AuthenticationResult.failure("Password is required for LDAP authentication");
        }
        String password = new String(credentialBytes, StandardCharsets.UTF_8);

        // Get or create LDAP client for this integration
        LdapClient ldapClient = getOrCreateClient(integration.getProperties());

        try {
            // Step 1: Check if user exists in LDAP
            String userDn = ldapClient.getUserDn(username);
            if (userDn == null) {
                LOG.info("LDAP user not found: {}", username);
                return AuthenticationResult.failure("User not found in LDAP: " + username);
            }

            // Step 2: Validate password
            boolean passwordValid = ldapClient.checkPassword(username, password);
            if (!passwordValid) {
                LOG.info("LDAP password validation failed for user: {}", username);
                return AuthenticationResult.failure("Invalid LDAP password");
            }

            // Step 3: Extract LDAP groups
            List<String> ldapGroups = ldapClient.getGroups(username);
            Set<String> externalGroups = new HashSet<>(ldapGroups);

            if (LOG.isDebugEnabled()) {
                LOG.debug("LDAP authentication successful for user: {}, groups: {}",
                          username, externalGroups);
            }

            // Step 4: Build Principal with external groups
            // Note: ROLE_MAPPING (groups → roles) is handled by fe-core/Auth, NOT here
            Principal principal = BasicPrincipal.builder()
                    .name(username)
                    .authenticator(PLUGIN_NAME)
                    .externalPrincipal(userDn)  // LDAP DN
                    .externalGroups(externalGroups)  // LDAP groups
                    .build();

            return AuthenticationResult.success(principal);

        } catch (Exception e) {
            LOG.error("LDAP authentication error for user: {}", username, e);
            throw new AuthenticationException("LDAP authentication failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void validate(AuthenticationIntegration integration) throws AuthenticationException {
        Map<String, String> properties = integration.getProperties();

        // Validate required configuration
        String server = properties.get("server");
        if (Strings.isNullOrEmpty(server)) {
            throw new AuthenticationException("LDAP server is required");
        }

        String baseDn = properties.get("base_dn");
        if (Strings.isNullOrEmpty(baseDn)) {
            throw new AuthenticationException("LDAP base_dn is required");
        }

        // Optional but recommended: bind credentials for group lookup
        String bindDn = properties.get("bind_dn");
        String bindPassword = properties.get("bind_password");
        if (!Strings.isNullOrEmpty(bindDn) && Strings.isNullOrEmpty(bindPassword)) {
            LOG.warn("bind_dn is set but bind_password is missing");
        }
    }

    @Override
    public void initialize(AuthenticationIntegration integration) throws AuthenticationException {
        try {
            closeClientIfExists();
            ldapClient = createClient(integration.getProperties());
            LOG.info("LDAP client initialized for integration: {}", integration.getName());
        } catch (Exception e) {
            throw new AuthenticationException("Failed to initialize LDAP client: " + e.getMessage(), e);
        }
    }

    @Override
    public void reload(AuthenticationIntegration integration) throws AuthenticationException {
        LOG.info("Reloading LDAP client for integration: {}", integration.getName());
        initialize(integration);
    }

    @Override
    public void close() {
        closeClientIfExists();
    }

    private LdapClient getOrCreateClient(Map<String, String> config)
            throws AuthenticationException {
        LdapClient localClient = ldapClient;
        if (localClient != null) {
            return localClient;
        }
        synchronized (this) {
            if (ldapClient == null) {
                try {
                    ldapClient = createClient(config);
                } catch (Exception e) {
                    throw new AuthenticationException("Failed to create LDAP client: " + e.getMessage(), e);
                }
            }
            return ldapClient;
        }
    }

    private void closeClientIfExists() {
        LdapClient oldClient = ldapClient;
        ldapClient = null;
        if (oldClient != null) {
            oldClient.close();
        }
    }

    /**
     * Creates a new LDAP client.
     * Protected for testing purposes (to allow mocking).
     */
    protected LdapClient createClient(Map<String, String> config) {
        return new LdapClient(config);
    }
}
