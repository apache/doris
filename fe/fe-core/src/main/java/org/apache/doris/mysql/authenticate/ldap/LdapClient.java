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

package org.apache.doris.mysql.authenticate.ldap;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.SymmetricEncryption;
import org.apache.doris.persist.LdapInfo;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.ldap.AuthenticationException;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.AbstractContextMapper;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.pool.factory.PoolingContextSource;
import org.springframework.ldap.pool.validation.DefaultDirContextValidator;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.support.LdapEncoder;
import org.springframework.ldap.transaction.compensating.manager.TransactionAwareContextSourceProxy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// This class is used to connect to the LDAP service.
public class LdapClient {
    private static final Logger LOG = LogManager.getLogger(LdapClient.class);

    private volatile ClientInfo clientInfo;

    @Data
    private static class ClientInfo {
        // Checking the user password requires creating a new connection with the user dn and password.
        // Due to this, these connections cannot be pooled.
        private LdapTemplate ldapTemplateNoPool;
        // Use ldap connection pool, connect to bind ldap admin dn and admin password.
        private LdapTemplate ldapTemplatePool;

        private String ldapPassword;

        public ClientInfo(String ldapPassword) {
            this.ldapPassword = ldapPassword;
            setLdapTemplateNoPool(ldapPassword);
            if (LdapConfig.ldap_search_use_pool) {
                setLdapTemplatePool(ldapPassword);
            }
        }

        // Returns the LdapTemplate for search operations:
        // pooled if ldap_search_use_pool=true, non-pooled otherwise.
        public LdapTemplate getSearchTemplate() {
            return ldapTemplatePool != null ? ldapTemplatePool : ldapTemplateNoPool;
        }

        private void setLdapTemplateNoPool(String ldapPassword) {
            LdapContextSource contextSource = new LdapContextSource();
            String url = LdapConfig.getConnectionURL(
                    NetUtils.getHostPortInAccessibleFormat(LdapConfig.ldap_host, LdapConfig.ldap_port));

            contextSource.setUrl(url);
            contextSource.setUserDn(LdapConfig.ldap_admin_name);
            contextSource.setPassword(ldapPassword);
            setLdapTimeoutProperties(contextSource);
            contextSource.afterPropertiesSet();
            ldapTemplateNoPool = new LdapTemplate(contextSource);
            ldapTemplateNoPool.setIgnorePartialResultException(true);
        }

        private void setLdapTemplatePool(String ldapPassword) {
            LdapContextSource contextSource = new LdapContextSource();
            String url = LdapConfig.getConnectionURL(
                    NetUtils.getHostPortInAccessibleFormat(LdapConfig.ldap_host, LdapConfig.ldap_port));

            contextSource.setUrl(url);
            contextSource.setUserDn(LdapConfig.ldap_admin_name);
            contextSource.setPassword(ldapPassword);
            setLdapTimeoutProperties(contextSource);
            contextSource.afterPropertiesSet();

            PoolingContextSource poolingContextSource = new PoolingContextSource();
            poolingContextSource.setDirContextValidator(new DefaultDirContextValidator());
            poolingContextSource.setContextSource(contextSource);
            poolingContextSource.setMaxActive(LdapConfig.ldap_pool_max_active);
            poolingContextSource.setMaxTotal(LdapConfig.ldap_pool_max_total);
            poolingContextSource.setMaxIdle(LdapConfig.ldap_pool_max_idle);
            poolingContextSource.setMaxWait(LdapConfig.ldap_pool_max_wait);
            poolingContextSource.setMinIdle(LdapConfig.ldap_pool_min_idle);
            poolingContextSource.setWhenExhaustedAction(LdapConfig.ldap_pool_when_exhausted);
            poolingContextSource.setTestOnBorrow(LdapConfig.ldap_pool_test_on_borrow);
            poolingContextSource.setTestOnReturn(LdapConfig.ldap_pool_test_on_return);
            poolingContextSource.setTestWhileIdle(LdapConfig.ldap_pool_test_while_idle);

            TransactionAwareContextSourceProxy proxy = new TransactionAwareContextSourceProxy(poolingContextSource);
            ldapTemplatePool = new LdapTemplate(proxy);
            ldapTemplatePool.setIgnorePartialResultException(true);
        }

        public boolean checkUpdate(String ldapPassword) {
            return this.ldapPassword == null || !this.ldapPassword.equals(ldapPassword);
        }

        private void setLdapTimeoutProperties(LdapContextSource contextSource) {
            Map<String, Object> baseEnv = new HashMap<>();
            if (LdapConfig.ldap_read_timeout_ms > 0) {
                baseEnv.put("com.sun.jndi.ldap.read.timeout",
                        String.valueOf(LdapConfig.ldap_read_timeout_ms));
            }
            if (LdapConfig.ldap_connect_timeout_ms > 0) {
                baseEnv.put("com.sun.jndi.ldap.connect.timeout",
                        String.valueOf(LdapConfig.ldap_connect_timeout_ms));
            }
            if (!baseEnv.isEmpty()) {
                contextSource.setBaseEnvironmentProperties(baseEnv);
            }
        }

    }

    private void init() {
        LdapInfo ldapInfo = Env.getCurrentEnv().getAuth().getLdapInfo();
        if (ldapInfo == null || !ldapInfo.isValid()) {
            LOG.error("LDAP configuration is incorrect or LDAP admin password is not set.");
            ErrorReport.report(ErrorCode.ERROR_LDAP_CONFIGURATION_ERR);
            throw new RuntimeException("LDAP configuration is incorrect or LDAP admin password is not set.");
        }

        String ldapPassword = SymmetricEncryption.decrypt(ldapInfo.getLdapPasswdEncrypted(),
                ldapInfo.getSecretKey(), ldapInfo.getIv());
        if (clientInfo == null || clientInfo.checkUpdate(ldapPassword)) {
            synchronized (LdapClient.class) {
                if (clientInfo == null || clientInfo.checkUpdate(ldapPassword)) {
                    clientInfo = new ClientInfo(ldapPassword);
                }
            }
        }
    }

    boolean doesUserExist(String userName) {
        String user = getUserDn(userName);
        if (user == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("User:{} does not exist in LDAP.", userName);
            }
            return false;
        }
        return true;
    }

    boolean checkPassword(String userName, String password) {
        init();
        long start = System.currentTimeMillis();
        try {
            clientInfo.getLdapTemplateNoPool().authenticate(org.springframework.ldap.query.LdapQueryBuilder.query()
                    .base(LdapConfig.ldap_user_basedn)
                    .filter(applyLoginFilter(LdapConfig.ldap_user_filter, userName)), password);
            long elapsed = System.currentTimeMillis() - start;
            if (LOG.isDebugEnabled()) {
                LOG.debug("LdapClient.checkPassword: user={}, success=true, elapsed={}ms",
                        userName, elapsed);
            }
            return true;
        } catch (AuthenticationException e) {
            long elapsed = System.currentTimeMillis() - start;
            if (LOG.isDebugEnabled()) {
                LOG.debug("LdapClient.checkPassword: user={}, success=false, elapsed={}ms, "
                                + "errorClass={}, errorMessage={}",
                        userName, elapsed, e.getClass().getSimpleName(), e.getMessage());
            }
            return false;
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            LOG.warn("LdapClient.checkPassword failed: user={}, elapsed={}ms, "
                            + "errorClass={}, errorMessage={}",
                    userName, elapsed, e.getClass().getSimpleName(), e.getMessage(), e);
            return false;
        }
    }

    // Search group DNs by 'member' attribution.
    List<String> getGroups(String userName) {
        long start = System.currentTimeMillis();
        List<String> groups = Lists.newArrayList();
        if (LdapConfig.ldap_group_basedn.isEmpty()) {
            return groups;
        }
        String userDn = getUserDn(userName);
        if (userDn == null) {
            return groups;
        }
        List<String> groupDns;
        if (!LdapConfig.ldap_group_filter.isEmpty()) {
            // Support Open Directory implementations
            String filter = applyLoginFilter(LdapConfig.ldap_group_filter, userName);
            groupDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                    .attributes("dn")
                    .base(LdapConfig.ldap_group_basedn)
                    .filter(filter));
        } else {
            // Standard LDAP using member attribute
            groupDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                    .base(LdapConfig.ldap_group_basedn)
                    .where("member").is(userDn));
        }

        if (groupDns == null) {
            return groups;
        }

        // group dn like: 'cn=groupName,ou=groups,dc=example,dc=com', we only need the groupName.
        for (String dn : groupDns) {
            String[] strings = dn.split("[,=]", 3);
            if (strings.length > 2) {
                groups.add(strings[1]);
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        if (LOG.isDebugEnabled()) {
            LOG.debug("LdapClient.getGroups: user={}, groups={}, elapsed={}ms",
                    userName, groups.size(), elapsed);
        }
        return groups;
    }

    private String getUserDn(String userName) {
        List<String> userDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                .base(LdapConfig.ldap_user_basedn).filter(applyLoginFilter(LdapConfig.ldap_user_filter, userName)));
        if (userDns == null || userDns.isEmpty()) {
            return null;
        }
        if (userDns.size() > 1) {
            String msg = String.format("[%s] not unique in LDAP server: [%s]",
                    applyLoginFilter(LdapConfig.ldap_user_filter, userName), userDns);
            LOG.error(msg);
            ErrorReport.report(ErrorCode.ERROR_LDAP_USER_NOT_UNIQUE_ERR, userName);
            throw new RuntimeException(msg);
        }
        return userDns.get(0);
    }

    public List<String> getDn(LdapQuery query) {
        init();
        long start = System.currentTimeMillis();
        try {
            List<String> result = clientInfo.getSearchTemplate().search(query,
                    new AbstractContextMapper<String>() {
                        protected String doMapFromContext(DirContextOperations ctx) {
                            return ctx.getNameInNamespace();
                        }
                    });
            long elapsed = System.currentTimeMillis() - start;
            if (LOG.isDebugEnabled()) {
                LOG.debug("LdapClient.getDn: base={}, elapsed={}ms, results={}",
                        query.base(), elapsed, result == null ? 0 : result.size());
            }
            return result;
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            String msg
                    = "Failed to retrieve the user's Distinguished Name (DN),"
                    + "This may be due to incorrect LDAP configuration or an unset/incorrect LDAP admin password.";
            LOG.warn("LdapClient.getDn failed: base={}, elapsed={}ms, error={}",
                    query.base(), elapsed, e.getMessage(), e);
            ErrorReport.report(ErrorCode.ERROR_LDAP_CONFIGURATION_ERR);
            throw new RuntimeException(msg);
        }
    }

    private String applyLoginFilter(String filter, String userName) {
        if (filter == null) {
            return null;
        }
        return filter.replace("{login}", LdapEncoder.filterEncode(userName));
    }
}
