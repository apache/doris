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
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.AbstractContextMapper;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.pool.factory.PoolingContextSource;
import org.springframework.ldap.pool.validation.DefaultDirContextValidator;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.transaction.compensating.manager.TransactionAwareContextSourceProxy;

import java.util.List;

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
            setLdapTemplatePool(ldapPassword);
        }

        private void setLdapTemplateNoPool(String ldapPassword) {
            LdapContextSource contextSource = new LdapContextSource();
            String url = "ldap://" + NetUtils
                    .getHostPortInAccessibleFormat(LdapConfig.ldap_host, LdapConfig.ldap_port);

            contextSource.setUrl(url);
            contextSource.setUserDn(LdapConfig.ldap_admin_name);
            contextSource.setPassword(ldapPassword);
            contextSource.afterPropertiesSet();
            ldapTemplateNoPool = new LdapTemplate(contextSource);
        }

        private void setLdapTemplatePool(String ldapPassword) {
            LdapContextSource contextSource = new LdapContextSource();
            String url = "ldap://" + NetUtils
                    .getHostPortInAccessibleFormat(LdapConfig.ldap_host, LdapConfig.ldap_port);

            contextSource.setUrl(url);
            contextSource.setUserDn(LdapConfig.ldap_admin_name);
            contextSource.setPassword(ldapPassword);
            contextSource.setPooled(true);
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
        }

        public boolean checkUpdate(String ldapPassword) {
            return this.ldapPassword == null || !this.ldapPassword.equals(ldapPassword);
        }
    }

    private void init() {
        LdapInfo ldapInfo = Env.getCurrentEnv().getAuth().getLdapInfo();
        if (ldapInfo == null || !ldapInfo.isValid()) {
            LOG.error("info is null, maybe no ldap admin password is set.");
            ErrorReport.report(ErrorCode.ERROR_LDAP_CONFIGURATION_ERR);
            throw new RuntimeException("ldapTemplate is not initialized");
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
        try {
            clientInfo.getLdapTemplateNoPool().authenticate(org.springframework.ldap.query.LdapQueryBuilder.query()
                    .base(LdapConfig.ldap_user_basedn)
                    .filter(getUserFilter(LdapConfig.ldap_user_filter, userName)), password);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Search group DNs by 'member' attribution.
    List<String> getGroups(String userName) {
        List<String> groups = Lists.newArrayList();
        if (LdapConfig.ldap_group_basedn.isEmpty()) {
            return groups;
        }
        String userDn = getUserDn(userName);
        if (userDn == null) {
            return groups;
        }
        List<String> groupDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                .base(LdapConfig.ldap_group_basedn)
                .where("member").is(userDn));
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
        return groups;
    }

    private String getUserDn(String userName) {
        List<String> userDns = getDn(org.springframework.ldap.query.LdapQueryBuilder.query()
                .base(LdapConfig.ldap_user_basedn).filter(getUserFilter(LdapConfig.ldap_user_filter, userName)));
        if (userDns == null || userDns.isEmpty()) {
            return null;
        }
        if (userDns.size() > 1) {
            LOG.error("{} not unique in LDAP server:{}",
                    getUserFilter(LdapConfig.ldap_user_filter, userName), userDns);
            ErrorReport.report(ErrorCode.ERROR_LDAP_USER_NOT_UNIQUE_ERR, userName);
            throw new RuntimeException("User is not unique");
        }
        return userDns.get(0);
    }

    private List<String> getDn(LdapQuery query) {
        init();
        try {
            return clientInfo.getLdapTemplatePool().search(query, new AbstractContextMapper<String>() {
                protected String doMapFromContext(DirContextOperations ctx) {
                    return ctx.getNameInNamespace();
                }
            });
        } catch (Exception e) {
            LOG.error("Get user dn fail.", e);
            ErrorReport.report(ErrorCode.ERROR_LDAP_CONFIGURATION_ERR);
            throw e;
        }
    }

    private String getUserFilter(String userFilter, String userName) {
        return userFilter.replaceAll("\\{login}", userName);
    }
}
