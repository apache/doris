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

package org.apache.doris.stack.connector;

import org.apache.doris.stack.model.ldap.LdapConnectionInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfoReq;
import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPSearchException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LdapClient {

    /**
     * ldap Attributes
     */
    private static final String FIRST_NAME = "givenname";

    private static final String LAST_NAME = "sn";

    private static final String EMAIL = "mail";

    private static final String GROUPS = "groups";

    private static final String PASSWORD = "userpassword";

    /**
     * get LDAP connection information
     *
     * @param ldapConnectionInfo
     * @return
     */
    public LDAPConnection getConnection(LdapConnectionInfo ldapConnectionInfo) {
        try {
            LDAPConnection connection = new LDAPConnection();
            connection.connect(ldapConnectionInfo.getHost(), ldapConnectionInfo.getPort());
            connection.bind(ldapConnectionInfo.getBindDn(), ldapConnectionInfo.getPassword());
            return connection;
        } catch (LDAPException e) {
            log.error("get LDAP connection error: {}", e);
        }
        return null;
    }

    /**
     * search LDAP user
     *
     * @param ldapConnection
     * @param ldapUserInfoReq
     * @return
     */
    public LdapUserInfo getUser(LDAPConnection ldapConnection, LdapUserInfoReq ldapUserInfoReq) {

        LdapUserInfo ldapUserInfo = new LdapUserInfo();
        ldapUserInfo.setExist(false);
        for (String baseDn : ldapUserInfoReq.getBaseDn()) {
            Filter userFilter = Filter
                    .createEqualityFilter(ldapUserInfoReq.getUserAttribute(), ldapUserInfoReq.getUserValue());
            SearchRequest searchRequest = new SearchRequest(baseDn, SearchScope.SUB, userFilter);
            searchRequest.setSizeLimit(1);
            try {
                SearchResult searchResult = ldapConnection.search(searchRequest);

                // check
                if (searchResult.getEntryCount() == 0 || searchResult.getSearchEntries().isEmpty()) {
                    log.error("We got No Entries for: {}", searchRequest.getFilter());
                    continue;
                }

                SearchResultEntry entry = searchResult.getSearchEntries().get(0);
                ldapUserInfo.setDn(entry.getDN());
                ldapUserInfo.setFirstName(entry.getAttributeValue(FIRST_NAME));
                ldapUserInfo.setLastName(entry.getAttributeValue(LAST_NAME));
                ldapUserInfo.setEmail(entry.getAttributeValue(EMAIL));
                ldapUserInfo.setGroups(entry.getAttributeValue(GROUPS));
                ldapUserInfo.setExist(true);
                ldapUserInfo.setPassword(entry.getAttributeValue(PASSWORD));

                return ldapUserInfo;
            } catch (LDAPSearchException e) {
                log.error("LDAP search user error: {}", e);
            } catch (LDAPException e) {
                log.error("LDAP exception error: {}", e);
            }
        }
        return ldapUserInfo;
    }

    /**
     * Authenticate the LDAP user and return the user information
     *
     * @param ldapConnection
     * @param ldapUserInfoReq
     * @return
     */
    public LdapUserInfo authenticate(LDAPConnection ldapConnection, LdapUserInfoReq ldapUserInfoReq) {

        LdapUserInfo ldapUserInfo = new LdapUserInfo();
        ldapUserInfo.setAuth(false);
        for (String baseDn : ldapUserInfoReq.getBaseDn()) {
            Filter userFilter = Filter.createEqualityFilter(ldapUserInfoReq.getUserAttribute(), ldapUserInfoReq.getUserValue());
            SearchRequest searchRequest = new SearchRequest(baseDn, SearchScope.SUB, userFilter);
            searchRequest.setSizeLimit(1);
            try {
                SearchResult searchResult = ldapConnection.search(searchRequest);

                // check
                if (searchResult.getEntryCount() > 1) {
                    log.error("We got more than one Entry for: {}", searchRequest.getFilter());
                }
                if (searchResult.getEntryCount() == 0 || searchResult.getSearchEntries().isEmpty()) {
                    log.error("We got No Entries for: {}", searchRequest.getFilter());
                    return ldapUserInfo;
                }

                SearchResultEntry entry = searchResult.getSearchEntries().get(0);
                BindRequest bindRequest = new SimpleBindRequest(entry.getDN(), ldapUserInfoReq.getPassword());
                ldapConnection.bind(bindRequest);

                ldapUserInfo.setDn(entry.getDN());
                ldapUserInfo.setFirstName(entry.getAttributeValue(FIRST_NAME));
                ldapUserInfo.setLastName(entry.getAttributeValue(LAST_NAME));
                ldapUserInfo.setEmail(entry.getAttributeValue(EMAIL));
                ldapUserInfo.setGroups(entry.getAttributeValue(GROUPS));
                ldapUserInfo.setAuth(true);
                return ldapUserInfo;
            } catch (LDAPSearchException e) {
                log.error("LDAP search user error: {}", e);
            } catch (LDAPException e) {
                log.error("LDAP authenticate failed error: {}", e);
            } catch (Exception e) {
                log.error("LDAP authenticate error: {}", e);
            }
        }
        return ldapUserInfo;
    }
}
