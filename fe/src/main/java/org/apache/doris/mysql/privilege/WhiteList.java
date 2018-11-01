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

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PaloAuth.PrivLevel;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// grant privs.. on db.tbl to user@['domain.name']
// revoke privs on db.tbl from user@['domain.name']
public class WhiteList implements Writable {
    private static final Logger LOG = LogManager.getLogger(WhiteList.class);

    // domain name -> (tbl pattern -> privs)
    // Domain names which need to be resolved to IPs.
    // Currently, only implemented for Baidu Name Service (BNS)
    private Map<String, Map<TablePattern, PrivBitSet>> domainMap = Maps.newConcurrentMap();

    // Domain name to resolved IPs
    private Map<String, Set<String>> resolvedIPMap = Maps.newConcurrentMap();

    private byte[] password;

    @Deprecated
    protected Set<String> ipWhiteLists = Sets.newHashSet();
    @Deprecated
    protected Set<String> starIpWhiteLists = Sets.newHashSet();

    public WhiteList() {
    }

    @Deprecated
    public Set<String> getIpWhiteLists() {
        return ipWhiteLists;
    }

    @Deprecated
    public Set<String> getStarIpWhiteLists() {
        return starIpWhiteLists;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public void addDomainWithPrivs(String domainName, TablePattern tblPattern, PrivBitSet privs) {
        Map<TablePattern, PrivBitSet> privsMap = domainMap.get(domainName);
        if (privsMap == null) {
            privsMap = Maps.newConcurrentMap();
            domainMap.put(domainName, privsMap);
        }
        PrivBitSet existingPrivs = privsMap.get(tblPattern);
        if (existingPrivs == null) {
            existingPrivs = privs;
            privsMap.put(tblPattern, existingPrivs);
        } else {
            existingPrivs.or(privs);
        }
    }

    public void revokePrivsFromDomain(String domainName, TablePattern tblPattern, PrivBitSet privs,
            boolean errOnNonExist, boolean check) throws DdlException {
        Map<TablePattern, PrivBitSet> privsMap = domainMap.get(domainName);
        if (privsMap == null && errOnNonExist) {
            throw new DdlException("Domain " + domainName + " does not exist");
        }

        PrivBitSet existingPrivs = privsMap.get(tblPattern);
        if (existingPrivs == null) {
            throw new DdlException("No such grants on " + tblPattern);
        }

        if (!check) {
            existingPrivs.remove(privs);
        }
    }

    public void updateResolovedIps(String qualifiedUser, String domain, Set<String> newResolvedIPs) {
        Map<TablePattern, PrivBitSet> privsMap = domainMap.get(domain);
        if (privsMap == null) {
            LOG.debug("domain does not exist in white list: {}", domain);
            return;
        }

        Set<String> preResolvedIPs = resolvedIPMap.get(domain);
        if (preResolvedIPs == null) {
            preResolvedIPs = Sets.newHashSet();
        }

        // 1. grant for newly added IPs
        for (String newIP : newResolvedIPs) {
            UserIdentity userIdent = new UserIdentity(qualifiedUser, newIP);
            userIdent.setIsAnalyzed();
            for (Map.Entry<TablePattern, PrivBitSet> entry : privsMap.entrySet()) {
                try {
                    // we copy the PrivBitSet, cause we don't want use the same PrivBitSet object in different place.
                    // otherwise, when we change the privs of the domain, the priv entry will be changed synchronously,
                    // which is not expected.
                    Catalog.getCurrentCatalog().getAuth().grantPrivs(userIdent, entry.getKey(),
                                                                     entry.getValue().copy(),
                                                                     false /* err on non exist */,
                                                                     true /* set by resolver */);
                } catch (DdlException e) {
                    LOG.warn("should not happen", e);
                }
            }

            // set password
            try {
                Catalog.getCurrentCatalog().getAuth().setPasswordInternal(userIdent, password,
                                                                          true /* add if not exist */,
                                                                          true /* set by resolver */,
                                                                          true /* is replay */);
            } catch (DdlException e) {
                LOG.warn("should not happen", e);
            }
        }

        // 2. delete privs which does not exist anymore.
        for (String preIP : preResolvedIPs) {
            if (!newResolvedIPs.contains(preIP)) {
                UserIdentity userIdent = new UserIdentity(qualifiedUser, preIP);
                userIdent.setIsAnalyzed();
                for (Map.Entry<TablePattern, PrivBitSet> entry : privsMap.entrySet()) {
                    try {
                        Catalog.getCurrentCatalog().getAuth().revokePrivs(userIdent, entry.getKey(),
                                                                          entry.getValue(),
                                                                          false, /* err on non exist */
                                                                          true /* set by domain */,
                                                                          true /* delete entry when empty */);
                    } catch (DdlException e) {
                        LOG.warn("should not happen", e);
                    }
                }

                // delete password
                Catalog.getCurrentCatalog().getAuth().deletePassworEntry(userIdent);
            }
        }

        // update resolved ip map
        resolvedIPMap.put(domain, newResolvedIPs);
    }

    public Map<String, Set<String>> getResolvedIPs() {
        return resolvedIPMap;
    }

    public boolean containsDomain(String domain) {
        return domainMap.containsKey(domain);
    }

    public Set<String> getAllDomains() {
        return Sets.newHashSet(domainMap.keySet());
    }

    public void updateDomainMap(String domain, Map<TablePattern, PrivBitSet> privs) {
        domainMap.put(domain, privs);
    }

    public void getAuthInfo(String qualifiedUser, List<List<String>> userAuthInfos) {
        LOG.debug("get domain privs for {}, domain map: {}", qualifiedUser, domainMap);
        for (Map.Entry<String, Map<TablePattern, PrivBitSet>> entry : domainMap.entrySet()) {
            List<String> userEntry = Lists.newArrayList();
            UserIdentity tmpUserIdent = new UserIdentity(qualifiedUser, entry.getKey(), true);
            tmpUserIdent.setIsAnalyzed();

            Map<TablePattern, PrivBitSet> privsMap = entry.getValue();
            // global privs
            for (Map.Entry<TablePattern, PrivBitSet> privsEntry : privsMap.entrySet()) {
                if (privsEntry.getKey().getPrivLevel() != PrivLevel.GLOBAL) {
                    continue;
                }
                userEntry.add(tmpUserIdent.toString());
                userEntry.add(password == null ? "N/A" : "Yes");
                userEntry.add(privsEntry.getValue().toString());
                // at most one global priv entry
                break;
            }
            if (userEntry.isEmpty()) {
                userEntry.add(tmpUserIdent.toString());
                userEntry.add("N/A");
                userEntry.add("N/A");
            }

            // db privs
            List<String> dbPrivs = Lists.newArrayList();
            for (Map.Entry<TablePattern, PrivBitSet> privsEntry : privsMap.entrySet()) {
                if (privsEntry.getKey().getPrivLevel() != PrivLevel.DATABASE) {
                    continue;
                }
                dbPrivs.add(privsEntry.getKey().getQuolifiedDb() + ": " + privsEntry.getValue().toString());
            }
            if (!dbPrivs.isEmpty()) {
                userEntry.add(Joiner.on("\n").join(dbPrivs));
            } else {
                userEntry.add("N/A");
            }

            // tbl privs
            List<String> tblPrivs = Lists.newArrayList();
            for (Map.Entry<TablePattern, PrivBitSet> privsEntry : privsMap.entrySet()) {
                if (privsEntry.getKey().getPrivLevel() != PrivLevel.TABLE) {
                    continue;
                }
                tblPrivs.add(privsEntry.getKey().toString() + ": " + privsEntry.getValue().toString());
            }
            if (!tblPrivs.isEmpty()) {
                userEntry.add(Joiner.on("\n").join(tblPrivs));
            } else {
                userEntry.add("N/A");
            }

            userAuthInfos.add(userEntry);
        }
    }

    @Override
    public String toString() {
        return domainMap.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(domainMap.size());
        for (String domain : domainMap.keySet()) {
            Text.writeString(out, domain);
            Map<TablePattern, PrivBitSet> privsMap = domainMap.get(domain);
            out.writeInt(privsMap.size());
            for (Map.Entry<TablePattern, PrivBitSet> entry : privsMap.entrySet()) {
                entry.getKey().write(out);
                entry.getValue().write(out);
            }
        }

        if (password == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(password.length);
            out.write(password);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43) {
            int ipWhiteListsLen = in.readInt();
            for (int i = 0; i < ipWhiteListsLen; i++) {
                ipWhiteLists.add(Text.readString(in));
            }
            LOG.debug("get white list ip: {}", ipWhiteLists);

            int starIpWhiteListsLen = in.readInt();
            for (int i = 0; i < starIpWhiteListsLen; i++) {
                starIpWhiteLists.add(Text.readString(in));
            }
            LOG.debug("get star white list ip: {}", starIpWhiteLists);

            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String domainName = Text.readString(in);
                Map<TablePattern, PrivBitSet> privMap = Maps.newConcurrentMap();
                // NOTICE: for forward compatibility. but we can't get user's privs here,
                // so set it to empty privs on *.*. and we will set privs later
                TablePattern tablePattern = TablePattern.ALL;
                PrivBitSet privs = PrivBitSet.of();
                privMap.put(tablePattern, privs);
                domainMap.put(domainName, privMap);
            }
            LOG.debug("get domain map: {}", domainMap);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_43) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String domain = Text.readString(in);
                Map<TablePattern, PrivBitSet> privsMap = Maps.newConcurrentMap();
                domainMap.put(domain, privsMap);

                int count = in.readInt();
                for (int j = 0; j < count; j++) {
                    TablePattern tablePattern = TablePattern.read(in);
                    PrivBitSet privs = PrivBitSet.read(in);
                    privsMap.put(tablePattern, privs);
                }
            }

            if (in.readBoolean()) {
                int passwordLen = in.readInt();
                password = new byte[passwordLen];
                in.readFully(password);
            }
        }
    }
}
