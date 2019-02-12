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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.DppConfig;
import org.apache.doris.thrift.TAgentServiceVersion;
import org.apache.doris.thrift.TFetchResourceResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class UserPropertyMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(UserPropertyMgr.class);

    protected Map<String, UserProperty> propertyMap = Maps.newHashMap();
    public static final String ROOT_USER = "root";
    public static final String SYSTEM_RESOURCE_USER = "system";
    private AtomicLong resourceVersion = new AtomicLong(0);

    public UserPropertyMgr() {
    }

    public void addUserResource(String qualifiedUser, boolean isSystemUser) {
        UserProperty property = propertyMap.get(qualifiedUser);
        if (property != null) {
            return;
        }

        property = new UserProperty(qualifiedUser);

        // set user properties
        try {
            if (isSystemUser) {
                setSystemUserDefaultResource(property);
            } else {
                setNormalUserDefaultResource(property);
            }
        } catch (DdlException e) {
            // this should not happen, because the value is set by us!!
        }

        propertyMap.put(qualifiedUser, property);
        resourceVersion.incrementAndGet();
    }

    /*
     * Try to grant privs to whitelist of the user. 
     */
    public void addOrGrantWhiteList(UserIdentity userIdentity, Map<TablePattern, PrivBitSet> privsMap,
            byte[] password, boolean errOnExist, boolean errOnNonExist) throws DdlException {
        Preconditions.checkArgument(userIdentity.isDomain());
        UserProperty property = propertyMap.get(userIdentity.getQualifiedUser());
        if (property == null) {
            if (errOnNonExist) {
                throw new DdlException("user " + userIdentity + " does not exist");
            }
            property = new UserProperty(userIdentity.getQualifiedUser());
        }

        property.addOrGrantWhiteList(userIdentity.getHost(), privsMap, password, errOnExist);
        // update propertyMap after addOrGrantWhiteList, cause addOrGrantWhiteList may throw exception
        propertyMap.put(userIdentity.getQualifiedUser(), property);
    }

    public void revokePrivsFromWhiteList(UserIdentity userIdentity, Map<TablePattern, PrivBitSet> privsMap,
            boolean errOnNonExist) throws DdlException {
        Preconditions.checkArgument(userIdentity.isDomain());
        UserProperty property = propertyMap.get(userIdentity.getQualifiedUser());
        if (property == null && errOnNonExist) {
            throw new DdlException("User " + userIdentity.getQualifiedUser() + " does not exist");
        }

        property.revokePrivsFromWhiteList(userIdentity.getHost(), privsMap, errOnNonExist);
    }

    public void dropUser(String qualifiedUser) {
        propertyMap.remove(qualifiedUser);
        resourceVersion.incrementAndGet();
    }

    public void setPassword(UserIdentity userIdent, byte[] password) throws DdlException {
        Preconditions.checkArgument(userIdent.isDomain());
        UserProperty property = propertyMap.get(userIdent.getQualifiedUser());
        if (property == null) {
            throw new DdlException("user " + userIdent.getQualifiedUser() + " does not exist");
        }

        if (property.getWhiteList().containsDomain(userIdent.getHost())) {
            throw new DdlException("user " + userIdent + " does not exist");
        }

        property.getWhiteList().setPassword(password);
    }

    public void updateUserProperty(String user, List<Pair<String, String>> properties) throws DdlException {
        UserProperty property = propertyMap.get(user);
        if (property == null) {
            throw new DdlException("Unknown user(" + user + ")");
        }

        property.update(properties);
    }

    public long getMaxConn(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        if (existProperty == null) {
            return 0;
        }
        return existProperty.getMaxConn();
    }

    public int getPropertyMapSize() {
        return propertyMap.size();
    }

    private void setSystemUserDefaultResource(UserProperty user) throws DdlException {
        UserResource userResource = user.getResource();
        userResource.updateResource("CPU_SHARE", 100);
        userResource.updateResource("IO_SHARE", 100);
        userResource.updateResource("SSD_READ_MBPS", 30);
        userResource.updateResource("SSD_WRITE_MBPS", 30);
        userResource.updateResource("HDD_READ_MBPS", 30);
        userResource.updateResource("HDD_WRITE_MBPS", 30);
    }

    private void setNormalUserDefaultResource(UserProperty user) throws DdlException {
        UserResource userResource = user.getResource();
        userResource.updateResource("CPU_SHARE", 1000);
        userResource.updateResource("IO_SHARE", 1000);
        userResource.updateResource("SSD_READ_IOPS", 1000);
        userResource.updateResource("HDD_READ_IOPS", 80);
        userResource.updateResource("SSD_READ_MBPS", 30);
        userResource.updateResource("HDD_READ_MBPS", 30);
    }

    public TFetchResourceResult toResourceThrift() {
        TFetchResourceResult tResult = new TFetchResourceResult();
        tResult.setProtocolVersion(TAgentServiceVersion.V1);
        tResult.setResourceVersion(resourceVersion.get());

        for (Map.Entry<String, UserProperty> entry : propertyMap.entrySet()) {
            tResult.putToResourceByUser(entry.getKey(), entry.getValue().getResource().toThrift());
        }

        return tResult;
    }

    public Pair<String, DppConfig> getLoadClusterInfo(String qualifiedUser, String cluster) throws DdlException {
        Pair<String, DppConfig> loadClusterInfo = null;

        if (!propertyMap.containsKey(qualifiedUser)) {
            throw new DdlException("User " + qualifiedUser + " does not exist");
        }

        UserProperty property = propertyMap.get(qualifiedUser);
        loadClusterInfo = property.getLoadClusterInfo(cluster);
        return loadClusterInfo;
    }

    public List<List<String>> fetchUserProperty(String qualifiedUser) throws AnalysisException {
        if (!propertyMap.containsKey(qualifiedUser)) {
            throw new AnalysisException("User " + qualifiedUser + " does not exist");
        }

        UserProperty property = propertyMap.get(qualifiedUser);
        return property.fetchProperty();
    }

    public void getCopiedWhiteList(Map<String, Set<String>> userMap) {
        LOG.debug("get property map: {}", propertyMap);
        for (Map.Entry<String, UserProperty> entry : propertyMap.entrySet()) {
            Set<String> domains = entry.getValue().getWhiteList().getAllDomains();
            if (domains.isEmpty()) {
                continue;
            }
            userMap.put(entry.getKey(), domains);
        }
    }

    public void updateResolovedIps(String qualifiedUser, String domain, Set<String> resolvedIPs) {
        if (!propertyMap.containsKey(qualifiedUser)) {
            return;
        }

        UserProperty property = propertyMap.get(qualifiedUser);
        property.getWhiteList().updateResolovedIps(qualifiedUser, domain, resolvedIPs);
    }

    public UserProperty getUserProperty(String qualifiedUserName) {
        return propertyMap.get(qualifiedUserName);
    }

    @Deprecated
    public void addUserPropertyUnchecked(UserProperty userProperty) {
        Preconditions.checkState(Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43);
        propertyMap.put(userProperty.getQualifiedUser(), userProperty);
    }
    
    public void transform(PaloAuth auth) {
        Preconditions.checkState(Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43);
        for (UserProperty userProperty : propertyMap.values()) {
            auth.transformAndAddOldUserProperty(userProperty);
        }
    }

    public void getUserAuthInfo(List<List<String>> userAuthInfos, UserIdentity specifiedUserIdent) {
        for (UserProperty property : propertyMap.values()) {
            if (specifiedUserIdent != null
                    && !property.getQualifiedUser().equals(specifiedUserIdent.getQualifiedUser())) {
                continue;
            }
            property.getAuthInfo(userAuthInfos);
        }
    }

    public static UserPropertyMgr read(DataInput in) throws IOException {
        UserPropertyMgr userPropertyMgr = new UserPropertyMgr();
        userPropertyMgr.readFields(in);
        return userPropertyMgr;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(propertyMap.size());
        for (Map.Entry<String, UserProperty> entry : propertyMap.entrySet()) {
            entry.getValue().write(out);
        }
        // Write resource version
        out.writeLong(resourceVersion.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            UserProperty userProperty = UserProperty.read(in);
            propertyMap.put(userProperty.getQualifiedUser(), userProperty);
            LOG.debug("read user property: {}: {}", userProperty.getQualifiedUser(), userProperty);
        }
        // Read resource
        resourceVersion = new AtomicLong(in.readLong());
    }
}

