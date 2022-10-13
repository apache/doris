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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.DppConfig;
import org.apache.doris.resource.Tag;
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
    public static final String LDAP_RESOURCE_USER = "ldap";

    private static final UserProperty LDAP_PROPERTY = new UserProperty(LDAP_RESOURCE_USER);

    static {
        try {
            setNormalUserDefaultResource(LDAP_PROPERTY);
        } catch (DdlException e) {
            LOG.error("init DEFAULT_PROPERTY error.", e);
            throw new RuntimeException(e);
        }
    }

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

    public void setPasswordForDomain(UserIdentity userIdentity, byte[] password, boolean errOnExist,
            boolean errOnNonExist) throws DdlException {
        Preconditions.checkArgument(userIdentity.isDomain());
        UserProperty property = propertyMap.get(userIdentity.getQualifiedUser());
        if (property == null) {
            if (errOnNonExist) {
                throw new DdlException("user " + userIdentity + " does not exist");
            }
            property = new UserProperty(userIdentity.getQualifiedUser());
        }
        property.setPasswordForDomain(userIdentity.getHost(), password, errOnExist);
        // update propertyMap after setPasswordForDomain, cause setPasswordForDomain may throw exception
        propertyMap.put(userIdentity.getQualifiedUser(), property);
    }

    public void removeDomainFromUser(UserIdentity userIdentity) {
        Preconditions.checkArgument(userIdentity.isDomain());
        UserProperty userProperty = propertyMap.get(userIdentity.getQualifiedUser());
        if (userProperty == null) {
            return;
        }
        userProperty.removeDomain(userIdentity.getHost());
        resourceVersion.incrementAndGet();
    }

    public void dropUser(UserIdentity userIdent) {
        if (propertyMap.remove(userIdent.getQualifiedUser()) != null) {
            LOG.info("drop user {} from user property manager", userIdent.getQualifiedUser());
        }
    }

    public void updateUserProperty(String user, List<Pair<String, String>> properties) throws UserException {
        UserProperty property = propertyMap.get(user);
        if (property == null) {
            throw new DdlException("Unknown user(" + user + ")");
        }

        property.update(properties);
    }

    public long getMaxConn(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return 0;
        }
        return existProperty.getMaxConn();
    }

    public long getMaxQueryInstances(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return Config.default_max_query_instances;
        }
        return existProperty.getMaxQueryInstances();
    }

    public Set<Tag> getResourceTags(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return UserProperty.INVALID_RESOURCE_TAGS;
        }
        return existProperty.getCopiedResourceTags();
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

    private static void setNormalUserDefaultResource(UserProperty user) throws DdlException {
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

        UserProperty property = propertyMap.get(qualifiedUser);
        property = getLdapPropertyIfNull(qualifiedUser, property);
        if (property == null) {
            throw new DdlException("User " + qualifiedUser + " does not exist");
        }
        loadClusterInfo = property.getLoadClusterInfo(cluster);
        return loadClusterInfo;
    }

    public List<List<String>> fetchUserProperty(String qualifiedUser) throws AnalysisException {
        UserProperty property = propertyMap.get(qualifiedUser);
        property = getLdapPropertyIfNull(qualifiedUser, property);
        if (property == null) {
            throw new AnalysisException("User " + qualifiedUser + " does not exist");
        }
        return property.fetchProperty();
    }

    // return a map from domain name -> set of user names
    public void getAllDomains(Set<String> allDomains) {
        LOG.debug("get property map: {}", propertyMap);
        for (Map.Entry<String, UserProperty> entry : propertyMap.entrySet()) {
            Set<String> domains = entry.getValue().getWhiteList().getAllDomains();
            allDomains.addAll(domains);
        }
    }

    // check if specified user identity has password
    public boolean doesUserHasPassword(UserIdentity userIdent) {
        Preconditions.checkState(userIdent.isDomain());
        if (!propertyMap.containsKey(userIdent.getQualifiedUser())) {
            return false;
        }
        return propertyMap.get(userIdent.getQualifiedUser()).getWhiteList().hasPassword(userIdent.getHost());
    }

    public boolean doesUserExist(UserIdentity userIdent) {
        Preconditions.checkState(userIdent.isDomain());
        if (!propertyMap.containsKey(userIdent.getQualifiedUser())) {
            return false;
        }
        return propertyMap.get(userIdent.getQualifiedUser()).getWhiteList().containsDomain(userIdent.getHost());
    }

    public void addUserPrivEntriesByResolvedIPs(Map<String, Set<String>> resolvedIPsMap) {
        for (UserProperty userProperty : propertyMap.values()) {
            userProperty.getWhiteList()
                    .addUserPrivEntriesByResolvedIPs(userProperty.getQualifiedUser(), resolvedIPsMap);
        }
    }

    public String[] getSqlBlockRules(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return new String[]{};
        }
        return existProperty.getSqlBlockRules();
    }

    public int getCpuResourceLimit(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return -1;
        }
        return existProperty.getCpuResourceLimit();
    }

    public long getExecMemLimit(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return -1;
        }
        return existProperty.getExecMemLimit();
    }

    private UserProperty getLdapPropertyIfNull(String qualifiedUser, UserProperty existProperty) {
        if (existProperty == null && Env.getCurrentEnv().getAuth().getLdapManager().doesUserExist(qualifiedUser)) {
            return LDAP_PROPERTY;
        }
        return existProperty;
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
