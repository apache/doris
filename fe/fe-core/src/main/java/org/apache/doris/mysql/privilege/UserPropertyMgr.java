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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class UserPropertyMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(UserPropertyMgr.class);

    protected Map<String, UserProperty> propertyMap = Maps.newHashMap();
    public static final String ROOT_USER = "root";
    public static final String SYSTEM_RESOURCE_USER = "system";
    public static final String LDAP_RESOURCE_USER = "ldap";

    private static final UserProperty LDAP_PROPERTY = new UserProperty(LDAP_RESOURCE_USER);
    private AtomicLong resourceVersion = new AtomicLong(0);

    public UserPropertyMgr() {
    }

    public void addUserResource(String qualifiedUser) {
        UserProperty property = propertyMap.get(qualifiedUser);
        if (property != null) {
            return;
        }

        property = new UserProperty(qualifiedUser);
        propertyMap.put(qualifiedUser, property);
        resourceVersion.incrementAndGet();
    }

    public void dropUser(UserIdentity userIdent) {
        if (propertyMap.remove(userIdent.getQualifiedUser()) != null) {
            LOG.info("drop user {} from user property manager", userIdent.getQualifiedUser());
        }
    }

    public void updateUserProperty(String user, List<Pair<String, String>> properties, boolean isReplay)
            throws UserException {
        UserProperty property = propertyMap.get(user);
        if (property == null) {
            throw new DdlException("Unknown user(" + user + ")");
        }

        property.update(properties, isReplay);
    }

    public int getQueryTimeout(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return 0;
        }
        return existProperty.getQueryTimeout();
    }

    public int getInsertTimeout(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return 0;
        }
        return existProperty.getInsertTimeout();
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

    public int getParallelFragmentExecInstanceNum(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return -1;
        }
        return existProperty.getParallelFragmentExecInstanceNum();
    }

    public Set<Tag> getResourceTags(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return UserProperty.INVALID_RESOURCE_TAGS;
        }
        Set<Tag> tags = existProperty.getCopiedResourceTags();
        // only root and admin can return empty tag.
        // empty tag means user can access all backends.
        // for normal user, if tag is empty and not set force_olap_table_replication_allocation, use default tag.
        if (tags.isEmpty() && !(qualifiedUser.equalsIgnoreCase(Auth.ROOT_USER)
                || qualifiedUser.equalsIgnoreCase(Auth.ADMIN_USER))
                && Config.force_olap_table_replication_allocation.isEmpty()) {
            tags = Sets.newHashSet(Tag.DEFAULT_BACKEND_TAG);
        }
        return tags;
    }

    public boolean isAllowResourceTagDowngrade(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        if (existProperty == null) {
            return false;
        }
        return existProperty.isAllowResourceTagDowngrade();
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

    public String[] getSqlBlockRules(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return new String[] {};
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

    public String getWorkloadGroup(String qualifiedUser) {
        UserProperty existProperty = propertyMap.get(qualifiedUser);
        existProperty = getLdapPropertyIfNull(qualifiedUser, existProperty);
        if (existProperty == null) {
            return null;
        }
        return existProperty.getWorkloadGroup();
    }

    public Pair<Boolean, String> isWorkloadGroupInUse(String groupName) {
        for (Entry<String, UserProperty> entry : propertyMap.entrySet()) {
            if (entry.getValue().getWorkloadGroup().equals(groupName)) {
                return Pair.of(true, entry.getKey());
            }
        }
        return Pair.of(false, "");
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("read user property: {}: {}", userProperty.getQualifiedUser(), userProperty);
            }
        }
        // Read resource
        resourceVersion = new AtomicLong(in.readLong());
    }
}
