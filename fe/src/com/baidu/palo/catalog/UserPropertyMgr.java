// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.catalog;

import com.baidu.palo.analysis.AlterUserStmt;
import com.baidu.palo.analysis.AlterUserType;
import com.baidu.palo.analysis.SetUserPropertyStmt;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.publish.FixedTimePublisher;
import com.baidu.palo.common.publish.Listener;
import com.baidu.palo.common.publish.TopicUpdate;
import com.baidu.palo.load.DppConfig;
import com.baidu.palo.persist.EditLog;
import com.baidu.palo.thrift.TAgentServiceVersion;
import com.baidu.palo.thrift.TFetchResourceResult;
import com.baidu.palo.thrift.TTopicItem;
import com.baidu.palo.thrift.TTopicType;
import com.baidu.palo.cluster.ClusterNamespace;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO(dhc): we don't consider "drop database"
public class UserPropertyMgr {
    private static final Logger LOG = LogManager.getLogger(UserPropertyMgr.class);

    private EditLog editLog;
    protected Map<String, UserProperty> userMap;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private static final String ROOT_USER = "root";
    private static final String SYSTEM_RESOURCE_USER = "system";
    private AtomicLong resourceVersion;

    public UserPropertyMgr() {
        userMap = Maps.newConcurrentMap();
        // there is no problem to init the root's password in constructor.
        // because during the log replay, the real root password will cover the
        // initial password.
        unprotectAddUser("", ROOT_USER, new byte[0]);
        unprotectAddUser("", SYSTEM_RESOURCE_USER, new byte[0]);
        resourceVersion = new AtomicLong(0);
    }

    public static String getRootName() {
        return ROOT_USER;
    }

    private void readLock() {
        this.lock.readLock().lock();
    }

    private void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    // Register callback to FixedTimePublisher
    public void setUp() {
        FixedTimePublisher.getInstance().register(new FixedTimePublisher.Callback() {
            @Override
            public TopicUpdate getTopicUpdate() {
                TopicUpdate update = new TopicUpdate(TTopicType.RESOURCE);
                TTopicItem tTopicItem = new TTopicItem("version");
                tTopicItem.setInt_value(resourceVersion.get());
                update.addUpdates(tTopicItem);
                return update;
            }

            @Override
            public Listener getListener() {
                return null;
            }
        }, Config.meta_resource_publish_interval_ms);
    }

    public List<List<String>> fetchAccessResourceResult() {
        List<List<String>> result = Lists.newArrayList();
        readLock();
        try {
            for (Map.Entry<String, UserProperty> entry : userMap.entrySet()) {
                String userName = entry.getKey();
                UserProperty userProperty = entry.getValue();

                if (Strings.isNullOrEmpty(userName) || (userProperty == null)) {
                    continue;
                }

                String privilegeResult = userProperty.fetchPrivilegeResult();
                result.add(Arrays.asList(userProperty.getUser(), new String(userProperty.getPassword()),
                        String.valueOf(userProperty.isAdmin()), String.valueOf(userProperty.isSuperuser()),
                        String.valueOf(userProperty.getMaxConn()), privilegeResult));
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    // we provide four function to support ddl stmt: addUser, setPasswd, grant,
    // dropUser
    // we provide two funciton to support search stmt: hasAccess(),
    // getIsAdmin(), getPassword()
    public void addUser(String cluster, String user, byte[] password, boolean isSuperuser) throws DdlException {
        writeLock();
        try {
            checkUserNotExists(user);
            UserProperty resource = unprotectAddUser(cluster, user, password);
            resource.setIsSuperuser(isSuperuser);
            // all user has READ_ONLY privilege to InfoSchemaDb
            this.getAccessResource(user).setAccess(ClusterNamespace.getFullName(cluster, InfoSchemaDb.DATABASE_NAME),
                                                   AccessPrivilege.READ_ONLY);
            String msg = "addUser username=" + user + " password='" + password;
            writeEditsOfAlterAccess(resource, msg);
        } finally {
            writeUnlock();
        }
    }

    public void setPasswd(String user, byte[] password) throws DdlException {
        writeLock();
        try {
            checkUserExists(user);
            this.getAccessResource(user).setPassword(password);
            writeEditsOfAlterAccess(this.getAccessResource(user), "set password");
        } finally {
            writeUnlock();
        }
    }

    public void grant(String user, String db, AccessPrivilege privilege) throws DdlException {
        if (Catalog.getInstance().getDb(db) == null) {
            throw new DdlException("db[" + db + "] does not exist");
        }

        writeLock();
        try {
            checkUserExists(user);
            this.getAccessResource(user).setAccess(db, privilege);

            String msg = "grant user " + user + " db " + db + " privilege " + privilege;
            writeEditsOfAlterAccess(this.getAccessResource(user), msg);
        } finally {
            writeUnlock();
        }
    }

    public void revoke(String user, String db) throws DdlException {
        // we do not check if db is exist in catalog
        // db may be dropped or renamed

        writeLock();
        try {
            checkUserExists(user);
            this.getAccessResource(user).revokeAccess(db);

            String msg = "revoke user " + user + " db " + db;
            writeEditsOfAlterAccess(this.getAccessResource(user), msg);
        } finally {
            writeUnlock();
        }
    }

    public void dropUser(String user) throws DdlException {
        writeLock();
        try {
            checkUserExists(user);
            unprotectDropUser(user);
            editLog.logDropUser(user);
            resourceVersion.incrementAndGet();
        } finally {
            writeUnlock();
        }
    }

    // functions bellow are used to get user information: hasAccess() IsAdmin(),
    // getPassword()
    // TODO(add functionget and set maxCount of user)
    public boolean checkAccess(String user, String db, AccessPrivilege priv) {
        readLock();
        try {
            if (!isUserExists(user)) {
                return false;
            }
            return this.getAccessResource(user).checkAccess(db, priv);
        } finally {
            readUnlock();
        }
    }

    public boolean checkUserAccess(String opUser, String user) {
        if (Strings.isNullOrEmpty(opUser) || Strings.isNullOrEmpty(user)) {
            return false;
        }
        if (isAdmin(opUser)) {
            return true;
        }
        if (isSuperuser(opUser) && !isSuperuser(user)) {
            return true;
        }
        if (opUser.equals(user)) {
            return true;
        }
        return false;
    }

    public byte[] getPassword(String user) {
        readLock();
        try {
            if (!isUserExists(user)) {
                return null;
            }
            return this.getAccessResource(user).getPassword();
        } finally {
            readUnlock();
        }
    }

    public boolean isAdmin(String user) {
        readLock();
        try {
            if (!isUserExists(user)) {
                return false;
            }
            return this.getAccessResource(user).isAdmin();
        } finally {
            readUnlock();
        }
    }

    public boolean isSuperuser(String user) {
        readLock();
        try {
            if (!isUserExists(user)) {
                return false;
            }
            return this.getAccessResource(user).isSuperuser();
        } finally {
            readUnlock();
        }
    }

    public long getMaxConn(String user) {
        readLock();
        try {
            if (!isUserExists(user)) {
                return 0;
            }
            return this.getAccessResource(user).getMaxConn();
        } finally {
            readUnlock();
        }
    }

    private boolean isUserExists(String user) {
        if (Strings.isNullOrEmpty(user)) {
            return false;
        }
        if (this.getAccessResource(user) == null) {
            return false;
        }
        return true;
    }

    // this two function used to read snapshot or write snapshot
    public void write(DataOutput out) throws IOException {
        int numUsers = userMap.size();
        out.writeInt(numUsers);

        for (Map.Entry<String, UserProperty> entry : userMap.entrySet()) {
            entry.getValue().write(out);
        }
        // Write resource version
        out.writeLong(resourceVersion.get());
    }

    public void readFields(DataInput in) throws IOException {
        int numUsers = in.readInt();

        for (int i = 0; i < numUsers; ++i) {
            UserProperty userProperty = UserProperty.read(in);
            userMap.put(userProperty.getUser(), userProperty);
        }

        // Read resource
        resourceVersion = new AtomicLong(in.readLong());
    }

    public int getUserMapSize() {
        return userMap.size();
    }

    // Editlog will call this four function to playback journal
    public void unprotectDropUser(String user) {
        userMap.remove(user);
        // TODO(zhaochun): Now we add resource version every time.
        resourceVersion.incrementAndGet();
    }

    public void replayDropUser(String user) {
        writeLock();
        try {
            unprotectDropUser(user);
        } finally {
            writeUnlock();
        }
    }

    public void unprotectAlterAccess(UserProperty userProperty) {
        userMap.put(userProperty.getUser(), userProperty);
        // TODO(zhaochun): Now we add resource version every time.
        resourceVersion.incrementAndGet();
    }

    public void replayAlterAccess(UserProperty userProperty) {
        writeLock();
        try {
            unprotectAlterAccess(userProperty);
        } finally {
            writeUnlock();
        }
    }

    // private function which used to support four public function
    private void checkUserExists(String user) throws DdlException {
        if (Strings.isNullOrEmpty(user)) {
            throw new DdlException(new String("user is null"));
        }
        if (userMap.get(user) == null) {
            throw new DdlException(new String("user dosn't exists"));
        }
    }

    public void checkUserIfExist(String user) throws DdlException {
        readLock();
        try {
            checkUserExists(user);
        } finally {
            readUnlock();
        }
    }

    private void checkUserNotExists(String user) throws DdlException {
        if (Strings.isNullOrEmpty(user)) {
            throw new DdlException(new String("user is null"));
        }
        if (userMap.get(user) != null) {
            throw new DdlException(new String("user has existed"));
        }
    }

    private UserProperty unprotectAddUser(String cluster, String user, byte[] password) {
        UserProperty resource = new UserProperty();
        resource.setUser(user);
        resource.setPassword(password);
        resource.setClusterName(cluster);
        // 默认“root”用户是管理员，其他需要其他接口
        if (user.equals(ROOT_USER)) {
            resource.setIsAdmin(true);
        }
        try {
            if (user.equals(SYSTEM_RESOURCE_USER)) {
                setSystemUserDefaultResource(resource);
            }
            if (!user.equals(SYSTEM_RESOURCE_USER) && !user.equals(ROOT_USER)) {
                setNormalUserDefaultResource(resource);
            }
        } catch (DdlException e) {
            // this should not happen, because the value is set by us!!
        }
        userMap.put(user, resource);
        return resource;
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

    private void writeEditsOfAlterAccess(UserProperty resource, String msg) {
        editLog.logAlterAccess(resource);
        resourceVersion.incrementAndGet();
    }

    private UserProperty getAccessResource(String user) {
        return userMap.get(user);
    }

    public void updateUserProperty(SetUserPropertyStmt stmt) throws DdlException {
        writeLock();
        try {
            UserProperty property = userMap.get(stmt.getUser());
            if (property == null) {
                throw new DdlException("Unknown user(" + stmt.getUser() + ")");
            }

            property.update(stmt.getPropertyList());

            writeEditsOfAlterAccess(property, "update user property");
        } finally {
            writeUnlock();
        }
    }

    public TFetchResourceResult toResourceThrift() {
        TFetchResourceResult tResult = new TFetchResourceResult();
        tResult.setProtocolVersion(TAgentServiceVersion.V1);
        tResult.setResourceVersion(resourceVersion.get());
        readLock();
        try {
            for (Map.Entry<String, UserProperty> entry : userMap.entrySet()) {
                tResult.putToResourceByUser(entry.getKey(), entry.getValue().getResource().toThrift());
            }
        } finally {
            readUnlock();
        }
        return tResult;
    }

    public Pair<String, DppConfig> getClusterInfo(String user, String cluster) throws DdlException {
        Pair<String, DppConfig> clusterInfo = null;

        readLock();
        try {
            if (!userMap.containsKey(user)) {
                throw new DdlException("User[" + user + "] does not exist");
            }

            UserProperty property = userMap.get(user);
            clusterInfo = property.getClusterInfo(cluster);
        } finally {
            readUnlock();
        }

        return clusterInfo;
    }

    public List<List<String>> fetchUserProperty(String user) throws AnalysisException {
        readLock();
        try {
            if (!userMap.containsKey(user)) {
                throw new AnalysisException("User[" + user + "] does not exist");
            }

            UserProperty property = userMap.get(user);
            return property.fetchProperty();
        } finally {
            readUnlock();
        }
    }

    public void alterUser(AlterUserStmt stmt) throws DdlException {
        List<String> ips = stmt.getIps();
        List<String> starIps = stmt.getStarIps();
        List<String> hosts = stmt.getHosts();
        String user = stmt.getUser();
        AlterUserType type = stmt.getAlterUserType();

        // check host if can dns
        if (type == AlterUserType.ADD_USER_WHITELIST) {
            for (String host : hosts) {
                boolean isAvaliable = DomainParserServer.getInstance().isAvaliableDomain(host);
                if (!isAvaliable) {
                    String msg = "May be error hostname. host=" + host;
                    LOG.warn("alter user={} stmt={} occur dns Exception msg={}", stmt.getUser(), stmt, msg);
                    throw new DdlException(msg);
                }
            }
        }

        writeLock();
        try {
            UserProperty property = userMap.get(user);
            if (property == null) {
                throw new DdlException("use dosn't exists user=" + user);
            }

            WhiteList whiteList = property.getWhiteList();
            String msg = type.toString();

            switch (type) {
                case ADD_USER_WHITELIST: {
                    whiteList.addWhiteList(ips, starIps, hosts);
                    break;
                }
                case DELETE_USER_WHITELIST: {
                    whiteList.deleteWhiteList(ips, starIps, hosts);
                    break;
                }
                default: {
                    LOG.warn("alterUser occur unkown type = {}", type);
                    throw new RuntimeException("unkown type");
                }
            }
            // write editlog
            writeEditsOfAlterAccess(this.getAccessResource(user), msg);
        } finally {
            writeUnlock();
        }
    }

    public boolean checkWhiltListAccess(String user, String remoteIp) {
        if (user.equals("root") && remoteIp.equals("127.0.0.1")) {
            return true;
        }
        readLock();
        try {
            UserProperty property = userMap.get(user);
            if (property == null) {
                return false;
            }
            return property.getWhiteList().hasAccess(remoteIp);
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> showWhiteList(String user) {
        List<List<String>> result = Lists.newArrayList();
        readLock();
        try {
            // ordinary user
            if (!isSuperuser(user)) {
                WhiteList whitelist = userMap.get(user).getWhiteList();
                List<String> row = Lists.newArrayList();
                row.add(user);
                row.add(whitelist.toString());
                result.add(row);
                return result;
            } else {
                for (Map.Entry<String, UserProperty> entry : userMap.entrySet()) {
                    String candidateUser = entry.getKey();
                    boolean addRow = false;
                    // admin can see every one's whitelist
                    // superuse can see own and odinary user's whitelist
                    // ordinary can see own whitelist
                    if (isAdmin(user)) {
                        addRow = true;
                    } else if (user.equals(candidateUser)) {
                        addRow = true;
                    } else if (!isSuperuser(candidateUser)) {
                        addRow = true;
                    }
                    if (addRow) {
                        WhiteList whitelist = userMap.get(candidateUser).getWhiteList();
                        List<String> row = Lists.newArrayList();
                        row.add(candidateUser);
                        row.add(whitelist.toString());
                        result.add(row);
                    }
                }
            }

        } finally {
            readUnlock();
        }
        return result;
    }

    public int getWhiteListSize(String userName) throws DdlException {
        readLock();
        try {
            checkUserExists(userName);
            return userMap.get(userName).getWhiteList().getSize();
        } finally {
            readUnlock();
        }
    }
}
