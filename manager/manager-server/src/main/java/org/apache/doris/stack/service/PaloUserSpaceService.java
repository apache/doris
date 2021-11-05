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

package org.apache.doris.stack.service;

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.exception.DorisHttpPortErrorException;
import org.apache.doris.stack.exception.DorisIpErrorException;
import org.apache.doris.stack.exception.DorisJdbcPortErrorException;
import org.apache.doris.stack.exception.DorisUerOrPassErrorException;
import org.apache.doris.stack.exception.DorisUserNoPermissionException;
import org.apache.doris.stack.model.ldap.LdapUserInfo;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.user.UserGroupRole;
import org.apache.doris.stack.model.request.space.ClusterCreateReq;
import org.apache.doris.stack.model.request.space.UserSpaceCreateReq;
import org.apache.doris.stack.model.request.space.UserSpaceUpdateReq;
import org.apache.doris.stack.model.response.space.UserSpaceInfo;
import org.apache.doris.stack.model.response.user.GroupMember;
import org.apache.doris.stack.util.UuidUtil;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.MailComponent;
import org.apache.doris.stack.component.ManagerMetaSyncComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.connector.PaloLoginClient;
import org.apache.doris.stack.connector.PaloQueryClient;
import org.apache.doris.stack.dao.ClusterInfoRepository;
import org.apache.doris.stack.dao.CoreSessionRepository;
import org.apache.doris.stack.dao.CoreUserRepository;
import org.apache.doris.stack.dao.PermissionsGroupMembershipRepository;
import org.apache.doris.stack.dao.PermissionsGroupRoleRepository;
import org.apache.doris.stack.driver.JdbcSampleClient;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.PermissionsGroupMembershipEntity;
import org.apache.doris.stack.entity.PermissionsGroupRoleEntity;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.exception.DorisConnectionException;
import org.apache.doris.stack.exception.DorisSpaceDuplicatedException;
import org.apache.doris.stack.exception.NameDuplicatedException;
import org.apache.doris.stack.exception.StudioNotInitException;
import org.apache.doris.stack.exception.UserEmailDuplicatedException;
import org.apache.doris.stack.service.construct.MetadataService;
import org.apache.doris.stack.service.config.ConfigConstant;
import org.apache.doris.stack.service.user.AuthenticationService;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Description：doris Manager Create user Doris cluster space when servicing
 */
@Service
@Slf4j
public class PaloUserSpaceService extends BaseService {

    public static final String ADMIN_USER_NAME = "Administrators_";

    public static final String ALL_USER_NAME = "All Users_";

    private static final String PALO_ANALYZER_USER_NAME = "Analyzer";

    @Autowired
    private UtilService utilService;

    @Autowired
    private CoreUserRepository userRepository;

    @Autowired
    private PermissionsGroupRoleRepository groupRoleRepository;

    @Autowired
    private ClusterInfoRepository clusterInfoRepository;

    @Autowired
    private PaloLoginClient paloLoginClient;

    @Autowired
    private ClusterUserComponent clusterUserComponent;

    @Autowired
    private SettingComponent settingComponent;

    @Autowired
    private LdapComponent ldapComponent;

    @Autowired
    private PermissionsGroupMembershipRepository membershipRepository;

    @Autowired
    private CoreSessionRepository coreSessionRepository;

    @Autowired
    private MailComponent mailComponent;

    @Autowired
    private MetadataService managerMetadataService;

    @Autowired
    private ManagerMetaSyncComponent managerMetaSyncComponent;

    @Autowired
    private JdbcSampleClient jdbcClient;

    @Autowired
    private PaloQueryClient queryClient;

    @Transactional
    public int create(UserSpaceCreateReq createReq) throws Exception {
        log.debug("Super user create palo user space.");
        // Verify whether the initialization of the space authentication method is completed
        SettingEntity enabled = settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY);

        if (enabled == null || StringUtils.isEmpty(enabled.getValue())) {
            log.debug("The auth type not be inited.");
            throw new StudioNotInitException();
        }

        // LDAP enable?
        boolean ldapEnable = enabled.getValue().equals(InitStudioReq.AuthType.ldap.name());

        checkRequestBody(createReq.hasEmptyField(ldapEnable));

        // check mail format
        utilService.emailCheck(createReq.getUser().getEmail());

        // check space name
        nameCheck(createReq.getName());

        // create space information
        ClusterInfoEntity clusterInfoInit = null;
        // Initialize space administrator user information
        CoreUserEntity userEntity = null;

        // LDAP enable?
        if (ldapEnable) {
            // If the added local administrator already exists - > operate the local administrator,
            // otherwise - > find through LDAP
            List<CoreUserEntity> userList = userRepository.getByEmail(createReq.getUser().getEmail());
            if (userList != null && userList.size() == 1) {
                CoreUserEntity user = userList.get(0);
                SettingEntity defaultGroup = settingComponent.readSetting(ConfigConstant.DEFAULT_GROUP_KEY);
                List<PermissionsGroupMembershipEntity> groupUser = membershipRepository
                        .getByUserIdNoDefaultGroup(user.getId(), Integer.parseInt(defaultGroup.getValue()));
                if (groupUser != null && groupUser.size() > 0) {
                    log.error("Email address already belong to cluster.");
                    throw new UserEmailDuplicatedException();
                }

                // Delete default group user
                membershipRepository.deleteByUserIdAndGroupId(Integer.parseInt(defaultGroup.getValue()), user.getId());
                userEntity = new CoreUserEntity(user);
                userEntity.setSuperuser(true);
            } else {
                LdapUserInfo ldapUser = ldapComponent.searchUserByEmail(createReq.getUser().getEmail());
                userEntity = new CoreUserEntity(createReq.getUser(), ldapUser);
                userEntity.setLdapAuth(true);
                userEntity.setPassword(ldapUser.getPassword());
            }
        } else {
            // Check whether the administrator user already exists
            log.debug("Check admin user email {} is duplicate.", createReq.getUser().getEmail());
            List<CoreUserEntity> userEntities = userRepository.getByEmail(createReq.getUser().getEmail());
            if (userEntities != null && userEntities.size() != 0) {
                log.error("The admin user email address already in use.");
                throw new UserEmailDuplicatedException();
            }

            // Check whether the password meets the specification
            utilService.passwordCheck(createReq.getUser().getPassword());
            userEntity = new CoreUserEntity(createReq.getUser());
        }

        if (createReq.getCluster() != null) {
            log.debug("Add doris cluster info.");
            clusterInfoInit = validateCluster(createReq.getCluster());
        } else {
            log.debug("Skip add palo cluster info.");
            clusterInfoInit = new ClusterInfoEntity();
        }
        clusterInfoInit.setName(createReq.getName());
        clusterInfoInit.setDescription(createReq.getDescribe());
        clusterInfoInit.setAdminUserMail(createReq.getUser().getEmail());
        clusterInfoInit.setCreateTime(new Timestamp(System.currentTimeMillis()));

        ClusterInfoEntity clusterInfoSave = clusterInfoRepository.save(clusterInfoInit);

        // Create a space administrator user
        log.debug("Add admin user for palo cluster space");

        // update password and store
        utilService.setPassword(userEntity, createReq.getUser().getPassword());
        int userId = userRepository.save(userEntity).getId();
        log.debug("Add admin user {} for palo cluster space success.", userId);

        // Initialize the permission group management information of the space
        initSpaceAdminPermisssion(userId, clusterInfoSave);

        // Initialize the correspondence between permission group and Doris virtual user
        if (createReq.getCluster() != null) {
            initGroupPaloUser(clusterInfoSave);
        }

        // Synchronize Doris metadata
        managerMetadataService.syncMetadataByCluster(clusterInfoSave);

        // Send invitation mail
        String joinUrl = "";
        mailComponent.sendInvitationMail(createReq.getUser().getEmail(),
                AuthenticationService.SUPER_USER_NAME_VALUE, joinUrl);

        return clusterInfoSave.getId();
    }

    /**
     * Change cluster space information
     * 1. Only modify the space name and other information;
     * 2. Add new cluster connection information;
     * TODO: Currently, modifying cluster information is not supported
     * @param userId
     * @param spaceId
     * @param updateReq
     * @return
     * @throws Exception
     */
    @Transactional
    public UserSpaceInfo update(int userId, int spaceId, UserSpaceUpdateReq updateReq) throws Exception {
        checkRequestBody(updateReq.hasEmptyField());

        log.debug("update space {} information.", spaceId);
        ClusterInfoEntity clusterInfo = clusterInfoRepository.findById(spaceId).get();

        clusterUserComponent.checkUserBelongToCluster(userId, spaceId);

        if (!clusterInfo.getName().equals(updateReq.getName())) {
            nameCheck(updateReq.getName());
        }
        log.debug("The name not be changed");
        clusterInfo.setName(updateReq.getName());
        clusterInfo.setDescription(updateReq.getDescribe());
        clusterInfo.setUpdateTime(new Timestamp(System.currentTimeMillis()));
        log.debug("update space info.");

        // Determine whether the cluster information needs to be changed
        if (updateReq.getCluster() != null) {
            checkRequestBody(updateReq.hasEmptyField());
            // TODO: if the user name and password of Doris cluster already exist, they cannot be modified
            if (clusterInfo.getAddress() != null) {
                log.error("The space palo cluster information already exists.");
                throw new DorisSpaceDuplicatedException();
            }
            validateCluster(updateReq.getCluster());

            clusterInfo.updateByClusterInfo(updateReq.getCluster());

            // Initialize the correspondence between permission group and Doris virtual user
            initGroupPaloUser(clusterInfo);

            // Synchronize Doris metadata
            managerMetadataService.syncMetadataByCluster(clusterInfo);
        }
        ClusterInfoEntity clusterInfoSave = clusterInfoRepository.save(clusterInfo);

        UserSpaceInfo result = clusterInfoSave.transToModel();
        result.setSpaceAdminUser(getAdminGroupAllMail(clusterInfo.getAdminGroupId()));

        return result;
    }

    public ClusterInfoEntity validateCluster(ClusterCreateReq createReq) throws Exception {
        log.debug("validate palo cluster info.");
        checkRequestBody(createReq.hasEmptyField());
        log.info("Verify that the Palo cluster already exists.");
        List<ClusterInfoEntity> exsitEntities =
                clusterInfoRepository.getByAddressAndPort(createReq.getAddress(), createReq.getHttpPort());
        if (exsitEntities != null && exsitEntities.size() != 0) {
            log.error("The palo cluster {} is already associated with space.", createReq.getAddress() + ":"
                    + createReq.getHttpPort());
            throw new DorisSpaceDuplicatedException();
        }

        log.info("Verify that the Palo cluster is available");
        ClusterInfoEntity entity = new ClusterInfoEntity();
        entity.updateByClusterInfo(createReq);
        // Just verify whether the Doris HTTP interface can be accessed
        try {
            paloLoginClient.loginPalo(entity);
        } catch (Exception e) {
            log.error("Doris cluster http access error.");
            if (e.getMessage().contains("nodename nor servname provided, or not known")) {
                throw new DorisIpErrorException();
            } else if (e.getMessage().contains("failed: Connection refused (Connection refused)")) {
                throw new DorisHttpPortErrorException();
            } else if (e.getMessage().contains("Login palo error:Access denied for default_cluster")) {
                throw new DorisUserNoPermissionException();
            } else if (e.getMessage().contains("Login palo error:Access denied")) {
                throw new DorisUerOrPassErrorException();
            }
            throw new DorisConnectionException();
        }

        // Just verify whether the Doris JDBC protocol can be accessed
        try {
            jdbcClient.testConnetion(createReq.getAddress(), createReq.getQueryPort(),
                    ConstantDef.MYSQL_DEFAULT_SCHEMA, createReq.getUser(), createReq.getPasswd());
            log.debug("Doris cluster jdbc access success.");
        } catch (Exception e) {
            log.error("Doris cluster jdbc access error.");
            throw new DorisJdbcPortErrorException();
        }

        // The manager function is enabled by default
        entity.setManagerEnable(true);
        return entity;
    }

    public boolean nameCheck(String name) throws Exception {
        log.debug("Check cluster name {} is duplicate.", name);
        List<ClusterInfoEntity> clusterInfos = clusterInfoRepository.getByName(name);
        if (clusterInfos != null && clusterInfos.size() != 0) {
            log.error("The space name {} already exists.", name);
            throw new NameDuplicatedException();
        }
        return true;
    }

    public List<UserSpaceInfo> getAllSpaceBySuperUser() {
        log.debug("Super user get all space list.");
        // Get a list of all spaces and sort them from small to large according to ID
        List<ClusterInfoEntity> clusterInfos = clusterInfoRepository.findAll(Sort.by("id").ascending());
        List<UserSpaceInfo> spaceInfos = Lists.newArrayList();
        for (ClusterInfoEntity clusterInfo : clusterInfos) {
            UserSpaceInfo spaceInfo = clusterInfo.transToModel();
            spaceInfo.setSpaceAdminUser(getAdminGroupAllMail(clusterInfo.getAdminGroupId()));
            spaceInfos.add(spaceInfo);
        }
        return spaceInfos;
    }

    public UserSpaceInfo getById(int userId, int spaceId) throws Exception {
        log.debug("User {} get space {} info.", userId, spaceId);
        ClusterInfoEntity clusterInfo = clusterInfoRepository.findById(spaceId).get();

        if (userId > 0) {
            // If you are not a super administrator, you need to check whether the user is a space administrator group
            clusterUserComponent.checkUserBelongToCluster(userId, spaceId);
        }

        UserSpaceInfo result = clusterInfo.transToModel();
        result.setSpaceAdminUser(getAdminGroupAllMail(clusterInfo.getAdminGroupId()));

        return result;
    }

    /**
     * Delete a space's information
     * 1. Information of space itself
     * 2. Space permissions and user group information
     * 3. User information of space
     *
     * @param spaceId
     * @throws Exception
     */
    @Transactional
    public void deleteSpace(int spaceId) throws Exception {
        ClusterInfoEntity clusterInfo = clusterInfoRepository.findById(spaceId).get();

        // delete cluster information
        clusterInfoRepository.deleteById(spaceId);

        // delete cluster configuration
        log.debug("delete cluster {} config infos.", spaceId);
        settingComponent.deleteAdminSetting(spaceId);

        log.debug("delete cluster {} manager metadata and data permission.", spaceId);
        managerMetaSyncComponent.deleteClusterMetadata(clusterInfo);

        // Delete the doris jdbc agent account
        PermissionsGroupRoleEntity allUserGroup = groupRoleRepository.findById(clusterInfo.getAllUserGroupId()).get();
        log.debug("Delete cluster {} analyzer user {}.", spaceId, allUserGroup.getPaloUserName());
        queryClient.deleteUser(ConstantDef.DORIS_DEFAULT_NS, ConstantDef.MYSQL_DEFAULT_SCHEMA, clusterInfo, allUserGroup.getPaloUserName());

        // Get all user groups in the cluster
        HashSet<Integer> groupIds = groupRoleRepository.getGroupIdByClusterId(spaceId);
        // Get all users in the cluster
        Set<Integer> userIds = membershipRepository.getByGroupId(groupIds);

        // Delete permission user group and delete the mapping relationship between user and group
        log.debug("delete cluster {} all permission group and group user membership.", spaceId);
        for (int groupId : groupIds) {
            groupRoleRepository.deleteById(groupId);
            membershipRepository.deleteByGroupId(groupId);
        }

        SettingEntity defaultGroup = settingComponent.readSetting(ConfigConstant.DEFAULT_GROUP_KEY);
        int defaultGroupId = Integer.parseInt(defaultGroup.getValue());

        // delete user information
        log.debug("delete cluster {} all user info.", spaceId);
        for (int userId : userIds) {
            CoreUserEntity userEntity = userRepository.findById(userId).get();
            if (userEntity.getLdapAuth()) {
                PermissionsGroupMembershipEntity permissionsGroupMembershipEntity = new PermissionsGroupMembershipEntity();
                permissionsGroupMembershipEntity.setGroupId(defaultGroupId);
                permissionsGroupMembershipEntity.setUserId(userId);
                membershipRepository.save(permissionsGroupMembershipEntity);
            } else {
                // clear session
                coreSessionRepository.deleteByUserId(userId);
                // delete user
                userRepository.deleteById(userId);
            }
        }
    }

    /**
     * Initialize the Palo cluster user and password corresponding to the space user group
     *
     * @param clusterInfo
     */
    private void initGroupPaloUser(ClusterInfoEntity clusterInfo) throws Exception {
        log.debug("Palo cluster validate，init group palo user.");

        // The administrator group directly initializes the admin privileged
        PermissionsGroupRoleEntity adminGroup = groupRoleRepository.findById(clusterInfo.getAdminGroupId()).get();
        adminGroup.setPaloUserName(clusterInfo.getUser());
        adminGroup.setPassword(clusterInfo.getPasswd());
        groupRoleRepository.save(adminGroup);

        // create select user in Doris for manager jdbc access
        PermissionsGroupRoleEntity allUserGroup = groupRoleRepository.findById(clusterInfo.getAllUserGroupId()).get();

        String userName = PALO_ANALYZER_USER_NAME + UuidUtil.newUuidString();
        String password = queryClient.createUser(ConstantDef.DORIS_DEFAULT_NS, ConstantDef.MYSQL_DEFAULT_SCHEMA,
                clusterInfo, userName);
        allUserGroup.setPaloUserName(userName);
        allUserGroup.setPassword(password);

        groupRoleRepository.save(allUserGroup);
        log.debug("save palo user for group");
    }

    /**
     * Initialize permission groups in space
     *
     * @param userId
     * @param clusterInfoEntity
     */
    private void initSpaceAdminPermisssion(int userId, ClusterInfoEntity clusterInfoEntity) {
        int clusterId = clusterInfoEntity.getId();
        log.debug("Init palo user cluster {} admin user {} permissions.", clusterId, userId);

        // Each space needs to create two user groups, admin and all user
        // Create space admin group
        int adminGroupId = clusterUserComponent.addPermissionsGroup(ADMIN_USER_NAME + clusterId,
                clusterId, UserGroupRole.Administrator);
        log.debug("Init palo user space {} admin group is {}.", clusterId, adminGroupId);

        // Create space all user group
        int allUserGroupId = clusterUserComponent.addPermissionsGroup(ALL_USER_NAME + clusterId,
                clusterId, UserGroupRole.Analyzer);
        log.debug("Init palo user space {} all user group is {}.", clusterId, allUserGroupId);

        // Initialize the relationship between permission groups and users
        clusterUserComponent.addGroupUserMembership(userId, adminGroupId);
        clusterUserComponent.addGroupUserMembership(userId, allUserGroupId);
        log.debug("Init group and user memership completed.");

        clusterInfoEntity.setAdminUserId(userId);
        clusterInfoEntity.setAdminGroupId(adminGroupId);
        clusterInfoEntity.setAllUserGroupId(allUserGroupId);

        clusterInfoRepository.save(clusterInfoEntity);
        log.debug("Save palo user space user and group.");
    }

    private List<String> getAdminGroupAllMail(int groupId) {
        List<GroupMember> members = clusterUserComponent.getGroupMembers(groupId);
        List<String> allMails = new ArrayList<>();
        for (GroupMember member : members) {
            allMails.add(member.getEmail());
        }

        return allMails;
    }
}
