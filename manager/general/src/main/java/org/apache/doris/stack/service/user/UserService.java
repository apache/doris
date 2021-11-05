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

package org.apache.doris.stack.service.user;

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.ldap.LdapConnectionInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfoReq;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.user.PasswordUpdateReq;
import org.apache.doris.stack.model.request.user.UserAddReq;
import org.apache.doris.stack.model.request.user.UserUpdateReq;
import org.apache.doris.stack.model.response.user.UserInfo;
import org.apache.doris.stack.constant.PropertyDefine;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.MailComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.connector.LdapClient;
import org.apache.doris.stack.dao.ClusterInfoRepository;
import org.apache.doris.stack.dao.CoreSessionRepository;
import org.apache.doris.stack.dao.CoreUserRepository;
import org.apache.doris.stack.dao.PermissionsGroupMembershipRepository;
import org.apache.doris.stack.dao.PermissionsGroupRoleRepository;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.PermissionsGroupMembershipEntity;
import org.apache.doris.stack.entity.PermissionsGroupRoleEntity;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.exception.NoPermissionException;
import org.apache.doris.stack.exception.ResetPasswordException;
import org.apache.doris.stack.exception.UserEmailDuplicatedException;
import org.apache.doris.stack.exception.UserLoginException;
import org.apache.doris.stack.service.BaseService;
import org.apache.doris.stack.service.config.ConfigConstant;
import org.apache.doris.stack.service.UtilService;
import com.google.common.collect.Lists;
import com.unboundid.ldap.sdk.LDAPConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class UserService extends BaseService {

    @Autowired
    private CoreSessionRepository sessionRepository;

    @Autowired
    private CoreUserRepository userRepository;

    @Autowired
    private PermissionsGroupMembershipRepository membershipRepository;

    @Autowired
    private UtilService utilService;

    @Autowired
    private MailComponent mailComponent;

    @Autowired
    private ClusterUserComponent clusterUserComponent;

    @Autowired
    private LdapComponent ldapComponent;

    @Autowired
    private LdapClient ldapClient;

    @Autowired
    private SettingComponent settingComponent;

    @Autowired
    private PermissionsGroupRoleRepository permissionsGroupRoleRepository;

    @Autowired
    private ClusterInfoRepository clusterInfoRepository;

    @Autowired
    private Environment environment;

    /**
     * Get all users of the current Doris cluster space
     *
     * @param includeDeactivated
     * @param userId
     * @return
     * @throws Exception
     */
    public List<UserInfo> getAllUser(boolean includeDeactivated, int userId) throws Exception {
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(userId);
        int clusterId = clusterInfo.getId();

        if (includeDeactivated) {
            log.debug("get space {} all users", clusterId);
        } else {
            log.debug("get space {} all active users", clusterId);
        }

        // Get the members of all user groups in the current cluster, that is, all users
        log.debug("Get all user for cluster {}.", clusterId);
        List<Integer> allUserIds = membershipRepository.getUserIdsByGroupId(clusterInfo.getAllUserGroupId());

        log.debug("cluster {} all users count is {}.", clusterId, allUserIds.size());

        List<UserInfo> userInfos = Lists.newArrayList();
        for (int id : allUserIds) {
            CoreUserEntity userEntity = userRepository.findById(id).get();
            if (!includeDeactivated && !userEntity.isActive()) {
                continue;
            }
            UserInfo userInfo = getUserByEntity(userEntity);
            userInfos.add(userInfo);
        }

        if (includeDeactivated) {
            log.debug("get all users end.");
        } else {
            log.debug("get all active users end.");
        }

        return userInfos;
    }

    /**
     * Get current user information according to ID
     *
     * @param userId
     * @return
     * @throws Exception
     */
    public UserInfo getCurrentUser(int userId) throws Exception {
        log.debug("Get current user by id {}.", userId);
        int clusterId = clusterUserComponent.getClusterIdByUserId(userId);
        CoreUserEntity userEntity = userRepository.findById(userId).get();

        UserInfo userInfo;
        boolean managerEnable;
        if (clusterId > 0) {
            userInfo = getUserByEntity(userEntity);

            userInfo.setSpaceId(clusterId);
            ClusterInfoEntity clusterInfo = clusterInfoRepository.findById(clusterId).get();
            userInfo.setSpaceName(clusterInfo.getName());
            userInfo.setCollectionId(clusterInfo.getCollectionId());
            if (clusterInfo.getAddress() == null) {
                userInfo.setSpaceComplete(false);
            } else {
                userInfo.setSpaceComplete(true);
            }
            managerEnable = clusterInfo.isManagerEnable();
        } else {
            userInfo = getDefaultUserByEntity(userEntity);
            userInfo.setSpaceComplete(false);
            managerEnable = false;
        }

        SettingEntity authType = settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY);
        if (!authType.equals(Optional.empty()) && !StringUtils.isEmpty(authType.getValue())) {
            userInfo.setAuthType(InitStudioReq.AuthType.valueOf(authType.getValue()));
        }

        String deployName = environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY);
        userInfo.setDeployType(deployName);
        if (deployName.equals(PropertyDefine.DEPLOY_TYPE_MANAGER)) {
            log.debug("manager deploy manager enable true.");
            userInfo.setManagerEnable(true);
        } else {
            log.debug("studio deploy manager enable {}.", managerEnable);
            userInfo.setManagerEnable(managerEnable);
        }

        return userInfo;
    }

    /**
     * The user information is obtained according to the ID,
     * which can only be obtained by the user itself or the admin user
     *
     * @param userId
     * @param requestUserId
     * @return
     * @throws Exception
     */
    public UserInfo getUserById(int userId, int requestUserId) throws Exception {
        log.debug("Get request user id {}", requestUserId);
        checUserSelfOrSuperUser(requestUserId, userId);
        clusterUserComponent.checkUserSameCluster(requestUserId, userId);
        return getCurrentUser(userId);
    }

    /**
     * Add a new user and send an invitation email. Please reset the password
     *
     * @param userAddReq
     * @param invitorId
     * @throws Exception
     */
    @Transactional
    public UserInfo addUser(UserAddReq userAddReq, int invitorId) throws Exception {
        checkRequestBody(userAddReq.hasEmptyField(ldapComponent.enabled()));

        log.debug("Invitor {} add user", invitorId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(invitorId);

        CoreUserEntity invitor = userRepository.findById(invitorId).get();

        boolean isLdap = ldapComponent.enabled();

        // Check whether the mailbox format meets the specification
        utilService.emailCheck(userAddReq.getEmail());

        CoreUserEntity newUser;
        // Open LDAP
        if (isLdap) {

            // If the added user already exists - > operate the local user, otherwise - > find through LDAP
            List<CoreUserEntity> userList = userRepository.getByEmail(userAddReq.getEmail());
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
                newUser = new CoreUserEntity(user);
            } else {
                newUser = searchByLdap(userAddReq);
            }
        } else {
            // Check whether the mailbox is duplicate
            checkEmailDuplicate(userAddReq.getEmail());

            // Create user
            newUser = new CoreUserEntity(userAddReq);

            if (userAddReq.getPassword() != null) {
                // Check whether the password meets the specification
                utilService.passwordCheck(userAddReq.getPassword());
            }
        }

        // Set the user password and store the user into the database
        utilService.setPassword(newUser, userAddReq.getPassword());

        // Set password reset token
        String resetTokenStr = utilService.resetUserToken(newUser, true);

        CoreUserEntity saveNewUser = userRepository.save(newUser);

        UserInfo userInfo = saveNewUser.castToUserInfo();

        // Add user to default user group
        log.debug("Add user default group.");
        clusterUserComponent.addGroupUserMembership(saveNewUser.getId(), clusterInfo.getAllUserGroupId());

        List<Integer> groupIds = Lists.newArrayList(clusterInfo.getAllUserGroupId());

        // If it is a manager application, the user will also be added to the admin user group by default
        String deployName = environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY);
        if (deployName.equals(PropertyDefine.DEPLOY_TYPE_MANAGER)) {
            log.debug("Manager deploy, user add admin group.");
            clusterUserComponent.addGroupUserMembership(saveNewUser.getId(), clusterInfo.getAdminGroupId());
            groupIds.add(clusterInfo.getAdminGroupId());
        }

        userInfo.setGroupIds(groupIds);

        log.debug("Add user success.");

        // If it is LDAP authentication, send the login link directly. If it is studio's own authentication,
        // send the reset password link
        String joinUrl = isLdap ? "" : utilService.getUserJoinUrl(saveNewUser.getId(), resetTokenStr);

        // Send user invitation email
        mailComponent.sendInvitationMail(userAddReq.getEmail(), invitor.getFirstName(), joinUrl);

        log.debug("Send invitation mail.");

        return userInfo;
    }

    /**
     * search ldap user
     *
     * @param userAddReq
     * @return
     * @throws Exception
     */
    private CoreUserEntity searchByLdap(UserAddReq userAddReq) throws Exception {
        LdapConnectionInfo connInfo = ldapComponent.getConnInfo();
        LDAPConnection connection = ldapClient.getConnection(connInfo);

        LdapUserInfoReq userInfoReq = new LdapUserInfoReq();
        userInfoReq.setBaseDn(ldapComponent.getBaseDn());
        userInfoReq.setUserAttribute(connInfo.getAttributeEmail());
        userInfoReq.setUserValue(userAddReq.getEmail());

        LdapUserInfo ldapUser = ldapClient.getUser(connection, userInfoReq);
        if (ldapUser.getExist()) {
            CoreUserEntity user = new CoreUserEntity(userAddReq, ldapUser);
            return user;
        }
        log.error("Ldap user {} is not exist, please check and try again.", userAddReq.getEmail());
        throw new UserLoginException();
    }

    @Transactional
    public UserInfo updateUser(UserUpdateReq userUpdateReq, int requestId, int userId) throws Exception {
        log.debug("user {} update user {} info.", requestId, userId);

        checkRequestBody(userUpdateReq.hasEmptyField());

        checUserSelfOrSuperUser(requestId, userId);

        clusterUserComponent.checkUserSameCluster(requestId, userId);

        CoreUserEntity userEntity = userRepository.findById(userId).get();

        utilService.checkUserActive(userEntity);

        if (!userEntity.getEmail().equals(userUpdateReq.getEmail())) {
            checkEmailDuplicate(userUpdateReq.getEmail());

            // Check whether the mailbox format meets the specification
            utilService.emailCheck(userUpdateReq.getEmail());
            log.debug("The mail changed.");

            userEntity.setEmail(userUpdateReq.getEmail());
        }

        userEntity.setFirstName(userUpdateReq.getName());
        userEntity.setLocale(userUpdateReq.getLocale());
        userEntity.setUpdatedAt(new Timestamp(System.currentTimeMillis()));

        CoreUserEntity updateUser = userRepository.save(userEntity);
        log.debug("update user into database.");

        return getUserByEntity(updateUser);
    }

    @Transactional
    public UserInfo reactivateUser(int userId, int requestId) throws Exception {
        log.debug("Admin user {} reactivate not active user {}.", requestId, userId);
        clusterUserComponent.checkUserSameCluster(requestId, userId);
        CoreUserEntity user = userRepository.findById(userId).get();
        if (user.isActive()) {
            log.error("Reactivate an active user");
            throw new NoPermissionException();
        }

        if (userId == requestId) {
            log.error("Reactivate an super user self");
            throw new NoPermissionException();
        }

        log.debug("Update user to active.");
        user.setActive(true);
        user.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        CoreUserEntity updateUser = userRepository.save(user);
        return getUserByEntity(updateUser);
    }

    @Transactional
    public boolean stopUser(int userId, int requestId) throws Exception {
        log.debug("Admin user {} stop active user {}.", requestId, userId);
        clusterUserComponent.checkUserSameCluster(requestId, userId);
        CoreUserEntity user = userRepository.findById(userId).get();
        utilService.checkUserActive(user);
        if (userId == requestId) {
            log.error("Stop an super user self");
            throw new NoPermissionException();
        }

        if (!user.isActive()) {
            log.error("stop an not active user");
            throw new NoPermissionException();
        }
        log.debug("Update user to not active.");
        user.setActive(false);
        user.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        userRepository.save(user);
        return true;
    }

    @Transactional
    public boolean moveUser(int userId, int requestId) throws Exception {
        log.debug("Admin user {} move active user {}.", requestId, userId);
        int clusterId = clusterUserComponent.checkUserSameCluster(requestId, userId);
        CoreUserEntity user = userRepository.findById(userId).get();
        utilService.checkUserActive(user);
        if (!user.getLdapAuth()) {
            log.error("Unsupported move studio user.");
            throw new NoPermissionException();
        }

        if (userId == requestId) {
            log.error("You can't move out of yourself.");
            throw new NoPermissionException();
        }

        HashSet<Integer> groupIds = permissionsGroupRoleRepository.getGroupIdByClusterId(clusterId);
        for (int groupId : groupIds) {
            membershipRepository.deleteByUserIdAndGroupId(groupId, userId);
        }
        // Default group association
        SettingEntity defaultGroup = settingComponent.readSetting(ConfigConstant.DEFAULT_GROUP_KEY);
        PermissionsGroupMembershipEntity permissionsGroupMembershipEntity = new PermissionsGroupMembershipEntity();
        permissionsGroupMembershipEntity.setGroupId(Integer.parseInt(defaultGroup.getValue()));
        permissionsGroupMembershipEntity.setUserId(userId);
        membershipRepository.save(permissionsGroupMembershipEntity);

        user.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        userRepository.save(user);
        return true;
    }

    /**
     * Modify the user password and delete all old sessions of the user
     *
     * @param updateReq
     * @param userId
     * @param requestId
     * @return
     * @throws Exception
     */
    @Transactional
    public UserInfo updatePassword(PasswordUpdateReq updateReq, int userId, int requestId) throws Exception {
        log.debug("update password user: {} requestUserId: {}", userId, requestId);
        CoreUserEntity requestUser = userRepository.findById(requestId).get();
        CoreUserEntity updateUser;
        // Users modify their own passwords
        if (requestId == userId && updateReq.getOldPassword() != null) {
            checkRequestBody(updateReq.hasEmptyField());

            log.debug("update user {} password.", userId);

            if (updateReq.checkPasswdSame()) {
                log.error("The new password is the same as the old one.");
                throw new ResetPasswordException();
            }

            CoreUserEntity userEntity = userRepository.findById(userId).get();
            // Verify that the old password is correct
            utilService.verifyPassword(userEntity.getPasswordSalt(), updateReq.getOldPassword(), userEntity.getPassword());

            // Check whether the new password meets the specification
            utilService.passwordCheck(updateReq.getPassword());

            utilService.setPassword(userEntity, updateReq.getPassword());

            updateUser = userRepository.save(userEntity);

            // The password has been changed.
            // You want to delete the previous authentication session information of the user
            log.debug("Delete user old session.");
            sessionRepository.deleteByUserId(userEntity.getId());
        } else if (requestUser.isSuperuser()) {
            checkRequestBody(updateReq.getPassword() == null);
            clusterUserComponent.checkUserSameCluster(requestId, userId);
            CoreUserEntity userEntity = userRepository.findById(userId).get();

            // Check whether the new password meets the specification
            utilService.passwordCheck(updateReq.getPassword());
            utilService.setPassword(userEntity, updateReq.getPassword());

            updateUser = userRepository.save(userEntity);

            log.debug("Delete user old session.");
            sessionRepository.deleteByUserId(userEntity.getId());

        } else {
            log.error("The request user {} neither a super user nor the user {} itself.", requestId, userId);
            throw new NoPermissionException();
        }
        return getUserByEntity(updateUser);
    }

    @Transactional
    public boolean setQbnewb(int requestId, int userId) throws Exception {
        log.debug("Set qpnewb false.");

        checUserSelfOrSuperUser(requestId, userId);

        clusterUserComponent.checkUserSameCluster(requestId, userId);

        CoreUserEntity userEntity = userRepository.findById(userId).get();

        boolean isQbnewb = userEntity.isQbnewb();

        userEntity.setQbnewb(!isQbnewb);
        userEntity.setUpdatedAt(new Timestamp(System.currentTimeMillis()));

        log.debug("update user info.");

        userRepository.save(userEntity);

        log.debug("Set qpnewb false success.");

        return true;
    }

    @Transactional
    public boolean sendInvite(int requestId, int userId) throws Exception {
        log.debug("Invitor {} resend the user invite email for a given use {}", requestId, userId);

        CoreUserEntity invitor = userRepository.findById(requestId).get();

        CoreUserEntity user = userRepository.findById(userId).get();

        utilService.checkUserActive(user);

        //Set password reset token
        String resetTokenStr = utilService.resetUserToken(user, true);
        user.setUpdatedAt(new Timestamp(System.currentTimeMillis()));

        CoreUserEntity newUser = userRepository.save(user);

        String joinUrl = utilService.getUserJoinUrl(newUser.getId(), resetTokenStr);

        // Send user invitation email
        mailComponent.sendInvitationMail(newUser.getEmail(), invitor.getFirstName(), joinUrl);

        return true;
    }

    @Transactional
    public void deleteUser(int userId) {
        log.debug("delete user, userId: {}", userId);

        userRepository.deleteById(userId);

        sessionRepository.deleteByUserId(userId);

        membershipRepository.deleteByUserId(userId);

        log.debug("delete successful.");
    }

    /**
     * Initialize default user groups at system startup
     */
    @Transactional
    public void initDefaultUserGroup() {
        try {
            log.debug("Init default user group.");
            SettingEntity groupIdSetting = settingComponent.readSetting(ConfigConstant.DEFAULT_GROUP_KEY);

            PermissionsGroupRoleEntity permissionsGroupRoleEntity = permissionsGroupRoleRepository.getByGroupName(ConstantDef.USER_DEFAULT_GROUP_NAME);
            if (groupIdSetting == null || StringUtils.isEmpty(groupIdSetting.getValue())) {
                log.debug("Default group setting not exist.");
                int defaultGroupId = 0;
                if (permissionsGroupRoleEntity != null) {
                    log.debug("Default permission group already exist");
                    defaultGroupId = permissionsGroupRoleEntity.getGroupId();
                } else {
                    permissionsGroupRoleRepository.deleteByGroupName(ConstantDef.USER_DEFAULT_GROUP_NAME);
                    defaultGroupId = createDefault();
                }
                settingComponent.addNewSetting(ConfigConstant.DEFAULT_GROUP_KEY, String.valueOf(defaultGroupId));
                log.debug("Init default user group done.");
            } else {
                log.debug("Default group setting already exist.");
                int oldGroupId = Integer.parseInt(groupIdSetting.getValue());
                if (permissionsGroupRoleEntity != null && permissionsGroupRoleEntity.getGroupId() == oldGroupId) {
                    log.debug("Default user group already exist");
                } else {
                    log.debug("Default permission group change.");
                    settingComponent.deleteSetting(ConfigConstant.DEFAULT_GROUP_KEY);
                    permissionsGroupRoleRepository.deleteByGroupName(ConstantDef.USER_DEFAULT_GROUP_NAME);
                    int newDefaultGroupId = createDefault();
                    settingComponent.addNewSetting(ConfigConstant.DEFAULT_GROUP_KEY, String.valueOf(newDefaultGroupId));

                    updateDefaultMembership(oldGroupId, newDefaultGroupId);

                    log.debug("Update Default permission group success.");
                }
            }
            log.debug("Init default user group success.");
        } catch (Exception e) {
            log.error("Init default user group error.");
            e.printStackTrace();
        }
    }

    private void checUserSelfOrSuperUser(int requestUserId, int userId) throws Exception {
        log.debug("check request user {} is super or user {} itself.", requestUserId, userId);
        CoreUserEntity requestUser = userRepository.findById(requestUserId).get();
        if (!requestUser.isSuperuser() && userId != requestUserId) {
            log.error("The request user {} neither a super user nor the user {} itself.", requestUserId, userId);
            throw new NoPermissionException();
        }
    }

    private void checkEmailDuplicate(String email) throws Exception {
        log.debug("Check email {} is duplicate.", email);
        List<CoreUserEntity> userEntities = userRepository.getByEmail(email);
        if (userEntities != null && userEntities.size() != 0) {
            log.error("The email address already in use.");
            throw new UserEmailDuplicatedException();
        }
    }

    private UserInfo getDefaultUserByEntity(CoreUserEntity userEntity) throws Exception {
        log.debug("Get user info by user entity.");
        UserInfo userInfo = userEntity.castToUserInfo();
        userInfo.setLdapAuth(userEntity.getLdapAuth());

        return userInfo;
    }

    private UserInfo getUserByEntity(CoreUserEntity userEntity) throws Exception {
        log.debug("Get user info by user entity.");
        UserInfo userInfo = userEntity.castToUserInfo();
        int userId = userEntity.getId();
        userInfo.setLdapAuth(userEntity.getLdapAuth());
        log.debug("get user permission group.");
        List<PermissionsGroupMembershipEntity> membershipEntities =
                membershipRepository.getByUserId(userId);
        List<Integer> groupIds = Lists.newArrayList();
        for (PermissionsGroupMembershipEntity membershipEntity : membershipEntities) {
            groupIds.add(membershipEntity.getGroupId());
        }
        userInfo.setGroupIds(groupIds);
        return userInfo;
    }

    private void updateDefaultMembership(int oldGroupId, int newGroupId) {
        List<PermissionsGroupMembershipEntity> membershipGroups = membershipRepository.getByGroupId(oldGroupId);
        log.debug("Default permission group membership change.");
        for (PermissionsGroupMembershipEntity membershipEntity : membershipGroups) {
            membershipEntity.setGroupId(newGroupId);
            membershipRepository.save(membershipEntity);
        }
        log.debug("Update Default permission group membership success.");
    }

    private int createDefault() {
        // permissions_group_role
        PermissionsGroupRoleEntity permissionsGroupRoleEntity = new PermissionsGroupRoleEntity();
        permissionsGroupRoleEntity.setClusterId(ConstantDef.NON_CLUSTER);
        permissionsGroupRoleEntity.setGroupName(ConstantDef.USER_DEFAULT_GROUP_NAME);
        PermissionsGroupRoleEntity permissionGroupRole = permissionsGroupRoleRepository.save(permissionsGroupRoleEntity);

        return permissionGroupRole.getGroupId();
    }
}
