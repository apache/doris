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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.apache.doris.stack.constant.PropertyDefine;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.service.config.ConfigConstant;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;
import org.springframework.transaction.annotation.Transactional;

import org.apache.doris.stack.model.ldap.LdapConnectionInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfoReq;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.user.PasswordUpdateReq;
import org.apache.doris.stack.model.request.user.UserAddReq;
import org.apache.doris.stack.model.request.user.UserUpdateReq;
import org.apache.doris.stack.model.response.user.UserInfo;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.MailComponent;
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
import org.apache.doris.stack.exception.RequestFieldNullException;
import org.apache.doris.stack.exception.ResetPasswordException;
import org.apache.doris.stack.exception.UserEmailDuplicatedException;
import org.apache.doris.stack.exception.UserLoginException;
import org.apache.doris.stack.service.UtilService;
import com.unboundid.ldap.sdk.LDAPConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

@RunWith(JUnit4.class)
@Slf4j
public class UserServiceTest {

    @InjectMocks
    UserService userService;

    @Mock
    private CoreSessionRepository sessionRepository;

    @Mock
    private CoreUserRepository userRepository;

    @Mock
    private PermissionsGroupMembershipRepository membershipRepository;

    @Mock
    private UtilService utilService;

    @Mock
    private MailComponent mailComponent;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Mock
    private LdapComponent ldapComponent;

    @Mock
    private LdapClient ldapClient;

    @Mock
    private SettingComponent settingComponent;

    @Mock
    private PermissionsGroupRoleRepository permissionsGroupRoleRepository;

    @Mock
    private ClusterInfoRepository clusterInfoRepository;

    @Mock
    private Environment environment;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAllUserIncludeDeactivated() throws Exception {
        log.debug("test get all user.");
        int userId = 1;

        boolean includeDeactivated = true;
        int clusterId = 2;
        int allUserGroupId = 3;
        int groupId = 4;
        ClusterInfoEntity clusterInfoEntity = new ClusterInfoEntity();
        clusterInfoEntity.setId(clusterId);
        clusterInfoEntity.setAllUserGroupId(allUserGroupId);
        // mock cluster
        when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfoEntity);
        List<Integer> allUserIds = Lists.newArrayList(userId);
        // mock all user group id
        when(membershipRepository.getUserIdsByGroupId(allUserGroupId)).thenReturn(allUserIds);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        List<UserInfo> userInfos = Lists.newArrayList();
        UserInfo userInfo = getUserByEntity(userEntity, userId, groupId);
        userInfos.add(userInfo);
        Assert.assertEquals(userInfos, userService.getAllUser(includeDeactivated, userId));
    }

    @Test
    public void testGetAllUser() throws Exception {
        log.debug("test get all user.");
        int userId = 1;
        // Disabled users are not included
        boolean includeDeactivated = false;
        int clusterId = 2;
        int allUserGroupId = 3;
        int groupId = 4;
        ClusterInfoEntity clusterInfoEntity = new ClusterInfoEntity();
        clusterInfoEntity.setId(clusterId);
        clusterInfoEntity.setAllUserGroupId(allUserGroupId);
        // mock cluster
        when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfoEntity);
        List<Integer> allUserIds = Lists.newArrayList(userId);
        // mock all user group id
        when(membershipRepository.getUserIdsByGroupId(allUserGroupId)).thenReturn(allUserIds);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        userEntity.setActive(false);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        List<UserInfo> userInfos = Lists.newArrayList();
        UserInfo userInfo = getUserByEntity(userEntity, userId, groupId);
        userInfos.add(userInfo);

        Assert.assertEquals(Lists.emptyList(), userService.getAllUser(includeDeactivated, userId));
    }

    private UserInfo getUserByEntity(CoreUserEntity userEntity, int userId, int groupId) throws Exception {
        UserInfo userInfo = userEntity.castToUserInfo();
        userInfo.setLdapAuth(false);
        PermissionsGroupMembershipEntity permissionsGroupMembershipEntity = new PermissionsGroupMembershipEntity();
        permissionsGroupMembershipEntity.setUserId(userId);
        permissionsGroupMembershipEntity.setGroupId(groupId);
        List<PermissionsGroupMembershipEntity> membershipEntities =
                Lists.newArrayList(permissionsGroupMembershipEntity);
        // mock permission list
        when(membershipRepository.getByUserId(userId)).thenReturn(membershipEntities);
        List<Integer> groupIds = Lists.newArrayList(groupId);
        userInfo.setGroupIds(groupIds);
        return userInfo;
    }

    @Test
    public void testGetCurrentUser() throws Exception {
        log.debug("test get current user.");
        int userId = 1;
        int clusterId = 2;
        int groupId = 3;
        int collectionId = 4;
        // mock cluster id
        when(clusterUserComponent.getClusterIdByUserId(userId)).thenReturn(clusterId);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        UserInfo userInfo = getUserByEntity(userEntity, userId, groupId);
        userInfo.setSpaceId(clusterId);
        userInfo.setCollectionId(collectionId);
        ClusterInfoEntity clusterInfoEntity = new ClusterInfoEntity();
        clusterInfoEntity.setId(clusterId);
        clusterInfoEntity.setCollectionId(collectionId);
        // mock cluster
        when(clusterInfoRepository.findById(clusterId)).thenReturn(Optional.of(clusterInfoEntity));
        userInfo.setSpaceComplete(false);
        SettingEntity settingEntity = new SettingEntity();
        settingEntity.setKey("auth_type");
        settingEntity.setValue("ldap");
        // mock auth type
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(settingEntity);
        userInfo.setAuthType(InitStudioReq.AuthType.ldap);
        //
        String deployName = PropertyDefine.DEPLOY_TYPE_MANAGER;
        when(environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY)).thenReturn(deployName);
        userInfo.setDeployType(deployName);
        userInfo.setManagerEnable(true);
        Assert.assertEquals(userInfo, userService.getCurrentUser(userId));

        clusterInfoEntity.setAddress("127.0.0.1");
        userInfo.setSpaceComplete(true);
        // Verify that the cluster address is set in the result space
        Assert.assertEquals(userInfo, userService.getCurrentUser(userId));
        clusterInfoEntity.setId(0);
        // cluster id is 0
        when(clusterUserComponent.getClusterIdByUserId(userId)).thenReturn(0);
        userInfo = getDefaultUserByEntity(userEntity);
        userInfo.setSpaceComplete(false);
        userInfo.setGroupIds(null);
        userInfo.setAuthType(InitStudioReq.AuthType.ldap);

        deployName = PropertyDefine.DEPLOY_TYPE_STUDIO;
        when(environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY)).thenReturn(deployName);
        userInfo.setDeployType(deployName);
        userInfo.setManagerEnable(clusterInfoEntity.isManagerEnable());

        Assert.assertEquals(userInfo, userService.getCurrentUser(userId));
    }

    private UserInfo getDefaultUserByEntity(CoreUserEntity userEntity) throws Exception {

        UserInfo userInfo = userEntity.castToUserInfo();
        userInfo.setLdapAuth(userEntity.getLdapAuth());

        return userInfo;
    }

    @Test
    public void testGetUserById() throws Exception {
        log.debug("test get user by id.");
        int userId = 1;
        int requestUserId = 2;
        int clusterId = 2;
        int groupId = 3;
        int collectionId = 4;
        CoreUserEntity requestUser = new CoreUserEntity();
        requestUser.setId(requestUserId);
        when(userRepository.findById(requestUserId)).thenReturn(Optional.of(requestUser));
        // request exception
        try {
            userService.getUserById(userId, requestUserId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        requestUserId = userId;
        when(clusterUserComponent.checkUserSameCluster(requestUserId, userId)).thenReturn(clusterId);

        // mock cluster id
        when(clusterUserComponent.getClusterIdByUserId(userId)).thenReturn(clusterId);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        UserInfo userInfo = getUserByEntity(userEntity, userId, groupId);
        userInfo.setSpaceId(clusterId);
        userInfo.setCollectionId(collectionId);
        ClusterInfoEntity clusterInfoEntity = new ClusterInfoEntity();
        clusterInfoEntity.setId(clusterId);
        clusterInfoEntity.setCollectionId(collectionId);
        // mock cluster
        when(clusterInfoRepository.findById(clusterId)).thenReturn(Optional.of(clusterInfoEntity));
        userInfo.setSpaceComplete(false);
        SettingEntity settingEntity = new SettingEntity();
        settingEntity.setKey(ConfigConstant.AUTH_TYPE_KEY);
        settingEntity.setValue("ldap");
        // mock auth type
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(settingEntity);
        userInfo.setAuthType(InitStudioReq.AuthType.ldap);

        String deployName = PropertyDefine.DEPLOY_TYPE_MANAGER;
        when(environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY)).thenReturn(deployName);
        userInfo.setDeployType(deployName);
        userInfo.setManagerEnable(true);
        // 验证结果
        Assert.assertEquals(userInfo, userService.getUserById(userId, requestUserId));
    }

    @Transactional
    @Test
    public void testAddUser() throws Exception {
        log.debug("test add user.");
        int invitorId = 1;
        int userId = 2;
        int defaultGroupId = 3;
        int allUserGroupId = 4;
        String email = "caijunhui@baidu.com";
        UserAddReq userAddReq = new UserAddReq();
        when(ldapComponent.enabled()).thenReturn(true);
        // request exception
        try {
            userService.addUser(userAddReq, invitorId);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }
        userAddReq.setEmail(email);
        ClusterInfoEntity clusterInfoEntity = new ClusterInfoEntity();
        clusterInfoEntity.setAllUserGroupId(allUserGroupId);
        // mock cluster
        when(clusterUserComponent.getClusterByUserId(invitorId)).thenReturn(clusterInfoEntity);
        CoreUserEntity invitor = new CoreUserEntity();
        invitor.setId(invitorId);
        // mock invitor
        when(userRepository.findById(invitorId)).thenReturn(Optional.of(invitor));
        // check email
        when(utilService.emailCheck(email)).thenReturn(true);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setEmail(email);
        userEntity.setId(userId);
        List<CoreUserEntity> userEntities = Lists.newArrayList(userEntity);
        // mock user
        when(userRepository.getByEmail(email)).thenReturn(userEntities);
        SettingEntity defaultGroup = new SettingEntity();
        defaultGroup.setKey("default-group-id");
        defaultGroup.setValue(String.valueOf(defaultGroupId));
        // mock default
        when(settingComponent.readSetting("default-group-id")).thenReturn(defaultGroup);
        // user group exception
        PermissionsGroupMembershipEntity permissionsGroupMembershipEntity = new PermissionsGroupMembershipEntity();
        permissionsGroupMembershipEntity.setGroupId(1);
        permissionsGroupMembershipEntity.setUserId(userId);
        List<PermissionsGroupMembershipEntity> groupUser = Lists.newArrayList(permissionsGroupMembershipEntity);
        when(membershipRepository.getByUserIdNoDefaultGroup(userId, defaultGroupId)).thenReturn(groupUser).thenReturn(null);
        try {
            userService.addUser(userAddReq, invitorId);
        } catch (Exception e) {
            Assert.assertEquals(UserEmailDuplicatedException.MESSAGE, e.getMessage());
        }
        // Remove user from default group
        membershipRepository.deleteByUserIdAndGroupId(defaultGroupId, userId);
        CoreUserEntity newUser = new CoreUserEntity(userEntity);
        // save user
        when(userRepository.save(any(CoreUserEntity.class))).thenReturn(userEntity);
        UserInfo userInfo = userEntity.castToUserInfo();

        String deployName = PropertyDefine.DEPLOY_TYPE_MANAGER;
        when(environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY)).thenReturn(deployName);

        userInfo.setGroupIds(Lists.newArrayList(allUserGroupId, clusterInfoEntity.getAdminGroupId()));
        Assert.assertEquals(userInfo, userService.addUser(userAddReq, invitorId));

        // ldap

        when(userRepository.getByEmail(email)).thenReturn(null);
        LdapConnectionInfo connectionInfo = new LdapConnectionInfo();
        connectionInfo.setAttributeEmail(email);
        // mock ldap connection information
        when(ldapComponent.getConnInfo()).thenReturn(connectionInfo);
        LDAPConnection connection = new LDAPConnection();
        when(ldapClient.getConnection(connectionInfo)).thenReturn(connection);
        LdapUserInfoReq userInfoReq = new LdapUserInfoReq();
        // mock ldap base dn
        when(ldapComponent.getBaseDn()).thenReturn(Lists.newArrayList("dc=163", "dc=com"));
        userInfoReq.setBaseDn(Lists.newArrayList("dc=163", "dc=com"));
        userInfo.setLdapAuth(true);
        userInfoReq.setUserAttribute(email);
        userAddReq.setEmail(email);
        userInfoReq.setUserValue(email);
        LdapUserInfo ldapUser = new LdapUserInfo();
        ldapUser.setExist(false);
        // mock ldap user
        when(ldapClient.getUser(any(LDAPConnection.class), any(LdapUserInfoReq.class))).thenReturn(ldapUser);
        // ldap user not exsit
        try {
            userService.addUser(userAddReq, invitorId);
        } catch (Exception e) {
            Assert.assertEquals(UserLoginException.MESSAGE, e.getMessage());
        }
        // ldap user exsit
        ldapUser.setExist(true);
        CoreUserEntity user = new CoreUserEntity(userAddReq, ldapUser);
        // save user
        when(userRepository.save(any(CoreUserEntity.class))).thenReturn(user);
        UserInfo newUserInfo = user.castToUserInfo();

        deployName = PropertyDefine.DEPLOY_TYPE_STUDIO;
        when(environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY)).thenReturn(deployName);

        newUserInfo.setGroupIds(Lists.newArrayList(allUserGroupId));

        Assert.assertEquals(newUserInfo, userService.addUser(userAddReq, invitorId));

        // ldap not enable
        userAddReq.setName("name");
        when(ldapComponent.enabled()).thenReturn(false);
        when(userRepository.getByEmail(email)).thenReturn(Lists.newArrayList(user));
        //email already be used
        try {
            userService.addUser(userAddReq, invitorId);
        } catch (Exception e) {
            Assert.assertEquals(UserEmailDuplicatedException.MESSAGE, e.getMessage());
        }
        // email use
        when(userRepository.getByEmail(email)).thenReturn(null);
        newUser = new CoreUserEntity(userAddReq);
        userAddReq.setPassword("mypassword");
        // save user
        when(userRepository.save(any(CoreUserEntity.class))).thenReturn(newUser);
        userInfo = newUser.castToUserInfo();
        userInfo.setGroupIds(Lists.newArrayList(allUserGroupId));

        Assert.assertEquals(userInfo, userService.addUser(userAddReq, invitorId));
    }

    @Test
    @Transactional
    public void testUpdateUser() throws Exception {
        log.debug("test update user.");
        String name = "name";
        String email = "caijunhui@baidu.com";
        int userId = 1;
        int requestId = 2;
        int groupId = 3;
        int clusterId = 4;
        UserUpdateReq userUpdateReq = new UserUpdateReq();
        // request empty
        try {
            userService.updateUser(userUpdateReq, requestId, userId);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }
        userUpdateReq.setEmail(email);
        userUpdateReq.setName(name);
        CoreUserEntity requestUser = new CoreUserEntity();
        requestUser.setId(requestId);
        // mock user
        when(userRepository.findById(requestId)).thenReturn(Optional.of(requestUser));
        // mock cluster id
        when(clusterUserComponent.checkUserSameCluster(requestId, userId)).thenReturn(clusterId);
        // no permission
        try {
            userService.updateUser(userUpdateReq, requestId, userId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        requestId = userId;

        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        // mock email
        when(userRepository.getByEmail("a" + email)).thenReturn(null);
        userEntity.setEmail("a" + email);
        userEntity.setFirstName(name);
        // save user
        when(userRepository.save(any(CoreUserEntity.class))).thenReturn(userEntity);

        UserInfo userInfo = userEntity.castToUserInfo();
        userEntity.setId(userId);
        PermissionsGroupMembershipEntity permissionsGroupMembershipEntity = new PermissionsGroupMembershipEntity();
        permissionsGroupMembershipEntity.setUserId(userId);
        permissionsGroupMembershipEntity.setGroupId(groupId);
        // mock permission group
        when(membershipRepository.getByUserId(userId)).thenReturn(Lists.newArrayList(permissionsGroupMembershipEntity));
        userInfo.setGroupIds(Lists.newArrayList(groupId));
        //
        UserInfo userInfoActual = userService.updateUser(userUpdateReq, requestId, userId);
        Assert.assertEquals(email, userInfoActual.getEmail());
        Assert.assertEquals(userInfo.getName(), userInfoActual.getName());
        Assert.assertEquals(userInfo.getGroupIds(), userInfoActual.getGroupIds());
    }

    @Test
    @Transactional
    public void testReactivateUser() throws Exception {
        log.debug("test reactivate user.");
        int userId = 1;
        int requestId = 2;
        int groupId = 3;
        // mock cluster id
        when(clusterUserComponent.checkUserSameCluster(requestId, userId)).thenReturn(1);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        userEntity.setActive(true);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        // The user has been activated
        try {
            userService.reactivateUser(userId, requestId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        userEntity.setActive(false);
        requestId = userId;
        // The administrator user activates itself
        try {
            userService.reactivateUser(userId, requestId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        requestId = userId + 1;
        // save user
        when(userRepository.save(userEntity)).thenReturn(userEntity);
        UserInfo userInfo = getUserByEntity(userEntity, userId, groupId);
        UserInfo userInfoActual = userService.reactivateUser(userId, requestId);

        Assert.assertTrue(userInfoActual.isActive());
        Assert.assertEquals(Lists.newArrayList(groupId), userInfoActual.getGroupIds());
        Assert.assertEquals(userInfo.isLdapAuth(), userInfoActual.isLdapAuth());
    }

    @Test
    @Transactional
    public void testStopUser() throws Exception {
        log.debug("test stop user.");
        int userId = 1;
        int requestId = 2;
        // mock cluster id
        when(clusterUserComponent.checkUserSameCluster(requestId, userId)).thenReturn(1);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        userEntity.setActive(false);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));

        requestId = userId;
        // The administrator user deactivates itself
        try {
            userService.stopUser(userId, requestId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        requestId = userId + 1;
        // The user is not activated and cannot be deactivated
        try {
            userService.stopUser(userId, requestId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        userEntity.setActive(true);
        // save user
        when(userRepository.save(userEntity)).thenReturn(userEntity);

        Assert.assertTrue(userService.stopUser(userId, requestId));
    }

    @Transactional
    @Test
    public void testMoveUser() throws Exception {
        log.debug("test move user.");
        int userId = 1;
        int requestId = 1;
        int clusterId = 3;
        int groupId = 4;
        // mock cluster id
        when(clusterUserComponent.checkUserSameCluster(requestId, userId + 1)).thenReturn(clusterId);
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        userEntity.setLdapAuth(false);
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        // move non LDAP users
        try {
            userService.moveUser(userId, requestId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        userEntity.setLdapAuth(true);
        // move itself
        try {
            userService.moveUser(userId, requestId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        userId = requestId + 1;
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        HashSet<Integer> groupIds = new HashSet<>();
        groupIds.add(groupId);
        // mock group list
        when(permissionsGroupRoleRepository.getGroupIdByClusterId(clusterId)).thenReturn(groupIds);
        SettingEntity defaultGroup = new SettingEntity();
        defaultGroup.setKey("default-group-id");
        defaultGroup.setValue("" + groupId);
        // mock default group
        when(settingComponent.readSetting("default-group-id")).thenReturn(defaultGroup);
        PermissionsGroupMembershipEntity permissionsGroupMembershipEntity = new PermissionsGroupMembershipEntity();
        permissionsGroupMembershipEntity.setGroupId(groupId);
        permissionsGroupMembershipEntity.setUserId(userId);
        // save permission group
        when(membershipRepository.save(any(PermissionsGroupMembershipEntity.class))).thenReturn(permissionsGroupMembershipEntity);
        // save user
        when(userRepository.save(any(CoreUserEntity.class))).thenReturn(userEntity);

        Assert.assertTrue(userService.moveUser(userId, requestId));
    }

    @Transactional
    @Test
    public void testUpdatePassword() throws Exception {
        log.debug("test update password.");

        PasswordUpdateReq updateReq = new PasswordUpdateReq();
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(1);
        userEntity.setSuperuser(false);
        when(userRepository.findById(1)).thenReturn(Optional.of(userEntity));
        // Non administrators modify their own passwords
        try {
            userService.updatePassword(updateReq, 2, 1);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
        updateReq.setOldPassword("oldpassword");
        updateReq.setPassword("oldpassword");
        // Password not modified
        try {
            userService.updatePassword(updateReq, 1, 1);
        } catch (Exception e) {
            Assert.assertEquals(ResetPasswordException.MESSAGE, e.getMessage());
        }
        updateReq.setPassword("newpassword");
        // save user
        when(userRepository.save(userEntity)).thenReturn(userEntity);
        Assert.assertEquals(getUserByEntity(userEntity, 1, 2), userService.updatePassword(updateReq, 1, 1));

        CoreUserEntity user = new CoreUserEntity();
        user.setId(2);
        // mock user
        when(userRepository.findById(2)).thenReturn(Optional.of(user));
        // save user
        when(userRepository.save(user)).thenReturn(user);
        userEntity.setSuperuser(true);

        Assert.assertEquals(getUserByEntity(user, 2, 2), userService.updatePassword(updateReq, 2, 1));

    }

    @Test
    @Transactional
    public void testSetQbnewb() throws Exception {
        log.debug("test set Qbnewb.");
        int userId = 1;
        int requestId = 1;
        CoreUserEntity requestUser = new CoreUserEntity();
        requestUser.setId(requestId);
        // mock user
        when(userRepository.findById(requestId)).thenReturn(Optional.of(requestUser));
        CoreUserEntity userEntity = requestUser;
        userEntity.setQbnewb(!userEntity.isQbnewb());

        Assert.assertTrue(userService.setQbnewb(requestId, userId));
    }

    @Test
    @Transactional
    public void testSendInvite() throws Exception {
        log.debug("test send invite.");
        int userId = 1;
        int requestId = 2;
        CoreUserEntity invitor = new CoreUserEntity();
        invitor.setId(requestId);
        CoreUserEntity user = new CoreUserEntity();
        user.setId(userId);
        user.setEmail("9943094@baidu.com");
        user.setFirstName("first");
        // mock Invitor
        when(userRepository.findById(requestId)).thenReturn(Optional.of(invitor));
        // mock user
        when(userRepository.findById(userId)).thenReturn(Optional.of(user));
        String resetTokenStr = "resetToken";
        // mock reset password token
        when(utilService.resetUserToken(user, true)).thenReturn(resetTokenStr);
        // save user
        when(userRepository.save(user)).thenReturn(user);
        String joinUrl = "/auth/reset_password/1_resetToken/#new";
        // mock join url
        when(utilService.getUserJoinUrl(userId, resetTokenStr)).thenReturn(joinUrl);
        mailComponent.sendInvitationMail(user.getEmail(), invitor.getFirstName(), joinUrl);

        Assert.assertTrue(userService.sendInvite(requestId, userId));
    }

    @Test
    @Transactional
    public void testDeleteUser() throws Exception {
        log.debug("test delete user.");
        int userId = 1;
        userRepository.deleteById(userId);
        sessionRepository.deleteByUserId(userId);
        membershipRepository.deleteByUserId(userId);
        userService.deleteUser(userId);
    }

    @Test
    @Transactional
    public void testInitDefaultUserGroup() throws Exception {
        log.debug("test init default user group.");
        String defaultUserName = "Default";
        int groupId = 1;
        SettingEntity groupIdEntity = new SettingEntity();
        groupIdEntity.setKey("default-group-id");
        // mock default group
        when(settingComponent.readSetting("default-group-id")).thenReturn(groupIdEntity);
        PermissionsGroupRoleEntity permissionsGroupRoleEntity = new PermissionsGroupRoleEntity();
        permissionsGroupRoleEntity.setGroupName(defaultUserName);
        permissionsGroupRoleEntity.setGroupId(groupId);
        // mock permission user group
        when(permissionsGroupRoleRepository.getByGroupName(defaultUserName)).thenReturn(permissionsGroupRoleEntity);

        // save default group
        when(settingComponent.addNewSetting(anyString(), anyString())).thenReturn(groupIdEntity);
        // The default test group configuration does not exist, and the permission entity does not exist
        userService.initDefaultUserGroup();

        // The test default group configuration does not exist, and the compatible permission entity does not exist
        userService.initDefaultUserGroup();

        when(permissionsGroupRoleRepository.getByGroupName(defaultUserName)).thenReturn(null);
        permissionsGroupRoleEntity.setClusterId(-1);
        when(permissionsGroupRoleRepository.save(any(PermissionsGroupRoleEntity.class))).thenReturn(permissionsGroupRoleEntity);
        // The default test group configuration does not exist, the permission user group and
        // permission entity do not exist, or the permission user group does not exist
        userService.initDefaultUserGroup();

        when(permissionsGroupRoleRepository.getByGroupName(defaultUserName)).thenReturn(permissionsGroupRoleEntity);
        groupIdEntity.setValue(String.valueOf(groupId));
        // The default test group configuration exists, and the permission entity exists
        userService.initDefaultUserGroup();

        // The test default group configuration exists, and the compatible permission entity does not exist
        userService.initDefaultUserGroup();

        when(permissionsGroupRoleRepository.getByGroupName(defaultUserName)).thenReturn(null);
        PermissionsGroupMembershipEntity membershipEntity = new PermissionsGroupMembershipEntity();
        membershipEntity.setId(groupId);
        // Returns the list of permission user groups and user corresponding entities
        when(membershipRepository.getByGroupId(groupId)).thenReturn(Lists.newArrayList(membershipEntity));
        when(membershipRepository.save(membershipEntity)).thenReturn(membershipEntity);
        // The default test group configuration exists, the permission user group and permission entity do not exist,
        // or the permission user group does not exist
        userService.initDefaultUserGroup();
    }
}
