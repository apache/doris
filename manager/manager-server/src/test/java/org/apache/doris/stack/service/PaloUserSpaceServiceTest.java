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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.ldap.LdapUserInfo;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.user.UserGroupRole;
import org.apache.doris.stack.model.request.space.ClusterCreateReq;
import org.apache.doris.stack.model.request.space.UserSpaceCreateReq;
import org.apache.doris.stack.model.request.space.UserSpaceUpdateReq;
import org.apache.doris.stack.model.response.space.UserSpaceInfo;
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
import org.apache.doris.stack.exception.NoPermissionException;
import org.apache.doris.stack.exception.PaloRequestException;
import org.apache.doris.stack.exception.RequestFieldNullException;
import org.apache.doris.stack.exception.StudioNotInitException;
import org.apache.doris.stack.exception.UserEmailDuplicatedException;
import org.apache.doris.stack.exception.UserLoginException;
import org.apache.doris.stack.service.construct.MetadataService;
import org.apache.doris.stack.service.config.ConfigConstant;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.Sort;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@RunWith(JUnit4.class)
@Slf4j
public class PaloUserSpaceServiceTest {

    @InjectMocks
    private PaloUserSpaceService spaceService;

    @Mock
    private JdbcSampleClient jdbcClient;

    @Mock
    private UtilService utilService;

    @Mock
    private CoreUserRepository userRepository;

    @Mock
    private PermissionsGroupRoleRepository groupRoleRepository;

    @Mock
    private ClusterInfoRepository clusterInfoRepository;

    @Mock
    private PaloLoginClient paloLoginClient;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Mock
    private SettingComponent settingComponent;

    @Mock
    private LdapComponent ldapComponent;

    @Mock
    private PermissionsGroupMembershipRepository membershipRepository;

    @Mock
    private CoreSessionRepository coreSessionRepository;

    @Mock
    private MailComponent mailComponent;

    @Mock
    private MetadataService managerMetadataService;

    @Mock
    private ManagerMetaSyncComponent managerMetaSyncComponent;

    @Mock
    private PaloQueryClient queryClient;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Creation of manager authentication mode for test space
     */
    @Test
    public void createStudioUserSpaceTest() {
        log.debug("create studio user space test");
        UserSpaceCreateReq createReq = new UserSpaceCreateReq();

        // If not initialized authentication type
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(null);
        try {
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), StudioNotInitException.MESSAGE);
        }

        // After initialization, use the manager authentication method
        SettingEntity authSetting = new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.studio.name());
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(authSetting);

        // request exception
        try {
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        String clusterName = "doris1";
        createReq.setName(clusterName);
        UserSpaceCreateReq.UserAdminInfo userAdminInfo = new UserSpaceCreateReq.UserAdminInfo();
        userAdminInfo.setEmail("test@baidu.com");
        userAdminInfo.setName("test");
        userAdminInfo.setPassword("test@123");
        createReq.setUser(userAdminInfo);

        // Duplicate space name
        List<ClusterInfoEntity> clusterInfos = new ArrayList<>();
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        clusterInfo.setName(clusterName);
        clusterInfos.add(clusterInfo);
        when(clusterInfoRepository.getByName(clusterName)).thenReturn(clusterInfos);
        try {
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), NameDuplicatedException.MESSAGE);
        }

        // Space name does not duplicate
        when(clusterInfoRepository.getByName(clusterName)).thenReturn(new ArrayList<>());

        // admin user duplicated
        List<CoreUserEntity> userEntities = new ArrayList<>();
        CoreUserEntity coreUserEntity = new CoreUserEntity();
        coreUserEntity.setEmail(createReq.getUser().getEmail());
        userEntities.add(coreUserEntity);
        when(userRepository.getByEmail(createReq.getUser().getEmail())).thenReturn(userEntities);
        try {
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), UserEmailDuplicatedException.MESSAGE);
        }

        // admin user name does not duplicate
        when(userRepository.getByEmail(createReq.getUser().getEmail())).thenReturn(new ArrayList<>());

        // Create a space that is not bound to the Doris cluster for the time being
        // mock cluster
        int clusterId = 1;
        ClusterInfoEntity clusterInfoSave = new ClusterInfoEntity();
        clusterInfoSave.setId(clusterId);
        clusterInfoSave.setName(createReq.getName());
        clusterInfoSave.setDescription(createReq.getDescribe());
        clusterInfoSave.setAdminUserMail(createReq.getUser().getEmail());
        clusterInfoSave.setCreateTime(new Timestamp(System.currentTimeMillis()));
        when(clusterInfoRepository.save(any())).thenReturn(clusterInfoSave);
        // mock user group
        int adminGroupId = 1;
        when(clusterUserComponent.addPermissionsGroup(PaloUserSpaceService.ADMIN_USER_NAME + clusterId,
                clusterId, UserGroupRole.Administrator)).thenReturn(adminGroupId);
        int allUserGroupId = 2;
        when(clusterUserComponent.addPermissionsGroup(PaloUserSpaceService.ALL_USER_NAME + clusterId,
                clusterId, UserGroupRole.Analyzer)).thenReturn(allUserGroupId);
        PermissionsGroupRoleEntity adminGroup = new PermissionsGroupRoleEntity();
        when(groupRoleRepository.findById(adminGroupId)).thenReturn(Optional.of(adminGroup));
        PermissionsGroupRoleEntity allUserGroup = new PermissionsGroupRoleEntity();
        when(groupRoleRepository.findById(allUserGroupId)).thenReturn(Optional.of(allUserGroup));

        // mock cluster admin user
        CoreUserEntity userEntity = new CoreUserEntity(createReq.getUser());
        userEntity.setId(1);
        when(userRepository.save(any())).thenReturn(userEntity);
        try {
            int result = spaceService.create(createReq);
            Assert.assertEquals(result, clusterId);
        } catch (Exception e) {
            log.error("create space error");
            e.printStackTrace();
        }

        // Create a space bound to the Doris cluster
        // Cluster information exception
        ClusterCreateReq clusterCreateReq = new ClusterCreateReq();
        createReq.setCluster(clusterCreateReq);
        try {
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // The cluster information is normal, but the cluster information is repeated
        // mock request
        String clusterAddress = "10.0.0.1";
        clusterCreateReq.setAddress(clusterAddress);
        int httpPort = 8080;
        clusterCreateReq.setHttpPort(httpPort);
        clusterCreateReq.setQueryPort(8081);
        clusterCreateReq.setUser("test");
        clusterCreateReq.setPasswd("test");
        createReq.setCluster(clusterCreateReq);
        // mock duplicated cluster
        List<ClusterInfoEntity> exsitEntities = new ArrayList<>();
        ClusterInfoEntity clusterInfoEntity = new ClusterInfoEntity();
        clusterInfoEntity.setAddress(clusterAddress);
        clusterInfoEntity.setHttpPort(httpPort);
        exsitEntities.add(clusterInfoEntity);
        when(clusterInfoRepository.getByAddressAndPort(clusterAddress, httpPort)).thenReturn(exsitEntities);
        try {
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), DorisSpaceDuplicatedException.MESSAGE);
        }

        when(clusterInfoRepository.getByAddressAndPort(clusterAddress, httpPort)).thenReturn(new ArrayList<>());

        // Cluster information is created correctly
        try {
            when(paloLoginClient.loginPalo(any())).thenReturn(true);
            when(queryClient.createUser(any(), any(), any(), any())).thenReturn("123456");
            int result = spaceService.create(createReq);
            Assert.assertEquals(result, clusterId);
        } catch (Exception e) {
            log.error("create space error");
            e.printStackTrace();
        }

        // The cluster information is not repeated, but the JDBC protocol connection fails
        try {
            when(jdbcClient.testConnetion(clusterCreateReq.getAddress(), clusterCreateReq.getQueryPort(),
                    ConstantDef.MYSQL_DEFAULT_SCHEMA, clusterCreateReq.getUser(),
                    clusterCreateReq.getPasswd())).thenThrow(new SQLException("test"));
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), DorisConnectionException.MESSAGE);
        }

        // The cluster information is not repeated, but the HTTP protocol connection failed
        try {
            when(paloLoginClient.loginPalo(any())).thenThrow(new PaloRequestException("test"));
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), DorisConnectionException.MESSAGE);
        }
    }

    /**
     * Creation of LDAP authentication mode in test space
     */
    @Test
    public void createLdapUserSpaceTest() {
        log.debug("create ldap user space test");
        UserSpaceCreateReq createReq = new UserSpaceCreateReq();
        String clusterName = "doris1";
        createReq.setName(clusterName);
        UserSpaceCreateReq.UserAdminInfo userAdminInfo = new UserSpaceCreateReq.UserAdminInfo();
        userAdminInfo.setEmail("test@baidu.com");
        createReq.setUser(userAdminInfo);

        // After initialization, use LDAP authentication
        SettingEntity authSetting = new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.ldap.name());
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(authSetting);

        // mock cluster
        int clusterId = 1;
        ClusterInfoEntity clusterInfoSave = new ClusterInfoEntity();
        clusterInfoSave.setId(clusterId);
        clusterInfoSave.setName(createReq.getName());
        clusterInfoSave.setDescription(createReq.getDescribe());
        clusterInfoSave.setAdminUserMail(createReq.getUser().getEmail());
        clusterInfoSave.setCreateTime(new Timestamp(System.currentTimeMillis()));
        when(clusterInfoRepository.save(any())).thenReturn(clusterInfoSave);

        // If the user has not logged in to manager
        when(userRepository.getByEmail(createReq.getUser().getEmail())).thenReturn(new ArrayList<>());

        // Synchronize users from LADP and create spaces
        // mock cluster admin user
        LdapUserInfo ldapUser = new LdapUserInfo();
        ldapUser.setEmail(createReq.getUser().getEmail());
        CoreUserEntity userEntity = new CoreUserEntity(createReq.getUser(), ldapUser);
        userEntity.setId(1);
        when(userRepository.save(any())).thenReturn(userEntity);
        try {
            when(ldapComponent.searchUserByEmail(createReq.getUser().getEmail())).thenReturn(ldapUser);
            int result = spaceService.create(createReq);
            Assert.assertEquals(result, clusterId);
        } catch (Exception e) {
            log.error("create space error");
            e.printStackTrace();
        }

        // Failed to synchronize users from LADP, user information error
        try {
            when(ldapComponent.searchUserByEmail(createReq.getUser().getEmail())).thenThrow(new UserLoginException());
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), UserLoginException.MESSAGE);
        }

        // mock user
        List<CoreUserEntity> userEntities = new ArrayList<>();
        CoreUserEntity coreUserEntity = new CoreUserEntity();
        coreUserEntity.setId(1);
        coreUserEntity.setEmail(createReq.getUser().getEmail());
        userEntities.add(coreUserEntity);
        when(userRepository.getByEmail(createReq.getUser().getEmail())).thenReturn(userEntities);

        // mock user group
        int defaultGroupId = 1;
        SettingEntity defaultGroup = new SettingEntity(ConfigConstant.DEFAULT_GROUP_KEY, String.valueOf(defaultGroupId));
        when(settingComponent.readSetting(ConfigConstant.DEFAULT_GROUP_KEY)).thenReturn(defaultGroup);
        when(membershipRepository.getByUserIdNoDefaultGroup(1, defaultGroupId)).thenReturn(new ArrayList<>());
        try {
            int result = spaceService.create(createReq);
            Assert.assertEquals(result, clusterId);
        } catch (Exception e) {
            log.error("create space error");
            e.printStackTrace();
        }

        // If the user is not in the default user group, it means that the user has been added to the space
        List<PermissionsGroupMembershipEntity> groupUser = new ArrayList<>();
        PermissionsGroupMembershipEntity membershipEntity = new PermissionsGroupMembershipEntity();
        membershipEntity.setGroupId(1);
        membershipEntity.setUserId(1);
        groupUser.add(membershipEntity);
        when(membershipRepository.getByUserIdNoDefaultGroup(1, defaultGroupId)).thenReturn(groupUser);
        try {
            spaceService.create(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), UserEmailDuplicatedException.MESSAGE);
        }
    }

    /**
     * Test update spatial information
     */
    @Test
    public void updateTest() {
        log.debug("space update test.");
        int userId = 1;
        int spaceId = 1;
        UserSpaceUpdateReq updateReq = new UserSpaceUpdateReq();

        // request exception
        try {
            spaceService.update(userId, spaceId, updateReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        updateReq.setName("doris2");

        // mock cluster
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        int adminGroupId = 1;
        int allUserGroupId = 2;
        clusterInfo.setId(spaceId);
        clusterInfo.setName("doris1");
        clusterInfo.setAdminGroupId(adminGroupId);
        clusterInfo.setAllUserGroupId(allUserGroupId);
        when(clusterInfoRepository.findById(spaceId)).thenReturn(Optional.of(clusterInfo));
        // mock default premission group
        PermissionsGroupRoleEntity adminGroup = new PermissionsGroupRoleEntity();
        when(groupRoleRepository.findById(adminGroupId)).thenReturn(Optional.of(adminGroup));
        PermissionsGroupRoleEntity allUserGroup = new PermissionsGroupRoleEntity();
        when(groupRoleRepository.findById(allUserGroupId)).thenReturn(Optional.of(allUserGroup));

        // Change the user name and password information of the space normally
        // mock cluster
        clusterInfo.setName(updateReq.getName());
        when(clusterInfoRepository.save(any())).thenReturn(clusterInfo);
        try {
            // Users and clusters belong to the same space
            when(clusterUserComponent.checkUserBelongToCluster(userId, spaceId)).thenReturn(true);
            UserSpaceInfo result = spaceService.update(userId, spaceId, updateReq);
            Assert.assertEquals(result.getName(), clusterInfo.getName());
        } catch (Exception e) {
            log.error("Update space error.");
            e.printStackTrace();
        }

        // Cluster information of normal added space
        // mock request
        ClusterCreateReq clusterCreateReq = new ClusterCreateReq();
        String clusterAddress = "10.0.0.1";
        clusterCreateReq.setAddress(clusterAddress);
        int httpPort = 8080;
        clusterCreateReq.setHttpPort(httpPort);
        clusterCreateReq.setQueryPort(8081);
        clusterCreateReq.setUser("test");
        clusterCreateReq.setPasswd("test");
        updateReq.setCluster(clusterCreateReq);
        when(clusterInfoRepository.getByAddressAndPort(clusterAddress, httpPort)).thenReturn(new ArrayList<>());
        // Cluster information added correctly
        try {
            when(paloLoginClient.loginPalo(any())).thenReturn(true);
            // Users and clusters belong to the same space
            when(clusterUserComponent.checkUserBelongToCluster(userId, spaceId)).thenReturn(true);
            when(queryClient.createUser(any(), any(), any(), any())).thenReturn("123456");
            UserSpaceInfo result = spaceService.update(userId, spaceId, updateReq);
            Assert.assertEquals(result.getName(), clusterInfo.getName());
        } catch (Exception e) {
            log.error("create space error");
            e.printStackTrace();
        }

        // The user and the cluster are not in the same space,
        // and the user has no right to modify the space information
        try {
            // Users and clusters belong to the same space
            when(clusterUserComponent.checkUserBelongToCluster(userId, spaceId)).thenThrow(new NoPermissionException());
            spaceService.update(userId, spaceId, updateReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), NoPermissionException.MESSAGE);
        }

    }

    /**
     * Test get a list of all spaces
     */
    @Test
    public void getAllSpaceBySuperUserTest() {
        log.debug("get all space test.");
        ClusterInfoEntity clusterInfoEntity1 = new ClusterInfoEntity();
        clusterInfoEntity1.setId(1);
        clusterInfoEntity1.setName("doris1");

        ClusterInfoEntity clusterInfoEntity2 = new ClusterInfoEntity();
        clusterInfoEntity2.setId(2);
        clusterInfoEntity2.setName("doris2");

        List<ClusterInfoEntity> clusterInfos = Lists.newArrayList(clusterInfoEntity1, clusterInfoEntity2);
        when(clusterInfoRepository.findAll(Sort.by("id").ascending())).thenReturn(clusterInfos);

        List<UserSpaceInfo> result = spaceService.getAllSpaceBySuperUser();
        Assert.assertEquals(result.size(), clusterInfos.size());
    }

    /**
     * The test obtains spatial information according to the ID
     */
    @Test
    public void getByIdTest() {
        int userId = 0;
        int spaceId = 1;

        // mock cluster
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        clusterInfo.setId(spaceId);
        clusterInfo.setName("doris1");
        when(clusterInfoRepository.findById(spaceId)).thenReturn(Optional.of(clusterInfo));

        // Super administrator get information
        try {
            spaceService.getById(userId, spaceId);
        } catch (Exception e) {
            log.error("Update space error.");
            e.printStackTrace();
        }

        // General space administrator to get information
        userId = 1;
        try {
            // Users and clusters belong to the same space
            when(clusterUserComponent.checkUserBelongToCluster(userId, spaceId)).thenReturn(true);
            spaceService.getById(userId, spaceId);
        } catch (Exception e) {
            log.error("Update space error.");
            e.printStackTrace();
        }

        // The space administrator does not have permission to obtain other space information
        userId = 2;
        try {
            // Users and clusters belong to the same space
            when(clusterUserComponent.checkUserBelongToCluster(userId, spaceId)).thenThrow(new NoPermissionException());
            spaceService.getById(userId, spaceId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), NoPermissionException.MESSAGE);
        }
    }

    @Test
    public void deleteSpaceTest() {
        int spaceId = 1;
        int allUserGroupId = 1;

        // mock cluster
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        clusterInfo.setId(spaceId);
        clusterInfo.setName("doris1");
        clusterInfo.setCollectionId(1);
        clusterInfo.setManagerEnable(true);
        clusterInfo.setAllUserGroupId(allUserGroupId);
        when(clusterInfoRepository.findById(spaceId)).thenReturn(Optional.of(clusterInfo));

        // mock all user group
        PermissionsGroupRoleEntity allUserGroup = new PermissionsGroupRoleEntity();
        allUserGroup.setGroupId(allUserGroupId);
        allUserGroup.setPaloUserName("user");
        allUserGroup.setPassword("123456");
        when(groupRoleRepository.findById(allUserGroupId)).thenReturn(Optional.of(allUserGroup));

        // mock user group id and user id
        HashSet<Integer> groupIds = Sets.newHashSet(1, 2);
        when(groupRoleRepository.getGroupIdByClusterId(spaceId)).thenReturn(groupIds);
        Set<Integer> userIds = Sets.newHashSet(1, 2);
        when(membershipRepository.getByGroupId(groupIds)).thenReturn(userIds);

        // mock default user group
        SettingEntity defaultGroup = new SettingEntity(ConfigConstant.DEFAULT_GROUP_KEY, "1");
        when(settingComponent.readSetting(ConfigConstant.DEFAULT_GROUP_KEY)).thenReturn(defaultGroup);

        // mock user
        // user 1 is manager user
        CoreUserEntity user1 = new CoreUserEntity();
        user1.setId(1);
        user1.setLdapAuth(false);
        when(userRepository.findById(1)).thenReturn(Optional.of(user1));

        // user 1 is ldap user
        CoreUserEntity user2 = new CoreUserEntity();
        user2.setId(2);
        user2.setLdapAuth(true);
        when(userRepository.findById(2)).thenReturn(Optional.of(user2));

        try {
            spaceService.deleteSpace(spaceId);
        } catch (Exception e) {
            log.error("delete space error.");
            e.printStackTrace();
        }
    }
}
