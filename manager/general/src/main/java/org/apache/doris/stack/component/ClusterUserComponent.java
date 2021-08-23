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

package org.apache.doris.stack.component;

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.request.user.UserGroupRole;
import org.apache.doris.stack.dao.ClusterInfoRepository;
import org.apache.doris.stack.dao.CoreUserRepository;
import org.apache.doris.stack.dao.PermissionsGroupMembershipRepository;
import org.apache.doris.stack.dao.PermissionsGroupRoleRepository;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.PermissionsGroupMembershipEntity;
import org.apache.doris.stack.entity.PermissionsGroupRoleEntity;
import org.apache.doris.stack.exception.NoPermissionException;
import org.apache.doris.stack.model.response.user.GroupMember;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description：The engine cluster management tool class is mainly responsible for
 * verifying whether the user has the permission of cluster space
 * TODO:The current user can only be in one user space
 */
@Component
@Slf4j
public class ClusterUserComponent extends BaseService {

    @Autowired
    private ClusterInfoRepository clusterInfoRepository;

    @Autowired
    private PermissionsGroupRoleRepository groupRoleRepository;

    @Autowired
    private PermissionsGroupMembershipRepository membershipRepository;

    @Autowired
    private CoreUserRepository userRepository;

    /**
     * Find the corresponding spatial information according to the user information
     *
     * @param userId
     * @return
     * @throws Exception
     */
    public ClusterInfoEntity getClusterByUserId(int userId) throws Exception {
        int clusterId = getClusterIdByUserId(userId);
        if (clusterId == ConstantDef.NON_CLUSTER) {
            log.error("User not belong to any cluster space, please assign it first.");
            throw new NoPermissionException();
        }
        ClusterInfoEntity clusterInfo = clusterInfoRepository.findById(clusterId).get();
        log.debug("The user {} cluster is {}.", userId, clusterId);
        return clusterInfo;
    }

    /**
     * Judge whether the user is a user in the space
     *
     * @param userId
     * @return
     * @throws Exception
     */
    public boolean checkUserBelongToCluster(int userId, int clusterId) throws Exception {
        int userClusterId = getClusterIdByUserId(userId);
        if (userClusterId != clusterId) {
            log.error("The user {} is not a space {} user.", userId, clusterId);
            throw new NoPermissionException();
        }
        return true;
    }

    /**
     * Check whether the space administrator and the user requesting the operation are the same user space
     *
     * @param requestId
     * @param userId
     * @return
     */
    public int checkUserSameCluster(int requestId, int userId) throws Exception {
        int requestClusterId = getClusterIdByUserId(requestId);
        int clusterId = getClusterIdByUserId(userId);

        if (requestClusterId != clusterId) {
            log.error("The users cluster id not same.");
            throw new NoPermissionException();
        }
        return clusterId;
    }

    public int getClusterIdByUserId(int userId) throws Exception {
        log.debug("Get user {} palo cluster id.", userId);
        List<PermissionsGroupMembershipEntity> memberships = membershipRepository.getByUserId(userId);
        if (memberships == null || memberships.size() == 0) {
            log.error("The user {} no have group.", userId);
            throw new NoPermissionException();
        }

        int groupId = memberships.get(0).getGroupId();

        PermissionsGroupRoleEntity groupRoleEntity = groupRoleRepository.findById(groupId).get();

        return groupRoleEntity.getClusterId();
    }

    /**
     * Initialize the correspondence between permission groups and users
     *
     * @param userId
     * @param groupId
     * @return
     */
    public int addGroupUserMembership(int userId, int groupId) {
        PermissionsGroupMembershipEntity amdinMembershipEntity =
                new PermissionsGroupMembershipEntity(userId, groupId);
        return membershipRepository.save(amdinMembershipEntity).getId();
    }

    /**
     * Create a user group and return the user group ID
     *
     * @return
     */
    public int addPermissionsGroup(String name, int clusterId, UserGroupRole role) {
        // Create a user group and bind the relationship with the Doris cluster and user information.
        PermissionsGroupRoleEntity groupRoleEntity =
                new PermissionsGroupRoleEntity(name, role.name(), clusterId);
        int groupId = groupRoleRepository.save(groupRoleEntity).getGroupId();
        log.debug("create group {}.", groupId);

        return groupId;
    }

    /**
     * Get all members of a permission group
     *
     * @param groupId
     * @return
     */
    public List<GroupMember> getGroupMembers(int groupId) {
        log.debug("get group {} all members.", groupId);
        List<PermissionsGroupMembershipEntity> users = membershipRepository.getByGroupId(groupId);

        List<GroupMember> members = new ArrayList<>();
        for (PermissionsGroupMembershipEntity membershipEntity : users) {
            // get user
            CoreUserEntity userEntity = userRepository.findById(membershipEntity.getUserId()).get();

            // construct Member
            GroupMember member = new GroupMember();
            member.setMembershipId(membershipEntity.getId());
            member.setUserId(membershipEntity.getUserId());
            member.setName(userEntity.getFirstName());
            member.setEmail(userEntity.getEmail());

            // add list
            members.add(member);
        }
        return members;
    }
}
