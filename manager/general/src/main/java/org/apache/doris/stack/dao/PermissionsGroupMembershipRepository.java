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

package org.apache.doris.stack.dao;

import org.apache.doris.stack.entity.PermissionsGroupMembershipEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Set;

public interface PermissionsGroupMembershipRepository extends
        JpaRepository<PermissionsGroupMembershipEntity, Integer> {

    @Query("select p from PermissionsGroupMembershipEntity p where p.userId = :userId")
    List<PermissionsGroupMembershipEntity> getByUserId(@Param("userId") int userId);

    @Query("select p from PermissionsGroupMembershipEntity p where p.userId = :userId and p.groupId <> :groupId")
    List<PermissionsGroupMembershipEntity> getByUserIdNoDefaultGroup(@Param("userId") int userId,
                                                                     @Param("groupId") int groupId);

    @Query("select p from PermissionsGroupMembershipEntity p where p.userId = :userId and p.groupId = :groupId")
    List<PermissionsGroupMembershipEntity> getByUserIdAndGroupId(@Param("userId") int userId,
                                                                 @Param("groupId") int groupId);

    @Query("select p.userId from PermissionsGroupMembershipEntity p where p.groupId = :groupId")
    List<Integer> getUserIdsByGroupId(@Param("groupId") int groupId);

    @Query("select p from PermissionsGroupMembershipEntity p where p.groupId = :groupId")
    List<PermissionsGroupMembershipEntity> getByGroupId(@Param("groupId") int groupId);

    @Query("select p.userId from PermissionsGroupMembershipEntity p where p.groupId in (:groupIds)")
    Set<Integer> getByGroupId(@Param("groupIds") Set<Integer> groupIds);

    @Modifying
    @Query("delete from PermissionsGroupMembershipEntity p where p.groupId = :groupId")
    void deleteByGroupId(@Param("groupId") int groupId);

    @Modifying
    @Query("delete from PermissionsGroupMembershipEntity p where p.userId = :userId")
    void deleteByUserId(@Param("userId") int userId);

    @Modifying
    @Query("delete from PermissionsGroupMembershipEntity p where p.groupId = :groupId and p.userId = :userId")
    void deleteByUserIdAndGroupId(@Param("groupId") int groupId, @Param("userId") int userId);

}
