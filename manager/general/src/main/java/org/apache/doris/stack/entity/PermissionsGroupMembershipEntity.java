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

package org.apache.doris.stack.entity;

import org.apache.doris.stack.model.response.user.UserGroupMembership;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @Description：Mapping relationship table between permission group and user
 */
@Entity
@Table(name = "permissions_group_membership")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PermissionsGroupMembershipEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name = "user_id", nullable = false)
    private int userId;

    @Column(name = "group_id", nullable = false)
    private int groupId;

    public PermissionsGroupMembershipEntity(int userId, int groupId) {
        this.userId = userId;
        this.groupId = groupId;
    }

    public UserGroupMembership castToModel() {
        return new UserGroupMembership(this.id, this.groupId, this.userId);
    }
}
