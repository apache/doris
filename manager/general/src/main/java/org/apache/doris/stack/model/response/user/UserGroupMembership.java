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

package org.apache.doris.stack.model.response.user;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class UserGroupMembership {
    private int membershipId;

    private int groupId;

    private int userId;

    @JSONField(name = "membership_id")
    @JsonProperty("membership_id")
    public int getMembershipId() {
        return membershipId;
    }

    @JSONField(name = "membership_id")
    @JsonProperty("membership_id")
    public void setMembershipId(int membershipId) {
        this.membershipId = membershipId;
    }

    @JSONField(name = "group_id")
    @JsonProperty("group_id")
    public int getGroupId() {
        return groupId;
    }

    @JSONField(name = "group_id")
    @JsonProperty("group_id")
    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    @JSONField(name = "user_id")
    @JsonProperty("user_id")
    public int getUserId() {
        return userId;
    }

    @JSONField(name = "user_id")
    @JsonProperty("user_id")
    public void setUserId(int userId) {
        this.userId = userId;
    }
}
