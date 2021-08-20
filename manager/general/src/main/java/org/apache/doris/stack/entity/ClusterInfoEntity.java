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

import org.apache.doris.stack.model.request.space.ClusterCreateReq;
import org.apache.doris.stack.model.request.space.ClusterType;
import org.apache.doris.stack.model.response.space.UserSpaceInfo;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @Description：Doris cluster space information
 */
@Entity
@Table(name = "cluster_info")
@Data
@NoArgsConstructor
public class ClusterInfoEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(length = 100, nullable = false)
    private String name;

    private String address;

    private int httpPort;

    private int queryPort;

    /**
     * Admin user of Doris cluster
     */
    @Column(name = "[user]", length = 100)
    private String user;

    /**
     * Doris user password
     * TODO:The subsequent storage shall be encrypted to prevent the leakage of password information
     */
    @Column(length = 100)
    private String passwd;

    @Column(length = 50)
    private String sessionId;

    private Timestamp createTime;

    private Timestamp updateTime;

    /**
     * Broker name information for file import
     */
    private String brokerName;

    /**
     * Description information
     */
    private String description;

    private int adminUserId;

    private String adminUserMail;

    private int adminGroupId;

    private int allUserGroupId;

    private int collectionId;

    private boolean isActive = true;

    /**
     * Engine type（Doris/Mysql/Dae）
     */
    private String type;

    @Column(name = "timezone", length = 254)
    private String timezone;

    private boolean managerEnable;

    public void updateByClusterInfo(ClusterCreateReq createReq) {
        this.address = createReq.getAddress();
        this.httpPort = createReq.getHttpPort();
        this.queryPort = createReq.getQueryPort();
        this.user = createReq.getUser();
        this.passwd = createReq.getPasswd();
        this.updateTime = new Timestamp(System.currentTimeMillis());
        if (createReq.getType() == null) {
            this.type = ClusterType.Doris.name();
        } else {
            this.type = createReq.getType().name();
        }
    }

    public UserSpaceInfo transToModel() {
        UserSpaceInfo userSpaceInfo = new UserSpaceInfo();
        userSpaceInfo.setId(this.id);
        userSpaceInfo.setName(this.name);
        userSpaceInfo.setDescription(this.description);
        userSpaceInfo.setPaloAddress(this.address);
        userSpaceInfo.setHttpPort(this.httpPort);
        userSpaceInfo.setQueryPort(this.queryPort);
        userSpaceInfo.setPaloAdminUser(this.user);
        userSpaceInfo.setUpdateTime(this.updateTime);
        userSpaceInfo.setCreateTime(this.createTime);
        userSpaceInfo.setAllUserGroupId(this.allUserGroupId);
        userSpaceInfo.setAdminGroupId(this.adminGroupId);
        userSpaceInfo.setPublicCollectionId(this.collectionId);
        return userSpaceInfo;
    }
}
