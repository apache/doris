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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.doris.stack.constants.Flag;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "agent_role")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgentRoleEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column
    private String host;

    @Column(name = "cluster_id")
    private int clusterId;

    @Column(name = "role")
    private String role;

    //FOLLOWER / OBSERVER
    @Column(name = "fe_node_type")
    private String feNodeType;

    @Column(name = "install_dir", length = 1024)
    private String installDir;

    @Column(name = "register")
    private Flag register;

    public AgentRoleEntity(String host, String role, String feNodeType, String installDir, Flag register) {
        this.host = host;
        this.role = role;
        this.feNodeType = feNodeType;
        this.installDir = installDir;
        this.register = register;
    }
}
