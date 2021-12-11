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
import org.apache.doris.stack.constants.AgentStatus;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * agent entity
 **/
@Entity
@Table(name = "agent")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name = "host")
    private String host;

    @Column(name = "port")
    private int port;

    @Column(name = "cluster_id")
    private int clusterId;

    @Column(name = "install_dir", length = 1024)
    private String installDir;

    @Enumerated(EnumType.STRING)
    private AgentStatus status;

    @Column(name = "register_time")
    private Date registerTime;

    @Column(name = "last_reported_time")
    private Date lastReportedTime;

    public AgentEntity(String host, int port, String installDir, AgentStatus status) {
        this.host = host;
        this.port = port;
        this.installDir = installDir;
        this.status = status;
        this.registerTime = new Date();
    }

    public AgentEntity(String host, String installDir, AgentStatus status, int clusterId) {
        this.host = host;
        this.installDir = installDir;
        this.status = status;
        this.clusterId = clusterId;
    }
}
