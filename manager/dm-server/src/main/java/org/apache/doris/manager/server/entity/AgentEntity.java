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
package org.apache.doris.manager.server.entity;

import java.util.Date;

/**
 * agent entity
 **/
public class AgentEntity {

    private Integer id;

    private String host;

    private Integer port;

    private String status;

    private Date registerTime;

    private Date lastReportedTime;

    public AgentEntity() {
    }

    public AgentEntity(Integer id, String host, Integer port, String status, Date registerTime, Date lastReportedTime) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.status = status;
        this.registerTime = registerTime;
        this.lastReportedTime = lastReportedTime;
    }

    public AgentEntity(String host, Integer port, String status) {
        this.host = host;
        this.port = port;
        this.status = status;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(Date registerTime) {
        this.registerTime = registerTime;
    }

    public Date getLastReportedTime() {
        return lastReportedTime;
    }

    public void setLastReportedTime(Date lastReportedTime) {
        this.lastReportedTime = lastReportedTime;
    }
}
