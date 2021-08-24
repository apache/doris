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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import java.sql.Timestamp;

@Entity
@Table(name = "login_history")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginHistoryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name = "timestamp", nullable = false)
    private Timestamp timestamp;

    @Column(name = "user_id", nullable = false)
    private int userId;

    @Column(length = 254, name = "session_id", nullable = false)
    private String sessionId;

    @Column(length = 254, name = "device_id", nullable = false)
    private String deviceId;

    @Column(length = 254, name = "device_description", nullable = false)
    private String deviceDescription;

    @Column(length = 254, name = "ip_address", nullable = false)
    private String ipAddress;

    public LoginHistoryEntity(Timestamp timestamp, int userId, String sessionId, String deviceId,
                              String deviceDescription, String ipAddress) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.sessionId = sessionId;
        this.deviceId = deviceId;
        this.deviceDescription = deviceDescription;
        this.ipAddress = ipAddress;
    }
}
