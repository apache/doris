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
@Table(name = "manager_database")
@Data
@NoArgsConstructor
public class ManagerDatabaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    /**
     * ID of the engine cluster
     */
    @Column(name = "cluster_id", nullable = false)
    private int clusterId;

    @Column(name = "created_at", nullable = false)
    private Timestamp createdAt;

    @Column(name = "updated_at", nullable = false)
    private Timestamp updatedAt;

    @Column(length = 254, nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    private String type;

    public ManagerDatabaseEntity(int clusterId, String name, String description) {
        this.clusterId = clusterId;
        this.name = name;
        this.description = description;

        this.updatedAt = new Timestamp(System.currentTimeMillis());
        this.createdAt = new Timestamp(System.currentTimeMillis());
    }

}
