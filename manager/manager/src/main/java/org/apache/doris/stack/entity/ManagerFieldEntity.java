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

import org.apache.doris.stack.model.palo.TableSchemaInfo;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Entity
@Table(name = "manager_field")
@Data
@NoArgsConstructor
@Slf4j
public class ManagerFieldEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name = "created_at", nullable = false)
    private Timestamp createdAt;

    @Column(name = "updated_at", nullable = false)
    private Timestamp updatedAt;

    @Column(length = 254, nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "position", nullable = false)
    private int position;

    @Column(name = "table_id", nullable = false)
    private int tableId;

    @Column(name = "database_type", columnDefinition = "TEXT")
    private String databaseType;

    @Column(name = "is_null", length = 30)
    private String isNull;

    @Column(name = "default_val")
    private String defaultVal;

    @Column(name = "[key]", length = 30)
    private String key;

    @Column(name = "aggr_type")
    private String aggrType;

    public ManagerFieldEntity(int tableId, TableSchemaInfo.Schema field, int position) {
        this.updatedAt = new Timestamp(System.currentTimeMillis());
        this.createdAt = new Timestamp(System.currentTimeMillis());

        this.name = field.getField();
        this.position = position;
        this.tableId = tableId;
        this.databaseType = field.getType();
        this.description = field.getComment();
        this.isNull = field.getIsNull();
        this.defaultVal = field.getDefaultVal();
        this.key = field.getKey();
        this.aggrType = getAggrType();
    }

    public TableSchemaInfo.Schema transToModel() {
        TableSchemaInfo.Schema schema = new TableSchemaInfo.Schema();
        schema.setKey(this.key);
        schema.setAggrType(this.aggrType);
        schema.setComment(this.description);
        schema.setField(this.name);
        schema.setIsNull(this.isNull);
        schema.setType(this.databaseType);
        schema.setDefaultVal(this.defaultVal);
        return schema;
    }
}
