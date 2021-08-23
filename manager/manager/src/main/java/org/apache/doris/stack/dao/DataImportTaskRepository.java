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

import org.apache.doris.stack.entity.DataImportTaskEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.sql.Timestamp;
import java.util.List;

public interface DataImportTaskRepository extends JpaRepository<DataImportTaskEntity, Long>,
        JpaSpecificationExecutor<DataImportTaskEntity> {

    @Query("select d from DataImportTaskEntity d where d.dbId = :dbId and d.taskName = :taskName")
    List<DataImportTaskEntity> getByDbIdAndName(@Param("dbId") int dbId,
                                                @Param("taskName") String taskName);

    @Query("select d from DataImportTaskEntity d where d.tableId = :tableId")
    List<DataImportTaskEntity> getByTableId(@Param("tableId") int tableId);

    @Query("select count(d) from DataImportTaskEntity d where d.tableId = :tableId")
    int countByTableId(@Param("tableId") int tableId);

    @Modifying
    @Query("delete from DataImportTaskEntity d where d.createTime < :expireTime")
    void deleteExpireData(@Param("expireTime") Timestamp expireTime);

}
