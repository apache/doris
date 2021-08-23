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

import org.apache.doris.stack.entity.ManagerTableEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface ManagerTableRepository extends JpaRepository<ManagerTableEntity, Integer> {

    @Query("select d from ManagerTableEntity d where d.dbId = :dbId and d.name = :name")
    List<ManagerTableEntity> getByDbIdAndName(@Param("dbId") int dbId,
                                              @Param("name") String name);

    @Query("select d from ManagerTableEntity d where d.dbId = :dbId")
    List<ManagerTableEntity> getByDbId(@Param("dbId") int dbId);
}
