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

import org.apache.doris.stack.entity.ActivityEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface ActivityRepository extends JpaRepository<ActivityEntity, Integer>,
        PagingAndSortingRepository<ActivityEntity, Integer> {

    @Query("select s from ActivityEntity s where s.model = :model")
    List<ActivityEntity> getByModel(@Param("model") String model);

    @Query("select s from ActivityEntity s where s.model = :model and s.userId = :userId")
    List<ActivityEntity> getByModelAndUserId(@Param("model") String model, @Param("userId") int userId);

    @Query("select s from ActivityEntity s where s.model = :model and s.modelId = :modelId and s.userId = :userId")
    List<ActivityEntity> getByModelAndModelIdAndUserId(@Param("model") String model,
                                                       @Param("modelId") int modelId,
                                                       @Param("userId") int userId);

    @Query("select max(s.id) as max_id, s.userId, s.model, s.modelId from ActivityEntity s where s.model in (:models) "
            + "and s.userId = :userId group by s.userId, s.model, s.modelId order by max_id desc")
    List<List<String>> getByModelsGroupByUserIdAndModelAndModelId(@Param("models") List<String> models,
                                                           @Param("userId") int userId, Pageable pageable);

    Page<ActivityEntity> findByUserId(int userId, Pageable pageable);

}
