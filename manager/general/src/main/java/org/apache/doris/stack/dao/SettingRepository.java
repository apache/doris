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

import org.apache.doris.stack.entity.SettingEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface SettingRepository extends JpaRepository<SettingEntity, String> {

    @Query("select s from SettingEntity s where s.key in (:keys)")
    List<SettingEntity> getSettingByKeys(@Param("keys") List<String> keys);

    @Query("select s.value from SettingEntity s where s.key like ?1%")
    List<String> getAllByPrefix(String prefix);

    @Transactional
    @Modifying
    @Query("delete from SettingEntity s where s.key like ?1%")
    void deleteAllByPrefix(String prefix);
}
