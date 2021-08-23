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

import org.apache.doris.stack.entity.CoreUserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface CoreUserRepository extends JpaRepository<CoreUserEntity, Integer> {
    @Query("select c from CoreUserEntity c where c.email = :email")
    List<CoreUserEntity> getByEmail(@Param("email") String email);

    @Query("select c from CoreUserEntity c where c.email = :email and c.ldapAuth = :ldapAuth")
    List<CoreUserEntity> getByEmailAndLdapAuth(@Param("email") String email, @Param("ldapAuth") boolean ldapAuth);

    @Query("select c from CoreUserEntity c where c.isSuperuser = :isSuperuser and c.isActive = :isActive")
    List<CoreUserEntity> getActiveAdminUser(@Param("isSuperuser") boolean isSuperuser,
                                            @Param("isActive") boolean isActive);

    @Query("select c.id from CoreUserEntity c where c.ldapAuth = false and c.id in (:userIds)")
    List<Integer> getAllStudioUser(@Param("userIds") List<Integer> userIds);

    @Query("select c.id from CoreUserEntity c where c.ldapAuth = true and c.id in (:userIds)")
    List<Integer> getAllLdapUser(@Param("userIds") List<Integer> userIds);

    @Transactional
    @Modifying
    @Query("delete from CoreUserEntity c where c.id in (:userIds)")
    void deleteByUserIds(@Param("userIds") List<Integer> userIds);

    @Query("select c from CoreUserEntity c where c.email = :email and c.idaasAuth = :idaasAuth")
    List<CoreUserEntity> getByEmailAndIdaasAuth(@Param("email") String email, @Param("idaasAuth") boolean idaasAuth);
}
