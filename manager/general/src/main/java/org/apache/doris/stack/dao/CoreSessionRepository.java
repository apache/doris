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

import org.apache.doris.stack.entity.CoreSessionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.sql.Timestamp;
import java.util.List;

public interface CoreSessionRepository extends JpaRepository<CoreSessionEntity, String> {

    @Modifying
    @Query("delete from CoreSessionEntity c where c.userId = :userId")
    void deleteByUserId(@Param("userId") int userId);

    @Modifying
    @Query("delete from CoreSessionEntity c where c.userId in (:userIds)")
    void deleteByUserId(@Param("userIds") List<Integer> userIds);

    @Modifying
    @Query("delete from CoreSessionEntity c where c.createdAt < :expireTime")
    void deleteExpireSession(@Param("expireTime") Timestamp expireTime);

    @Query("select count(c.id) from CoreSessionEntity c where c.userId = :userId and c.createdAt > :timeBefore")
    Integer getSessionCountBeforeByUserId(@Param("userId") int userId, @Param("timeBefore") Timestamp timeBefore);

    @Query("select count(c.id) from CoreSessionEntity c where c.userId = :userId")
    Integer getSessionCountByUserId(@Param("userId") int userId);

    // jpa不支持limit
    @Modifying
    @Query(value = "delete from core_session  where core_session.user_id = :userId order by core_session.created_at limit :count",
            nativeQuery = true)
    void deleteSessionByUserId(@Param("userId") int userId, @Param("count") int count);

    @Modifying
    @Query(value = "delete from core_session  where core_session.user_id = :userId and core_session.created_at > "
            + ":timeBefore order by core_session.created_at limit :count", nativeQuery = true)
    void deleteSessionBeforeByUserId(@Param("userId") int userId, @Param("timeBefore") Timestamp timeBefore,
                                     @Param("count") int count);

}
