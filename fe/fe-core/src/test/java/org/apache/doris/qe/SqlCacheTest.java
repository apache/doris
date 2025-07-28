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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class SqlCacheTest {
    @Test
    public void testCacheKey() {
        TUniqueId queryId = new TUniqueId();
        UUID uuid = UUID.randomUUID();
        queryId.setHi(uuid.getMostSignificantBits());
        queryId.setLo(uuid.getLeastSignificantBits());
        UserIdentity admin = new UserIdentity("admin", "127.0.0.1");

        SqlCacheContext cacheContext = new SqlCacheContext(admin);
        cacheContext.setOriginSql("SELECT * FROM tbl");
        PUniqueId key1 = cacheContext.doComputeCacheKeyMd5(ImmutableSet.of());

        SqlCacheContext cacheContext2 = new SqlCacheContext(admin);
        cacheContext2.setOriginSql(
                "-- Same query with comments and extra spaces\n"
                    + "/* Comment */  SELECT   *   FROM   tbl  "
        );
        PUniqueId key2 = cacheContext2.doComputeCacheKeyMd5(ImmutableSet.of());
        Assertions.assertEquals(key1, key2);

        SqlCacheContext cacheContext3 = new SqlCacheContext(admin);
        cacheContext3.setOriginSql(
                "-- Same query with comments and extra spaces\n"
                        + "/* Comment */  SELeCT   *   FROM   tbl  "
        );
        PUniqueId key3 = cacheContext3.doComputeCacheKeyMd5(ImmutableSet.of());
        Assertions.assertNotEquals(key1, key3);
    }
}
