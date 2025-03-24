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

        SqlCacheContext cacheContext = new SqlCacheContext(admin, queryId);
        cacheContext.setOriginSql("SELECT * FROM tbl");
        PUniqueId key1 = cacheContext.doComputeCacheKeyMd5(ImmutableSet.of());

        SqlCacheContext cacheContext2 = new SqlCacheContext(admin, queryId);
        cacheContext2.setOriginSql(
                "-- Same query with comments and extra spaces\n"
                    + "/* Comment */  SELECT   *   FROM   tbl  "
        );
        PUniqueId key2 = cacheContext2.doComputeCacheKeyMd5(ImmutableSet.of());
        Assertions.assertEquals(key1, key2);

        SqlCacheContext cacheContext3 = new SqlCacheContext(admin, queryId);
        cacheContext3.setOriginSql(
                "-- Same query with comments and extra spaces\n"
                        + "/* Comment */  SELeCT   *   FROM   tbl  "
        );
        PUniqueId key3 = cacheContext3.doComputeCacheKeyMd5(ImmutableSet.of());
        Assertions.assertNotEquals(key1, key3);
    }
}
