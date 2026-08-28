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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IPv4Literal;
import org.apache.doris.analysis.IPv6Literal;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo.HashType;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashDistributionPrunerTest {

    @Test
    public void test() {
        List<Long> tabletIds = Lists.newArrayListWithExpectedSize(300);
        for (long i = 0; i < 300; i++) {
            tabletIds.add(i);
        }

        // distribution columns
        Column dealDate = new Column("dealDate", PrimitiveType.DATE, false);
        Column mainBrandId = new Column("main_brand_id", PrimitiveType.CHAR, false);
        Column itemThirdCateId = new Column("item_third_cate_id", PrimitiveType.CHAR, false);
        Column channel = new Column("channel", PrimitiveType.CHAR, false);
        Column shopType = new Column("shop_type", PrimitiveType.CHAR, false);
        List<Column> columns = Lists.newArrayList(dealDate, mainBrandId, itemThirdCateId, channel, shopType);

        // filters
        PartitionColumnFilter dealDatefilter = new PartitionColumnFilter();
        dealDatefilter.setLowerBound(new StringLiteral("2019-08-22"), true);
        dealDatefilter.setUpperBound(new StringLiteral("2019-08-22"), true);

        PartitionColumnFilter mainBrandFilter = new PartitionColumnFilter();
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("1323"));
        inList.add(new StringLiteral("2528"));
        inList.add(new StringLiteral("9610"));
        inList.add(new StringLiteral("3893"));
        inList.add(new StringLiteral("6121"));
        mainBrandFilter.setInPredicate(new InPredicate(new SlotRef(null, "main_brand_id"), inList, false));

        PartitionColumnFilter itemThirdFilter = new PartitionColumnFilter();
        List<Expr> inList2 = Lists.newArrayList();
        inList2.add(new StringLiteral("9719"));
        inList2.add(new StringLiteral("11163"));
        itemThirdFilter.setInPredicate(new InPredicate(new SlotRef(null, "item_third_cate_id"), inList2, false));

        PartitionColumnFilter channelFilter = new PartitionColumnFilter();
        List<Expr> inList3 = Lists.newArrayList();
        inList3.add(new StringLiteral("1"));
        inList3.add(new StringLiteral("3"));
        channelFilter.setInPredicate(new InPredicate(new SlotRef(null, "channel"), inList3, false));

        PartitionColumnFilter shopTypeFilter = new PartitionColumnFilter();
        List<Expr> inList4 = Lists.newArrayList();
        inList4.add(new StringLiteral("2"));
        shopTypeFilter.setInPredicate(new InPredicate(new SlotRef(null, "shop_type"), inList4, false));

        Map<String, PartitionColumnFilter> filters = new CaseInsensitiveMap();
        filters.put("DEALDATE", dealDatefilter);
        filters.put("MAIN_BRAND_ID", mainBrandFilter);
        filters.put("ITEM_THIRD_CATE_ID", itemThirdFilter);
        filters.put("CHANNEL", channelFilter);
        filters.put("SHOP_TYPE", shopTypeFilter);

        HashDistributionPruner pruner = new HashDistributionPruner(null, tabletIds, columns, filters, tabletIds.size(),
                true);

        Collection<Long> results = pruner.prune();
        // 20 = 1 * 5 * 2 * 2 * 1 (element num of each filter)
        Assert.assertEquals(20, results.size());

        filters.get("SHOP_TYPE").getInPredicate().addChild(new StringLiteral("4"));
        results = pruner.prune();
        // 40 = 1 * 5 * 2 * 2 * 2 (element num of each filter)
        // 39 is because these is hash conflict
        Assert.assertEquals(39, results.size());

        filters.get("SHOP_TYPE").getInPredicate().addChild(new StringLiteral("5"));
        filters.get("SHOP_TYPE").getInPredicate().addChild(new StringLiteral("6"));
        filters.get("SHOP_TYPE").getInPredicate().addChild(new StringLiteral("7"));
        filters.get("SHOP_TYPE").getInPredicate().addChild(new StringLiteral("8"));
        results = pruner.prune();
        // 120 = 1 * 5 * 2 * 2 * 6 (element num of each filter) > 100
        Assert.assertEquals(300, results.size());

        // check hash conflict
        inList4.add(new StringLiteral("4"));
        PartitionKey hashKey = new PartitionKey();
        Set<Long> tablets = Sets.newHashSet();
        hashKey.pushColumn(new StringLiteral("2019-08-22"), PrimitiveType.DATE);
        for (Expr inLiteral : inList) {
            hashKey.pushColumn((StringLiteral) inLiteral, PrimitiveType.CHAR);
            for (Expr inLiteral2 : inList2) {
                hashKey.pushColumn((StringLiteral) inLiteral2, PrimitiveType.CHAR);
                for (Expr inLiteral3 : inList3) {
                    hashKey.pushColumn((StringLiteral) inLiteral3, PrimitiveType.CHAR);
                    for (Expr inLiteral4 : inList4) {
                        hashKey.pushColumn((StringLiteral) inLiteral4, PrimitiveType.CHAR);
                        long hashValue = hashKey.getHashValue();
                        tablets.add(tabletIds.get((int) ((hashValue & 0xffffffff) % tabletIds.size())));
                        hashKey.popColumn();
                    }
                    hashKey.popColumn();
                }
                hashKey.popColumn();
            }
            hashKey.popColumn();
        }

        Assert.assertEquals(39, tablets.size());
    }

    // Identity bucketing treats each value's canonical bytes as an unsigned integer with its first
    // byte least significant, then appends multiple columns before taking the bucket modulus. This
    // must remain bit-identical with BE tablet routing and bucket-shuffle partitioning.
    @Test
    public void testIdentityPrune() {
        List<Long> tabletIds = Lists.newArrayListWithExpectedSize(512);
        for (long i = 0; i < 512; i++) {
            tabletIds.add(i);
        }
        Column shardNum = new Column("shard_num", PrimitiveType.BIGINT, false);
        List<Column> columns = Lists.newArrayList(shardNum);

        // in-range: shard_num = 100 -> 100 % 512 = 100
        assertIdentityBucket(tabletIds, columns, "SHARD_NUM", new IntLiteral(100), 100L);
        // wraps: 600 % 512 = 88
        assertIdentityBucket(tabletIds, columns, "SHARD_NUM", new IntLiteral(600), 88L);
        // Two's-complement bytes are interpreted as unsigned. A power-of-two modulus therefore
        // still maps -1 to the final bucket.
        assertIdentityBucket(tabletIds, columns, "SHARD_NUM", new IntLiteral(-1), 511L);

        // LARGEINT uses all 128 bits of its canonical little-endian representation.
        Column bigId = new Column("big_id", PrimitiveType.LARGEINT, false);
        List<Column> bigCols = Lists.newArrayList(bigId);
        BigInteger huge = BigInteger.ONE.shiftLeft(100).add(BigInteger.valueOf(5));
        long expected = huge.mod(BigInteger.valueOf(512)).longValue();
        assertIdentityBucket(tabletIds, bigCols, "BIG_ID", new LargeIntLiteral(huge), expected);

        // With a non-power-of-two bucket count, -1 is UINT32_MAX rather than signed -1.
        List<Long> tenTablets = Lists.newArrayListWithExpectedSize(10);
        for (long i = 0; i < 10; i++) {
            tenTablets.add(i);
        }
        assertIdentityBucket(tenTablets, columns, "SHARD_NUM", new IntLiteral(-1), 5L);
    }

    @Test
    public void testIdentityPruneWithMultipleTypedColumns() {
        List<Long> tabletIds = Lists.newArrayListWithExpectedSize(257);
        for (long i = 0; i < 257; i++) {
            tabletIds.add(i);
        }
        List<Column> columns = Lists.newArrayList(
                new Column("id", PrimitiveType.INT, false),
                new Column("name", PrimitiveType.VARCHAR, false));

        Map<String, PartitionColumnFilter> filters = new CaseInsensitiveMap();
        PartitionColumnFilter idFilter = new PartitionColumnFilter();
        idFilter.setLowerBound(new IntLiteral(1), true);
        idFilter.setUpperBound(new IntLiteral(1), true);
        filters.put("ID", idFilter);
        PartitionColumnFilter nameFilter = new PartitionColumnFilter();
        nameFilter.setLowerBound(new StringLiteral("A"), true);
        nameFilter.setUpperBound(new StringLiteral("A"), true);
        filters.put("NAME", nameFilter);

        HashDistributionPruner pruner = new HashDistributionPruner(null, tabletIds, columns, filters,
                tabletIds.size(), true, HashType.IDENTITY);
        // append(uint32_le(1), bytes("A")) = 1 * 256 + 65; 321 % 257 = 64
        Assert.assertEquals(Lists.newArrayList(64L), pruner.prune());
    }

    @Test
    public void testIdentityPruneWithIpCanonicalBytes() throws Exception {
        PartitionKey ipv4 = new PartitionKey();
        ipv4.pushColumn(new IPv4Literal("1.2.3.4"), PrimitiveType.IPV4);
        Assert.assertEquals(255, ipv4.getIdentityHashValue(257));

        PartitionKey ipv6 = new PartitionKey();
        ipv6.pushColumn(new IPv6Literal("::1"), PrimitiveType.IPV6);
        Assert.assertEquals(256, ipv6.getIdentityHashValue(257));
    }

    private void assertIdentityBucket(List<Long> tabletIds, List<Column> columns, String colName, Expr value,
            long expectedBucket) {
        PartitionColumnFilter filter = new PartitionColumnFilter();
        filter.setLowerBound((LiteralExpr) value, true);
        filter.setUpperBound((LiteralExpr) value, true);
        Map<String, PartitionColumnFilter> filters = new CaseInsensitiveMap();
        filters.put(colName, filter);

        HashDistributionPruner pruner = new HashDistributionPruner(null, tabletIds, columns, filters, tabletIds.size(),
                true, HashType.IDENTITY);
        Collection<Long> results = pruner.prune();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(Long.valueOf(expectedBucket), results.iterator().next());
    }

}
