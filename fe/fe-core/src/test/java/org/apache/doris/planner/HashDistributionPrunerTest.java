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
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.junit.Assert;
import org.junit.Test;

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

        HashDistributionPruner pruner = new HashDistributionPruner(tabletIds, columns, filters, tabletIds.size(), true);

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

}
