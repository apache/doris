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

suite("test_mv_select") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS mv_test_table_t"
    sql """
	    CREATE TABLE `mv_test_table_t` (
        `Uid` bigint(20) NOT NULL,
        `DateCode` int(11) NOT NULL,
        `ProductId` bigint(20) NOT NULL,
        `LiveSales` int(11) REPLACE NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`Uid`, `DateCode`, `ProductId`)
        DISTRIBUTED BY HASH(`Uid`, `ProductId`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql "ALTER TABLE mv_test_table_t ADD ROLLUP rollup_mv_test_table_t(ProductId,DateCode,Uid);"

    explain {
        sql ("""select Uid
                        from mv_test_table_t  
                where ProductId = 3570093298674738221  and DateCode >=20230919 and DateCode <=20231018
                        group by Uid;""")
        contains "mv_test_table_t"
    }

    sql """drop table if exists SkuUniqDailyCounter"""
    sql """CREATE TABLE `SkuUniqDailyCounter` (
            `ProductId` bigint(20) NOT NULL,
            `DateCode` int(11) NOT NULL,
            `BrandCid` bigint(20) NOT NULL,
            `ShopId` varchar(30) NOT NULL,
            `BigCid` int(11) REPLACE NULL,
            `FirstCid` int(11) REPLACE NULL,
            `SecondCid` int(11) REPLACE NULL,
            `ThirdCid` int(11) REPLACE NULL,
            `FourthCid` int(11) REPLACE NULL,
            `Price` int(11) REPLACE NULL,
            `CosRatio` int(11) REPLACE NULL,
            `PmtOrders` int(11) REPLACE NULL,
            `PmtViews` int(11) REPLACE NULL,
            `PmtUsers` int(11) REPLACE NULL,
            `VideoPrice` int(11) REPLACE NULL,
            `LivePrice` int(11) REPLACE NULL,
            `LiveSales` int(11) REPLACE NULL,
            `LiveOrders` int(11) REPLACE NULL,
            `LiveGmv` bigint(20) REPLACE NULL,
            `VideoSales` int(11) REPLACE NULL,
            `VideoGmv` bigint(20) REPLACE NULL,
            `SubtractSales` int(11) REPLACE NULL,
            `SubtractGmv` bigint(20) REPLACE NULL,
            `FinalSales` int(11) REPLACE NULL,
            `FinalGmv` bigint(20) REPLACE NULL,
            `RoomIds` bitmap BITMAP_UNION NOT NULL,
            `LiveUids` bitmap BITMAP_UNION NOT NULL,
            `VideoAwemeIds` bitmap BITMAP_UNION NOT NULL,
            `VideoUids` bitmap BITMAP_UNION NOT NULL,
            `Uids` bitmap BITMAP_UNION NOT NULL,
            `PlayCount` bigint(20) REPLACE NULL,
            `LikeCount` bigint(20) REPLACE NULL,
            `SelfViews` int(11) REPLACE NULL,
            `FinalViews` int(11) REPLACE NULL,
            `SelfSales` int(11) REPLACE NULL,
            `LiveUsers` bigint(20) REPLACE NULL,
            `LivePriceArray` bitmap BITMAP_UNION NOT NULL,
            `VideoPriceArray` bitmap BITMAP_UNION NOT NULL,
            `NearlyLivePrice` int(11) REPLACE NULL,
            `NearlyVideoPrice` int(11) REPLACE NULL,
            `NaturalSales` int(11) REPLACE NULL,
            `NaturalGmv` bigint(20) REPLACE NULL,
            INDEX idx_shopid (`ShopId`) USING BITMAP COMMENT ''
            ) ENGINE=OLAP
            AGGREGATE KEY(`ProductId`, `DateCode`, `BrandCid`, `ShopId`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ProductId`) BUCKETS 8
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );"""
    def delta_time = 1000
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }
    sql """ALTER TABLE SkuUniqDailyCounter
            ADD ROLLUP rollup_index_shopid(ShopId,
            DateCode,
            ProductId,
            BrandCid,
            BigCid,
            FirstCid,
            SecondCid,
            ThirdCid,
            FourthCid,
            Price,
            CosRatio,
            PmtOrders,
            PmtViews,
            PmtUsers,
            VideoPrice,
            LivePrice,
            LiveSales,
            LiveOrders,
            LiveGmv,
            VideoSales,
            VideoGmv,
            SubtractSales,
            SubtractGmv,
            FinalSales,
            FinalGmv,
            RoomIds,
            LiveUids,
            VideoAwemeIds,
            VideoUids,
            Uids,
            PlayCount,
            LikeCount,
            SelfViews,
            FinalViews,
            SelfSales,
            LiveUsers,
            LivePriceArray,
            VideoPriceArray,
            NearlyLivePrice,
            NearlyVideoPrice,
            NaturalSales,
            NaturalGmv
            ); """
    wait_for_latest_op_on_table_finish("SkuUniqDailyCounter",60000);
    sql """ALTER TABLE SkuUniqDailyCounter 
            ADD ROLLUP rollup_index_brandcid(BrandCid,
            DateCode,
            ProductId,
            ShopId,
            BigCid,
            FirstCid,
            SecondCid,
            ThirdCid,
            FourthCid,
            Price,
            CosRatio,
            PmtOrders,
            PmtViews,
            PmtUsers,
            VideoPrice,
            LivePrice,
            LiveSales,
            LiveOrders,
            LiveGmv,
            VideoSales,
            VideoGmv,
            SubtractSales,
            SubtractGmv,
            FinalSales,
            FinalGmv,
            RoomIds,
            LiveUids,
            VideoAwemeIds,
            VideoUids,
            Uids,
            PlayCount,
            LikeCount,
            SelfViews,
            FinalViews,
            SelfSales,
            LiveUsers,
            LivePriceArray,
            VideoPriceArray,
            NearlyLivePrice,
            NearlyVideoPrice,
            NaturalSales,
            NaturalGmv
            );"""
    wait_for_latest_op_on_table_finish("SkuUniqDailyCounter",60000);

    explain {
        sql ("""select ProductId,sum(FinalSales) Sales,Sum(FinalGmv) Gmv ,SUM(NaturalSales) NaturalSales,sum(NaturalGmv) NaturalGmv  ,BITMAP_UNION_COUNT(Uids) as Users      from SkuUniqDailyCounter      where BrandCid=742502946 and DateCode >=20240315 and  DateCode <= 20240328      Group by ProductId        order by Gmv DESC LIMIT 10;""")
        contains "rollup_index_brandcid"
    }

}