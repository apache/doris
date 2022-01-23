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

#include "olap/memory/mem_tablet.h"

#include <gtest/gtest.h>

#include "olap/memory/mem_tablet_scan.h"
#include "olap/memory/write_txn.h"
#include "olap/tablet_meta.h"
#include "test_util/test_util.h"

namespace doris {
namespace memory {

struct TData {
    int32_t id;
    int32_t uv;
    int32_t pv;
    int8_t city;
};

TEST(MemTablet, writescan) {
    const int num_insert = LOOP_LESS_OR_MORE(2000, 2000000);
    const int insert_per_write = LOOP_LESS_OR_MORE(500, 500000);
    const int num_update = LOOP_LESS_OR_MORE(10, 10000);
    const int update_time = 3;
    scoped_refptr<Schema> sc;
    ASSERT_TRUE(Schema::create("id int,uv int,pv int,city tinyint null", &sc).ok());
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    std::vector<TColumn> columns(sc->num_columns());
    for (size_t i = 0; i < sc->num_columns(); i++) {
        const ColumnSchema* cs = sc->get(i);
        col_idx_to_unique_id[i] = cs->cid();
        TColumn& c = columns[i];
        c.__set_column_name(cs->name());
        TColumnType tct;
        if (cs->type() == ColumnType::OLAP_FIELD_TYPE_INT) {
            tct.__set_type(TPrimitiveType::INT);
        } else if (cs->type() == ColumnType::OLAP_FIELD_TYPE_TINYINT) {
            tct.__set_type(TPrimitiveType::TINYINT);
        } else {
            ASSERT_TRUE(false);
        }
        c.__set_column_type(tct);
        c.__set_is_allow_null(cs->is_nullable());
        c.__set_is_key(cs->is_key());
        c.__set_aggregation_type(TAggregationType::REPLACE);
    }
    TTabletSchema tschema;
    tschema.__set_short_key_column_count(1);
    tschema.__set_keys_type(TKeysType::UNIQUE_KEYS);
    tschema.__set_columns(columns);
    tschema.__set_is_in_memory(false);
    tschema.__set_schema_hash(1);
    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1, 1, 1, 1, 1, tschema, static_cast<uint32_t>(sc->cid_size()),
                           col_idx_to_unique_id, TabletUid(1, 1), TTabletType::TABLET_TYPE_MEMORY,
                           TStorageMedium::HDD));
    std::shared_ptr<MemTablet> tablet = MemTablet::create_tablet_from_meta(tablet_meta, nullptr);
    ASSERT_TRUE(tablet->init().ok());

    uint64_t cur_version = 0;
    std::vector<TData> alldata(num_insert);

    // insert
    srand(1);
    size_t nrow = 0;
    for (int insert_start = 0; insert_start < num_insert; insert_start += insert_per_write) {
        std::unique_ptr<WriteTxn> wtx;
        EXPECT_TRUE(tablet->create_write_txn(&wtx).ok());
        PartialRowWriter writer(wtx->get_schema_ptr());
        int insert_end = std::min(insert_start + insert_per_write, num_insert);
        EXPECT_TRUE(writer.start_batch(insert_per_write + 1, insert_per_write * 32).ok());
        for (int i = insert_start; i < insert_end; i++) {
            nrow++;
            EXPECT_TRUE(writer.start_row().ok());
            int id = i;
            int uv = rand() % 10000;
            int pv = rand() % 10000;
            int8_t city = rand() % 100;
            alldata[i].id = id;
            alldata[i].uv = uv;
            alldata[i].pv = pv;
            alldata[i].city = city;
            EXPECT_TRUE(writer.set("id", &id).ok());
            EXPECT_TRUE(writer.set("uv", &uv).ok());
            EXPECT_TRUE(writer.set("pv", &pv).ok());
            EXPECT_TRUE(writer.set("city", city % 2 == 0 ? nullptr : &city).ok());
            EXPECT_TRUE(writer.end_row().ok());
        }
        std::vector<uint8_t> wtxn_buff;
        EXPECT_TRUE(writer.finish_batch(&wtxn_buff).ok());
        PartialRowBatch* batch = wtx->new_batch();
        EXPECT_TRUE(batch->load(std::move(wtxn_buff)).ok());
        EXPECT_TRUE(tablet->commit_write_txn(wtx.get(), ++cur_version).ok());
        wtx.reset();
    }

    // update
    for (int i = 0; i < update_time; i++) {
        std::unique_ptr<WriteTxn> wtx;
        EXPECT_TRUE(tablet->create_write_txn(&wtx).ok());
        PartialRowWriter writer(wtx->get_schema_ptr());
        EXPECT_TRUE(writer.start_batch(num_update + 1, num_update * 32).ok());
        size_t nrow = 0;
        for (int j = 0; j < num_update; j++) {
            nrow++;
            EXPECT_TRUE(writer.start_row().ok());
            int id = rand() % num_insert;
            int uv = rand() % 10000;
            int pv = rand() % 10000;
            int8_t city = rand() % 100;
            alldata[id].uv = uv;
            alldata[id].pv = pv;
            alldata[id].city = city;
            EXPECT_TRUE(writer.set("id", &id).ok());
            EXPECT_TRUE(writer.set("pv", &pv).ok());
            EXPECT_TRUE(writer.set("city", city % 2 == 0 ? nullptr : &city).ok());
            EXPECT_TRUE(writer.end_row().ok());
        }
        std::vector<uint8_t> wtxn_buff;
        EXPECT_TRUE(writer.finish_batch(&wtxn_buff).ok());
        PartialRowBatch* batch = wtx->new_batch();
        EXPECT_TRUE(batch->load(std::move(wtxn_buff)).ok());
        EXPECT_TRUE(tablet->commit_write_txn(wtx.get(), ++cur_version).ok());
        wtx.reset();
    }

    // scan perf test
    {
        double t0 = GetMonoTimeSecondsAsDouble();
        std::unique_ptr<ScanSpec> scanspec(new ScanSpec({"pv"}, cur_version));
        std::unique_ptr<MemTabletScan> scan;
        ASSERT_TRUE(tablet->scan(&scanspec, &scan).ok());
        const RowBlock* rblock = nullptr;
        while (true) {
            EXPECT_TRUE(scan->next_block(&rblock).ok());
            if (!rblock) {
                break;
            }
        }
        double t = GetMonoTimeSecondsAsDouble() - t0;
        LOG(INFO) << StringPrintf("scan %d record, time: %.3lfs %.0lf row/s", num_insert, t,
                                  num_insert / t);
        scan.reset();
    }

    // scan result validation
    {
        std::unique_ptr<ScanSpec> scanspec(new ScanSpec({"pv"}, cur_version));
        std::unique_ptr<MemTabletScan> scan;
        ASSERT_TRUE(tablet->scan(&scanspec, &scan).ok());
        size_t curidx = 0;
        while (true) {
            const RowBlock* rblock = nullptr;
            EXPECT_TRUE(scan->next_block(&rblock).ok());
            if (!rblock) {
                break;
            }
            size_t nrows = rblock->num_rows();
            const ColumnBlock& cb = rblock->get_column(0);
            for (size_t i = 0; i < nrows; i++) {
                int32_t value = cb.data().as<int32_t>()[i];
                EXPECT_EQ(value, alldata[curidx].pv);
                curidx++;
            }
        }
        EXPECT_EQ(curidx, (size_t)num_insert);
        scan.reset();
    }
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
