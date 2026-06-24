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
//
// Branch-coverage tests for the VariantParse and RowStoreFill transform stages,
// plus the row-store derived-column generator (horizontal one-shot materialize
// and the vertical bounded-batch pump). The coverage targets and per-branch
// oracle values come from dev-docs/transform-chain-test-design.md §2 (V1/V2/V3,
// R1-R5 + RS-pump-by-rows / RS-pump-by-bytes / RS-pump-single-oversize / RS-pu).

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/transform/block_transform.h"
#include "testutil/variant_util.h"
#include "util/jsonb/serialize.h"
#include "util/jsonb_document.h"

namespace doris {

using segment_v2::build_transform_chain;
using segment_v2::DerivedColumn;
using segment_v2::materialize_derived_columns;
using segment_v2::TransformExecContext;

class VariantRowStoreTest : public MowTransformTestBase {
protected:
    RowsetWriterContext direct_rwc(const TabletSchemaSPtr& schema) {
        RowsetWriterContext c;
        c.tablet_schema = schema;
        c.write_type = DataWriteType::TYPE_DIRECT;
        c.enable_unique_key_merge_on_write = true;
        return c;
    }
    TransformExecContext exec_ctx(const TabletSchemaSPtr& schema, RowsetWriterContext* rwc,
                                  int32_t segment_id = 0) {
        TransformExecContext ctx;
        ctx.tablet_schema = schema;
        ctx.write_type = rwc->write_type;
        ctx.rowset_ctx = rwc;
        ctx.segment_id = segment_id;
        return ctx;
    }

    // (k INT key, v VARIANT, delete-sign, vv INT nullable default 0) -- a variant
    // schema with one extra fixed value column so a fixed partial update can omit
    // some columns and exercise VariantParse on the fill-widened full block (V3).
    // Defined locally per the design's create_variant_pu_schema().
    TabletSchemaSPtr create_variant_pu_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);
        {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(0);
            c->set_name("k");
            c->set_type("INT");
            c->set_is_key(true);
            c->set_length(4);
            c->set_index_length(4);
            c->set_is_nullable(false);
            c->set_aggregation("NONE");
        }
        {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(1);
            c->set_name("v");
            c->set_type("VARIANT");
            c->set_is_key(false);
            c->set_length(2147483643);
            c->set_index_length(4);
            c->set_is_nullable(false);
            c->set_aggregation("NONE");
            c->set_variant_max_subcolumns_count(3);
        }
        {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(2);
            c->set_name(DELETE_SIGN);
            c->set_type("TINYINT");
            c->set_is_key(false);
            c->set_length(1);
            c->set_index_length(1);
            c->set_is_nullable(false);
            c->set_aggregation("NONE");
            c->set_default_value(std::to_string(0));
        }
        {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(3);
            c->set_name("vv");
            c->set_type("INT");
            c->set_is_key(false);
            c->set_length(4);
            c->set_index_length(4);
            c->set_is_nullable(true);
            c->set_aggregation("NONE");
            c->set_default_value(std::to_string(0));
        }
        pb.set_delete_sign_idx(2);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // Inserts one root-scalar JSON object string into a block's variant column.
    static void insert_variant_json(Block& block, size_t variant_pos, std::string_view json) {
        auto* variant = assert_cast<ColumnVariant*>(
                block.get_by_position(variant_pos).column->assert_mutable().get());
        VariantUtil::insert_root_scalar_field(
                *variant, Field::create_field<TYPE_STRING>(String(std::string(json))));
    }

    // Round-trips one finalized variant row back to canonical JSON (spaces stripped).
    static std::string variant_row_json(const Block& block, size_t variant_pos, size_t row) {
        const auto* parsed =
                assert_cast<const ColumnVariant*>(block.get_by_position(variant_pos).column.get());
        DataTypeSerDe::FormatOptions options;
        std::string json;
        parsed->serialize_one_row_to_string(static_cast<int64_t>(row), &json, options);
        std::erase(json, ' ');
        return json;
    }

    // Decodes one row-store JSONB cell back into a 1-row block of the non-row-store
    // columns, the way BaseTablet::fetch_value_through_row_column does on read.
    // Returns the decoded block; `block` is keyed by the schema's logical position.
    Block decode_row_store_cell(const TabletSchemaSPtr& schema, StringRef cell) {
        // Build a block + serdes for every non-row-store column, keyed by unique_id.
        std::vector<uint32_t> cids;
        for (size_t i = 0; i < schema->num_columns(); ++i) {
            if (!schema->column(i).is_row_store_column()) {
                cids.push_back(static_cast<uint32_t>(i));
            }
        }
        Block dst = schema->create_block_by_cids(cids);
        DataTypeSerDeSPtrs serdes = create_data_type_serdes(dst.get_data_types());
        std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
        std::vector<std::string> default_values(cids.size());
        for (size_t i = 0; i < cids.size(); ++i) {
            const TabletColumn& col = schema->column(cids[i]);
            col_uid_to_idx[static_cast<uint32_t>(col.unique_id())] = static_cast<uint32_t>(i);
            default_values[i] = col.default_value();
        }
        EXPECT_TRUE(JsonbSerializeUtil::jsonb_to_block(serdes, cell.data, cell.size, col_uid_to_idx,
                                                       dst, default_values, {})
                            .ok());
        return dst;
    }
};

// ===========================================================================
// VariantParseStage
// ===========================================================================

// V1: a schema with no variant column -> VariantParseStage is a pass-through;
// column count, row count and every cell value are left untouched.
TEST_F(VariantRowStoreTest, VariantParseNoVariantPassThrough) {
    auto schema = create_mow_schema(/*has_seq=*/false); // k v delete_sign: no variant
    ASSERT_EQ(schema->num_variant_columns(), 0U);
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = schema->create_block(); // full width, 2 rows
    IColumn* k = block.get_by_position(0).column->assert_mutable().get();
    IColumn* v = block.get_by_position(1).column->assert_mutable().get();
    IColumn* ds = block.get_by_position(2).column->assert_mutable().get();
    int32_t ks[] = {7, 9};
    int32_t vs[] = {70, 90};
    int8_t zero8 = 0;
    for (int i = 0; i < 2; ++i) {
        k->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
        v->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
        ds->insert_data(reinterpret_cast<const char*>(&zero8), sizeof(int8_t));
    }

    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    // unchanged: same width, same height, same values
    ASSERT_EQ(block.columns(), schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(read_int(block, 0, 0), 7);
    EXPECT_EQ(read_int(block, 0, 1), 9);
    EXPECT_EQ(read_int(block, 1, 0), 70);
    EXPECT_EQ(read_int(block, 1, 1), 90);
    EXPECT_EQ(read_tinyint(block, 2, 0), 0);
    EXPECT_EQ(read_tinyint(block, 2, 1), 0);
    // a non-variant table registers no derived column either
    EXPECT_EQ(ctx.derived_column.second, nullptr);
}

// V2: direct write parses the root-only variant in place -- the row finalizes
// and the original {"a":1,"b":"x"} survives parse + finalize as the same JSON.
TEST_F(VariantRowStoreTest, VariantParseDirectSingleRow) {
    auto schema = create_variant_schema(); // k(0) v VARIANT(1) delete_sign(2)
    ASSERT_EQ(schema->num_variant_columns(), 1U);
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = schema->create_block();
    int32_t k = 1;
    int8_t z = 0;
    block.get_by_position(0).column->assert_mutable()->insert_data(
            reinterpret_cast<const char*>(&k), sizeof(int32_t));
    insert_variant_json(block, 1, R"({"a":1,"b":"x"})");
    block.get_by_position(2).column->assert_mutable()->insert_data(
            reinterpret_cast<const char*>(&z), sizeof(int8_t));

    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_EQ(block.columns(), schema->num_columns());
    ASSERT_EQ(block.rows(), 1);
    const auto* parsed = assert_cast<const ColumnVariant*>(block.get_by_position(1).column.get());
    EXPECT_TRUE(parsed->is_finalized());
    const std::string json = variant_row_json(block, 1, 0);
    EXPECT_NE(json.find(R"("a":1)"), std::string::npos) << json;
    EXPECT_NE(json.find(R"("b":"x")"), std::string::npos) << json;
}

// V2 (multi-row): two distinct objects both finalize and each round-trips to its
// own inserted keys; the column width is unchanged.
TEST_F(VariantRowStoreTest, VariantParseDirectMultiRow) {
    auto schema = create_variant_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = schema->create_block();
    IColumn* k = block.get_by_position(0).column->assert_mutable().get();
    IColumn* ds = block.get_by_position(2).column->assert_mutable().get();
    int32_t ks[] = {1, 2};
    int8_t z = 0;
    k->insert_data(reinterpret_cast<const char*>(&ks[0]), sizeof(int32_t));
    insert_variant_json(block, 1, R"({"a":1})");
    ds->insert_data(reinterpret_cast<const char*>(&z), sizeof(int8_t));
    k->insert_data(reinterpret_cast<const char*>(&ks[1]), sizeof(int32_t));
    insert_variant_json(block, 1, R"({"a":2,"c":true})");
    ds->insert_data(reinterpret_cast<const char*>(&z), sizeof(int8_t));

    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_EQ(block.columns(), schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    const auto* parsed = assert_cast<const ColumnVariant*>(block.get_by_position(1).column.get());
    EXPECT_TRUE(parsed->is_finalized());
    const std::string json0 = variant_row_json(block, 1, 0);
    const std::string json1 = variant_row_json(block, 1, 1);
    EXPECT_NE(json0.find(R"("a":1)"), std::string::npos) << json0;
    EXPECT_NE(json1.find(R"("a":2)"), std::string::npos) << json1;
    // the variant serializes a JSON bool as an integer (true -> 1)
    EXPECT_NE(json1.find(R"("c":1)"), std::string::npos) << json1;
    // row 0 did not gain row 1's key
    EXPECT_EQ(json0.find(R"("c":)"), std::string::npos) << json0;
}

// V2 (empty block): an empty variant block parses without crashing and keeps its
// full width with zero rows.
TEST_F(VariantRowStoreTest, VariantParseEmptyBlock) {
    auto schema = create_variant_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = schema->create_block(); // typed columns, 0 rows
    ASSERT_EQ(block.rows(), 0);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    EXPECT_EQ(block.columns(), schema->num_columns());
    EXPECT_EQ(block.rows(), 0);
}

// V3: a fixed partial update + variant table. The full chain runs the fill FIRST
// (widening the narrow {k,v} block to full width, default-filling the omitted vv
// and delete_sign), THEN VariantParse on that full-width block. The variant `v` is
// carried in the narrow block; if parse ran before the fill it would have acted on
// the 2-column pre-fill block. Asserting the block widened to 4 columns AND the
// carried variant still finalized + round-trips proves parse saw the widened block.
// Empty history -> both keys are brand-new APPENDs (vv/delete_sign take defaults),
// so no variant ever needs to be written into a committed rowset.
TEST_F(VariantRowStoreTest, VariantParseAfterPartialUpdateFill) {
    auto schema = create_variant_pu_schema(); // k(0) v VARIANT(1) delete_sign(2) vv(3)
    ASSERT_EQ(schema->num_variant_columns(), 1U);

    auto tablet = make_tablet(schema, 8001);
    auto mow = make_mow_context(100, {}); // empty history -> every key is new

    // fixed PU on {k, v}: vv + delete_sign omitted (default-filled); v carries the
    // new variant data.
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", "v"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(8002);
    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.tablet_id = kTabletId;
    rwc.tablet = tablet;
    rwc.partial_update_info = pui;
    rwc.rowset_id = new_rsid;

    // the built chain places the fill before VariantParse
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(),
              (std::vector<std::string_view> {"Validate", "FixedPartialUpdateFill", "VariantParse",
                                              "RowStoreFill"}));

    TransformExecContext ctx = exec_ctx(schema, &rwc);
    ctx.tablet = tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;
    ctx.rowset_id = new_rsid;

    // narrow block in update_cids order {k, v}: 2 rows, k = 1 and 3
    Block block = schema->create_block_by_cids({0, 1});
    IColumn* kc = block.get_by_position(0).column->assert_mutable().get();
    int32_t k1 = 1;
    kc->insert_data(reinterpret_cast<const char*>(&k1), sizeof(int32_t));
    insert_variant_json(block, 1, R"({"p":7})");
    int32_t k3 = 3;
    kc->insert_data(reinterpret_cast<const char*>(&k3), sizeof(int32_t));
    insert_variant_json(block, 1, R"({"p":8})");

    auto saved = config::enable_merge_on_write_correctness_check;
    config::enable_merge_on_write_correctness_check = false;
    auto st = chain.apply(ctx, &block);
    config::enable_merge_on_write_correctness_check = saved;
    ASSERT_TRUE(st.ok()) << st;

    // the chain widened to full width and kept both rows
    ASSERT_EQ(block.columns(), schema->num_columns()); // 4
    ASSERT_EQ(block.rows(), 2);
    // VariantParse ran on the widened block: the carried variant finalized and
    // round-trips to its inserted key/value (a parse on the narrow pre-fill block
    // could not have left a finalized variant in a 4-column block).
    const auto* parsed = assert_cast<const ColumnVariant*>(block.get_by_position(1).column.get());
    EXPECT_TRUE(parsed->is_finalized());
    EXPECT_NE(variant_row_json(block, 1, 0).find(R"("p":7)"), std::string::npos);
    EXPECT_NE(variant_row_json(block, 1, 1).find(R"("p":8)"), std::string::npos);
    // vv was the omitted column for brand-new keys -> default 0 (the fill widened
    // the block before parse touched it).
    EXPECT_EQ(read_int(block, 3, 0), 0);
    EXPECT_EQ(read_int(block, 3, 1), 0);
}

// ===========================================================================
// RowStoreFillStage + RowStoreColumnGenerator
// ===========================================================================

// Builds a full-width row-store block of `rows` rows (k = i+1, v = base+10*i),
// row-store column left as a placeholder default for the generator to overwrite.
static Block make_row_store_block(const TabletSchemaSPtr& schema, int num_rows, int32_t base) {
    Block block = schema->create_block();
    IColumn* k = block.get_by_position(0).column->assert_mutable().get();
    IColumn* v = block.get_by_position(1).column->assert_mutable().get();
    IColumn* ds = block.get_by_position(2).column->assert_mutable().get();
    IColumn* rs = block.get_by_position(3).column->assert_mutable().get();
    int8_t zero8 = 0;
    for (int i = 0; i < num_rows; ++i) {
        int32_t kk = i + 1;
        int32_t vv = base + 10 * i;
        k->insert_data(reinterpret_cast<const char*>(&kk), sizeof(int32_t));
        v->insert_data(reinterpret_cast<const char*>(&vv), sizeof(int32_t));
        ds->insert_data(reinterpret_cast<const char*>(&zero8), sizeof(int8_t));
        rs->insert_default(); // placeholder, replaced by the generator
    }
    return block;
}

// R1: an empty block returns OK and registers NO generator (early return before
// the schema scan).
TEST_F(VariantRowStoreTest, RowStoreFillRowsZeroNoGenerator) {
    auto schema = create_row_store_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = schema->create_block(); // 0 rows
    ASSERT_EQ(block.rows(), 0);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    EXPECT_EQ(ctx.derived_column.second, nullptr);
}

// R2: a non-empty row-store block registers a generator for the hidden row-store
// column (cid 3), with a non-null generator.
TEST_F(VariantRowStoreTest, RowStoreFillRegistersGenerator) {
    auto schema = create_row_store_schema(); // k(0) v(1) delete_sign(2) row_store(3)
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = make_row_store_block(schema, 2, /*base=*/10);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_NE(ctx.derived_column.second, nullptr);
    EXPECT_EQ(ctx.derived_column.first, 3U);
}

// R3 (+R2+R5): horizontal one-shot materialize fills every row with real, distinct,
// non-empty JSONB.
TEST_F(VariantRowStoreTest, RowStoreFillMaterializeHorizontal) {
    auto schema = create_row_store_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = make_row_store_block(schema, 2, /*base=*/10); // v = 10, 20
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_NE(ctx.derived_column.second, nullptr);
    ASSERT_EQ(ctx.derived_column.first, 3U);

    ASSERT_TRUE(materialize_derived_columns(ctx.derived_column, &block).ok());
    ASSERT_EQ(block.rows(), 2);
    const auto& rs_str = assert_cast<const ColumnString&>(*block.get_by_position(3).column);
    ASSERT_EQ(rs_str.size(), 2U);
    StringRef row0 = rs_str.get_data_at(0);
    StringRef row1 = rs_str.get_data_at(1);
    EXPECT_GT(row0.size, 0U);
    EXPECT_GT(row1.size, 0U);
    EXPECT_NE(row0.to_string(), row1.to_string()); // v differs (10 vs 20)
}

// R3 (content): decode the materialized JSONB of one row and check the exact
// uid->value mapping. The whole-row store encodes every non-row-store column keyed
// by unique_id (uid0=k, uid1=v, uid2=delete_sign) and never the row-store column.
TEST_F(VariantRowStoreTest, RowStoreFillMaterializeContent) {
    auto schema = create_row_store_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    // one row: k=42, v=-5, delete_sign=0
    Block block = schema->create_block();
    int32_t k = 42;
    int32_t v = -5;
    int8_t z = 0;
    block.get_by_position(0).column->assert_mutable()->insert_data(
            reinterpret_cast<const char*>(&k), sizeof(int32_t));
    block.get_by_position(1).column->assert_mutable()->insert_data(
            reinterpret_cast<const char*>(&v), sizeof(int32_t));
    block.get_by_position(2).column->assert_mutable()->insert_data(
            reinterpret_cast<const char*>(&z), sizeof(int8_t));
    block.get_by_position(3).column->assert_mutable()->insert_default();

    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_NE(ctx.derived_column.second, nullptr);
    ASSERT_TRUE(materialize_derived_columns(ctx.derived_column, &block).ok());

    const auto& rs_str = assert_cast<const ColumnString&>(*block.get_by_position(3).column);
    ASSERT_EQ(rs_str.size(), 1U);
    StringRef cell = rs_str.get_data_at(0);
    ASSERT_GT(cell.size, 0U);

    Block decoded = decode_row_store_cell(schema, cell); // positions: k, v, delete_sign
    ASSERT_EQ(decoded.rows(), 1);
    EXPECT_EQ(read_int(decoded, 0, 0), 42);    // uid 0 = k
    EXPECT_EQ(read_int(decoded, 1, 0), -5);    // uid 1 = v
    EXPECT_EQ(read_tinyint(decoded, 2, 0), 0); // uid 2 = delete_sign

    // the JSONB object holds exactly the three non-row-store uids {0,1,2} and
    // never the row-store column's own uid (3).
    const JsonbDocument* doc = nullptr;
    ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(cell.data, cell.size, &doc).ok());
    std::unordered_set<int> key_ids;
    // JsonbDocument's object iterator is not a standard range; the explicit
    // begin/end loop is intentional.
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (auto it = (*doc)->begin(); it != (*doc)->end(); ++it) {
        key_ids.insert(static_cast<int>(it->getKeyId()));
    }
    EXPECT_TRUE(key_ids.count(0)) << "missing uid 0 (k)";
    EXPECT_TRUE(key_ids.count(1)) << "missing uid 1 (v)";
    EXPECT_TRUE(key_ids.count(2)) << "missing uid 2 (delete_sign)";
    EXPECT_FALSE(key_ids.count(3)) << "row-store column uid 3 must not be encoded";
}

// R4 (by rows): drive the registered generator directly as the vertical writer
// does -- a fresh clone_empty() dst per batch, max_bytes huge, batch_rows = 2.
// Over 5 rows this yields 2,2,1 and walks pos 0->2->4->5, and the concatenation
// matches the one-shot materialize.
TEST_F(VariantRowStoreTest, RowStorePumpByRows) {
    auto schema = create_row_store_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = make_row_store_block(schema, 5, /*base=*/100);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_NE(ctx.derived_column.second, nullptr);
    const auto& gen = *ctx.derived_column.second;
    const uint32_t cid = ctx.derived_column.first;

    // oracle: one-shot materialize on a copy
    Block oracle = make_row_store_block(schema, 5, /*base=*/100);
    ASSERT_TRUE(materialize_derived_columns(ctx.derived_column, &oracle).ok());
    const auto& oracle_str = assert_cast<const ColumnString&>(*oracle.get_by_position(cid).column);

    const size_t num_rows = block.rows();
    const size_t batch_rows = 2;
    const size_t big_bytes = std::numeric_limits<size_t>::max();
    std::vector<size_t> batch_sizes;
    std::vector<std::string> produced;
    size_t pos = 0;
    while (pos < num_rows) {
        auto dst = block.get_by_position(cid).column->clone_empty();
        size_t max_rows = std::min(batch_rows, num_rows - pos);
        size_t rows = gen.generate(block, pos, max_rows, big_bytes, dst.get());
        ASSERT_GT(rows, 0U); // R5
        batch_sizes.push_back(rows);
        const auto& dst_str = assert_cast<const ColumnString&>(*dst);
        ASSERT_EQ(dst_str.size(), rows);
        for (size_t r = 0; r < rows; ++r) {
            produced.push_back(dst_str.get_data_at(r).to_string());
        }
        pos += rows;
    }
    EXPECT_EQ(pos, num_rows);
    EXPECT_EQ(batch_sizes, (std::vector<size_t> {2, 2, 1}));
    ASSERT_EQ(produced.size(), num_rows);
    for (size_t r = 0; r < num_rows; ++r) {
        EXPECT_EQ(produced[r], oracle_str.get_data_at(r).to_string()) << "row " << r;
    }
}

// R4 (by bytes): batch_rows unbounded, max_bytes set just above one real row's
// byte size (measured at runtime, never hardcoded). Each batch uses a fresh dst,
// so block_to_jsonb stops once that batch's accumulated byte_size >= max_bytes:
// it writes a row, then breaks -> one row per batch -> 5 batches summing to 5.
TEST_F(VariantRowStoreTest, RowStorePumpByBytes) {
    auto schema = create_row_store_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = make_row_store_block(schema, 5, /*base=*/100);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_NE(ctx.derived_column.second, nullptr);
    const auto& gen = *ctx.derived_column.second;
    const uint32_t cid = ctx.derived_column.first;

    // measure a real single-row size first
    size_t single_row_bytes = 0;
    {
        auto probe = block.get_by_position(cid).column->clone_empty();
        size_t rows = gen.generate(block, 0, 1, std::numeric_limits<size_t>::max(), probe.get());
        ASSERT_EQ(rows, 1U);
        single_row_bytes = assert_cast<const ColumnString&>(*probe).byte_size();
        ASSERT_GT(single_row_bytes, 0U);
    }
    // threshold at exactly one row -> block_to_jsonb writes one row, sees
    // byte_size() >= max_bytes, and breaks, so each batch yields one row.
    const size_t max_bytes = single_row_bytes;

    const size_t num_rows = block.rows();
    std::vector<size_t> batch_sizes;
    size_t pos = 0;
    size_t total = 0;
    while (pos < num_rows) {
        auto dst = block.get_by_position(cid).column->clone_empty();
        // max_rows must be finite (<= remaining): the generator passes it
        // straight to block_to_jsonb as num_rows; the byte cap forces the early
        // break within the batch. (The real vertical writer caps it at
        // num_rows_per_block.)
        size_t rows = gen.generate(block, pos, num_rows - pos, max_bytes, dst.get());
        ASSERT_GT(rows, 0U); // R5
        const auto& dst_str = assert_cast<const ColumnString&>(*dst);
        ASSERT_EQ(dst_str.size(), rows);
        batch_sizes.push_back(rows);
        total += rows;
        pos += rows;
    }
    EXPECT_EQ(total, num_rows);
    // byte threshold ~ one row -> at least 3 batches (here exactly 5, one per row)
    EXPECT_GE(batch_sizes.size(), 3U);
    for (size_t s : batch_sizes) {
        EXPECT_EQ(s, 1U);
    }
}

// R5: a single oversize row with max_bytes = 1 still produces exactly 1 row --
// block_to_jsonb writes the row before testing the byte budget, so it never
// returns 0.
TEST_F(VariantRowStoreTest, RowStorePumpSingleOversize) {
    auto schema = create_row_store_schema();
    RowsetWriterContext rwc = direct_rwc(schema);
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(schema, &rwc);

    Block block = make_row_store_block(schema, 1, /*base=*/100);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_NE(ctx.derived_column.second, nullptr);
    const auto& gen = *ctx.derived_column.second;
    const uint32_t cid = ctx.derived_column.first;

    // max_rows is the batch cap (= remaining rows; the generator forwards it to
    // block_to_jsonb as num_rows, so it must be <= block.rows()). max_bytes=1 is
    // smaller than the single row, but block_to_jsonb writes one row before it
    // checks the byte cap, so it still yields exactly one row (the >=1 guarantee).
    auto dst = block.get_by_position(cid).column->clone_empty();
    size_t rows = gen.generate(block, 0, /*max_rows=*/1, /*max_bytes=*/1, dst.get());
    EXPECT_EQ(rows, 1U);
    const auto& dst_str = assert_cast<const ColumnString&>(*dst);
    ASSERT_EQ(dst_str.size(), 1U);
    EXPECT_GT(dst_str.get_data_at(0).size, 0U);
}

// RS-pu: a fixed partial update on a row-store table. The chain runs the fill
// FIRST (widening {k} to full width and reading v/delete_sign from history), then
// RowStoreFill registers the generator on the full-width block. Materializing
// must reflect the post-fill final values (v from history).
TEST_F(VariantRowStoreTest, RowStoreFillAfterPartialUpdate) {
    auto schema = create_row_store_schema(); // k(0) v(1) delete_sign(2) row_store(3)

    // history: k=1 -> v=11, k=2 -> v=22
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 8101, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(8102);
    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.tablet_id = kTabletId;
    rwc.tablet = tablet;
    rwc.partial_update_info = pui;
    rwc.rowset_id = new_rsid;

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(),
              (std::vector<std::string_view> {"Validate", "FixedPartialUpdateFill", "VariantParse",
                                              "RowStoreFill"}));

    TransformExecContext ctx = exec_ctx(schema, &rwc);
    ctx.tablet = tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;
    ctx.rowset_id = new_rsid;

    Block block = schema->create_block_by_cids({0}); // narrow: key only
    IColumn* kc = block.get_by_position(0).column->assert_mutable().get();
    for (int32_t kk : {1, 2}) {
        kc->insert_data(reinterpret_cast<const char*>(&kk), sizeof(int32_t));
    }

    auto saved = config::enable_merge_on_write_correctness_check;
    config::enable_merge_on_write_correctness_check = false;
    auto st = chain.apply(ctx, &block);
    config::enable_merge_on_write_correctness_check = saved;
    ASSERT_TRUE(st.ok()) << st;

    // widened to full width; generator registered on the post-fill block
    ASSERT_EQ(block.columns(), schema->num_columns()); // 4
    ASSERT_EQ(block.rows(), 2);
    ASSERT_NE(ctx.derived_column.second, nullptr);
    ASSERT_EQ(ctx.derived_column.first, 3U);

    ASSERT_TRUE(materialize_derived_columns(ctx.derived_column, &block).ok());
    const auto& rs_str = assert_cast<const ColumnString&>(*block.get_by_position(3).column);
    ASSERT_EQ(rs_str.size(), 2U);

    // each JSONB cell reflects the filled value: v read from history (11, 22).
    Block d0 = decode_row_store_cell(schema, rs_str.get_data_at(0));
    Block d1 = decode_row_store_cell(schema, rs_str.get_data_at(1));
    EXPECT_EQ(read_int(d0, 0, 0), 1);  // k
    EXPECT_EQ(read_int(d0, 1, 0), 11); // v from history
    EXPECT_EQ(read_int(d1, 0, 0), 2);
    EXPECT_EQ(read_int(d1, 1, 0), 22);
}

} // namespace doris
