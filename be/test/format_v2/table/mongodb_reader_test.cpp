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

#include "format_v2/table/mongodb_reader.h"

#include <bson/bson.h>
#include <gtest/gtest.h>
#include <mongoc/mongoc.h>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type_serde/bson_value_view.h"
#include "format_v2/column_mapper.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"

namespace doris::format::mongodb {
namespace {
struct StreamState {
    int opens = 0;
    int closes = 0;
    int reads = 0;
    bool fail_open = false;
    bool fail_read = false;
    std::string kind;
    std::vector<std::string> documents {R"({"id":1,"value":10})", R"({"id":2,"value":20})",
                                        R"({"id":3,"value":30})"};
};
class FixtureStream final : public MongoCursorStream {
public:
    explicit FixtureStream(std::shared_ptr<StreamState> state) : _state(std::move(state)) {}
    ~FixtureStream() override {
        if (_document) {
            bson_destroy(_document);
        }
    }
    Status open(const TMongoScanDesc& d, std::function<bool()> cancelled) override {
        ++_state->opens;
        _state->kind = d.object_kind;
        _cancelled = std::move(cancelled);
        return _state->fail_open ? Status::IOError("fixture open failed") : Status::OK();
    }
    Status next(const bson_t** document, bool* eof) override {
        ++_state->reads;
        if (_cancelled()) {
            return Status::Cancelled("fixture cancelled");
        }
        if (_state->fail_read) {
            return Status::IOError("fixture getMore failed");
        }
        if (_document) {
            bson_destroy(_document);
            _document = nullptr;
        }
        *eof = _position == _state->documents.size();
        if (!*eof) {
            const auto& json = _state->documents[_position++];
            _document = bson_new_from_json(reinterpret_cast<const uint8_t*>(json.data()),
                                           json.size(), nullptr);
            if (!_document) {
                return Status::Corruption("invalid fixture");
            }
            *document = _document;
        }
        return Status::OK();
    }
    void close() override { ++_state->closes; }

private:
    std::shared_ptr<StreamState> _state;
    std::function<bool()> _cancelled;
    size_t _position = 0;
    bson_t* _document = nullptr;
};
TFileRangeDesc range() {
    TMongoScanDesc d;
    d.__set_version(1);
    d.__set_catalog_id(1);
    d.__set_configuration_version("fixture-config");
    d.__set_database("sample");
    d.__set_collection("records");
    d.__set_object_kind("collection");
    d.__set_schema_version("fixture-schema");
    d.__set_uri("mongodb://localhost:27017");
    d.__set_read_preference("primary");
    d.__set_read_concern("majority");
    d.__set_batch_size(2);
    d.__set_timeout_ms(30000);
    d.__set_connect_timeout_ms(1000);
    d.__set_server_selection_timeout_ms(1000);
    d.__set_socket_timeout_ms(1000);
    d.__set_pool_max_clients(2);
    d.__set_bindings_json(R"({"version":1,"columns":[
        {"name":"id","path":["id"],"bson_types":[16],"children":[]},
        {"name":"value","path":["value"],"bson_types":[16],"children":[]}]})");
    TTableFormatFileDesc table;
    table.__set_table_format_type("mongodb");
    table.__set_mongodb_params(d);
    TFileRangeDesc result;
    result.__set_format_type(TFileFormatType::FORMAT_MONGODB);
    result.__set_path("/dummyPath");
    result.__set_table_format_params(table);
    return result;
}
TMongoScanDesc snapshot_descriptor() {
    auto descriptor = range().table_format_params.mongodb_params;
    descriptor.__set_read_concern("snapshot");
    descriptor.__set_collection_uuid("00000000000000000000000000000001");
    TMongoSnapshot snapshot;
    snapshot.__set_timestamp_seconds(1700000000);
    snapshot.__set_timestamp_increment(7);
    snapshot.__set_source_kind("replica_set");
    snapshot.__set_source_id("000000000000000000000001");
    snapshot.__set_topology_version(1);
    snapshot.__set_max_wire_version(25);
    descriptor.__set_snapshot(snapshot);
    return descriptor;
}

void check_snapshot_source(const TMongoSnapshot& snapshot, const std::string& hello_json,
                           const std::string& identity_json, bool expected) {
    using Bson = std::unique_ptr<bson_t, decltype(&bson_destroy)>;
    Bson hello(bson_new_from_json(reinterpret_cast<const uint8_t*>(hello_json.data()),
                                  hello_json.size(), nullptr),
               bson_destroy);
    Bson identity(bson_new_from_json(reinterpret_cast<const uint8_t*>(identity_json.data()),
                                     identity_json.size(), nullptr),
                  bson_destroy);
    ASSERT_NE(hello, nullptr);
    ASSERT_NE(identity, nullptr);
    auto status = validate_mongodb_snapshot_source(snapshot, *hello, *identity);
    EXPECT_EQ(status.ok(), expected) << status.to_string();
}

class MongoReaderTest : public testing::Test {
protected:
    void SetUp() override {
        DescriptorTblBuilder builder(&_pool);
        builder.declare_tuple() << std::make_tuple(std::make_shared<DataTypeInt32>(),
                                                   std::string("id"))
                                << std::make_tuple(std::make_shared<DataTypeInt32>(),
                                                   std::string("value"));
        _slots = builder.build()->get_tuple_descriptor(0)->slots();
    }
    std::unique_ptr<MongoFileReader> reader() {
        auto props = std::make_shared<io::FileSystemProperties>();
        auto file = std::make_unique<io::FileDescription>();
        auto result = std::make_unique<MongoFileReader>(
                props, file, _io, &_profile, _range, _slots,
                [state = _responses] { return std::make_unique<FixtureStream>(state); });
        EXPECT_TRUE(result->init(&_state).ok());
        result->set_batch_size(2);
        return result;
    }
    std::shared_ptr<FileScanRequest> request(std::initializer_list<int> ids) {
        auto result = std::make_shared<FileScanRequest>();
        FileScanRequestBuilder builder(result.get());
        for (int id : ids) {
            EXPECT_TRUE(builder.add_non_predicate_column(LocalColumnId(id)).ok());
        }
        return result;
    }
    Block block(const MongoFileReader& reader, const FileScanRequest& request) {
        std::vector<ColumnDefinition> schema;
        EXPECT_TRUE(reader.get_schema(&schema).ok());
        ColumnsWithTypeAndName columns(request.block_column_count());
        for (auto [id, position] : request.local_positions) {
            const auto& column = schema[id.value()];
            columns[position.value()] = {column.type->create_column(), column.type, column.name};
        }
        return {columns};
    }
    void check_owned_containers(const Block& output, const FileScanRequest& projection) {
        auto value = [&](int id) {
            return output.get_by_position(projection.local_positions.at(LocalColumnId(id)).value());
        };
        const auto json = value(2);
        EXPECT_NE(json.type->to_string(*json.column, 0).find("9007199254740993"),
                  std::string::npos);
        const auto tags = value(3);
        EXPECT_EQ(tags.type->to_string(*tags.column, 0), "[\"alpha\", null]");
        const auto nested = value(4);
        EXPECT_NE(nested.type->to_string(*nested.column, 0).find("first"), std::string::npos);
    }
    ObjectPool _pool;
    RuntimeState _state;
    RuntimeProfile _profile {"mongo_reader_test"};
    std::shared_ptr<io::IOContext> _io = std::make_shared<io::IOContext>();
    TFileRangeDesc _range = range();
    std::vector<SlotDescriptor*> _slots;
    std::shared_ptr<StreamState> _responses = std::make_shared<StreamState>();
};
} // namespace

TEST_F(MongoReaderTest, BatchesAndProjectionOrderUseNativeBson) {
    auto scan = reader();
    auto projection = request({1, 0});
    ASSERT_TRUE(scan->open(projection).ok());
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_FALSE(eof);
    auto position = projection->local_positions.at(LocalColumnId(1)).value();
    const auto& value =
            assert_cast<const ColumnNullable&>(*output.get_by_position(position).column);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(value.get_nested_column()).get_data()[1], 20);
    output = block(*scan, *projection);
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_TRUE(eof);
    EXPECT_EQ(_responses->closes, 1);
    ASSERT_TRUE(scan->close().ok());
    EXPECT_EQ(_responses->closes, 1);
}
TEST_F(MongoReaderTest, EveryLogicalObjectKindKeepsItsNamespace) {
    for (const auto& kind : {"collection", "view", "timeseries"}) {
        _range.table_format_params.mongodb_params.__set_object_kind(kind);
        auto scan = reader();
        ASSERT_TRUE(scan->open(request({0})).ok());
        EXPECT_EQ(_responses->kind, kind);
        ASSERT_TRUE(scan->close().ok());
    }
    EXPECT_EQ(_responses->closes, 3);
}
TEST_F(MongoReaderTest, OpenAndGetMoreErrorsReleaseStreamExactlyOnce) {
    _responses->fail_open = true;
    {
        auto scan = reader();
        EXPECT_FALSE(scan->open(request({0})).ok());
    }
    EXPECT_EQ(_responses->closes, 1);
    _responses->fail_open = false;
    _responses->fail_read = true;
    {
        auto scan = reader();
        auto projection = request({0});
        ASSERT_TRUE(scan->open(projection).ok());
        auto output = block(*scan, *projection);
        size_t rows = 0;
        bool eof = false;
        EXPECT_FALSE(scan->get_block(&output, &rows, &eof).ok());
    }
    EXPECT_EQ(_responses->closes, 2);
}
TEST_F(MongoReaderTest, StoppingBeforeReadReleasesCursor) {
    auto scan = reader();
    auto projection = request({0});
    ASSERT_TRUE(scan->open(projection).ok());
    _io->should_stop = true;
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    EXPECT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(_responses->reads, 0);
    EXPECT_EQ(_responses->closes, 1);
}
TEST_F(MongoReaderTest, RowConversionErrorRollsBackPreviouslyWrittenColumns) {
    _responses->documents = {R"({"id":1,"value":"wrong"})"};
    auto scan = reader();
    auto projection = request({0, 1});
    ASSERT_TRUE(scan->open(projection).ok());
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    EXPECT_FALSE(scan->get_block(&output, &rows, &eof).ok());
    for (size_t i = 0; i < output.columns(); ++i) {
        EXPECT_EQ(output.get_by_position(i).column->size(), 0);
    }
    EXPECT_EQ(_responses->closes, 1);
}

TEST_F(MongoReaderTest, VariableWidthPayloadsSurviveCursorAdvanceAndClose) {
    auto text = std::make_shared<DataTypeString>();
    auto strings = std::make_shared<DataTypeArray>(make_nullable(text));
    auto structure =
            std::make_shared<DataTypeStruct>(DataTypes {make_nullable(text)}, Strings {"label"});
    const DataTypes types {text, std::make_shared<DataTypeVarbinary>(),
                           std::make_shared<DataTypeJsonb>(), strings, structure};
    const Strings names {"text", "bytes", "json", "tags", "nested"};
    _slots.clear();
    for (size_t i = 0; i < types.size(); ++i) {
        TSlotDescriptor descriptor;
        descriptor.__set_id(i);
        descriptor.__set_parent(0);
        descriptor.__set_slotType(types[i]->to_thrift());
        descriptor.__set_nullIndicatorBit(0);
        descriptor.__set_colName(names[i]);
        descriptor.__set_isMaterialized(true);
        _slots.push_back(_pool.add(new SlotDescriptor(descriptor)));
    }
    _range.table_format_params.mongodb_params.__set_bindings_json(R"({"version":1,"columns":[
        {"name":"text","path":["text"],"bson_types":[2],"children":[]},
        {"name":"bytes","path":["bytes"],"bson_types":[5],"subtype":0,"children":[]},
        {"name":"json","path":["json"],"bson_types":[3],"children":[]},
        {"name":"tags","path":["tags"],"bson_types":[4],"children":[
            {"name":"element","path":["element"],"bson_types":[2],"children":[]}]},
        {"name":"nested","path":["nested"],"bson_types":[3],"children":[
            {"name":"label","path":["label"],"bson_types":[2],"children":[]}]}]})");
    const std::string payload(8192, 'x');
    _responses->documents = {
            R"({"text":")" + payload + R"(","bytes":{"$binary":{"base64":"YQBi","subType":"00"}},
                "json":{"n":{"$numberLong":"9007199254740993"}},"tags":["alpha",null],
                "nested":{"label":"first"}})",
            R"({"text":"second","bytes":{"$binary":{"base64":"eHl6","subType":"00"}},
                "json":{"n":2},"tags":[],"nested":{"label":"second"}})"};
    auto scan = reader();
    scan->set_batch_size(1);
    auto projection = request({4, 2, 0, 3, 1});
    ASSERT_TRUE(scan->open(projection).ok());
    auto first = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    const auto status = scan->get_block(&first, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows, 1);
    auto second = block(*scan, *projection);
    ASSERT_TRUE(scan->get_block(&second, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    // Advance destroys the first BSON buffer; close destroys the last one before assertions.
    ASSERT_TRUE(scan->close().ok());
    auto value = [&](const Block& output, int id) {
        return output.get_by_position(projection->local_positions.at(LocalColumnId(id)).value());
    };
    EXPECT_EQ(value(first, 0).column->get_data_at(0).to_string(), payload);
    EXPECT_EQ(value(first, 1).column->get_data_at(0).to_string(), std::string("a\0b", 3));
    check_owned_containers(first, *projection);
    EXPECT_EQ(value(second, 0).column->get_data_at(0).to_string(), "second");
    EXPECT_EQ(value(second, 1).column->get_data_at(0).to_string(), "xyz");
}
TEST_F(MongoReaderTest, NullMismatchPolicyPreservesRowAndOtherFields) {
    _range.table_format_params.mongodb_params.__set_type_mismatch_as_null(true);
    _responses->documents = {R"({"id":1,"value":"wrong"})", R"({"id":2,"value":20})"};
    auto scan = reader();
    auto projection = request({0, 1});
    ASSERT_TRUE(scan->open(projection).ok());
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    auto pos = projection->local_positions.at(LocalColumnId(1)).value();
    const auto& column = assert_cast<const ColumnNullable&>(*output.get_by_position(pos).column);
    EXPECT_TRUE(column.is_null_at(0));
    EXPECT_FALSE(column.is_null_at(1));
}
TEST_F(MongoReaderTest, EmptyAndDestructorPathsReleaseCursor) {
    _responses->documents.clear();
    {
        auto scan = reader();
        auto projection = request({0});
        ASSERT_TRUE(scan->open(projection).ok());
        auto output = block(*scan, *projection);
        size_t rows = 0;
        bool eof = false;
        EXPECT_TRUE(scan->get_block(&output, &rows, &eof).ok());
        EXPECT_TRUE(eof);
        EXPECT_EQ(rows, 0);
    }
    EXPECT_EQ(_responses->closes, 1);
    {
        auto scan = reader();
        ASSERT_TRUE(scan->open(request({0})).ok());
    }
    EXPECT_EQ(_responses->closes, 2);
}
TEST_F(MongoReaderTest, InvalidDescriptorsAndInternalBucketsAreRejected) {
    EXPECT_FALSE(validate_mongodb_range(TFileRangeDesc()).ok());
    for (const auto& name : {"system.buckets.measurements", "system.views"}) {
        auto invalid = range();
        invalid.table_format_params.mongodb_params.__set_collection(name);
        EXPECT_FALSE(validate_mongodb_range(invalid).ok());
    }
    auto invalid = range();
    invalid.table_format_params.mongodb_params.__set_batch_size(0);
    EXPECT_FALSE(validate_mongodb_range(invalid).ok());
    invalid = range();
    invalid.table_format_params.mongodb_params.__set_read_concern("snapshot");
    EXPECT_FALSE(validate_mongodb_range(invalid).ok());
}

TEST_F(MongoReaderTest, BsonByteBudgetBoundsBlocksWithLargeUnprojectedFields) {
    const std::string payload(4 * 1024 * 1024, 'x');
    _responses->documents.clear();
    for (int i = 0; i < 3; ++i) {
        _responses->documents.push_back(R"({"id":1,"payload":")" + payload + "\"}");
    }
    auto scan = reader();
    scan->set_batch_size(100);
    auto projection = request({0});
    ASSERT_TRUE(scan->open(projection).ok());
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_FALSE(eof);
    output = block(*scan, *projection);
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_TRUE(eof);
}
TEST_F(MongoReaderTest, ReopeningAfterCloseDoesNotDuplicateProjectedColumns) {
    auto scan = reader();
    auto projection = request({0});
    ASSERT_TRUE(scan->open(projection).ok());
    EXPECT_FALSE(scan->open(projection).ok());
    ASSERT_TRUE(scan->close().ok());
    ASSERT_TRUE(scan->open(projection).ok());
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_EQ(output.rows(), 2);
    ASSERT_TRUE(scan->close().ok());
    EXPECT_EQ(_responses->closes, 2);
}

TEST(MongoNativeStreamTest, ReadPreferencePreservesUriTagsAndMaxStaleness) {
    mongoc_init();
    std::unique_ptr<mongoc_uri_t, decltype(&mongoc_uri_destroy)> uri(
            mongoc_uri_new("mongodb://localhost:27017/?readPreference=secondary&"
                           "readPreferenceTags=region:analytics&readPreferenceTags=&"
                           "maxStalenessSeconds=90"),
            mongoc_uri_destroy);
    ASSERT_NE(uri, nullptr);
    std::unique_ptr<mongoc_client_t, decltype(&mongoc_client_destroy)> client(
            mongoc_client_new_from_uri(uri.get()), mongoc_client_destroy);
    std::unique_ptr<mongoc_collection_t, decltype(&mongoc_collection_destroy)> collection(
            mongoc_client_get_collection(client.get(), "sample", "records"),
            mongoc_collection_destroy);
    ASSERT_TRUE(configure_mongodb_read_preference(collection.get(), "secondary").ok());
    const auto* prefs = mongoc_collection_get_read_prefs(collection.get());
    EXPECT_EQ(mongoc_read_prefs_get_mode(prefs), MONGOC_READ_SECONDARY);
    EXPECT_EQ(mongoc_read_prefs_get_max_staleness_seconds(prefs), 90);
    EXPECT_TRUE(bson_equal(mongoc_read_prefs_get_tags(prefs),
                           mongoc_read_prefs_get_tags(mongoc_uri_get_read_prefs_t(uri.get()))));
    EXPECT_FALSE(configure_mongodb_read_preference(collection.get(), "invalid").ok());
    EXPECT_FALSE(configure_mongodb_read_preference(collection.get(), "primary").ok());
    EXPECT_EQ(mongoc_read_prefs_get_mode(mongoc_collection_get_read_prefs(collection.get())),
              MONGOC_READ_SECONDARY);
}

TEST(MongoNativeStreamTest, CatalogModeAppliesWhenUriHasNoRoutingCriteria) {
    mongoc_init();
    std::unique_ptr<mongoc_client_t, decltype(&mongoc_client_destroy)> client(
            mongoc_client_new("mongodb://localhost:27017"), mongoc_client_destroy);
    std::unique_ptr<mongoc_collection_t, decltype(&mongoc_collection_destroy)> collection(
            mongoc_client_get_collection(client.get(), "sample", "records"),
            mongoc_collection_destroy);
    for (const auto& [name, mode] : std::vector<std::pair<std::string, mongoc_read_mode_t>> {
                 {"primary", MONGOC_READ_PRIMARY},
                 {"primaryPreferred", MONGOC_READ_PRIMARY_PREFERRED},
                 {"secondary", MONGOC_READ_SECONDARY},
                 {"secondaryPreferred", MONGOC_READ_SECONDARY_PREFERRED},
                 {"nearest", MONGOC_READ_NEAREST}}) {
        ASSERT_TRUE(configure_mongodb_read_preference(collection.get(), name).ok());
        EXPECT_EQ(mongoc_read_prefs_get_mode(mongoc_collection_get_read_prefs(collection.get())),
                  mode);
    }
}

TEST(MongoNativeStreamTest, SnapshotDescriptorRequiresACompleteSharedPin) {
    auto scan = range();
    auto& descriptor = scan.table_format_params.mongodb_params;
    descriptor.__set_read_concern("snapshot");
    TMongoSnapshot snapshot;
    snapshot.__set_timestamp_seconds(1700000000);
    snapshot.__set_timestamp_increment(7);
    snapshot.__set_source_kind("replica_set");
    snapshot.__set_source_id("000000000000000000000001");
    snapshot.__set_topology_version(1);
    snapshot.__set_max_wire_version(25);
    descriptor.__set_snapshot(snapshot);
    descriptor.__set_collection_uuid("00000000000000000000000000000001");
    EXPECT_TRUE(validate_mongodb_range(scan).ok());
    descriptor.snapshot.__set_timestamp_seconds(0);
    EXPECT_FALSE(validate_mongodb_range(scan).ok());
    descriptor.snapshot.__set_timestamp_seconds(1700000000);
    descriptor.__set_object_kind("view");
    EXPECT_FALSE(validate_mongodb_range(scan).ok());
}

TEST(MongoNativeStreamTest, SnapshotFindOptionsRetainTheExactBsonTimestamp) {
    auto descriptor = range().table_format_params.mongodb_params;
    descriptor.__set_read_concern("snapshot");
    TMongoSnapshot snapshot;
    snapshot.__set_timestamp_seconds(4294967295LL);
    snapshot.__set_timestamp_increment(4294967295LL);
    snapshot.__set_source_kind("replica_set");
    snapshot.__set_source_id("000000000000000000000001");
    snapshot.__set_topology_version(1);
    snapshot.__set_max_wire_version(25);
    descriptor.__set_snapshot(snapshot);
    descriptor.__set_collection_uuid("00000000000000000000000000000001");
    bson_t filter = BSON_INITIALIZER;
    bson_t options = BSON_INITIALIZER;
    ASSERT_TRUE(build_mongodb_find_options(descriptor, &filter, &options).ok());
    auto root = BsonValueView::document(options);
    const auto timestamp = root.at_path({"readConcern", "atClusterTime"});
    bson_destroy(&options);
    bson_destroy(&filter);
    ASSERT_FALSE(timestamp.missing);
    ASSERT_EQ(timestamp.value.value_type, BSON_TYPE_TIMESTAMP);
    EXPECT_EQ(timestamp.value.value.v_timestamp.timestamp, UINT32_MAX);
    EXPECT_EQ(timestamp.value.value.v_timestamp.increment, UINT32_MAX);
}

TEST(MongoNativeStreamTest, SnapshotRejectsIncompleteOrInconsistentContracts) {
    const std::vector<std::function<void(TMongoScanDesc&)>> invalid = {
            [](auto& d) { d.__set_read_concern("majority"); },
            [](auto& d) { d.__isset.snapshot = false; },
            [](auto& d) { d.snapshot.__isset.timestamp_seconds = false; },
            [](auto& d) { d.snapshot.__isset.timestamp_increment = false; },
            [](auto& d) { d.snapshot.__isset.source_kind = false; },
            [](auto& d) { d.snapshot.__isset.source_id = false; },
            [](auto& d) { d.snapshot.__isset.topology_version = false; },
            [](auto& d) { d.snapshot.__isset.max_wire_version = false; },
            [](auto& d) { d.snapshot.__set_timestamp_seconds(-1); },
            [](auto& d) { d.snapshot.__set_timestamp_seconds(4294967296LL); },
            [](auto& d) { d.snapshot.__set_timestamp_increment(-1); },
            [](auto& d) { d.snapshot.__set_timestamp_increment(4294967296LL); },
            [](auto& d) { d.snapshot.__set_source_kind("standalone"); },
            [](auto& d) { d.snapshot.__set_source_id("missing"); },
            [](auto& d) { d.snapshot.__set_topology_version(0); },
            [](auto& d) { d.snapshot.__set_max_wire_version(26); },
            [](auto& d) { d.__set_collection_uuid(""); },
            [](auto& d) { d.__set_object_kind("view"); },
            [](auto& d) { d.snapshot.__set_timeseries_options_json("{}"); }};
    for (size_t i = 0; i < invalid.size(); ++i) {
        SCOPED_TRACE(i);
        auto scan = range();
        scan.table_format_params.__set_mongodb_params(snapshot_descriptor());
        invalid[i](scan.table_format_params.mongodb_params);
        EXPECT_FALSE(validate_mongodb_range(scan).ok());
    }
}

TEST(MongoNativeStreamTest, SnapshotTimeSeriesRequiresPublicDefinitionMetadata) {
    auto scan = range();
    scan.table_format_params.__set_mongodb_params(snapshot_descriptor());
    auto& descriptor = scan.table_format_params.mongodb_params;
    descriptor.__set_object_kind("timeseries");
    descriptor.__set_collection_uuid("");
    EXPECT_FALSE(validate_mongodb_range(scan).ok());
    descriptor.snapshot.__set_timeseries_options_json(R"({"timeseries":{"timeField":"observed"}})");
    EXPECT_TRUE(validate_mongodb_range(scan).ok());
    for (const auto& definition : {std::string("invalid"), std::string("{}"),
                                   std::string(R"({"timeseries":1})"), std::string(16385, 'x')}) {
        descriptor.snapshot.__set_timeseries_options_json(definition);
        EXPECT_FALSE(validate_mongodb_range(scan).ok());
    }
}

TEST(MongoNativeStreamTest, SnapshotReplicaIdentityAndTopologyMustStillMatch) {
    const auto snapshot = snapshot_descriptor().snapshot;
    const std::string hello = R"({"setName":"fixture_rs","maxWireVersion":25})";
    const std::string identity = R"({"config":{"version":1,"settings":{
        "replicaSetId":{"$oid":"000000000000000000000001"}}}})";
    check_snapshot_source(snapshot, hello, identity, true);
    check_snapshot_source(snapshot, R"({"setName":"fixture_rs","maxWireVersion":21})", identity,
                          false);
    check_snapshot_source(snapshot, R"({"maxWireVersion":25})", identity, false);
    check_snapshot_source(snapshot, R"({"msg":"isdbgrid","maxWireVersion":25})", identity, false);
    check_snapshot_source(snapshot, hello, R"({"config":{"version":2,"settings":{
        "replicaSetId":{"$oid":"000000000000000000000001"}}}})",
                          false);
    check_snapshot_source(snapshot, hello, R"({"config":{"version":1,"settings":{
        "replicaSetId":{"$oid":"000000000000000000000002"}}}})",
                          false);
    check_snapshot_source(snapshot, hello, R"({"config":{"version":1,"settings":{}}})", false);
}

TEST(MongoNativeStreamTest, SnapshotMongosIdentitySurvivesRouterChangesOnlyWithinOneCluster) {
    auto snapshot = snapshot_descriptor().snapshot;
    snapshot.__set_source_kind("mongos");
    snapshot.__set_topology_version(0);
    const std::string hello = R"({"msg":"isdbgrid","maxWireVersion":25})";
    check_snapshot_source(snapshot, hello, R"({"clusterId":{"$oid":"000000000000000000000001"}})",
                          true);
    check_snapshot_source(snapshot, hello, R"({"clusterId":{"$oid":"000000000000000000000002"}})",
                          false);
    check_snapshot_source(snapshot, hello, R"({"clusterId":"not-an-object-id"})", false);
}

TEST(MongoNativeStreamTest, SnapshotInvalidPinFailsBeforeConnecting) {
    auto descriptor = snapshot_descriptor();
    descriptor.__set_uri("mongodb://127.0.0.1:1");
    descriptor.snapshot.__set_timestamp_seconds(0);
    auto stream = make_native_mongo_stream();
    EXPECT_EQ(stream->open(descriptor, [] { return false; }).code(), ErrorCode::INVALID_ARGUMENT);
    stream->close();
}

TEST(MongoNativeStreamTest, CancellationBeforeOpenDoesNotConnect) {
    auto stream = make_native_mongo_stream();
    auto descriptor = range().table_format_params.mongodb_params;
    EXPECT_EQ(stream->open(descriptor, [] { return true; }).code(), ErrorCode::CANCELLED);
    stream->close();
    stream->close();
}
TEST(MongoNativeStreamTest, InvalidUriIsRedactedAndPartialOpenCanBeClosed) {
    auto stream = make_native_mongo_stream();
    auto descriptor = range().table_format_params.mongodb_params;
    descriptor.__set_uri("fixture-secret-invalid-uri");
    auto status = stream->open(descriptor, [] { return false; });
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.to_string().find("fixture-secret"), std::string::npos);
    stream->close();
}
TEST(MongoNativeStreamTest, FailedConnectionReturnsErrorAndReleasesLeases) {
    for (int attempt = 0; attempt < 3; ++attempt) {
        auto stream = make_native_mongo_stream();
        auto descriptor = range().table_format_params.mongodb_params;
        descriptor.__set_uri("mongodb://fixture-user:fixture-secret@127.0.0.1:1");
        descriptor.__set_timeout_ms(100);
        descriptor.__set_server_selection_timeout_ms(25);
        descriptor.__set_connect_timeout_ms(25);
        descriptor.__set_socket_timeout_ms(25);
        auto status = stream->open(descriptor, [] { return false; });
        if (status.ok()) {
            const bson_t* document = nullptr;
            bool eof = false;
            status = stream->next(&document, &eof);
        }
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.to_string().find("fixture-secret"), std::string::npos);
        EXPECT_EQ(status.to_string().find("mongodb://"), std::string::npos);
        stream->close();
    }
}

TEST(MongoNativeStreamTest, MalformedPushdownBsonIsRejectedBeforeConnecting) {
    for (bool projection : {false, true}) {
        auto scan = range();
        auto& d = scan.table_format_params.mongodb_params;
        d.__set_type_mismatch_as_null(true);
        if (projection) {
            d.__set_projection_bson("invalid BSON");
        } else {
            d.__set_filter_bson("invalid BSON");
        }
        EXPECT_FALSE(validate_mongodb_range(scan).ok());
    }
}

namespace {
std::string bson_bytes(const bson_t& value) {
    return {reinterpret_cast<const char*>(bson_get_data(&value)), value.len};
}
} // namespace

TEST(MongoNativeStreamTest, TypedFindOptionsKeepDateIntegerAndProjectionBytes) {
    auto d = range().table_format_params.mongodb_params;
    d.__set_type_mismatch_as_null(true);
    bson_t source = BSON_INITIALIZER;
    bson_t bounds;
    BSON_APPEND_DOCUMENT_BEGIN(&source, "observed", &bounds);
    BSON_APPEND_DATE_TIME(&bounds, "$gte", -1);
    bson_append_document_end(&source, &bounds);
    BSON_APPEND_INT64(&source, "n", 9007199254740993LL);
    d.__set_filter_bson(bson_bytes(source));
    bson_destroy(&source);
    bson_t projection = BSON_INITIALIZER;
    BSON_APPEND_INT32(&projection, "observed", 1);
    BSON_APPEND_INT32(&projection, "_id", 0);
    d.__set_projection_bson(bson_bytes(projection));
    bson_destroy(&projection);
    bson_t filter = BSON_INITIALIZER;
    bson_t options = BSON_INITIALIZER;
    ASSERT_TRUE(build_mongodb_find_options(d, &filter, &options).ok());
    EXPECT_EQ(bson_bytes(filter), d.filter_bson);
    bson_iter_t it;
    ASSERT_TRUE(bson_iter_init_find(&it, &filter, "n"));
    ASSERT_EQ(bson_iter_type(&it), BSON_TYPE_INT64);
    EXPECT_EQ(bson_iter_int64(&it), 9007199254740993LL);
    ASSERT_TRUE(bson_iter_init_find(&it, &options, "projection"));
    uint32_t length;
    const uint8_t* bytes;
    bson_iter_document(&it, &length, &bytes);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(bytes), length), d.projection_bson);
    ASSERT_TRUE(bson_iter_init_find(&it, &options, "collation"));
    bson_t collation;
    bson_iter_document(&it, &length, &bytes);
    ASSERT_TRUE(bson_init_static(&collation, bytes, length));
    ASSERT_TRUE(bson_iter_init_find(&it, &collation, "locale"));
    EXPECT_STREQ(bson_iter_utf8(&it, nullptr), "simple");
    bson_destroy(&filter);
    bson_destroy(&options);
}

TEST(MongoNativeStreamTest, ViewProjectionNeverOverridesItsCollation) {
    auto d = range().table_format_params.mongodb_params;
    d.__set_object_kind("view");
    bson_t projection = BSON_INITIALIZER;
    BSON_APPEND_INT32(&projection, "value", 1);
    d.__set_projection_bson(bson_bytes(projection));
    bson_destroy(&projection);
    bson_t filter = BSON_INITIALIZER;
    bson_t options = BSON_INITIALIZER;
    ASSERT_TRUE(build_mongodb_find_options(d, &filter, &options).ok());
    bson_iter_t it;
    EXPECT_FALSE(bson_iter_init_find(&it, &options, "collation"));
    EXPECT_TRUE(bson_iter_init_find(&it, &options, "projection"));
    bson_destroy(&filter);
    bson_destroy(&options);
}

TEST(MongoNativeStreamTest, OldDescriptorsKeepEmptyFindOptions) {
    auto d = range().table_format_params.mongodb_params;
    bson_t filter = BSON_INITIALIZER;
    bson_t options = BSON_INITIALIZER;
    ASSERT_TRUE(build_mongodb_find_options(d, &filter, &options).ok());
    EXPECT_TRUE(bson_empty(&filter));
    EXPECT_TRUE(bson_empty(&options));
    bson_destroy(&filter);
    bson_destroy(&options);
}

TEST(MongoNativeStreamTest, UnsafeViewStrictAndOversizedDescriptorsAreRejected) {
    auto scan = range();
    auto& d = scan.table_format_params.mongodb_params;
    bson_t candidate = BSON_INITIALIZER;
    BSON_APPEND_INT32(&candidate, "value", 1);
    d.__set_filter_bson(bson_bytes(candidate));
    bson_destroy(&candidate);
    EXPECT_FALSE(validate_mongodb_range(scan).ok());
    d.__set_type_mismatch_as_null(true);
    EXPECT_TRUE(validate_mongodb_range(scan).ok());
    d.__set_object_kind("view");
    EXPECT_FALSE(validate_mongodb_range(scan).ok());
    d.__set_object_kind("collection");
    d.__set_filter_bson(std::string(1024 * 1024 + 1, 'x'));
    EXPECT_FALSE(validate_mongodb_range(scan).ok());
}

} // namespace doris::format::mongodb
