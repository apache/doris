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

#include "vec/aggregate_functions/aggregate_function_llm_agg.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "http/http_client.h"
#include "runtime/query_context.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_string.h"
#include "vec/common/arena.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

// declare function
void register_aggregate_function_llm_agg(AggregateFunctionSimpleFactory& factory);

class MockHttpClient : public HttpClient {
public:
    curl_slist* get() { return this->_header_list; }

private:
    std::unordered_map<std::string, std::string> _headers;
    std::string _content_type;
};

class AggregateFunctionLLMAggTest : public ::testing::Test {
public:
    void SetUp() override {
        _runtime_state = std::make_unique<MockRuntimeState>();
        _query_ctx = _runtime_state->_query_ctx_uptr;

        AggregateFunctionSimpleFactory factory;
        register_aggregate_function_llm_agg(factory);
        _factory = &factory;

        _data_types = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                       std::make_shared<DataTypeString>()};

        _agg_function = _factory->get("llm_agg", _data_types, false, -1);
        ASSERT_TRUE(_agg_function != nullptr);

        _agg_function->set_query_context(_query_ctx.get());
    }

    void TearDown() override { AggregateFunctionLLMAggData::_ctx = nullptr; }

protected:
    std::unique_ptr<MockRuntimeState> _runtime_state;
    std::shared_ptr<QueryContext> _query_ctx;
    AggregateFunctionSimpleFactory* _factory;
    DataTypes _data_types;
    AggregateFunctionPtr _agg_function;
    Arena _arena;
};

TEST_F(AggregateFunctionLLMAggTest, add_test) {
    auto resource_col = ColumnString::create();
    auto text_col = ColumnString::create();
    auto task_col = ColumnString::create();

    resource_col->insert_data("mock_resource", 13);
    text_col->insert_data("Hello world", 11);
    task_col->insert_data("summarize this text", 19);

    std::unique_ptr<char[]> memory(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    _agg_function->create(place);

    const IColumn* columns[3] = {resource_col.get(), text_col.get(), task_col.get()};
    _agg_function->add(place, columns, 0, _arena);

    const auto& data = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place);
    EXPECT_TRUE(data.inited);
    EXPECT_EQ(data._task, "summarize this text");
    EXPECT_EQ(data.data.size(), 11); // "Hello world"

    _agg_function->destroy(place);
}

TEST_F(AggregateFunctionLLMAggTest, multiple_add_test) {
    auto resource_col = ColumnString::create();
    auto text_col = ColumnString::create();
    auto task_col = ColumnString::create();

    std::vector<std::string> texts = {"First text", "Second text", "Third text"};

    for (const auto& text : texts) {
        resource_col->insert_data("mock_resource", 13);
        text_col->insert_data(text.c_str(), text.size());
        task_col->insert_data("summarize", 9);
    }

    std::unique_ptr<char[]> memory(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    _agg_function->create(place);
    const IColumn* columns[3] = {resource_col.get(), text_col.get(), task_col.get()};
    for (size_t i = 0; i < texts.size(); ++i) {
        _agg_function->add(place, columns, i, _arena);
    }

    const auto& data = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place);
    EXPECT_TRUE(data.inited);
    EXPECT_EQ(data._task, "summarize");
    std::string expected = "First text\nSecond text\nThird text";
    std::string actual(reinterpret_cast<const char*>(data.data.data()), data.data.size());
    EXPECT_EQ(actual, expected);

    _agg_function->destroy(place);
}

TEST_F(AggregateFunctionLLMAggTest, merge_test) {
    std::unique_ptr<char[]> memory1(new char[_agg_function->size_of_data()]);
    std::unique_ptr<char[]> memory2(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    AggregateDataPtr place2 = memory2.get();

    _agg_function->create(place1);
    _agg_function->create(place2);

    auto resource_col1 = ColumnString::create();
    auto text_col1 = ColumnString::create();
    auto task_col1 = ColumnString::create();

    resource_col1->insert_data("mock_resource", 13);
    text_col1->insert_data("First part", 10);
    task_col1->insert_data("analyze", 7);

    const IColumn* columns1[3] = {resource_col1.get(), text_col1.get(), task_col1.get()};
    _agg_function->add(place1, columns1, 0, _arena);

    auto resource_col2 = ColumnString::create();
    auto text_col2 = ColumnString::create();
    auto task_col2 = ColumnString::create();

    resource_col2->insert_data("mock_resource", 13);
    text_col2->insert_data("Second part", 11);
    task_col2->insert_data("analyze", 7);

    const IColumn* columns2[3] = {resource_col2.get(), text_col2.get(), task_col2.get()};
    _agg_function->add(place2, columns2, 0, _arena);

    _agg_function->merge(place1, place2, _arena);

    const auto& data = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place1);
    std::string actual(reinterpret_cast<const char*>(data.data.data()), data.data.size());
    std::string expected = "First part\nSecond part";
    EXPECT_EQ(actual, expected);

    _agg_function->destroy(place1);
    _agg_function->destroy(place2);
}

TEST_F(AggregateFunctionLLMAggTest, serialize_deserialize_test) {
    std::unique_ptr<char[]> memory1(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    _agg_function->create(place1);

    auto resource_col = ColumnString::create();
    auto text_col = ColumnString::create();
    auto task_col = ColumnString::create();

    resource_col->insert_data("mock_resource", 13);
    text_col->insert_data("Test data for serialization", 28);
    task_col->insert_data("process", 7);

    const IColumn* columns[3] = {resource_col.get(), text_col.get(), task_col.get()};
    _agg_function->add(place1, columns, 0, _arena);

    auto serialize_column = _agg_function->create_serialize_column();
    _agg_function->serialize_without_key_to_column(place1, *serialize_column);

    std::unique_ptr<char[]> memory2(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    _agg_function->create(place2);

    _agg_function->deserialize_and_merge_from_column(place2, *serialize_column, _arena);

    const auto& data1 = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place1);
    const auto& data2 = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place2);

    EXPECT_EQ(data1.inited, data2.inited);
    std::string str1(reinterpret_cast<const char*>(data1.data.data()), data1.data.size());
    std::string str2(reinterpret_cast<const char*>(data2.data.data()), data2.data.size());
    EXPECT_EQ(str1, str2);

    _agg_function->destroy(place1);
    _agg_function->destroy(place2);
}

TEST_F(AggregateFunctionLLMAggTest, reset_test) {
    std::unique_ptr<char[]> memory(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    _agg_function->create(place);

    auto resource_col = ColumnString::create();
    auto text_col = ColumnString::create();
    auto task_col = ColumnString::create();

    resource_col->insert_data("mock_resource", 13);
    text_col->insert_data("Some text", 9);
    task_col->insert_data("task", 4);

    const IColumn* columns[3] = {resource_col.get(), text_col.get(), task_col.get()};
    _agg_function->add(place, columns, 0, _arena);

    const auto& data_before = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place);
    EXPECT_TRUE(data_before.inited);
    EXPECT_FALSE(data_before.data.empty());

    _agg_function->reset(place);

    const auto& data_after = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place);
    EXPECT_FALSE(data_after.inited);
    EXPECT_TRUE(data_after.data.empty());
    EXPECT_TRUE(data_after._task.empty());

    _agg_function->destroy(place);
}

TEST_F(AggregateFunctionLLMAggTest, empty_data_test) {
    std::unique_ptr<char[]> memory(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    _agg_function->create(place);

    auto resource_col = ColumnString::create();
    auto text_col = ColumnString::create();
    auto task_col = ColumnString::create();

    resource_col->insert_data("mock_resource", 13);
    text_col->insert_data("", 0);
    task_col->insert_data("process", 7);

    const IColumn* columns[3] = {resource_col.get(), text_col.get(), task_col.get()};
    _agg_function->add(place, columns, 0, _arena);

    const auto& data = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place);
    EXPECT_TRUE(data.inited);
    EXPECT_EQ(data.data.size(), 0);

    _agg_function->destroy(place);
}

TEST_F(AggregateFunctionLLMAggTest, merge_empty_test) {
    std::unique_ptr<char[]> memory1(new char[_agg_function->size_of_data()]);
    std::unique_ptr<char[]> memory2(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    AggregateDataPtr place2 = memory2.get();

    _agg_function->create(place1);
    _agg_function->create(place2);

    auto resource_col = ColumnString::create();
    auto text_col = ColumnString::create();
    auto task_col = ColumnString::create();

    resource_col->insert_data("mock_resource", 13);
    text_col->insert_data("Test data", 9);
    task_col->insert_data("analyze", 7);

    const IColumn* columns[3] = {resource_col.get(), text_col.get(), task_col.get()};
    _agg_function->add(place1, columns, 0, _arena);

    _agg_function->merge(place1, place2, _arena);

    const auto& data = *reinterpret_cast<const AggregateFunctionLLMAggData*>(place1);
    std::string actual(reinterpret_cast<const char*>(data.data.data()), data.data.size());
    EXPECT_EQ(actual, "Test data");

    _agg_function->destroy(place1);
    _agg_function->destroy(place2);
}

TEST_F(AggregateFunctionLLMAggTest, return_type_test) {
    auto return_type = _agg_function->get_return_type();
    EXPECT_TRUE(return_type != nullptr);
    EXPECT_EQ(return_type->get_name(), "String");
}

TEST_F(AggregateFunctionLLMAggTest, mock_resource_send_request_test) {
    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"test input"};
    std::vector<std::string> task = {"summarize"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);
    auto col_task = ColumnHelper::create_column<DataTypeString>(task);

    std::unique_ptr<char[]> memory(new char[_agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    _agg_function->create(place);

    const IColumn* columns[3] = {col_resource.get(), col_text.get(), col_task.get()};
    _agg_function->add(place, columns, 0, _arena);

    ColumnString result_column;
    _agg_function->insert_result_into(place, result_column);

    StringRef result_ref = result_column.get_data_at(0);
    std::string result(result_ref.data, result_ref.size);

    _agg_function->destroy(place);
}

} // namespace doris::vectorized
