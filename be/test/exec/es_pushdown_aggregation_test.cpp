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
#include "exec/es_http_scan_node.h"

#include <gtest/gtest.h>
#include <vector>

#include "common/object_pool.h"
#include "common/logging.h"
#include "gen_cpp/PlanNodes_types.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "runtime/primitive_type.h"

namespace doris {

// it's a query with size = 2, result has four rows, so will send request three times.
// select IFNULL(color,"zero"),color,count(1),max(price),avg(price),count(price),max(sold),min(sold),count(sold) from doe group by color;
static const std::string es_req_with_group_by_first = "{\"query\":{\"match_all\":{}},"
                                                          "\"stored_fields\":\"_none_\","
                                                          "\"docvalue_fields\":[\"color.keyword\",\"price\",\"sold\"],"
                                                          "\"track_total_hits\":true,"
                                                          "\"sort\":[\"_doc\"],\"size\":0,"
                                                          "\"aggs\":{\"groupby\":{\"composite\":{"
                                                          "\"sources\":["
                                                          "{\"group_hold1\":{\"terms\":{\"field\":\"color.keyword\",\"missing_bucket\":true}}}],\"size\":2},"
                                                          "\"aggs\":{\"place_hold1\":{\"max\":{\"field\":\"price\"}},"
                                                          "\"place_hold2\":{\"sum\":{\"field\":\"price\"}},"
                                                          "\"place_hold3\":{\"value_count\":{\"field\":\"price\"}},"
                                                          "\"place_hold4\":{\"value_count\":{\"field\":\"price\"}},"
                                                          "\"place_hold5\":{\"max\":{\"field\":\"sold\"}},"
                                                          "\"place_hold6\":{\"min\":{\"field\":\"sold\"}},"
                                                          "\"place_hold7\":{\"value_count\":{\"field\":\"sold\"}}}}}}";
static const std::string es_resp_with_group_by_first = "{\"hits\":{\"total\":{\"value\":10,\"relation\":\"eq\"}},"
                                                           "\"aggregations\":{\"groupby\":{\"after_key\":{\"group_hold1\":\"blue\"},"
                                                           "\"buckets\":["
                                                           "{\"key\":{\"group_hold1\":null},\"doc_count\":2,"
                                                           "\"place_hold2\":{\"value\":10000.0},"
                                                           "\"place_hold1\":{\"value\":10000.0},"
                                                           "\"place_hold6\":{\"value\":1.4144544E12,\"value_as_string\":\"2014-10-28T00:00:00.000Z\"},"
                                                           "\"place_hold5\":{\"value\":1.4144544E12,\"value_as_string\":\"2014-10-28T00:00:00.000Z\"},"
                                                           "\"place_hold4\":{\"value\":1},"
                                                           "\"place_hold3\":{\"value\":1},"
                                                           "\"place_hold7\":{\"value\":1}},"
                                                           "{\"key\":{\"group_hold1\":\"blue\"},\"doc_count\":2,"
                                                           "\"place_hold2\":{\"value\":40000.0},"
                                                           "\"place_hold1\":{\"value\":25000.0},"
                                                           "\"place_hold6\":{\"value\":1.3921632E12,\"value_as_string\":\"2014-02-12T00:00:00.000Z\"},"
                                                           "\"place_hold5\":{\"value\":1.4042592E12,\"value_as_string\":\"2014-07-02T00:00:00.000Z\"},"
                                                           "\"place_hold4\":{\"value\":2},"
                                                           "\"place_hold3\":{\"value\":2},"
                                                           "\"place_hold7\":{\"value\":2}}]}}}";
static const std::string es_req_with_group_by_second = "{\"query\":{\"match_all\":{}},"
                                                           "\"stored_fields\":\"_none_\","
                                                           "\"docvalue_fields\":[\"color.keyword\",\"price\",\"sold\"],"
                                                           "\"track_total_hits\":true,"
                                                           "\"sort\":[\"_doc\"],\"size\":0,"
                                                           "\"aggs\":{\"groupby\":{\"composite\":{"
                                                           "\"sources\":["
                                                           "{\"group_hold1\":{\"terms\":{\"field\":\"color.keyword\",\"missing_bucket\":true}}}],\"size\":2,\"after\":{\"group_hold1\":\"blue\"}},"
                                                           "\"aggs\":{\"place_hold1\":{\"max\":{\"field\":\"price\"}},"
                                                           "\"place_hold2\":{\"sum\":{\"field\":\"price\"}},"
                                                           "\"place_hold3\":{\"value_count\":{\"field\":\"price\"}},"
                                                           "\"place_hold4\":{\"value_count\":{\"field\":\"price\"}},"
                                                           "\"place_hold5\":{\"max\":{\"field\":\"sold\"}},"
                                                           "\"place_hold6\":{\"min\":{\"field\":\"sold\"}},"
                                                           "\"place_hold7\":{\"value_count\":{\"field\":\"sold\"}}}}}}";
static const std::string es_resp_with_group_by_second = "{\"hits\":{\"total\":{\"value\":10,\"relation\":\"eq\"}},"
                                                            "\"aggregations\":{\"groupby\":{\"after_key\":{\"group_hold1\":\"red\"},"
                                                            "\"buckets\":["
                                                            "{\"key\":{\"group_hold1\":\"green\"},\"doc_count\":2,"
                                                            "\"place_hold2\":{\"value\":42000.0},"
                                                            "\"place_hold1\":{\"value\":30000.0},"
                                                            "\"place_hold6\":{\"value\":1.4003712E12,\"value_as_string\":\"2014-05-18T00:00:00.000Z\"},"
                                                            "\"place_hold5\":{\"value\":1.4084064E12,\"value_as_string\":\"2014-08-19T00:00:00.000Z\"},"
                                                            "\"place_hold4\":{\"value\":2},"
                                                            "\"place_hold3\":{\"value\":2},"
                                                            "\"place_hold7\":{\"value\":2}},"
                                                            "{\"key\":{\"group_hold1\":\"red\"},\"doc_count\":4,"
                                                            "\"place_hold2\":{\"value\":130000.0},"
                                                            "\"place_hold1\":{\"value\":80000.0},"
                                                            "\"place_hold6\":{\"value\":1.3885344E12,\"value_as_string\":\"2014-01-01T00:00:00.000Z\"},"
                                                            "\"place_hold5\":{\"value\":1.4151456E12,\"value_as_string\":\"2014-11-05T00:00:00.000Z\"},"
                                                            "\"place_hold4\":{\"value\":4},"
                                                            "\"place_hold3\":{\"value\":4},"
                                                            "\"place_hold7\":{\"value\":4}}]}}}";
static const std::string es_req_with_group_by_last = "{\"query\":{\"match_all\":{}},"
                                                         "\"stored_fields\":\"_none_\","
                                                         "\"docvalue_fields\":[\"color.keyword\",\"price\",\"sold\"],"
                                                         "\"track_total_hits\":true,"
                                                         "\"sort\":[\"_doc\"],\"size\":0,"
                                                         "\"aggs\":{\"groupby\":{\"composite\":{"
                                                         "\"sources\":["
                                                         "{\"group_hold1\":{\"terms\":{\"field\":\"color.keyword\",\"missing_bucket\":true}}}],\"size\":2,\"after\":{\"group_hold1\":\"red\"}},"
                                                         "\"aggs\":{\"place_hold1\":{\"max\":{\"field\":\"price\"}},"
                                                         "\"place_hold2\":{\"sum\":{\"field\":\"price\"}},"
                                                         "\"place_hold3\":{\"value_count\":{\"field\":\"price\"}},"
                                                         "\"place_hold4\":{\"value_count\":{\"field\":\"price\"}},"
                                                         "\"place_hold5\":{\"max\":{\"field\":\"sold\"}},"
                                                         "\"place_hold6\":{\"min\":{\"field\":\"sold\"}},"
                                                         "\"place_hold7\":{\"value_count\":{\"field\":\"sold\"}}}}}}";
static const std::string es_resp_with_group_by_last = "{\"hits\":{\"total\":{\"value\":10,\"relation\":\"eq\"}},"
                                                          "\"aggregations\":{\"groupby\":{\"buckets\":[]}}}";

// select count(1),max(price),sum(price),count(price),max(sold),min(sold),count(sold),count(price) from doe;
static const std::string es_req_without_group_by = "{\"query\":{\"match_all\":{}},"
                                                       "\"stored_fields\":\"_none_\","
                                                       "\"docvalue_fields\":[\"price\",\"sold\"],"
                                                       "\"track_total_hits\":true,"
                                                       "\"sort\":[\"_doc\"],\"size\":0,"
                                                       "\"aggs\":{\"place_hold1\":{\"max\":{\"field\":\"price\"}},"
                                                       "\"place_hold2\":{\"sum\":{\"field\":\"price\"}},"
                                                       "\"place_hold3\":{\"value_count\":{\"field\":\"price\"}},"
                                                       "\"place_hold4\":{\"max\":{\"field\":\"sold\"}},"
                                                       "\"place_hold5\":{\"min\":{\"field\":\"sold\"}},"
                                                       "\"place_hold6\":{\"value_count\":{\"field\":\"sold\"}}}}";
static const std::string es_resp_without_group_by = "{\"hits\":{\"total\":{\"value\":10,\"relation\":\"eq\"}},"
                                                        "\"aggregations\":{\"place_hold2\":{\"value\":222000.0},"
                                                        "\"place_hold1\":{\"value\":80000.0},"
                                                        "\"place_hold6\":{\"value\":9},"
                                                        "\"place_hold5\":{\"value\":1.3885344E12,\"value_as_string\":\"2014-01-01T00:00:00.000Z\"},"
                                                        "\"place_hold4\":{\"value\":1.4151456E12,\"value_as_string\":\"2014-11-05T00:00:00.000Z\"},"
                                                        "\"place_hold3\":{\"value\":9}}}";

// select count(1) from doe;
// At this case, agg dsl is empty, agg response is also empty.
static const std::string es_req_only_with_count = "{\"query\":{\"match_all\":{}},"
                                                      "\"stored_fields\":\"_none_\","
                                                      "\"docvalue_fields\":[\"price\"],"
                                                      "\"track_total_hits\":true,"
                                                      "\"sort\":[\"_doc\"],"
                                                      "\"size\":0,\"aggs\":{}}";
static const std::string es_resp_only_with_count = "{\"hits\":{\"total\":{\"value\":10,\"relation\":\"eq\"}}}";

static std::map<std::string, std::string> es_requests{
        {es_req_with_group_by_first, es_resp_with_group_by_first},
        {es_req_with_group_by_second, es_resp_with_group_by_second},
        {es_req_with_group_by_last, es_resp_with_group_by_last},
        {es_req_without_group_by, es_resp_without_group_by},
        {es_req_only_with_count, es_resp_only_with_count}
};

// mock
class RestSearchAction : public HttpHandler {
public:
    void handle(HttpRequest *req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "root") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
        if (req->method() == HttpMethod::POST) {
            std::string post_body = req->get_request_body();
            if (es_requests.count(post_body)) {
                std::string search_result = es_requests.at(post_body);
                HttpChannel::send_reply(req, search_result);
                LOG(INFO) << "Es get request succeed and it's valid";
            } else {
                HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "can't parse the request body, the test hasn't contained it");
            }
        } else {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "can't parse the request, method isn't [post]");
        }

    }
};

static RestSearchAction rest_search_action = RestSearchAction();
static EvHttpServer* mock_es_server = nullptr;
static int real_port = 0;

class PushDownAggregationEsTest : public testing::Test {
public:
    PushDownAggregationEsTest() : _runtime_state(TQueryGlobals()) {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = 2;
        ExecEnv* exec_env = ExecEnv::GetInstance();
        _runtime_state.init(fragment_id, query_options, TQueryGlobals(), exec_env);
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
    }
    ~PushDownAggregationEsTest() override {}

    static void SetUpTestCase() {
        mock_es_server = new EvHttpServer(0);
        mock_es_server->register_handler(POST, "/{index}/{type}/_search", &rest_search_action);
        mock_es_server->start();
        real_port = mock_es_server->get_real_port();
        ASSERT_NE(0, real_port);
    }

    static void TearDownTestCase() { delete mock_es_server; }

    void addSlotDescriptor(TDescriptorTable& t_desc_table, int id, int parent, PrimitiveType type, int columnPos, int offset,
                           int nullIndicatorByte, int nullIndicatorBit, string colName, int slotIdx, bool isMaterialized) {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(id);
        t_slot_desc.__set_parent(parent);
        t_slot_desc.__set_slotType(gen_type_desc(to_thrift(type)));
        t_slot_desc.__set_columnPos(columnPos);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(nullIndicatorByte);
        t_slot_desc.__set_nullIndicatorBit(nullIndicatorBit);
        t_slot_desc.__set_colName(colName);
        t_slot_desc.__set_slotIdx(slotIdx);
        t_slot_desc.__set_isMaterialized(isMaterialized);
        t_desc_table.slotDescriptors.push_back(t_slot_desc);
    }

    void addTupleDescriptor(TDescriptorTable& t_desc_table, int id, int offset, int numNullBytes, int tableId, int numNullSlots) {
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = id;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = numNullBytes;
        t_tuple_desc.tableId = tableId;
        t_tuple_desc.numNullSlots = numNullSlots;
        // if tableId = 0, __isset.tableId should be false, or will check tableId exist when make DescriptorTbl
        t_tuple_desc.__isset.tableId = tableId == 0 ? false : true;
        t_tuple_desc.__isset.numNullSlots = true;
        t_desc_table.__isset.slotDescriptors = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }

    void addTableDescriptor(TDescriptorTable& t_desc_table, int id, int numCols, int numClusteringCols, std::string tableName, std::string dbName) {
        TTableDescriptor t_table_desc;
        t_table_desc.id = id;
        t_table_desc.tableType = TTableType::ES_TABLE;
        t_table_desc.numCols = numCols;
        t_table_desc.numClusteringCols = numClusteringCols;
        t_table_desc.tableName = tableName;
        t_table_desc.dbName = dbName;
        t_table_desc.__isset.esTable = true;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;
    }

    void init(TDescriptorTable& t_desc_table, int row_desc_id, int scan_desc_id, std::vector<std::string>& aggregate_functions,
              int intermediate_tuple_id, std::vector<std::string>& group_by_column_names, std::vector<std::string>& aggregate_column_names) {
        DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);
        _runtime_state.set_desc_tbl(_desc_tbl);

        _tnode.node_id = 0;
        _tnode.node_type = TPlanNodeType::ES_HTTP_SCAN_NODE;
        _tnode.num_children = 0;
        _tnode.limit = -1;
        _tnode.row_tuples.push_back(row_desc_id);
        _tnode.nullable_tuples.push_back(false);
        _tnode.compact_data = false;
        _tnode.__isset.es_scan_node = true;
        _tnode.es_scan_node.tuple_id = scan_desc_id;
        std::map<std::string, std::string> properties{
            {"doc_values_mode", "1"},
            {"password", "root"},
            {"user", "root"}
        };
        _tnode.es_scan_node.__set_properties(properties);
        std::map<std::string, std::string> docvalue_context{
                {"color", "color.keyword"},
                {"price", "price"},
                {"sold", "sold"}
        };
        _tnode.es_scan_node.__set_docvalue_context(docvalue_context);
        std::map<std::string, std::string> fields_context{
                {"color", "color.keyword"},
        };
        _tnode.es_scan_node.__set_fields_context(fields_context);
        _tnode.es_scan_node.__set_is_aggregated(true);
        _tnode.es_scan_node.__set_aggregate_functions(aggregate_functions);
        _tnode.es_scan_node.__set_intermediate_tuple_id(intermediate_tuple_id);
        _tnode.es_scan_node.__set_group_by_column_names(group_by_column_names);
        _tnode.es_scan_node.__set_aggregate_column_names(aggregate_column_names);
    }

    void prepare(EsHttpScanNode& scan_node, Status& status) {
        status = scan_node.init(_tnode, &_runtime_state);
        ASSERT_TRUE(status.ok());

        status = scan_node.prepare(&_runtime_state);
        ASSERT_TRUE(status.ok());
        LOG(INFO) << "EsHttpScanNode init success";

        // scan range
        TEsScanRange es_scan_range;
        es_scan_range.__set_index("doe");
        es_scan_range.__set_type("_doc");
        es_scan_range.__set_shard_id(0);
        TNetworkAddress es_host;
        es_host.__set_hostname("127.0.0.1");
        es_host.__set_port(real_port);
        std::vector<TNetworkAddress> es_hosts;
        es_hosts.push_back(es_host);
        es_scan_range.__set_es_hosts(es_hosts);
        TScanRange scan_range;
        scan_range.__set_es_scan_range(es_scan_range);
        TScanRangeParams scan_range_params;
        scan_range_params.__set_scan_range(scan_range);
        std::vector<TScanRangeParams> scan_ranges;
        scan_ranges.push_back(scan_range_params);

        status = scan_node.set_scan_ranges(scan_ranges);
        ASSERT_TRUE(status.ok());

        status = scan_node.open(&_runtime_state);
        ASSERT_TRUE(status.ok());
        LOG(INFO) << "start thread, and send request to ES";
    }

    bool isEqual(double x, double y) {
        return abs(x-y) < 1e-10;
    }

protected:
    // for test avg
    struct AvgState {
        double sum = 0;
        int64_t count = 0;
    };

    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_state;
};

TEST_F(PushDownAggregationEsTest, query_with_group_by) {
    TDescriptorTable t_desc_table;

    // table descriptors
    addTableDescriptor(t_desc_table, 119092, 3, 0, "doe", "");

    {// scan descriptor
        addSlotDescriptor(t_desc_table, 0, 0, PrimitiveType::TYPE_VARCHAR, -1, 16, 0, 1, "color", 1, true);
        addSlotDescriptor(t_desc_table, 1, 0, PrimitiveType::TYPE_BIGINT, -1, 8, 0, 0, "price", 0, true);
        addSlotDescriptor(t_desc_table, 2, 0, PrimitiveType::TYPE_DATE, -1, 32, 0, 2, "sold", 2, true);

        addTupleDescriptor(t_desc_table, 0, 48, 1, 119092, 3);
    }

    {// the first phase of aggregate descriptor
        addSlotDescriptor(t_desc_table, 3, 1, PrimitiveType::TYPE_VARCHAR, -1, 40, 0, 1, "", 4, true);
        addSlotDescriptor(t_desc_table, 4, 1, PrimitiveType::TYPE_BIGINT, -1, 8, 0, -1, "", 0, true);
        addSlotDescriptor(t_desc_table, 5, 1, PrimitiveType::TYPE_BIGINT, -1, 16, 0, 0, "", 1, true);
        addSlotDescriptor(t_desc_table, 6, 1, PrimitiveType::TYPE_VARCHAR, -1, 56, 0, 2, "", 5, true);
        addSlotDescriptor(t_desc_table, 7, 1, PrimitiveType::TYPE_BIGINT, -1, 24, 0, -1, "", 2, true);
        addSlotDescriptor(t_desc_table, 8, 1, PrimitiveType::TYPE_DATETIME, -1, 72, 0, 3, "", 6, true);
        addSlotDescriptor(t_desc_table, 9, 1, PrimitiveType::TYPE_DATETIME, -1, 88, 0, 4, "", 7, true);
        addSlotDescriptor(t_desc_table, 10, 1, PrimitiveType::TYPE_BIGINT, -1, 32, 0, -1, "", 3, true);

        addTupleDescriptor(t_desc_table, 1, 104, 1, 0, 5);
    }

    std::vector<std::string> aggregate_functions{"count", "max", "avg", "count", "max", "min", "count"};
    std::vector<std::string> group_by_column_names{"color"};
    std::vector<std::string> aggregate_column_names{"", "price", "price", "price", "sold", "sold", "sold"};
    init(t_desc_table, 1, 0, aggregate_functions, 1, group_by_column_names, aggregate_column_names);

    LOG(INFO) << "finish mock data";

    EsHttpScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status;
    prepare(scan_node, status);

    int slot_size = scan_node._row_descriptor.tuple_descriptors()[0]->slots().size();
    ASSERT_EQ(slot_size, 8);

    bool eos = false;
    std::vector<std::string> answer;
    while (!eos) {
        RowBatch row_batch(scan_node._row_descriptor, _runtime_state.batch_size(),
                           _runtime_state.instance_mem_tracker().get());

        status = scan_node.get_next(&_runtime_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        if (eos) {
            ASSERT_EQ(answer.size(), 4);
            break;
        }

        int num = row_batch.num_rows();
        ASSERT_TRUE(num > 0);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            std::string ans = row->to_string(scan_node._row_descriptor);
            answer.push_back(ans);
            LOG(INFO) << "aggregate query with group by get " << i << "th row: " << ans;
            Tuple* tuple = row->get_tuple(0);
            SlotDescriptor* slot = scan_node._row_descriptor.tuple_descriptors()[0]->slots()[3];
            void* value = tuple->get_slot(slot->tuple_offset());
            ASSERT_EQ(slot->type(), PrimitiveType::TYPE_VARCHAR);
            StringValue* string_val = reinterpret_cast<StringValue*>(value);
            AvgState* avgVal = reinterpret_cast<AvgState*>(string_val->ptr);
            LOG(INFO) << "avg sum:" << avgVal->sum << ", count:" << avgVal->count;
            switch (answer.size()) {
                case 1:
                    ASSERT_TRUE(isEqual(avgVal->sum, 10000));
                    ASSERT_EQ(avgVal->count, 1);
                    break;
                case 2:
                    ASSERT_TRUE(isEqual(avgVal->sum, 40000));
                    ASSERT_EQ(avgVal->count, 2);
                    break;
                case 3:
                    ASSERT_TRUE(isEqual(avgVal->sum, 42000));
                    ASSERT_EQ(avgVal->count, 2);
                    break;
                case 4:
                    ASSERT_TRUE(isEqual(avgVal->sum, 130000));
                    ASSERT_EQ(avgVal->count, 4);
                    break;
                default:
                    ASSERT_TRUE(false);
                    break;
            }
        }
    }

    LOG(INFO) << "get aggregate result succeed from ES";

    vector<std::string> actual_answer_without_avg{
        "[(null21000012014-10-2800:00:002014-10-2800:00:001)]",
        "[(blue22500022014-07-0200:00:002014-02-1200:00:002)]",
        "[(green23000022014-08-1900:00:002014-05-1800:00:002)]",
        "[(red48000042014-11-0500:00:002014-01-0100:00:004)]"
    };

    // check answer is right or not
    for (int i = 0; i < answer.size(); i++) {
        std::stringstream buffer;
        buffer << answer[i];
        std::string answer_without_avg;
        // + 2 because time type is : 2021-xx-xx 00:00:00, and there are two time types
        for (int j = 0; j < slot_size + 2; j++) {
            string tmp;
            buffer >> tmp;
            if (j == 3) {
                // skip avg string, avg has checked before
                continue;
            }
            answer_without_avg += tmp;
        }
        ASSERT_EQ(answer_without_avg, actual_answer_without_avg[i]);
    }

    status = scan_node.close(&_runtime_state);
    ASSERT_TRUE(status.ok());
}

TEST_F(PushDownAggregationEsTest, query_without_group_by) {
    TDescriptorTable t_desc_table;
    // table descriptors
    addTableDescriptor(t_desc_table, 237349, 3, 0, "doe", "");

    {// scan descriptor
        addSlotDescriptor(t_desc_table, 0, 0, PrimitiveType::TYPE_BIGINT, -1, 8, 0, 0, "price", 0, true);
        addSlotDescriptor(t_desc_table, 1, 0, PrimitiveType::TYPE_DATETIME, -1, 16, 0, 1, "sold", 1, true);

        addTupleDescriptor(t_desc_table, 0, 32, 1, 237349, 2);
    }

    {// the first phase of aggregate descriptor
        addSlotDescriptor(t_desc_table, 2, 1, PrimitiveType::TYPE_BIGINT, -1, 8, 0, -1, "", 0, true);
        addSlotDescriptor(t_desc_table, 3, 1, PrimitiveType::TYPE_BIGINT, -1, 16, 0, 0, "", 1, true);
        addSlotDescriptor(t_desc_table, 4, 1, PrimitiveType::TYPE_BIGINT, -1, 24, 0, 1, "", 2, true);
        addSlotDescriptor(t_desc_table, 5, 1, PrimitiveType::TYPE_BIGINT, -1, 32, 0, -1, "", 3, true);
        addSlotDescriptor(t_desc_table, 6, 1, PrimitiveType::TYPE_DATETIME, -1, 48, 0, 2, "", 5, true);
        addSlotDescriptor(t_desc_table, 7, 1, PrimitiveType::TYPE_DATETIME, -1, 64, 0, 3, "", 6, true);
        addSlotDescriptor(t_desc_table, 8, 1, PrimitiveType::TYPE_BIGINT, -1, 40, 0, -1, "", 4, true);

        addTupleDescriptor(t_desc_table, 1, 80, 1, 0, 4);
    }

    std::vector<std::string> aggregate_functions{"count", "max", "sum", "count", "max", "min", "count"};
    std::vector<std::string> group_by_column_names;
    std::vector<std::string> aggregate_column_names{"", "price", "price", "price", "sold", "sold", "sold"};
    init(t_desc_table, 1, 0, aggregate_functions, 1, group_by_column_names, aggregate_column_names);

    LOG(INFO) << "finish mock data";

    EsHttpScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status;
    prepare(scan_node, status);

    int slot_size = scan_node._row_descriptor.tuple_descriptors()[0]->slots().size();
    ASSERT_EQ(slot_size, 7);

    bool eos = false;
    std::vector<std::string> answer;

    while (!eos) {
        RowBatch row_batch(scan_node._row_descriptor, _runtime_state.batch_size(),
                           _runtime_state.instance_mem_tracker().get());

        status = scan_node.get_next(&_runtime_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        if (eos) {
            ASSERT_EQ(answer.size(), 1);
            break;
        }

        int num = row_batch.num_rows();
        ASSERT_TRUE(num > 0);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            std::string ans = row->to_string(scan_node._row_descriptor);
            answer.push_back(ans);
            LOG(INFO) << "aggregate query without group by get " << i << "th row: " << ans;
        }
    }

    LOG(INFO) << "get aggregate result succeed from ES";

    // check answer is right or not
    ASSERT_EQ(answer[0], "[(10 80000 222000 9 2014-11-05 00:00:00 2014-01-01 00:00:00 9)]");

    status = scan_node.close(&_runtime_state);
    ASSERT_TRUE(status.ok());
}

TEST_F(PushDownAggregationEsTest, query_with_only_count) {
    TDescriptorTable t_desc_table;
    // table descriptors
    addTableDescriptor(t_desc_table, 237349, 3, 0, "doe", "");

    {// scan descriptor
        addSlotDescriptor(t_desc_table, 1, 0, PrimitiveType::TYPE_BIGINT, -1, 8, 0, 0, "price", 0, true);

        addTupleDescriptor(t_desc_table, 0, 16, 1, 237349, 1);
    }

    {// the first phase of aggregate descriptor
        addSlotDescriptor(t_desc_table, 0, 1, PrimitiveType::TYPE_BIGINT, -1, 0, 0, -1, "", 0, true);

        addTupleDescriptor(t_desc_table, 1, 8, 0, 0, 0);
    }

    std::vector<std::string> aggregate_functions{"count"};
    std::vector<std::string> group_by_column_names;
    std::vector<std::string> aggregate_column_names{""};
    init(t_desc_table, 1, 0, aggregate_functions, 1, group_by_column_names, aggregate_column_names);

    LOG(INFO) << "finish mock data";

    EsHttpScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status;
    prepare(scan_node, status);

    int slot_size = scan_node._row_descriptor.tuple_descriptors()[0]->slots().size();
    ASSERT_EQ(slot_size, 1);

    bool eos = false;
    std::vector<std::string> answer;

    while (!eos) {
        RowBatch row_batch(scan_node._row_descriptor, _runtime_state.batch_size(),
                           _runtime_state.instance_mem_tracker().get());

        status = scan_node.get_next(&_runtime_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        if (eos) {
            ASSERT_EQ(answer.size(), 1);
            break;
        }

        int num = row_batch.num_rows();
        ASSERT_TRUE(num > 0);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            std::string ans = row->to_string(scan_node._row_descriptor);
            answer.push_back(ans);
            LOG(INFO) << "aggregate query without group by get " << i << "th row: " << ans;
        }
    }

    LOG(INFO) << "get aggregate result succeed from ES";

    // check answer is right or not
    ASSERT_EQ(answer[0], "[(10)]");

    status = scan_node.close(&_runtime_state);
    ASSERT_TRUE(status.ok());
}

}// namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}