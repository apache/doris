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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "orc/sargs/SearchArgument.hh"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/types.h"
#include "testutil/desc_tbl_builder.h"
#include "vec/exec/format/orc/orc_memory_pool.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/utils/util.hpp"
namespace doris::vectorized {
class OrcReaderTest : public testing::Test {
public:
    OrcReaderTest() = default;
    ~OrcReaderTest() override = default;

private:
    // build orc_reader for table orders
    std::unique_ptr<OrcReader> make_orc_reader() {
        std::vector<std::string> column_names = {
                "o_orderkey",      "o_custkey", "o_orderstatus",  "o_totalprice", "o_orderdate",
                "o_orderpriority", "o_clerk",   "o_shippriority", "o_comment"};
        ObjectPool object_pool;
        DescriptorTblBuilder builder(&object_pool);
        builder.declare_tuple() << TYPE_INT << TYPE_INT << TYPE_CHAR << TYPE_DOUBLE << TYPE_DATE
                                << TYPE_CHAR << TYPE_CHAR << TYPE_INT << TYPE_CHAR;
        DescriptorTbl* desc_tbl = builder.build();
        auto* tuple_desc = const_cast<TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
        RowDescriptor row_desc(tuple_desc, false);
        TFileScanRangeParams params;
        TFileRangeDesc range;
        range.path = "./be/test/exec/test_data/orc_scanner/orders.orc";
        range.start_offset = 0;
        range.size = 1293;
        auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

        auto status = reader->init_reader(&column_names, nullptr, {}, false, tuple_desc, &row_desc,
                                          nullptr, nullptr);
        DCHECK(status.ok());
        return reader;
    }

    std::string build_search_argument(const std::string& expr) {
        auto exprx = apache::thrift::from_json_string<TExpr>(expr);
        VExprContextSPtr context;
        static_cast<void>(VExpr::create_expr_tree(exprx, context));
        auto reader = make_orc_reader();
        auto builder = orc::SearchArgumentFactory::newBuilder();
        reader->_build_search_argument(context->root(), builder);
        return builder->build()->toString();
    }
};

TEST_F(OrcReaderTest, test_build_search_argument) {
    ExecEnv::GetInstance()->set_orc_memory_pool(new ORCMemoryPool());

    std::vector<std::string> exprs = {
            R"|({"1":{"lst":["rec",13,{"1":{"i32":6},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"3":{"i32":3},"4":{"i32":2},"20":{"i32":-1},"29":{"tf":1}},{"1":{"i32":6},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"3":{"i32":3},"4":{"i32":2},"20":{"i32":-1},"29":{"tf":1}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"3":{"i32":11},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"lt"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"lt(int, int)"},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"28":{"i32":5},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0},"3":{"i32":-1}}},"20":{"i32":-1},"29":{"tf":1},"36":{"str":"o_orderkey"}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":100}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"3":{"i32":13},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"gt"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"gt(int, int)"},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"28":{"i32":5},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0},"3":{"i32":-1}}},"20":{"i32":-1},"29":{"tf":1},"36":{"str":"o_orderkey"}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":5999900}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":11},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"3":{"i32":5},"4":{"i32":4},"11":{"rec":{"1":{"tf":0}}},"20":{"i32":-1},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0},"3":{"i32":-1}}},"20":{"i32":-1},"29":{"tf":1},"36":{"str":"o_orderkey"}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":1000000}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":2000000}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":3000000}}},"20":{"i32":-1},"29":{"tf":0}}]}})|"};
    std::vector<std::string> result_search_arguments = {
            "leaf-0  =  (o_orderkey  <  100),  leaf-1  =  (o_orderkey  <=  5999900),  leaf-2  =  "
            "(o_orderkey  in  [1000000,  2000000,  3000000]),  expr  =  (or  leaf-0  (not  leaf-1) "
            " leaf-2)"};
    for (int i = 0; i < exprs.size(); i++) {
        auto search_argument = build_search_argument(exprs[i]);
        ASSERT_EQ(search_argument, result_search_arguments[i]);
    }
}

} // namespace doris::vectorized