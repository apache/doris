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

#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"

namespace doris {
using namespace ut_type;

TEST(FunctionUrlTEST, DomainTest) {
    std::string func_name = "domain";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            {{Null()}, Null()},
            {{STRING("http://paul@www.example.com:80/")}, STRING("www.example.com")},
            {{STRING("http:/paul/example/com")}, STRING("")},
            {{STRING("http://www.example.com?q=4")}, STRING("www.example.com")},
            {{STRING("http://127.0.0.1:443/")}, STRING("127.0.0.1")},
            {{STRING("//www.example.com")}, STRING("www.example.com")},
            {{STRING("//paul@www.example.com")}, STRING("www.example.com")},
            {{STRING("www.example.com")}, STRING("www.example.com")},
            {{STRING("example.com")}, STRING("example.com")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionUrlTEST, DomainWithoutWWWTest) {
    std::string func_name = "domain_without_www";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            {{Null()}, Null()},
            {{STRING("http://paul@www.example.com:80/")}, STRING("example.com")},
            {{STRING("http:/paul/example/com")}, STRING("")},
            {{STRING("http://www.example.com?q=4")}, STRING("example.com")},
            {{STRING("http://127.0.0.1:443/")}, STRING("127.0.0.1")},
            {{STRING("//www.example.com")}, STRING("example.com")},
            {{STRING("//paul@www.example.com")}, STRING("example.com")},
            {{STRING("www.example.com")}, STRING("example.com")},
            {{STRING("example.com")}, STRING("example.com")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionUrlTEST, ProtocolTest) {
    std::string func_name = "protocol";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            {{Null()}, Null()},
            {{STRING("http://paul@www.example.com:80/")}, STRING("http")},
            {{STRING("http:/paul/example/com")}, STRING("http")},
            {{STRING("http://www.example.com?q=4")}, STRING("http")},
            {{STRING("http://127.0.0.1:443/")}, STRING("http")},
            {{STRING("//www.example.com")}, STRING("")},
            {{STRING("//paul@www.example.com")}, STRING("")},
            {{STRING("www.example.com")}, STRING("")},
            {{STRING("example.com")}, STRING("")},
            {{STRING("https://example.com/")}, STRING("https")},
            {{STRING("svn+ssh://example.com?q=hello%20world")}, STRING("svn+ssh")},
            {{STRING("ftp://example.com/")}, STRING("ftp")},
            {{STRING("ftp!://example.com/")}, STRING("")},
            {{STRING("http://127.0.0.1:443/")}, STRING("http")},
            {{STRING("https!://example.com/")}, STRING("")},
            {{STRING("http!://example.com/")}, STRING("")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionUrlTEST, ParseUrlQueryKeyTest) {
    std::string func_name = "parse_url";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                                PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            // Every nullable argument must short-circuit independently in the keyed form.
            {{Null(), STRING("QUERY"), STRING("k")}, Null()},
            {{STRING("http://h/p?k=1"), Null(), STRING("k")}, Null()},
            {{STRING("http://h/p?k=1"), STRING("QUERY"), Null()}, Null()},
            // The only '?' is inside the fragment, so the url has no query component.
            {{STRING("http://h/p#f?k=v"), STRING("QUERY"), STRING("k")}, Null()},
            // The '#' comes before the '?', so it is a fragment instead of a query.
            {{STRING("http://h/p#f/?#k=v"), STRING("QUERY"), STRING("k")}, Null()},
            {{STRING("http://h/p#?k=v"), STRING("QUERY"), STRING("k")}, Null()},
            // The url has no query component at all.
            {{STRING("http://h/p&k=v"), STRING("QUERY"), STRING("k")}, Null()},
            // The key only exists in the path.
            {{STRING("http://h/p&k=v?x=1"), STRING("QUERY"), STRING("k")}, Null()},
            // The query component of this url is 'x=1'.
            {{STRING("http://h/p&k=v?x=1"), STRING("QUERY"), STRING("x")}, STRING("1")},
            // The key only exists in the fragment.
            {{STRING("http://h/p?x=1#f&k=v"), STRING("QUERY"), STRING("k")}, Null()},
            // The key exists in the path, in the query and in the fragment.
            {{STRING("http://h/p&k=v?k=1#f&k=2"), STRING("QUERY"), STRING("k")}, STRING("1")},
            {{STRING("http://h/p?a=1&k=2"), STRING("QUERY"), STRING("k")}, STRING("2")},
            // A duplicated key keeps the behaviour of returning the first value.
            {{STRING("http://h/p?k=1&k=2#f"), STRING("QUERY"), STRING("k")}, STRING("1")},
            {{STRING("http://h/p?k=1&k=2&k=3"), STRING("QUERY"), STRING("k")}, STRING("1")},
            // Only the query component is searched, so the duplicated key in the fragment is
            // not part of the result.
            {{STRING("http://h/p?k=1#k=2&k=3"), STRING("QUERY"), STRING("k")}, STRING("1")},
            // A key without any '=' is not a valid query parameter.
            {{STRING("http://h/p?k"), STRING("QUERY"), STRING("k")}, Null()},
            {{STRING("http://h/p?"), STRING("QUERY"), STRING("k")}, Null()},
            // The key is the first query parameter.
            {{STRING("http://h/p?k=1"), STRING("QUERY"), STRING("k")}, STRING("1")},
            {{STRING("  http://h/p?k=1  "), STRING("QUERY"), STRING("k")}, STRING("1")},
            {{STRING("  http://h/p?sk=0&k=1"), STRING("QUERY"), STRING("k")}, STRING("1")},
            {{STRING("  http://h/p?k&k=1"), STRING("QUERY"), STRING("k")}, STRING("1")},
            {{STRING("  http://h/p?k=&k=1"), STRING("QUERY"), STRING("k")}, STRING("")},
            {{STRING("http://h/p?k=1"), STRING("HOST"), STRING("k")}, Null()},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionUrlTEST, ParseUrlQueryTest) {
    std::string func_name = "parse_url";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            // The only '?' is inside the fragment, so the url has no query component. The
            // keyed form of parse_url must report the same.
            {{STRING("http://h/p#f?k=v"), STRING("QUERY")}, Null()},
            {{STRING("http://h/p#f/?#k=v"), STRING("QUERY")}, Null()},
            {{STRING("http://h/p#?k=v"), STRING("QUERY")}, Null()},
            // The url has no query component at all.
            {{STRING("http://h/p&k=v"), STRING("QUERY")}, Null()},
            {{STRING("http://h/p"), STRING("QUERY")}, Null()},
            // The query component starts at the first '?' and ends before the fragment.
            {{STRING("http://h/p?k=1"), STRING("QUERY")}, STRING("k=1")},
            {{STRING("http://h/p?k=1#f&k=2"), STRING("QUERY")}, STRING("k=1")},
            {{STRING("http://h/p?a=1&k=2"), STRING("QUERY")}, STRING("a=1&k=2")},
            // An empty query component is not NULL.
            {{STRING("http://h/p?"), STRING("QUERY")}, STRING("")},
            {{STRING("  http://h/p?k=1  "), STRING("QUERY")}, STRING("k=1")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionUrlTEST, ParseUrlAuthorityTest) {
    std::string func_name = "parse_url";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            // A nullable dynamic part must be handled row by row. A NULL part must not be
            // materialized as an invalid empty part while another row is being parsed.
            {{Null(), STRING("HOST")}, Null()},
            {{STRING("http://example.com/path?x=1"), STRING("HOST")}, STRING("example.com")},
            {{STRING("http://example.com/path?x=1"), Null()}, Null()},
            // A ':' in the path is not a port separator, and an '@' in the path is not a
            // userinfo separator.
            {{STRING("http://example.com/a:b"), STRING("HOST")}, STRING("example.com")},
            {{STRING("http://example.com/a:b"), STRING("PORT")}, Null()},
            {{STRING("http://example.com/a:b"), STRING("AUTHORITY")}, STRING("example.com")},
            {{STRING("http://example.com/a@b:c"), STRING("HOST")}, STRING("example.com")},
            {{STRING("http://example.com/a@b:c"), STRING("USERINFO")}, Null()},
            // A ':' in the query or the fragment is not a port separator either.
            {{STRING("http://example.com/p?r=http:8080"), STRING("PORT")}, Null()},
            {{STRING("http://example.com#f:1"), STRING("HOST")}, STRING("example.com")},
            {{STRING("http://example.com#f:1"), STRING("PORT")}, Null()},
            {{STRING("http://example.com?x=1"), STRING("AUTHORITY")}, STRING("example.com")},
            {{STRING("http://[2001:db8::1]:8080/a?x=1#r"), STRING("HOST")},
             STRING("[2001:db8::1]")},
            {{STRING("http://[2001:db8::1]:8080/a?x=1#r"), STRING("PORT")}, STRING("8080")},
            {{STRING("http://h/p#frag?x=1"), STRING("PATH")}, STRING("/p")},
            {{STRING("http://h/p#frag?x=1"), STRING("QUERY")}, Null()},
            // A real port and a real userinfo are still returned.
            {{STRING("http://user:pass@example.com:80/a:b"), STRING("HOST")},
             STRING("example.com")},
            {{STRING("http://user:pass@example.com:80/a:b"), STRING("PORT")}, STRING("80")},
            {{STRING("http://user:pass@example.com:80/a:b"), STRING("USERINFO")},
             STRING("user:pass")},
            {{STRING("http://user:pass@example.com:80/a:b"), STRING("AUTHORITY")},
             STRING("user:pass@example.com:80")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

} // namespace doris
