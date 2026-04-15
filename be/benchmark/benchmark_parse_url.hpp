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

#pragma once

#include <benchmark/benchmark.h>

#include <cstring>
#include <string>
#include <vector>

#include "core/string_ref.h"
#include "util/string_search.hpp"
#include "util/url_parser.h"

namespace doris {

// ===========================================================================
// Old implementation (baseline) — copied from the original url_parser.cpp
// before optimization, used for A/B comparison.
// ===========================================================================
class OldUrlParser {
public:
    static bool parse_url(const StringRef& url, UrlParser::UrlPart part, StringRef* result) {
        result->data = nullptr;
        result->size = 0;
        StringRef trimmed_url = url.trim();

        int32_t protocol_pos = _s_protocol_search.search(&trimmed_url);
        if (protocol_pos < 0) {
            return false;
        }

        StringRef protocol_end = trimmed_url.substring(protocol_pos + _s_protocol.size);

        switch (part) {
        case UrlParser::AUTHORITY: {
            int32_t end_pos = _s_slash_search.search(&protocol_end);
            *result = protocol_end.substring(0, end_pos);
            break;
        }
        case UrlParser::FILE:
        case UrlParser::PATH: {
            int32_t start_pos = _s_slash_search.search(&protocol_end);
            if (start_pos < 0) {
                return true;
            }
            StringRef path_start = protocol_end.substring(start_pos);
            int32_t end_pos;
            if (part == UrlParser::FILE) {
                end_pos = _s_hash_search.search(&path_start);
            } else {
                end_pos = _s_question_search.search(&path_start);
                if (end_pos < 0) {
                    end_pos = _s_hash_search.search(&path_start);
                }
            }
            *result = path_start.substring(0, end_pos);
            break;
        }
        case UrlParser::HOST: {
            int32_t start_pos = _s_at_search.search(&protocol_end);
            if (start_pos < 0) {
                start_pos = 0;
            } else {
                start_pos += _s_at.size;
            }
            StringRef host_start = protocol_end.substring(start_pos);
            int32_t query_start_pos = _s_question_search.search(&host_start);
            if (query_start_pos > 0) {
                host_start = host_start.substring(0, query_start_pos);
            }
            int32_t end_pos = _s_colon_search.search(&host_start);
            if (end_pos < 0) {
                end_pos = _s_slash_search.search(&host_start);
            }
            *result = host_start.substring(0, end_pos);
            break;
        }
        case UrlParser::PROTOCOL: {
            *result = trimmed_url.substring(0, protocol_pos);
            break;
        }
        case UrlParser::QUERY: {
            int32_t start_pos = _s_question_search.search(&protocol_end);
            if (start_pos < 0) {
                return false;
            }
            StringRef query_start = protocol_end.substring(start_pos + _s_question.size);
            int32_t end_pos = _s_hash_search.search(&query_start);
            *result = query_start.substring(0, end_pos);
            break;
        }
        case UrlParser::REF: {
            int32_t start_pos = _s_hash_search.search(&protocol_end);
            if (start_pos < 0) {
                return false;
            }
            *result = protocol_end.substring(start_pos + _s_hash.size);
            break;
        }
        case UrlParser::USERINFO: {
            int32_t end_pos = _s_at_search.search(&protocol_end);
            if (end_pos < 0) {
                return false;
            }
            *result = protocol_end.substring(0, end_pos);
            break;
        }
        case UrlParser::PORT: {
            int32_t start_pos = _s_at_search.search(&protocol_end);
            if (start_pos < 0) {
                start_pos = 0;
            } else {
                start_pos += _s_at.size;
            }
            StringRef host_start = protocol_end.substring(start_pos);
            int32_t end_pos = _s_colon_search.search(&host_start);
            if (end_pos < 0) {
                return false;
            }
            StringRef port_start_str = host_start.substring(end_pos + _s_colon.size);
            int32_t port_end_pos = _s_slash_search.search(&port_start_str);
            if (port_end_pos < 0) {
                port_end_pos = _s_question_search.search(&port_start_str);
            }
            *result = port_start_str.substring(0, port_end_pos);
            break;
        }
        case UrlParser::INVALID:
            return false;
        }
        return true;
    }

private:
    static const StringRef _s_protocol;
    static const StringRef _s_at;
    static const StringRef _s_slash;
    static const StringRef _s_colon;
    static const StringRef _s_question;
    static const StringRef _s_hash;
    static const StringSearch _s_protocol_search;
    static const StringSearch _s_at_search;
    static const StringSearch _s_slash_search;
    static const StringSearch _s_colon_search;
    static const StringSearch _s_question_search;
    static const StringSearch _s_hash_search;
};

// Static member definitions for old implementation.
const StringRef OldUrlParser::_s_protocol("://", 3);
const StringRef OldUrlParser::_s_at("@", 1);
const StringRef OldUrlParser::_s_slash("/", 1);
const StringRef OldUrlParser::_s_colon(":", 1);
const StringRef OldUrlParser::_s_question("?", 1);
const StringRef OldUrlParser::_s_hash("#", 1);
const StringSearch OldUrlParser::_s_protocol_search(&OldUrlParser::_s_protocol);
const StringSearch OldUrlParser::_s_at_search(&OldUrlParser::_s_at);
const StringSearch OldUrlParser::_s_slash_search(&OldUrlParser::_s_slash);
const StringSearch OldUrlParser::_s_colon_search(&OldUrlParser::_s_colon);
const StringSearch OldUrlParser::_s_question_search(&OldUrlParser::_s_question);
const StringSearch OldUrlParser::_s_hash_search(&OldUrlParser::_s_hash);

// ===========================================================================
// Test URL data — a mix of typical URL patterns.
// ===========================================================================
static const std::vector<std::string>& get_test_urls() {
    static const std::vector<std::string> urls = {
            "http://www.example.com/path/to/page?query=value&foo=bar#section",
            "https://user:password@mail.google.com:443/inbox?type=unread",
            "http://facebook.com/path/p1.php?query=1",
            "https://www.facebook.com/aa/bb?returnpage=https://www.facebook.com/aa/bb/cc",
            "http://www.baidu.com:9090/a/b/c.php",
            "http://example.com:80/docs/books/tutorial/index.html?name=networking#DOWNLOADING",
            "https://api.github.com/repos/apache/doris/pulls?state=open&per_page=100",
            "http://localhost:8080/api/v1/query",
            "https://cdn.jsdelivr.net/npm/vue@3.2.47/dist/vue.global.min.js",
            "http://user@host.com:3306/db?charset=utf8#tables",
            "https://www.amazon.com/dp/B09V3KXJPB?ref=cm_sw_r_cp_ud_dp",
            "http://192.168.1.1:8080/admin/dashboard?tab=overview",
            "https://docs.oracle.com/javase/tutorial/networking/urls/urlInfo.html",
            "http://example.org/path/to/resource",
            "https://search.yahoo.com/search?p=apache+doris&fr=yfp-t",
            "http://www.example.com",
    };
    return urls;
}

// ===========================================================================
// Benchmark: HOST extraction — Old vs New
// ===========================================================================
static void BM_ParseUrl_Host_Old(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = OldUrlParser::parse_url(url_ref, UrlParser::HOST, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

static void BM_ParseUrl_Host_New(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = UrlParser::parse_url(url_ref, UrlParser::HOST, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

// ===========================================================================
// Benchmark: PATH extraction — Old vs New
// ===========================================================================
static void BM_ParseUrl_Path_Old(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = OldUrlParser::parse_url(url_ref, UrlParser::PATH, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

static void BM_ParseUrl_Path_New(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = UrlParser::parse_url(url_ref, UrlParser::PATH, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

// ===========================================================================
// Benchmark: QUERY extraction — Old vs New
// ===========================================================================
static void BM_ParseUrl_Query_Old(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = OldUrlParser::parse_url(url_ref, UrlParser::QUERY, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

static void BM_ParseUrl_Query_New(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = UrlParser::parse_url(url_ref, UrlParser::QUERY, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

// ===========================================================================
// Benchmark: PORT extraction — Old vs New
// ===========================================================================
static void BM_ParseUrl_Port_Old(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = OldUrlParser::parse_url(url_ref, UrlParser::PORT, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

static void BM_ParseUrl_Port_New(benchmark::State& state) {
    const auto& urls = get_test_urls();
    const size_t n = urls.size();
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            StringRef url_ref(urls[i].data(), urls[i].size());
            StringRef result;
            bool ok = UrlParser::parse_url(url_ref, UrlParser::PORT, &result);
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * n);
}

BENCHMARK(BM_ParseUrl_Host_Old);
BENCHMARK(BM_ParseUrl_Host_New);
BENCHMARK(BM_ParseUrl_Path_Old);
BENCHMARK(BM_ParseUrl_Path_New);
BENCHMARK(BM_ParseUrl_Query_Old);
BENCHMARK(BM_ParseUrl_Query_New);
BENCHMARK(BM_ParseUrl_Port_Old);
BENCHMARK(BM_ParseUrl_Port_New);

} // namespace doris
