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

// SNII 黄金语料回读测试（跨版本 / 跨改动的查询语义锁定）。
//
// 用途：把一组「普通」SNII 段（无 CommonGrams、无打分扩展）用生产写入栈写到磁盘，
// 并把每条查询在写入时的结果（docid 集合、null bitmap、状态码）记成期望文件；
// 之后任何改动（删 CommonGrams、格式对齐、norms 写入……）都必须能原样回读这些段，
// 且每条查询结果逐条一致。段文件也可以由其它版本的 writer（例如生产分支）写出，
// 只要目录里有同名的 .expect 即可校验。
//
//   SNII_GOLDEN_DIR=<dir> SNII_GOLDEN_MODE=write   写段 + 写期望
//   SNII_GOLDEN_DIR=<dir>                          回读校验（默认）
//   未设置 SNII_GOLDEN_DIR                          跳过
//
// 校验模式下每条查询跑三遍：结果缓存关闭（冷）、缓存开启（冷）、缓存开启（热），
// 三遍都必须等于期望。

#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr int64_t kIndexId = 7001;
constexpr const char* kColumn = "c1";

// ---------------------------------------------------------------- 语料

using ScalarRow = std::optional<std::string>; // nullopt = NULL

std::vector<ScalarRow> scalar_corpus() {
    std::vector<ScalarRow> rows;
    rows.emplace_back("hello world hello doris");                                      // 0
    rows.emplace_back("");                                                             // 1 空串
    rows.emplace_back("The QUICK brown-fox; jumped!! over_the lazy dog 42 times");     // 2
    rows.emplace_back("重复 重复 重复 词元 Doris 数据库 全文检索 mixed 中英 tokens"); // 3
    rows.emplace_back(std::nullopt);                                                   // 4 NULL
    rows.emplace_back(std::nullopt);                                                   // 5 NULL
    rows.emplace_back(std::nullopt);                                                   // 6 NULL
    rows.emplace_back(std::string(300, 'x'));                                          // 7 > ignore_above
    rows.emplace_back("single");                                                       // 8
    rows.emplace_back("!!! ??? ,,,");                                                  // 9 无词元
    rows.emplace_back("hello world again and again and again");                       // 10
    rows.emplace_back(std::nullopt);                                                   // 11 NULL
    rows.emplace_back(std::string("\x1f") + "hidden term inside");                     // 12 内部命名空间前缀
    rows.emplace_back(std::string("\x1e") + "escaped start");                          // 13 转义前缀
    rows.emplace_back("prefix prefixes prefixing prefab");                             // 14
    rows.emplace_back("alpha beta gamma alpha beta alpha");                            // 15
    {
        std::string long_doc; // 16 超过 255 个词元（norm 饱和）
        for (int i = 0; i < 300; ++i) long_doc += (i ? " tok" : "tok");
        rows.emplace_back(std::move(long_doc));
    }
    rows.emplace_back("Ünïcode Straße naïve café");                                    // 17
    rows.emplace_back("hello");                                                        // 18
    rows.emplace_back("world hello");                                                  // 19
    return rows;
}

struct Query {
    InvertedIndexQueryType type;
    std::string text;
};

std::vector<Query> analyzed_queries() {
    using T = InvertedIndexQueryType;
    return {
            {T::MATCH_ANY_QUERY, "hello"},
            {T::MATCH_ANY_QUERY, "hello world"},
            {T::MATCH_ANY_QUERY, "nonexistent"},
            {T::MATCH_ANY_QUERY, "the"},
            {T::MATCH_ANY_QUERY, "重复"},
            {T::MATCH_ANY_QUERY, std::string("\x1f") + "hidden"},
            {T::MATCH_ANY_QUERY, "prefix"},
            {T::MATCH_ANY_QUERY, "tok"},
            {T::MATCH_ANY_QUERY, ""},
            {T::MATCH_ALL_QUERY, "hello world"},
            {T::MATCH_ALL_QUERY, "again and"},
            {T::MATCH_ALL_QUERY, "alpha beta gamma"},
            {T::MATCH_ALL_QUERY, "hello nonexistent"},
            {T::MATCH_PHRASE_QUERY, "hello world"},
            {T::MATCH_PHRASE_QUERY, "world hello"},
            {T::MATCH_PHRASE_QUERY, "again and again"},
            {T::MATCH_PHRASE_QUERY, "alpha beta"},
            {T::MATCH_PHRASE_QUERY, "重复 词元"},
            {T::MATCH_PHRASE_QUERY, "brown fox"},
            {T::MATCH_PHRASE_QUERY, "hello"},
            {T::MATCH_PHRASE_PREFIX_QUERY, "hello wor"},
            {T::MATCH_PHRASE_PREFIX_QUERY, "prefix pre"},
            {T::MATCH_PHRASE_PREFIX_QUERY, "alpha be"},
            {T::MATCH_PHRASE_PREFIX_QUERY, "again ag"},
            {T::MATCH_PHRASE_PREFIX_QUERY, "sing"},
            {T::EQUAL_QUERY, "single"},
            {T::EQUAL_QUERY, "hello world hello doris"},
    };
}

std::vector<Query> keyword_queries() {
    using T = InvertedIndexQueryType;
    return {
            {T::EQUAL_QUERY, "single"},
            {T::EQUAL_QUERY, ""},
            {T::EQUAL_QUERY, "hello world hello doris"},
            {T::EQUAL_QUERY, std::string(300, 'x')},
            {T::EQUAL_QUERY, std::string("\x1f") + "hidden term inside"},
            {T::EQUAL_QUERY, std::string("\x1e") + "escaped start"},
            {T::EQUAL_QUERY, "nonexistent"},
            {T::EQUAL_QUERY, "hello"},
            {T::MATCH_ANY_QUERY, "single"},
            {T::MATCH_ANY_QUERY, "hello world hello doris"},
            {T::MATCH_PHRASE_QUERY, "hello world hello doris"},
            {T::MATCH_PHRASE_PREFIX_QUERY, "hello wor"},
    };
}

struct Sample {
    std::string name;
    std::map<std::string, std::string> properties;
    bool keyword_lane;
    bool array = false; // ARRAY<STRING> 列：经 add_array_values / add_array_nulls 写入
};

std::vector<Sample> samples() {
    return {
            {"keyword_docs", {{"parser", "none"}}, true},
            {"keyword_phrase", {{"parser", "none"}, {"support_phrase", "true"}}, true},
            {"english_docs", {{"parser", "english"}, {"lower_case", "true"}}, false},
            {"english_phrase",
             {{"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}},
             false},
            {"unicode_phrase", {{"parser", "unicode"}, {"support_phrase", "true"}}, false},
            {"english_phrase_nolower",
             {{"parser", "english"}, {"lower_case", "false"}, {"support_phrase", "true"}},
             false},
            {"english_array_phrase",
             {{"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}},
             false,
             /*array=*/true},
            {"keyword_array_docs", {{"parser", "none"}}, true, /*array=*/true},
    };
}

// ARRAY 语料：每行是若干元素；nullopt 行 = 整行 NULL；元素级 NULL 用 std::nullopt 元素表示。
using ArrayRow = std::optional<std::vector<std::optional<std::string>>>;

std::vector<ArrayRow> array_corpus() {
    std::vector<ArrayRow> rows;
    rows.emplace_back(std::vector<std::optional<std::string>> {"hello world", "hello doris"}); // 0
    rows.emplace_back(std::vector<std::optional<std::string>> {});                            // 1 空数组
    rows.emplace_back(std::nullopt);                                                          // 2 NULL 行
    rows.emplace_back(std::vector<std::optional<std::string>> {"single"});                    // 3
    rows.emplace_back(std::vector<std::optional<std::string>> {"alpha beta", std::nullopt,
                                                               "gamma alpha"});               // 4 含元素 NULL
    rows.emplace_back(std::vector<std::optional<std::string>> {"world", "hello"});            // 5 跨元素不成短语
    rows.emplace_back(std::vector<std::optional<std::string>> {"重复 词元", "Doris 数据库"});   // 6
    rows.emplace_back(std::nullopt);                                                          // 7 NULL 行
    rows.emplace_back(std::vector<std::optional<std::string>> {"prefix prefixes", "",
                                                               "hello world hello doris"});   // 8
    return rows;
}

// ---------------------------------------------------------------- 工具

TabletIndex make_meta(const Sample& sample) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("golden_" + sample.name);
    pb.add_col_unique_id(0);
    for (const auto& [k, v] : sample.properties) {
        pb.mutable_properties()->insert({k, v});
    }
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

std::string hex_escape(std::string_view s) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    for (unsigned char c : s) {
        if (c < 0x21 || c == '%' || c == 0x7f) {
            out.push_back('%');
            out.push_back(digits[c >> 4]);
            out.push_back(digits[c & 0xf]);
        } else {
            out.push_back(static_cast<char>(c));
        }
    }
    return out;
}

std::string hex_unescape(std::string_view s) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            const std::string hex(s.substr(i + 1, 2));
            out.push_back(static_cast<char>(std::stoul(hex, nullptr, 16)));
            i += 2;
        } else {
            out.push_back(s[i]);
        }
    }
    return out;
}

std::string join_docids(const roaring::Roaring& bitmap) {
    std::string out;
    for (uint32_t docid : bitmap) {
        if (!out.empty()) out.push_back(',');
        out += std::to_string(docid);
    }
    return out;
}

const char* type_name(InvertedIndexQueryType t) {
    switch (t) {
    case InvertedIndexQueryType::EQUAL_QUERY:
        return "EQUAL";
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        return "MATCH_ANY";
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
        return "MATCH_ALL";
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        return "MATCH_PHRASE";
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
        return "MATCH_PHRASE_PREFIX";
    default:
        return "OTHER";
    }
}

struct Observation {
    std::string sample;
    InvertedIndexQueryType type;
    std::string text;
    int status_code = 0;
    std::string docids;      // 逗号分隔
    std::string null_docids; // 逗号分隔
    int null_status_code = 0;

    std::string line() const {
        return std::to_string(static_cast<int>(type)) + "\t" + hex_escape(text) + "\t" +
               std::to_string(status_code) + "\t" + docids + "\t" +
               std::to_string(null_status_code) + "\t" + null_docids;
    }
    std::string describe() const {
        return sample + " / " + type_name(type) + " / \"" + hex_escape(text) + "\"";
    }
};

std::optional<Observation> parse_line(const std::string& sample, const std::string& line) {
    std::vector<std::string> cols;
    std::string cur;
    for (char c : line) {
        if (c == '\t') {
            cols.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    cols.push_back(cur);
    if (cols.size() != 6) return std::nullopt;
    Observation o;
    o.sample = sample;
    o.type = static_cast<InvertedIndexQueryType>(std::stoi(cols[0]));
    o.text = hex_unescape(cols[1]);
    o.status_code = std::stoi(cols[2]);
    o.docids = cols[3];
    o.null_status_code = std::stoi(cols[4]);
    o.null_docids = cols[5];
    return o;
}

// 查询上下文：仿生产的 IndexQueryContext 装配（enable_query_cache 可选）。
struct QueryEnv {
    explicit QueryEnv(bool enable_query_cache) {
        TQueryOptions options;
        options.query_type = TQueryType::SELECT;
        options.enable_inverted_index_query_cache = enable_query_cache;
        options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
    }
    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

// ---------------------------------------------------------------- 写入

Status write_sample(const std::string& dir, const Sample& sample, const TabletIndex& meta) {
    const std::string prefix = dir + "/" + sample.name;
    const std::string file_path = InvertedIndexDescriptor::get_index_file_path_v2(prefix);
    auto fs = io::global_local_filesystem();
    bool exists = false;
    RETURN_IF_ERROR(fs->exists(file_path, &exists));
    if (exists) RETURN_IF_ERROR(fs->delete_file(file_path));
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(file_path, &file_writer));
    IndexFileWriter index_file_writer(fs, prefix, "golden_rowset", /*seg_id=*/0,
                                      InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                                      /*can_use_ram_dir=*/true, /*tablet_id=*/900);
    SniiIndexColumnWriter writer(&index_file_writer, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    RETURN_IF_ERROR(writer.init());
    if (sample.array) {
        // 仿 ArrayColumnWriter::append_nullable：所有行（含 NULL 行）都喂给 add_array_values，
        // NULL 行是空数组；行级 NULL 再通过 add_array_nulls 声明。
        const auto rows = array_corpus();
        std::vector<std::string> storage;
        std::vector<uint8_t> element_nulls;
        std::vector<uint64_t> offsets {0};
        std::vector<uint8_t> row_nulls;
        for (const auto& row : rows) {
            if (row.has_value()) {
                for (const auto& element : *row) {
                    storage.push_back(element.value_or(""));
                    element_nulls.push_back(element.has_value() ? 0 : 1);
                }
            }
            offsets.push_back(storage.size());
            row_nulls.push_back(row.has_value() ? 0 : 1);
        }
        std::vector<Slice> elements;
        elements.reserve(storage.size());
        for (const auto& value : storage) elements.emplace_back(value);
        RETURN_IF_ERROR(writer.add_array_values(
                sizeof(Slice), elements.data(), element_nulls.data(),
                reinterpret_cast<const uint8_t*>(offsets.data()), rows.size()));
        RETURN_IF_ERROR(writer.add_array_nulls(row_nulls.data(), rows.size()));
        RETURN_IF_ERROR(writer.finish());
        RETURN_IF_ERROR(index_file_writer.begin_close());
        RETURN_IF_ERROR(index_file_writer.finish_close());
        return Status::OK();
    }
    const auto rows = scalar_corpus();
    std::vector<std::string> batch;
    auto flush_batch = [&]() -> Status {
        if (batch.empty()) return Status::OK();
        std::vector<Slice> slices;
        slices.reserve(batch.size());
        for (const auto& row : batch) slices.emplace_back(row);
        RETURN_IF_ERROR(writer.add_values(kColumn, slices.data(), slices.size()));
        batch.clear();
        return Status::OK();
    };
    for (const auto& row : rows) {
        if (row.has_value()) {
            batch.push_back(*row);
        } else {
            RETURN_IF_ERROR(flush_batch());
            RETURN_IF_ERROR(writer.add_nulls(1));
        }
    }
    RETURN_IF_ERROR(flush_batch());
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(index_file_writer.begin_close());
    RETURN_IF_ERROR(index_file_writer.finish_close());
    return Status::OK();
}

// ---------------------------------------------------------------- 读取 + 查询

struct OpenedSample {
    std::shared_ptr<IndexFileReader> file_reader;
    std::shared_ptr<SniiIndexReader> index_reader;
    uint64_t doc_count = 0;
};

Status open_sample(const std::string& dir, const Sample& sample, const TabletIndex& meta,
                   OpenedSample* out) {
    const std::string prefix = dir + "/" + sample.name;
    out->file_reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                         InvertedIndexStorageFormatPB::SNII);
    RETURN_IF_ERROR(out->file_reader->init());
    auto logical_reader = out->file_reader->open_snii_index(&meta);
    if (!logical_reader.has_value()) return logical_reader.error();
    out->doc_count = logical_reader.value()->stats().doc_count;
    out->index_reader = SniiIndexReader::create_shared(&meta, out->file_reader,
                                                       InvertedIndexReaderType::FULLTEXT,
                                                       out->doc_count, sample.array);
    return Status::OK();
}

Observation observe(const Sample& sample, const OpenedSample& opened, const Query& query,
                    bool enable_query_cache) {
    QueryEnv env(enable_query_cache);
    Observation o;
    o.sample = sample.name;
    o.type = query.type;
    o.text = query.text;
    auto bitmap = std::make_shared<roaring::Roaring>();
    const Status st = opened.index_reader->query(env.context, kColumn,
                                                 Field::create_field<TYPE_STRING>(query.text),
                                                 query.type, bitmap);
    o.status_code = static_cast<int>(st.code());
    if (st.ok() && bitmap != nullptr) o.docids = join_docids(*bitmap);
    InvertedIndexQueryCacheHandle handle;
    const Status ns = opened.index_reader->read_null_bitmap(env.context, &handle);
    o.null_status_code = static_cast<int>(ns.code());
    if (ns.ok() && handle.get_bitmap() != nullptr) o.null_docids = join_docids(*handle.get_bitmap());
    return o;
}

// ---------------------------------------------------------------- fixture

class SniiGoldenCorpus : public testing::Test {
protected:
    void SetUp() override {
        const char* dir = std::getenv("SNII_GOLDEN_DIR");
        if (dir == nullptr || *dir == '\0') {
            GTEST_SKIP() << "SNII_GOLDEN_DIR 未设置";
        }
        _dir = dir;
        const char* mode = std::getenv("SNII_GOLDEN_MODE");
        _write_mode = mode != nullptr && std::string_view(mode) == "write";
        _previous_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _cache.reset(InvertedIndexQueryCache::create_global_cache(64 * 1024 * 1024, 4));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_cache.get());
    }
    void TearDown() override {
        if (_cache != nullptr) {
            ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_cache);
            _cache.reset();
        }
    }

    std::string _dir;
    bool _write_mode = false;
    InvertedIndexQueryCache* _previous_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _cache;
};

TEST_F(SniiGoldenCorpus, WriteOrVerify) {
    size_t mismatches = 0;
    size_t checked = 0;
    for (const Sample& sample : samples()) {
        const TabletIndex meta = make_meta(sample);
        const std::string expect_path = _dir + "/" + sample.name + ".expect";
        const auto queries = sample.keyword_lane ? keyword_queries() : analyzed_queries();

        if (_write_mode) {
            const Status ws = write_sample(_dir, sample, meta);
            ASSERT_TRUE(ws.ok()) << sample.name << ": " << ws.to_string();
            OpenedSample opened;
            const Status os = open_sample(_dir, sample, meta, &opened);
            ASSERT_TRUE(os.ok()) << sample.name << ": " << os.to_string();
            std::ofstream out(expect_path, std::ios::binary | std::ios::trunc);
            ASSERT_TRUE(out.good()) << expect_path;
            out << "# sample=" << sample.name << " doc_count=" << opened.doc_count;
            for (const auto& [k, v] : sample.properties) out << " " << k << "=" << v;
            out << "\n";
            for (const Query& query : queries) {
                out << observe(sample, opened, query, /*enable_query_cache=*/false).line()
                    << "\n";
            }
            continue;
        }

        // 校验模式：期望文件缺失 = 该样本不存在（可能由其它版本 writer 未写出），跳过并提示。
        std::ifstream in(expect_path, std::ios::binary);
        if (!in.good()) {
            ADD_FAILURE() << "缺少期望文件: " << expect_path;
            continue;
        }
        OpenedSample opened;
        const Status os = open_sample(_dir, sample, meta, &opened);
        if (!os.ok()) {
            ADD_FAILURE() << sample.name << " 打开失败: " << os.to_string();
            ++mismatches;
            continue;
        }
        std::string line;
        std::vector<Observation> expected;
        while (std::getline(in, line)) {
            if (line.empty() || line[0] == '#') continue;
            auto parsed = parse_line(sample.name, line);
            ASSERT_TRUE(parsed.has_value()) << "期望文件格式错误: " << expect_path << ": " << line;
            expected.push_back(*parsed);
        }
        for (const Observation& want : expected) {
            const Query query {want.type, want.text};
            const Observation cold = observe(sample, opened, query, false);
            const Observation cached_first = observe(sample, opened, query, true);
            const Observation cached_second = observe(sample, opened, query, true);
            for (const auto* got : {&cold, &cached_first, &cached_second}) {
                ++checked;
                const bool same = got->status_code == want.status_code &&
                                  got->docids == want.docids &&
                                  got->null_status_code == want.null_status_code &&
                                  got->null_docids == want.null_docids;
                if (!same) {
                    ++mismatches;
                    ADD_FAILURE() << want.describe() << "\n  期望: " << want.line()
                                  << "\n  实际: " << got->line();
                }
            }
        }
    }
    if (!_write_mode) {
        EXPECT_EQ(mismatches, 0) << "共校验 " << checked << " 条";
        std::cout << "[golden] checked=" << checked << " mismatches=" << mismatches << std::endl;
    }
}

} // namespace
} // namespace doris::segment_v2
