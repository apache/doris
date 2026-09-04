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

// gram 布尔查询的端到端测试：把一批真实文本经 **写入侧** 的 gram 族 analyzer 写成一个
// 真正的 SNII 段（SniiIndexColumnWriter -> IndexFileWriter -> .idx 文件），再用
// LogicalIndexReader 打开它，对若干 LIKE / REGEXP 模式跑
// 「RegexGramCompiler 编译 -> gram_boolean_query 求值」，并与暴力真值比对。
//
// 这是整条链路上唯一一个把 **阶段 B 的 docid 空间与词典键** 和 **阶段 C 的查找** 钉在
// 一起的测试：
//   * 词典键：写入侧由 GramTokenizer 产出 gram 词元并落成 SNII 词典 term；查询侧由
//     RegexGramCompiler 用同一个 GramScheme 推出 gram 字面量再去词典里查。任何一侧的
//     编码 / 大小写 / 哈希抽样口径漂移，都会让本测试出现「真值命中但位图未包含」的假阴性。
//   * docid 空间：段里既有空串行也有 NULL 行，它们同样占用 docid。写入侧一旦漏记一行
//     （例如 add_nulls 不推进 rid），后面所有行的 docid 都会整体错位，本测试同样会失败。
//
// 断言方向只有一个：位图必须是真值集合的 **超集**（truth => candidate）。gram 索引只做
// 候选收窄，多出来的候选行由上层表达式逐行复验，因此「多」是允许的、「漏」是致命的。

#include <gtest/gtest.h>
#include <re2/re2.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/gram/gram_family.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/inverted/gram/regex_gram_compiler.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/gram_boolean_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr const char* kTestDir = "./ut_dir/gram_boolean_query_e2e_test";
constexpr int64_t kIndexId = 9201;

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// 注入两套 gram 族策略（sparse 与 dense），手法同 snii_writer_test.cpp 的
// ScopedGramPolicies：替换 ExecEnv::_index_policy_mgr，析构时还原。id / name 为本文件专属。
// IndexPolicyMgr 的名字空间不分策略类型，因此 tokenizer 与 analyzer 不能同名。
class ScopedGramE2EPolicies {
public:
    ScopedGramE2EPolicies() {
        auto* exec_env = ExecEnv::GetInstance();
        _previous = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &_manager;

        TIndexPolicy sparse_tokenizer;
        sparse_tokenizer.id = 9201;
        sparse_tokenizer.name = "gram_e2e_sparse_tokenizer";
        sparse_tokenizer.type = TIndexPolicyType::TOKENIZER;
        sparse_tokenizer.properties["type"] = "ngram";
        sparse_tokenizer.properties["mode"] = "sparse";

        TIndexPolicy sparse_analyzer;
        sparse_analyzer.id = 9202;
        sparse_analyzer.name = sparse_analyzer_name();
        sparse_analyzer.type = TIndexPolicyType::ANALYZER;
        sparse_analyzer.properties["tokenizer"] = sparse_tokenizer.name;

        TIndexPolicy dense_tokenizer;
        dense_tokenizer.id = 9203;
        dense_tokenizer.name = "gram_e2e_dense_tokenizer";
        dense_tokenizer.type = TIndexPolicyType::TOKENIZER;
        dense_tokenizer.properties["type"] = "ngram";
        dense_tokenizer.properties["mode"] = "dense";
        dense_tokenizer.properties["min_gram"] = "3";

        TIndexPolicy dense_analyzer;
        dense_analyzer.id = 9204;
        dense_analyzer.name = dense_analyzer_name();
        dense_analyzer.type = TIndexPolicyType::ANALYZER;
        dense_analyzer.properties["tokenizer"] = dense_tokenizer.name;

        _manager.apply_policy_changes(
                {sparse_tokenizer, sparse_analyzer, dense_tokenizer, dense_analyzer}, {});
    }

    ~ScopedGramE2EPolicies() { ExecEnv::GetInstance()->_index_policy_mgr = _previous; }

    static std::string sparse_analyzer_name() { return "gram_e2e_sparse"; }
    static std::string dense_analyzer_name() { return "gram_e2e_dense"; }

    IndexPolicyMgr& manager() { return _manager; }

private:
    IndexPolicyMgr _manager;
    IndexPolicyMgr* _previous = nullptr;
};

std::map<std::string, std::string> gram_index_properties(const std::string& analyzer_name) {
    return {{"analyzer", analyzer_name}};
}

TabletIndex make_index_meta(const std::map<std::string, std::string>& properties) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("gram_e2e_idx");
    pb.add_col_unique_id(0);
    for (const auto& [key, value] : properties) {
        pb.mutable_properties()->insert({key, value});
    }
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

// 语料：约 30 行短文本，混合类日志的 ASCII 行、中文行、空串与 NULL。
// nullopt 表示 NULL 行——它同样占一个 docid，是 docid 空间对齐的关键探针。
std::vector<std::optional<std::string>> build_corpus() {
    return {
            "rpc error: code = Unavailable desc = timeout",
            "rpc error: code = DeadlineExceeded",
            "GET /images/x.gif 200 12ms",
            "POST /api/v1/login 500 user_id=abc",
            std::nullopt,
            "手机微博登录失败",
            "微博 POST 10.68.3.18:8080 error",
            "",
            "hello world",
            "hello doris",
            "world without hello",
            "abcdefg starts here",
            "prefix abcdefg middle",
            "xyz at the very start",
            "ends with xyz",
            "phone: 010-12345678",
            "ticket 123-4567 issued",
            "no digits at all here",
            std::nullopt,
            "手机号码 138-0000-0000",
            "Convert conversion successful",
            "timeout after error error error",
            "code = Unavailable",
            "code = OK",
            "",
            "aaa bbb ccc",
            "用户 user_id=42 手机 GET",
            "err0r: code = Unknown",
            "errxr: computed",
            "final row without markers",
    };
}

// 把语料喂给一个真实的 SniiIndexColumnWriter，落成一个真正的 .idx 文件。
// 连续的非 NULL 行合成一个 add_values 批次，连续的 NULL 行合成一个 add_nulls 批次，
// 与生产上 SegmentWriter 的调用形态一致。
void write_segment(const std::string& path, const TabletIndex& index_meta,
                   const std::vector<std::optional<std::string>>& corpus) {
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(path, &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), path, "gram_e2e_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/9201);
    SniiIndexColumnWriter writer(&index_file_writer, &index_meta,
                                 FieldType::OLAP_FIELD_TYPE_VARCHAR);
    assert_ok(writer.init());

    size_t i = 0;
    while (i < corpus.size()) {
        if (!corpus[i].has_value()) {
            size_t run = 0;
            while (i + run < corpus.size() && !corpus[i + run].has_value()) {
                ++run;
            }
            assert_ok(writer.add_nulls(static_cast<uint32_t>(run)));
            i += run;
            continue;
        }
        std::vector<Slice> slices;
        while (i < corpus.size() && corpus[i].has_value()) {
            slices.emplace_back(*corpus[i]);
            ++i;
        }
        assert_ok(writer.add_values("c1", slices.data(), slices.size()));
    }

    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());
}

// 已展开转义的 LIKE 模式原子：% / _ / 普通字符三选一。
struct LikeAtom {
    bool any_seq = false; // %
    bool any_one = false; // _
    char ch = 0;
};

std::vector<LikeAtom> parse_like_pattern(const std::string& pattern) {
    std::vector<LikeAtom> atoms;
    for (size_t p = 0; p < pattern.size(); ++p) {
        const char c = pattern[p];
        if (c == '\\' && p + 1 < pattern.size() &&
            (pattern[p + 1] == '%' || pattern[p + 1] == '_' || pattern[p + 1] == '\\')) {
            atoms.push_back({.any_seq = false, .any_one = false, .ch = pattern[p + 1]});
            ++p;
        } else if (c == '%') {
            atoms.push_back({.any_seq = true, .any_one = false, .ch = 0});
        } else if (c == '_') {
            atoms.push_back({.any_seq = false, .any_one = true, .ch = 0});
        } else {
            atoms.push_back({.any_seq = false, .any_one = false, .ch = c});
        }
    }
    return atoms;
}

// LIKE 真值：整串匹配，支持 % / _ 与 \% \_ \\ 转义。经典的 O(n*m) 动态规划，
// 刻意不复用被测代码的任何切分逻辑——它必须是一个独立的真值来源。
// `_` 按单字节匹配：本文件的模式只在纯 ASCII 字面量之间使用 `_`，而 UTF-8 里两个
// ASCII 字节之间不可能夹着多字节字符的续字节，因此这与「单字符」语义在本语料上等价。
bool like_match(const std::string& value, const std::string& pattern) {
    const std::vector<LikeAtom> atoms = parse_like_pattern(pattern);
    const size_t n = value.size();
    const size_t k = atoms.size();
    std::vector<char> prev(k + 1, 0);
    std::vector<char> cur(k + 1, 0);
    prev[0] = 1;
    for (size_t j = 1; j <= k; ++j) {
        prev[j] = static_cast<char>(atoms[j - 1].any_seq && prev[j - 1] != 0);
    }
    for (size_t i = 1; i <= n; ++i) {
        cur[0] = 0;
        for (size_t j = 1; j <= k; ++j) {
            const LikeAtom& a = atoms[j - 1];
            if (a.any_seq) {
                cur[j] = static_cast<char>(cur[j - 1] != 0 || prev[j] != 0);
            } else if (a.any_one) {
                cur[j] = prev[j - 1];
            } else {
                cur[j] = static_cast<char>(prev[j - 1] != 0 && value[i - 1] == a.ch);
            }
        }
        prev.swap(cur);
    }
    return prev[k] != 0;
}

struct PatternCase {
    bool is_like = false;
    const char* pattern = nullptr;
};

const std::vector<PatternCase>& pattern_cases() {
    static const std::vector<PatternCase> cases {
            {.is_like = true, .pattern = "%code = U%"},
            {.is_like = true, .pattern = "%手机%"},
            {.is_like = true, .pattern = "abc%"},
            {.is_like = true, .pattern = "%xyz%"},
            {.is_like = true, .pattern = "%err_r%"},
            {.is_like = true, .pattern = "%hello%world%"},
            {.is_like = false, .pattern = "err.r: co"},
            {.is_like = false, .pattern = "(手|微)博"},
            {.is_like = false, .pattern = "[0-9]{3}-[0-9]{4}"},
            {.is_like = false, .pattern = "hello|world"},
            {.is_like = false, .pattern = "Unavailable"},
            {.is_like = false, .pattern = "^rpc error"},
    };
    return cases;
}

// 暴力真值：对每一行独立判定模式是否匹配。NULL 行永不匹配。
roaring::Roaring brute_force_truth(const std::vector<std::optional<std::string>>& corpus,
                                   const PatternCase& pattern_case) {
    roaring::Roaring truth;
    std::unique_ptr<RE2> rx;
    if (!pattern_case.is_like) {
        rx = std::make_unique<RE2>(pattern_case.pattern, RE2::Quiet);
        EXPECT_TRUE(rx->ok()) << pattern_case.pattern;
    }
    for (uint32_t docid = 0; docid < corpus.size(); ++docid) {
        if (!corpus[docid].has_value()) {
            continue;
        }
        const std::string& value = *corpus[docid];
        const bool matched = pattern_case.is_like ? like_match(value, pattern_case.pattern)
                                                  : RE2::PartialMatch(value, *rx);
        if (matched) {
            truth.add(docid);
        }
    }
    return truth;
}

roaring::Roaring full_range(uint32_t num_docs) {
    roaring::Roaring full;
    full.addRange(0, num_docs);
    return full;
}

// 对一条模式跑「编译 -> 求值 -> 与真值比对」。抽成函数是为了压低 run_scheme() 的
// 认知复杂度（clang-tidy readability-function-cognitive-complexity）。
void check_pattern(const std::vector<std::optional<std::string>>& corpus,
                   const doris::snii::reader::LogicalIndexReader& index,
                   gram::RegexGramCompiler& compiler, const std::string& analyzer_name,
                   const PatternCase& pattern_case, int* indexable) {
    const auto num_docs = static_cast<uint32_t>(corpus.size());
    gram::GramQuery query;
    const Status compile_status = pattern_case.is_like
                                          ? compiler.compile_like(pattern_case.pattern, &query)
                                          : compiler.compile_regexp(pattern_case.pattern, &query);
    ASSERT_TRUE(compile_status.ok()) << pattern_case.pattern;

    doris::snii::query::LogicalIndexPostingSource source(index);
    roaring::Roaring bitmap;
    ASSERT_TRUE(doris::snii::query::gram_boolean_query(source, query, num_docs, &bitmap).ok())
            << pattern_case.pattern;

    if (query.is_all()) {
        // ALL 必须原样放行整个 docid 空间，否则就是在毫无信息的情况下裁行。
        EXPECT_TRUE(bitmap == full_range(num_docs)) << pattern_case.pattern;
    } else {
        ++(*indexable);
    }
    // 候选集合永远落在 [0, num_docs) 内。
    EXPECT_TRUE((bitmap - full_range(num_docs)).isEmpty()) << pattern_case.pattern;

    const roaring::Roaring truth = brute_force_truth(corpus, pattern_case);
    const roaring::Roaring missed = truth - bitmap;
    EXPECT_TRUE(missed.isEmpty()) << "FALSE NEGATIVE analyzer=" << analyzer_name
                                  << " pattern=" << pattern_case.pattern
                                  << " q=" << query.to_debug_string()
                                  << " missed=" << missed.toString();
}

// 对一套 gram 方案跑完整的「写段 -> 开段 -> 编译 -> 求值 -> 比对真值」。
void run_scheme(const std::string& analyzer_name, const std::string& segment_name) {
    ScopedGramE2EPolicies policies;
    const auto properties = gram_index_properties(analyzer_name);
    const TabletIndex index_meta = make_index_meta(properties);
    const auto corpus = build_corpus();
    const auto num_docs = static_cast<uint32_t>(corpus.size());

    const std::string path = std::string(kTestDir) + "/" + segment_name + ".idx";
    write_segment(path, index_meta, corpus);
    if (::testing::Test::HasFatalFailure()) {
        return;
    }

    // 查询侧完全按生产路径解析方案：只有索引属性 + 策略管理器。
    const std::optional<gram::GramScheme> scheme =
            gram::resolve_gram_scheme(properties, &policies.manager());
    ASSERT_TRUE(scheme.has_value()) << analyzer_name;

    doris::snii::io::LocalFileReader file;
    assert_ok(file.open(path));
    doris::snii::reader::SniiSegmentReader segment;
    assert_ok(doris::snii::reader::SniiSegmentReader::open(&file, &segment));
    ASSERT_EQ(segment.n_logical_indexes(), 1U);
    doris::snii::reader::LogicalIndexReader index;
    assert_ok(segment.open_index(static_cast<uint64_t>(kIndexId), "", &index));
    // docid 空间必须覆盖包括空串行与 NULL 行在内的每一行。
    ASSERT_EQ(index.stats().doc_count, num_docs);

    gram::RegexGramCompiler compiler(*scheme);
    int indexable = 0;
    for (const PatternCase& pattern_case : pattern_cases()) {
        check_pattern(corpus, index, compiler, analyzer_name, pattern_case, &indexable);
        if (::testing::Test::HasFatalFailure()) {
            return;
        }
    }
    // 覆盖率保护：至少三分之一的模式真的产生了可用于裁剪的查询，否则本测试会退化成
    // 「全是 ALL、什么都没验证」。阈值取三分之一而不是一半，是因为 SPARSE 方案按哈希抽样
    // 保留 gram，短字面量能被稳定钉住的比例天然低于 DENSE（实测 sparse 5/12、dense 8/12）；
    // 这里要挡的是「编译器或写入侧整体失效」，不是两种方案的裁剪力差异。
    EXPECT_GE(indexable, static_cast<int>(pattern_cases().size()) / 3)
            << "analyzer=" << analyzer_name;
}

class GramBooleanQueryE2E : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }
};

TEST_F(GramBooleanQueryE2E, SparseSchemeMatchesBruteForce) {
    run_scheme(ScopedGramE2EPolicies::sparse_analyzer_name(), "sparse");
}

TEST_F(GramBooleanQueryE2E, DenseSchemeMatchesBruteForce) {
    run_scheme(ScopedGramE2EPolicies::dense_analyzer_name(), "dense");
}

} // namespace
} // namespace doris::segment_v2
