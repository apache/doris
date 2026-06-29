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

// R13 CLucene 解耦防回归 guard。
//
// 目标：把"SNII 核心层 be/src/snii 零 CLucene 依赖"固化为常驻 CI 约束
// （决策文档：be/src/storage/index/snii/docs/reuse/R13-clucene-decoupling.md）。
// 核心层负责格式 / 存储 / 查询的字节路径，必须与 CLucene 完全解耦；CLucene
// 仅允许出现在集成层（be/src/storage/index/snii）的分词复用与基类签名兼容处。
//
// 实现：运行期定位源码树中的 be/src/snii 目录，逐文件执行等价于
//   grep -rniE "clucene|lucene::|CL_NS|<CLucene" be/src/snii
// 的扫描，断言命中数为 0。若有人向 be/src/snii 引入 lucene:: 等引用，本测试 FAIL。

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

namespace {

namespace fs = std::filesystem;

// 大小写不敏感，忠实复刻决策文档中的 grep -i 正则。
// 四个分支缺一不可：
//   clucene   -> 捕获 #include "CLucene/..."、CLuceneError 等标识符（含 <CLucene.h>）；
//   lucene::  -> 捕获 lucene::analysis::Analyzer 等命名空间用法；
//   CL_NS     -> 捕获 CL_NS_USE 等 CLucene 命名空间宏；
//   <CLucene  -> 捕获 #include <CLucene...> 形式的尖括号包含。
const std::regex kCluceneRefPattern(R"(clucene|lucene::|CL_NS|<CLucene)",
                                    std::regex::ECMAScript | std::regex::icase);

// 通过编译期 __FILE__ 反推源码树定位 be/src/snii。
// 该手法与 be/test/testutil/index_storage_test_util.cpp 的
// repo_root_from_this_source_file() 一致，已在 Doris 测试套件中验证可用：
// Doris 的 UT 始终在源码树内构建并运行，__FILE__ 指向真实源文件路径。
std::optional<fs::path> locate_snii_core_dir() {
    std::vector<fs::path> candidates;

#ifdef SNII_CORE_SOURCE_DIR
    // 可选：构建系统注入的精确路径（最高优先级）。
    // 注意：若由 CMake 注入，必须以带引号的字符串字面量形式定义，
    // 例如 add_definitions(-DSNII_CORE_SOURCE_DIR="${CMAKE_SOURCE_DIR}/src/snii")。
    candidates.emplace_back(SNII_CORE_SOURCE_DIR);
#endif

    std::error_code ec;
    fs::path source_path(__FILE__);
    if (source_path.is_relative()) {
        source_path = fs::absolute(source_path, ec);
    }
    // 自测试源文件目录逐级向上：既可能命中 <repo_root>/be/src/snii，
    // 也可能命中 <be>/src/snii，对目录布局变化具有鲁棒性，无需硬编码层级数。
    for (fs::path dir = source_path.parent_path(); !dir.empty();) {
        candidates.push_back(dir / "be" / "src" / "snii");
        candidates.push_back(dir / "src" / "snii");
        if (dir == dir.root_path()) {
            break;
        }
        dir = dir.parent_path();
    }

    // 兜底：环境变量 DORIS_HOME（与既有 testutil 一致）。
    if (const char* doris_home = std::getenv("DORIS_HOME"); doris_home != nullptr) {
        candidates.push_back(fs::path(doris_home) / "be" / "src" / "snii");
    }

    for (const auto& candidate : candidates) {
        if (fs::is_directory(candidate, ec)) {
            auto canonical = fs::weakly_canonical(candidate, ec);
            return ec ? candidate : canonical;
        }
    }
    return std::nullopt;
}

struct CluceneHit {
    std::string file;
    int line = 0;
    std::string text;
};

// 读取单个文件并收集匹配行；返回 false 表示文件无法打开（视为扫描缺口）。
bool scan_file_for_clucene(const fs::path& file, std::vector<CluceneHit>* hits) {
    std::ifstream input(file);
    if (!input.is_open()) {
        return false;
    }
    std::string line;
    int line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        if (std::regex_search(line, kCluceneRefPattern)) {
            hits->push_back({file.string(), line_no, line});
        }
    }
    return true;
}

} // namespace

// be/src/snii（SNII 核心层）必须保持零 CLucene 引用。
TEST(SniiCluceneDecouplingGuardTest, CoreHasNoCluceneRef) {
    const auto snii_core_dir = locate_snii_core_dir();
    ASSERT_TRUE(snii_core_dir.has_value())
            << "无法定位 SNII 核心源码目录 be/src/snii。请在源码树内运行测试，"
            << "或设置 DORIS_HOME / -DSNII_CORE_SOURCE_DIR。(__FILE__=" << __FILE__ << ")";

    std::error_code ec;
    fs::recursive_directory_iterator it(*snii_core_dir, ec);
    ASSERT_TRUE(!ec) << "无法遍历目录 " << snii_core_dir->string() << ": " << ec.message();
    const fs::recursive_directory_iterator end;

    std::vector<CluceneHit> hits;
    std::vector<std::string> unreadable;
    int scanned_files = 0;
    for (; it != end; it.increment(ec)) {
        ASSERT_TRUE(!ec) << "遍历 " << snii_core_dir->string() << " 出错: " << ec.message();
        const fs::directory_entry& entry = *it;
        std::error_code stat_ec;
        if (!entry.is_regular_file(stat_ec) || stat_ec) {
            continue;
        }
        ++scanned_files;
        if (!scan_file_for_clucene(entry.path(), &hits)) {
            unreadable.push_back(entry.path().string());
        }
    }

    // 防呆：必须确实扫描到文件，否则定位逻辑失效会让 guard 形同虚设（静默通过）。
    ASSERT_GT(scanned_files, 0) << "在 " << snii_core_dir->string()
                                << " 下未扫描到任何文件，guard 失效。";

    // 文件不可读会造成扫描缺口（可能漏检），以非致命方式暴露。
    EXPECT_TRUE(unreadable.empty())
            << "存在无法读取的文件，可能造成漏检：共 " << unreadable.size() << " 个，首个为 "
            << (unreadable.empty() ? std::string {} : unreadable.front());

    if (!hits.empty()) {
        std::ostringstream oss;
        oss << "SNII 核心层 " << snii_core_dir->string() << " 检出 " << hits.size()
            << " 处 CLucene 引用（应为 0，违反 R13 解耦约束）。"
            << "CLucene 仅允许出现在集成层 be/src/storage/index/snii：\n";
        for (const auto& hit : hits) {
            oss << "  " << hit.file << ":" << hit.line << ": " << hit.text << "\n";
        }
        FAIL() << oss.str();
    }
}
