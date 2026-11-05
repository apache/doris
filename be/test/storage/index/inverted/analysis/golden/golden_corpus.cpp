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

#include "storage/index/inverted/analysis/golden/golden_corpus.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <string_view>
#include <utility>

#include "common/check.h"

namespace doris::segment_v2::inverted_index::golden {

GoldenRandom::GoldenRandom(uint64_t seed) : _state(seed == 0 ? 0x9E3779B97F4A7C15ULL : seed) {}

uint64_t GoldenRandom::next() {
    _state ^= _state >> 12;
    _state ^= _state << 25;
    _state ^= _state >> 27;
    return _state * 0x2545F4914F6CDD1DULL;
}

uint32_t GoldenRandom::below(uint32_t bound) {
    DORIS_CHECK(bound > 0);
    return static_cast<uint32_t>((next() >> 32) % bound);
}

void append_utf8(std::string* out, uint32_t code_point) {
    if (code_point < 0x80) {
        out->push_back(static_cast<char>(code_point));
    } else if (code_point < 0x800) {
        out->push_back(static_cast<char>(0xC0 | (code_point >> 6)));
        out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
    } else if (code_point < 0x10000) {
        out->push_back(static_cast<char>(0xE0 | (code_point >> 12)));
        out->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
        out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
    } else {
        DORIS_CHECK(code_point <= 0x10FFFF);
        out->push_back(static_cast<char>(0xF0 | (code_point >> 18)));
        out->push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
        out->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
        out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
    }
}

namespace {

// Bytes given by value, for sequences that are not valid UTF-8 and so cannot be string literals
// without hex escapes swallowing the letters that follow them.
std::string bytes(std::initializer_list<int> values) {
    std::string out;
    for (int value : values) {
        out.push_back(static_cast<char>(value));
    }
    return out;
}

std::string repeat(std::string_view piece, size_t count) {
    std::string out;
    out.reserve(piece.size() * count);
    for (size_t i = 0; i < count; ++i) {
        out.append(piece);
    }
    return out;
}

// The first `size` bytes of `text` repeated as often as needed; may end inside a character.
std::string cycle_to(std::string_view text, size_t size) {
    std::string out;
    out.reserve(size);
    while (out.size() < size) {
        out.append(text.substr(0, size - out.size()));
    }
    return out;
}

// Han characters (three bytes each) for `size` bytes, padded with ASCII where size % 3 != 0.
std::string han_to(size_t size) {
    static constexpr std::string_view kHan = "我们研究数据库的分词技术和索引结构";
    std::string out = cycle_to(kHan, size / 3 * 3);
    out.append(size % 3, 'x');
    return out;
}

template <size_t N>
const char* pick(GoldenRandom& random, const char* const (&values)[N]) {
    return values[random.below(N)];
}

std::vector<std::string> read_lines(const std::string& path) {
    std::ifstream in(path);
    DORIS_CHECK(in.good()) << "cannot read " << path;
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            lines.push_back(std::move(line));
        }
    }
    return lines;
}

// Joins `words` into lines of `per_line` words: most lines run the words together as Chinese text
// does, every fourth line separates them.
void append_word_lines(const std::vector<std::string>& words, size_t per_line,
                       std::vector<std::string>* lines) {
    static constexpr const char* kSeparators[] = {" ", "，", "、"};
    for (size_t start = 0, line_index = 0; start < words.size(); start += per_line, ++line_index) {
        std::string line;
        for (size_t i = start; i < std::min(words.size(), start + per_line); ++i) {
            if (i > start && line_index % 4 == 3) {
                line += kSeparators[line_index / 4 % 3];
            }
            line += words[i];
        }
        lines->push_back(std::move(line));
    }
}

// The English stop words of the builtin parsers come first, so lines carry them often.
constexpr const char* kEnglishWords[] = {
        "a",          "an",        "and",        "are",       "as",         "at",
        "be",         "but",       "by",         "for",       "if",         "in",
        "into",       "is",        "it",         "no",        "not",        "of",
        "on",         "or",        "such",       "that",      "the",        "their",
        "then",       "there",     "these",      "they",      "this",       "to",
        "was",        "will",      "with",       "database",  "query",      "index",
        "inverted",   "analyzer",  "token",      "search",    "engine",     "apache",
        "doris",      "column",    "segment",    "storage",   "compaction", "cluster",
        "replica",    "tablet",    "schema",     "table",     "value",      "string",
        "number",     "phrase",    "prefix",     "wildcard",  "regexp",     "match",
        "score",      "ranking",   "relevance",  "document",  "field",      "term",
        "frequency",  "position",  "offset",     "length",    "buffer",     "memory",
        "thread",     "process",   "stream",     "reader",    "writer",     "filter",
        "normalizer", "tokenizer", "keyword",    "english",   "chinese",    "unicode",
        "running",    "runs",      "ran",        "quickly",   "quick",      "brown",
        "fox",        "jumps",     "over",       "lazy",      "dog",        "hello",
        "world",      "foo",       "bar",        "baz",       "data",       "information",
        "retrieval",  "latency",   "throughput", "benchmark", "compressed", "encoding",
        "identifier", "version",   "release",    "upgrade",   "rollback",   "before",
        "after",      "between",   "under",      "every",     "each",       "many",
        "other",      "first",     "second",     "last",      "next",       "good",
        "better",     "best",      "new",        "old",       "long",       "short",
        "high",       "low",       "open",       "close",     "read",       "write",
        "load",       "scan",      "sort",       "join",      "merge",      "split",
        "group",      "order",     "limit",      "count",     "average",    "maximum"};

// Words the tokenizers split, join, fold or keep in their own ways.
constexpr const char* kSpecialTokens[] = {"O'Neil's",
                                          "don't",
                                          "can't",
                                          "John's",
                                          "students'",
                                          "rock'n'roll",
                                          "U.S.A.",
                                          "e.g.",
                                          "Ph.D.",
                                          "e-mail",
                                          "state-of-the-art",
                                          "Wi-Fi",
                                          "wi_fi",
                                          "snake_case",
                                          "__init__",
                                          "camelCaseWord",
                                          "XMLHttpRequest",
                                          "PowerShot",
                                          "SD500",
                                          "j2se",
                                          "iPhone",
                                          "ABC123",
                                          "123abc",
                                          "4G",
                                          "mp3",
                                          "H2O",
                                          "3.14",
                                          "1,000",
                                          "1,000,000.50",
                                          "-42",
                                          "+7",
                                          "2026-11-03",
                                          "12:30:45",
                                          "v1.2.3",
                                          "10%",
                                          "$5.00",
                                          "#hashtag",
                                          "@mention",
                                          "foo.bar@example.com",
                                          "user+tag@sub.example.org",
                                          "https://doris.apache.org/docs/?q=index&x=1",
                                          "http://127.0.0.1:8030/api",
                                          "www.example.com",
                                          "C:\\Windows\\System32",
                                          "/usr/local/bin",
                                          "x86_64",
                                          "0x7FFF",
                                          "1e10",
                                          "21st",
                                          "AT&T",
                                          "R&D",
                                          "C++",
                                          "C#",
                                          ".NET",
                                          "Node.js",
                                          "...",
                                          "--",
                                          "->",
                                          "&&",
                                          "!=",
                                          "(parenthesized)",
                                          "[bracketed]",
                                          "{braced}",
                                          "\"quoted\"",
                                          "'single'",
                                          "`backtick`",
                                          "naïve",
                                          "café",
                                          "résumé",
                                          "Ångström",
                                          "Straße",
                                          "ŁÓDŹ",
                                          "İstanbul",
                                          "ΑΒΓ",
                                          "Привет",
                                          "ﬁnance",
                                          "Ⅻ",
                                          "①",
                                          "™",
                                          "½"};

constexpr const char* kAsciiSeparators[] = {" ",  " ",  " ",  " ",   "  ",   "\t", ", ",
                                            ". ", "; ", ": ", " - ", "/",    " (", ") ",
                                            "\"", "'",  "! ", "? ",  " \r ", "\f", "\v"};

constexpr const char* kHanWords[] = {
        "我们",     "你们",     "他们",   "数据库",   "数据", "分析",     "查询",   "索引",
        "倒排索引", "分词",     "分词器", "中文",     "英文", "日文",     "用户",   "系统",
        "时间",     "今天",     "明天",   "天气",     "很好", "非常",     "研究",   "生命",
        "起源",     "结婚",     "和尚",   "未结婚",   "的",   "了",       "是",     "在",
        "有",       "和",       "就",     "不",       "人",   "都",       "一",     "一个",
        "上",       "也",       "很",     "到",       "说",   "要",       "去",     "你",
        "会",       "着",       "没有",   "看",       "好",   "自己",     "这",     "那",
        "中华",     "人民",     "共和国", "北京",     "上海", "广州",     "深圳",   "杭州",
        "南京",     "长江",     "大桥",   "市长",     "大学", "清华大学", "计算机", "软件",
        "硬件",     "开发",     "测试",   "性能",     "优化", "存储",     "计算",   "分离",
        "云原生",   "实时",     "分析型", "数据仓库", "湖仓", "高并发",   "低延迟", "吞吐量",
        "工程师",   "程序员",   "产品",   "经理",     "公司", "手机",     "电脑",   "网络",
        "互联网",   "人工智能", "机器",   "学习",     "深度", "自然语言", "处理",   "搜索引擎",
        "全文检索", "相关性",   "打分",   "排序",     "结果", "问题",     "解决",   "方案",
        "设计",     "实现",     "验证",   "发布",     "版本", "升级",     "回滚",   "兼容",
        "格式",     "文件",     "目录",   "内存",     "线程", "进程"};

constexpr const char* kMixedTokens[] = {"B超",       "T恤",      "X光",    "卡拉OK", "U盘",
                                        "QQ号",      "iPhone15", "Win10",  "5G网络", "3D打印",
                                        "A股",       "PM2.5",    "AI芯片", "C语言",  "Doris2.1",
                                        "MySQL协议", "SQL查询",  "b超",    "t恤",    "Ｂ超"};

constexpr const char* kQuantities[] = {"三个人",       "5公斤",         "二十八块钱", "一百五十万",
                                       "第3名",        "2026年11月3日", "百分之五十", "3.5亿元",
                                       "两千零二十六", "十几个",        "半斤八两",   "1,000元",
                                       "12.5%"};

constexpr const char* kFullWidth[] = {
        "ＡＢＣ", "ａｂｃ", "１２３", "Ｄｏｒｉｓ", "ＨＥＬＬＯ　ＷＯＲＬＤ", "Ｃ＋＋", "＃１"};

constexpr const char* kCjkPunctuation[] = {"，", "。", "！", "？", "：", "；", "（", "）",
                                           "【", "】", "《", "》", "“",  "”",  "‘",  "’",
                                           "、", "…",  "——", "·",  "～", "　"};

// Sentences the Chinese segmenters are known to cut differently.
constexpr const char* kChineseSentences[] = {
        "我来到北京清华大学",
        "他来到了网易杭研大厦",
        "小明硕士毕业于中国科学院计算所，后在日本京都大学深造",
        "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作",
        "南京市长江大桥",
        "结婚的和尚未结婚的",
        "研究生命起源",
        "中华人民共和国万岁",
        "永和服装饰品有限公司",
        "乒乓球拍卖完了",
        "这个门把手坏了请把手拿开",
        "Apache Doris是一个基于MPP架构的高性能、实时的分析型数据库",
        "我在用iPhone15拍照，B超检查显示正常，T恤是XL码的",
        "ＡＢＣ公司的Ｄｏｒｉｓ集群有１２３个节点",
        "资料库與搜尋引擎：繁體中文的臺灣電腦軟體",
        "한국어 데이터베이스 검색 엔진 안녕하세요",
        "㐀㑇䶵是扩展A区的汉字，𠀀𠮷𪚥𩸽是扩展B区的汉字",
        "豈更車賈滑串句龜",
        "ᄀ"
        "ᅡ"
        "ᆨ"
        "와 한글 자모"};

constexpr const char* kKoreanWords[] = {"한국어",     "데이터베이스", "검색",   "엔진",
                                        "안녕하세요", "세계",         "도리스", "분석"};

// Words of the fixed kuromoji dictionary the golden environment builds, and the particles,
// auxiliaries and fillers it drops.
constexpr const char* kJapaneseWords[] = {"東京",
                                          "東",
                                          "京都",
                                          "京",
                                          "都",
                                          "大学",
                                          "東京大学",
                                          "関西",
                                          "国際",
                                          "空港",
                                          "関西国際空港",
                                          "日本",
                                          "日本語",
                                          "私",
                                          "今日",
                                          "天気",
                                          "いい",
                                          "食べ",
                                          "食べる",
                                          "住ん",
                                          "行っ",
                                          "行く",
                                          "コンピューター",
                                          "コンピュータ",
                                          "データベース",
                                          "データ",
                                          "ベース",
                                          "学生",
                                          "は",
                                          "に",
                                          "を",
                                          "の",
                                          "が",
                                          "で",
                                          "です",
                                          "ます",
                                          "まし",
                                          "た",
                                          "い",
                                          "、",
                                          "。",
                                          "えーと",
                                          "そして",
                                          "ＡＢＣ",
                                          "三",
                                          "百",
                                          "二十",
                                          "一",
                                          "円",
                                          "人"};

constexpr const char* kJapaneseSentences[] = {"私は東京大学に行きます",
                                              "東京都に住んでいます",
                                              "今日はいい天気です",
                                              "関西国際空港",
                                              "コンピューターのデータベース",
                                              "京都の大学に行った",
                                              "えーと、そして東京",
                                              "ＡＢＣの三百二十一円",
                                              "東京大学の学生",
                                              "人々は時々いすゞに乗る",
                                              "ｺﾝﾋﾟｭｰﾀｰ",
                                              "ﾊﾞｰﾄﾞｳｫｯﾁﾝｸﾞ",
                                              "ヴァイオリン",
                                              "ぁぃぅぇぉっゃゅょゎ",
                                              "Doris は MPP データベースです",
                                              "東京スカイツリー",
                                              "一二三四五六七八九十百千万億兆",
                                              "〇",
                                              "・",
                                              "「東京」『京都』",
                                              "ハローワールド！",
                                              "ＤＯＲＩＳ　ｄｏｒｉｓ"};

constexpr std::string_view kHiragana =
        "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわを"
        "んがぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽ";
constexpr std::string_view kKatakana =
        "アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲ"
        "ンガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポー";
constexpr std::string_view kKanji =
        "日本東京大学生語人時今天気食住行国際空港関西京都山川田中村本木林森水火土金月年先私者会社電"
        "車駅道路市町村県府北南西夏冬春秋花鳥風雲雪雨";

// A random run of `count` characters of a three-byte script.
std::string random_run(GoldenRandom& random, std::string_view script, size_t count) {
    const size_t characters = script.size() / 3;
    std::string out;
    for (size_t i = 0; i < count; ++i) {
        out.append(script.substr(random.below(static_cast<uint32_t>(characters)) * 3, 3));
    }
    return out;
}

std::string case_variant(GoldenRandom& random, std::string word) {
    switch (random.below(6)) {
    case 0:
        word[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(word[0])));
        break;
    case 1:
        for (char& c : word) {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        break;
    case 2:
        for (size_t i = 1; i < word.size(); i += 2) {
            word[i] = static_cast<char>(std::toupper(static_cast<unsigned char>(word[i])));
        }
        break;
    default:
        break;
    }
    return word;
}

CorpusFamily latin_words() {
    CorpusFamily family {.name = "latin_words", .lines = {}};
    for (const char* token : kSpecialTokens) {
        family.lines.emplace_back(token);
    }
    GoldenRandom random(0x1A71);
    for (int i = 0; i < 300; ++i) {
        std::string line;
        const uint32_t count = 1 + random.below(24);
        for (uint32_t j = 0; j < count; ++j) {
            if (j > 0) {
                line += pick(random, kAsciiSeparators);
            }
            if (random.below(10) < 7) {
                line += case_variant(random, pick(random, kEnglishWords));
            } else {
                line += pick(random, kSpecialTokens);
            }
        }
        family.lines.push_back(std::move(line));
    }
    return family;
}

std::string mixed_piece(GoldenRandom& random) {
    const uint32_t kind = random.below(100);
    if (kind < 50) {
        return pick(random, kHanWords);
    }
    if (kind < 70) {
        return case_variant(random, pick(random, kEnglishWords));
    }
    if (kind < 80) {
        return pick(random, kMixedTokens);
    }
    if (kind < 88) {
        return pick(random, kQuantities);
    }
    if (kind < 93) {
        return pick(random, kFullWidth);
    }
    return pick(random, kCjkPunctuation);
}

CorpusFamily mixed_zh_en() {
    CorpusFamily family {.name = "mixed_zh_en", .lines = {}};
    for (const char* token : kMixedTokens) {
        family.lines.emplace_back(token);
    }
    for (const char* token : kQuantities) {
        family.lines.emplace_back(token);
    }
    GoldenRandom random(0x21E5);
    for (int i = 0; i < 300; ++i) {
        std::string line;
        const uint32_t count = 2 + random.below(15);
        for (uint32_t j = 0; j < count; ++j) {
            if (j > 0) {
                const uint32_t separator = random.below(10);
                if (separator < 3) {
                    line += ' ';
                } else if (separator == 3) {
                    line += pick(random, kAsciiSeparators);
                }
            }
            line += mixed_piece(random);
        }
        family.lines.push_back(std::move(line));
    }
    return family;
}

CorpusFamily cjk_text() {
    CorpusFamily family {.name = "cjk_text", .lines = {}};
    for (const char* sentence : kChineseSentences) {
        family.lines.emplace_back(sentence);
    }
    for (const char* token : kFullWidth) {
        family.lines.emplace_back(token);
    }
    for (const char* token : kCjkPunctuation) {
        family.lines.emplace_back(token);
    }
    GoldenRandom random(0xC7C7);
    for (int i = 0; i < 250; ++i) {
        std::string line;
        const uint32_t count = 1 + random.below(16);
        for (uint32_t j = 0; j < count; ++j) {
            const uint32_t kind = random.below(20);
            if (kind < 14) {
                line += pick(random, kHanWords);
            } else if (kind < 16) {
                line += pick(random, kCjkPunctuation);
            } else if (kind < 18) {
                line += pick(random, kChineseSentences);
            } else {
                line += pick(random, kKoreanWords);
                line += ' ';
            }
        }
        family.lines.push_back(std::move(line));
    }
    return family;
}

CorpusFamily japanese_text() {
    CorpusFamily family {.name = "japanese_text", .lines = {}};
    for (const char* sentence : kJapaneseSentences) {
        family.lines.emplace_back(sentence);
    }
    for (const char* word : kJapaneseWords) {
        family.lines.emplace_back(word);
    }
    GoldenRandom random(0x7A9A);
    for (int i = 0; i < 200; ++i) {
        std::string line;
        const uint32_t count = 2 + random.below(8);
        for (uint32_t j = 0; j < count; ++j) {
            const uint32_t kind = random.below(20);
            if (kind < 9) {
                line += pick(random, kJapaneseWords);
            } else if (kind < 11) {
                line += random_run(random, kHiragana, 1 + random.below(6));
            } else if (kind < 13) {
                line += random_run(random, kKatakana, 2 + random.below(7));
            } else if (kind < 15) {
                line += random_run(random, kKanji, 1 + random.below(10));
            } else if (kind < 16) {
                line += std::to_string(random.below(10000));
            } else if (kind < 17) {
                line += case_variant(random, pick(random, kEnglishWords));
            } else if (kind < 18) {
                line += pick(random, kFullWidth);
            } else {
                line += pick(random, kJapaneseSentences);
            }
        }
        family.lines.push_back(std::move(line));
    }
    return family;
}

CorpusFamily invalid_utf8() {
    const std::vector<std::string> sequences = {
            bytes({0x80}),
            bytes({0xBF}),
            bytes({0x80, 0x80}),
            bytes({0xC0, 0x80}),                         // overlong NUL
            bytes({0xC1, 0xBF}),                         // overlong DEL
            bytes({0xC2}),                               // a lead byte and nothing after it
            bytes({0xC2, 0x41}),                         // a lead byte before ASCII
            bytes({0xE0, 0x80, 0x80}),                   // overlong, three bytes
            bytes({0xE0, 0x9F, 0xBF}),                   // overlong, three bytes
            bytes({0xE4, 0xB8}),                         // U+4E2D cut after two bytes
            bytes({0xE4, 0x41, 0xAD}),                   // a lead byte, ASCII, a continuation byte
            bytes({0xED, 0xA0, 0x80}),                   // high surrogate
            bytes({0xED, 0xBF, 0xBF}),                   // low surrogate
            bytes({0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80}), // U+1F600 as a CESU-8 pair
            bytes({0xF0, 0x80, 0x80, 0x80}),             // overlong, four bytes
            bytes({0xF0, 0x8F, 0xBF, 0xBF}),             // overlong, four bytes
            bytes({0xF0, 0x9F, 0x98}),                   // U+1F600 cut after three bytes
            bytes({0xF0, 0x9F}),                         // U+1F600 cut after two bytes
            bytes({0xF4, 0x90, 0x80, 0x80}),             // above U+10FFFF
            bytes({0xF5, 0x80, 0x80, 0x80}),
            bytes({0xF8, 0x88, 0x80, 0x80, 0x80}),       // five bytes
            bytes({0xFC, 0x84, 0x80, 0x80, 0x80, 0x80}), // six bytes
            bytes({0xFE}),
            bytes({0xFF}),
            bytes({0xFE, 0xFF}),
            bytes({0xFF, 0xFE}),
            bytes({0xEF, 0xBB, 0xBF}),       // a byte order mark, which is valid
            bytes({0xEF, 0xBF, 0xBE}),       // U+FFFE, a noncharacter, which is valid
            bytes({0xF4, 0x8F, 0xBF, 0xBF}), // U+10FFFF, which is valid
    };
    CorpusFamily family {.name = "invalid_utf8", .lines = {}};
    for (const std::string& s : sequences) {
        family.lines.push_back(s);
        family.lines.push_back(s + " abc 中文");
        family.lines.push_back("abc" + s + "def");
        family.lines.push_back("中" + s + "文");
        family.lines.push_back("abc " + s);
        family.lines.push_back("Ab" + s + "Cd ef");
        family.lines.push_back(s + s + s);
        family.lines.push_back("café" + s + "😀" + s);
    }
    GoldenRandom random(0xBAD8);
    for (int i = 0; i < 64; ++i) {
        std::string line;
        const uint32_t size = 1 + random.below(48);
        for (uint32_t j = 0; j < size; ++j) {
            line.push_back(static_cast<char>(random.below(256)));
        }
        family.lines.push_back(std::move(line));
    }
    for (int i = 0; i < 64; ++i) {
        std::string line;
        const uint32_t size = 1 + random.below(48);
        for (uint32_t j = 0; j < size; ++j) {
            line.push_back(static_cast<char>(random.below(4) == 0 ? 'a' + random.below(26)
                                                                  : 0x80 + random.below(128)));
        }
        family.lines.push_back(std::move(line));
    }
    return family;
}

CorpusFamily length_edges() {
    CorpusFamily family {.name = "length_edges", .lines = {}};
    for (size_t size :
         {1, 2, 15, 16, 17, 127, 128, 254, 255, 256, 257, 510, 511, 512, 1000, 1023, 1024, 1025}) {
        family.lines.emplace_back(size, 'a');
        family.lines.push_back("x " + std::string(size, 'b') + " y");
    }
    for (size_t size : {16383, 16384}) {
        family.lines.emplace_back(size, 'c');
    }
    for (size_t size : {254, 255, 256, 257, 510}) {
        family.lines.emplace_back(size, '7');
        family.lines.push_back(cycle_to("a1b2c3d4e5", size));
    }
    // A multi-byte character that ends at, straddles or starts at a byte boundary, after ASCII
    // letters and after Han text.
    for (size_t boundary : {254, 255, 256, 257}) {
        for (std::string_view character : {"é", "中", "あ", "😀"}) {
            for (size_t start = boundary - character.size(); start <= boundary; ++start) {
                family.lines.push_back(std::string(start, 'a') + std::string(character) + "bbbb");
                family.lines.push_back(han_to(start) + std::string(character) + "字字");
            }
        }
    }
    // A character outside the BMP, two UTF-16 code units, around the 255th code unit: the ICU
    // tokenizer counts its limit in code units. Han characters are one code unit each.
    for (size_t prefix : {253, 254, 255, 256}) {
        for (std::string_view character : {"😀", "𠀀"}) {
            family.lines.push_back(std::string(prefix, 'a') + std::string(character) + "bbbb");
            family.lines.push_back(repeat("汉", prefix) + std::string(character) + "字");
        }
    }
    for (size_t count : {84, 85, 86, 170, 171, 254, 255, 256}) {
        family.lines.push_back(repeat("汉", count));
        family.lines.push_back(repeat("か", count));
        family.lines.push_back(repeat("한", count));
    }
    // Whole values around the keyword tokenizer's 256-byte buffer.
    for (size_t size : {255, 256, 257, 512}) {
        family.lines.push_back(cycle_to("Ab cd, EF-gh 中文 ", size));
    }
    return family;
}

// Three-letter words that no stop word list contains.
std::string count_word(size_t index) {
    return std::string {'x', static_cast<char>('a' + index / 26 % 26),
                        static_cast<char>('a' + index % 26)};
}

CorpusFamily token_counts() {
    static constexpr std::string_view kHan = "数据库查询索引分词结构性能优化存储计算网络系统";
    CorpusFamily family {.name = "token_counts", .lines = {}};
    for (size_t count : {1, 255, 256, 257, 511, 512, 513}) {
        std::string words;
        std::string numbers;
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) {
                words += ' ';
                numbers += ' ';
            }
            words += count_word(i);
            numbers += std::to_string(i * 7);
        }
        family.lines.push_back(std::move(words));
        family.lines.push_back(std::move(numbers));
        family.lines.push_back(cycle_to(kHan, count * 3));
    }
    return family;
}

CorpusFamily chunk_boundaries() {
    // The IK segmenter reads 4096 bytes at a time and so do several readers: a character on the
    // boundary with every misalignment, after ASCII words and after Han text, and a dictionary
    // word cut at each of its characters.
    constexpr size_t kBoundary = 4096;
    static constexpr std::string_view kAsciiFiller = "lorem ipsum dolor sit amet consectetur ";
    CorpusFamily family {.name = "chunk_boundaries", .lines = {}};
    for (std::string_view character : {"é", "中", "😀"}) {
        for (size_t start = kBoundary - character.size(); start <= kBoundary; ++start) {
            family.lines.push_back(cycle_to(kAsciiFiller, start) + std::string(character) +
                                   " tail words");
            if (character.size() > 2) {
                family.lines.push_back(han_to(start) + std::string(character) + "结尾");
            }
        }
    }
    for (size_t cut = 1; cut < 7; ++cut) {
        family.lines.push_back(han_to(kBoundary - cut * 3) + "中华人民共和国万岁");
    }
    return family;
}

CorpusFamily dictionary_words(const CorpusSources& sources) {
    CorpusFamily family {.name = "dictionary_words", .lines = {}};
    // jieba, by frequency: an even sample over the frequency ranks, and the words that mix ASCII
    // with Han characters, which the chinese parser lowercases by its own rule.
    std::vector<std::pair<std::string, int64_t>> jieba;
    for (const std::string& line : read_lines(sources.jieba_dict)) {
        const size_t first_space = line.find(' ');
        DORIS_CHECK(first_space != std::string::npos) << line;
        const size_t second_space = line.find(' ', first_space + 1);
        jieba.emplace_back(
                line.substr(0, first_space),
                std::stoll(line.substr(first_space + 1, second_space - first_space - 1)));
    }
    DORIS_CHECK(jieba.size() > 10000) << sources.jieba_dict;
    std::ranges::stable_sort(jieba,
                             [](const auto& a, const auto& b) { return a.second > b.second; });
    std::vector<std::string> sampled;
    std::vector<std::string> with_ascii;
    for (size_t i = 0; i < jieba.size(); ++i) {
        const std::string& word = jieba[i].first;
        if (i % (jieba.size() / 1500) == 0) {
            sampled.push_back(word);
        }
        if (std::ranges::any_of(word, [](char c) {
                return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
            })) {
            with_ascii.push_back(word);
        }
    }
    // jieba has only a handful of them (B超, T恤, C++ and a few more): all of them, each also with
    // its letters in the other case.
    DORIS_CHECK(!with_ascii.empty()) << sources.jieba_dict;
    std::vector<std::string> ascii_words;
    for (const std::string& word : with_ascii) {
        std::string swapped = word;
        for (char& c : swapped) {
            if (c >= 'A' && c <= 'Z') {
                c = static_cast<char>(c - 'A' + 'a');
            } else if (c >= 'a' && c <= 'z') {
                c = static_cast<char>(c - 'a' + 'A');
            }
        }
        ascii_words.push_back(word);
        ascii_words.push_back(std::move(swapped));
    }
    append_word_lines(sampled, 8, &family.lines);
    append_word_lines(ascii_words, 4, &family.lines);

    const std::vector<std::string> ik_main = read_lines(sources.ik_main_dict);
    DORIS_CHECK(ik_main.size() > 10000) << sources.ik_main_dict;
    std::vector<std::string> ik_sampled;
    for (size_t i = 0; i < ik_main.size(); i += ik_main.size() / 1000) {
        ik_sampled.push_back(ik_main[i]);
    }
    append_word_lines(ik_sampled, 8, &family.lines);

    // Quantifiers after numerals, as the IK quantifier segmenter expects them.
    static constexpr const char* kNumerals[] = {"一", "三", "十二", "5",  "28",
                                                "两", "几", "第一", "3.5"};
    std::vector<std::string> quantities;
    const std::vector<std::string> quantifiers = read_lines(sources.ik_quantifier_dict);
    DORIS_CHECK(quantifiers.size() > 100) << sources.ik_quantifier_dict;
    for (size_t i = 0; i < quantifiers.size(); ++i) {
        quantities.push_back(kNumerals[i % std::size(kNumerals)] + quantifiers[i]);
    }
    append_word_lines(quantities, 6, &family.lines);
    return family;
}

CorpusFamily han_unknown() {
    static constexpr const char* kSeparators[] = {"", " ", "，", "。", "3", "x"};
    CorpusFamily family {.name = "han_unknown", .lines = {}};
    GoldenRandom random(0x4A11);
    for (int i = 0; i < 200; ++i) {
        std::string line;
        const uint32_t pieces = 1 + random.below(4);
        for (uint32_t j = 0; j < pieces; ++j) {
            if (j > 0) {
                line += pick(random, kSeparators);
            }
            const uint32_t count = 2 + random.below(15);
            for (uint32_t k = 0; k < count; ++k) {
                append_utf8(&line, random.below(8) == 0 ? 0x3400 + random.below(0x4DB6 - 0x3400)
                                                        : 0x4E00 + random.below(0x9FA6 - 0x4E00));
            }
        }
        family.lines.push_back(std::move(line));
    }
    return family;
}

// Each code point between Latin letters and between Han characters, sixteen code points a line.
std::vector<std::string> code_point_lines(const std::vector<uint32_t>& code_points) {
    std::vector<std::string> lines;
    for (size_t start = 0; start < code_points.size(); start += 16) {
        std::string line;
        for (size_t i = start; i < std::min(code_points.size(), start + 16); ++i) {
            line += 'a';
            append_utf8(&line, code_points[i]);
            line += "b 中";
            append_utf8(&line, code_points[i]);
            line += "文 ";
        }
        lines.push_back(std::move(line));
    }
    return lines;
}

CorpusFamily bmp_code_points() {
    // A sample of the BMP, dense where character classes change often. Every code point is in
    // the full scan.
    struct Range {
        uint32_t first;
        uint32_t last;
        uint32_t stride;
    };
    static constexpr Range kRanges[] = {
            {.first = 0x0000, .last = 0x007F, .stride = 1}, // ASCII
            {.first = 0x0080,
             .last = 0x2FFF,
             .stride = 5}, // Latin to the CJK radicals: scripts, punctuation, symbols
            {.first = 0x3000,
             .last = 0x30FF,
             .stride = 1}, // CJK symbols and punctuation, hiragana, katakana
            {.first = 0x3100,
             .last = 0x9FFF,
             .stride = 61}, // bopomofo, CJK extension A and unified ideographs
            {.first = 0xA000, .last = 0xABFF, .stride = 7},   // Yi to Meetei Mayek
            {.first = 0xAC00, .last = 0xD7AF, .stride = 127}, // Hangul syllables
            {.first = 0xD7B0, .last = 0xD7FF, .stride = 3},   // Hangul jamo extended B
            {.first = 0xD800, .last = 0xF8FF, .stride = 127}, // surrogates and private use
            {.first = 0xF900, .last = 0xFFFF, .stride = 3}, // compatibility ideographs to specials
    };
    std::vector<uint32_t> code_points;
    for (const Range& range : kRanges) {
        for (uint32_t code_point = range.first; code_point <= range.last;
             code_point += range.stride) {
            code_points.push_back(code_point);
        }
    }
    return {.name = "bmp_code_points", .lines = code_point_lines(code_points)};
}

CorpusFamily uax29_classes() {
    // A sample or two of every word break class of UAX #29 and of the classes the standard
    // tokenizer adds (ideographs, hiragana, South East Asian scripts, Hangul, emoji).
    static constexpr uint32_t kSamples[] = {0x000D,  0x000A,  0x000B,  0x2028, // CR, LF, Newline
                                            0x0301,  0x0903,  0x1F3FB,         // Extend
                                            0x200D,                            // ZWJ
                                            0x1F1E8,                           // Regional_Indicator
                                            0x00AD,  0x200E,                   // Format
                                            0x30A2,  0x30FC,  0xFF71,          // Katakana
                                            0x05D0,                            // Hebrew_Letter
                                            'a',     'Z',     0x00E9,  0x0430, 0x0905, // ALetter
                                            '\'',    '"',             // Single_Quote, Double_Quote
                                            '.',     0x2019,          // MidNumLet
                                            ':',     0x00B7,          // MidLetter
                                            ',',     ';',             // MidNum
                                            '0',     0x0660,  0xFF10, // Numeric
                                            '_',                      // ExtendNumLet
                                            ' ',     0x3000,          // WSegSpace
                                            0x4E00,  0x20000,         // ideographs
                                            0x3042,                   // hiragana
                                            0x0E01,  0x1000,          // South East Asian
                                            0xAC00,  0x1100,          // Hangul
                                            0x1F600, 0x2764,          // Extended_Pictographic
                                            '-',     '@',     0x3002,  '$',    0xE000}; // others
    static constexpr uint32_t kJoiners[] = {'\'', '"', '.',    0x2019, ':',    0x00B7, ',',
                                            ';',  '_', 0x0301, 0x00AD, 0x200D, 0x200E, 0x1F3FB};
    static constexpr uint32_t kWordSamples[] = {'a',    0x00E9, 0x05D0, '0',    0x0660,  0x30A2,
                                                0x4E00, 0x3042, 0xAC00, 0x0E01, 0x1F600, '_'};
    CorpusFamily family {.name = "uax29_classes", .lines = {}};
    // Every ordered pair, between spaces.
    for (uint32_t first : kSamples) {
        std::string line;
        for (uint32_t second : kSamples) {
            append_utf8(&line, first);
            append_utf8(&line, second);
            line += ' ';
        }
        family.lines.push_back(std::move(line));
    }
    // Word, joiner, word: the rules that look two characters ahead.
    for (uint32_t joiner : kJoiners) {
        for (uint32_t first : kWordSamples) {
            std::string line;
            for (uint32_t second : kWordSamples) {
                append_utf8(&line, first);
                append_utf8(&line, joiner);
                append_utf8(&line, second);
                line += ' ';
            }
            family.lines.push_back(std::move(line));
        }
    }
    // Sequences that join several code points into one grapheme, built from code points because
    // their joiners are invisible.
    for (const std::vector<uint32_t>& sequence : std::vector<std::vector<uint32_t>> {
                 {0x1F1E8, 0x1F1F3},                          // a flag
                 {0x1F1E8, 0x1F1F3, 0x1F1FA, 0x1F1F8},        // two flags
                 {0x1F1E8, 0x1F1F3, 0x1F1FA},                 // a flag and a lone indicator
                 {'a', 0x1F1E8, 0x1F1F3, 'b'},                // a flag between letters
                 {0x1F468, 0x200D, 0x1F469, 0x200D, 0x1F467}, // a ZWJ family
                 {0x1F3F3, 0xFE0F, 0x200D, 0x1F308},          // a ZWJ flag
                 {'a', 0x200D, 'b'},                          // a ZWJ between letters
                 {0x1F44D, 0x1F3FB},                          // a skin tone modifier
                 {'1', 0xFE0F, 0x20E3},                       // a keycap
                 {'x', 0x0327, 0x0300, 'y'}}) {               // two combining marks
        std::string line;
        for (uint32_t code_point : sequence) {
            append_utf8(&line, code_point);
        }
        family.lines.push_back(std::move(line));
    }
    return family;
}

CorpusFamily edge_lines() {
    CorpusFamily family {.name = "edge_lines", .lines = {}};
    for (const char* line : {"", " ", "\t", " \t\r\n\f\v ", "\r\n", "\n\n", "...", "!!!", "-", "_",
                             "'", "\"", "a", "A", "中", "é", "😀", "\xEF\xBB\xBF",
                             "\xEF\xBB\xBF"
                             "abc"}) {
        family.lines.emplace_back(line);
    }
    for (std::string_view line :
         {std::string_view("\0", 1), std::string_view("a\0b", 3), std::string_view("\0abc", 4),
          std::string_view("abc\0", 4), std::string_view("中\0文", 7),
          std::string_view("\0\0\0", 3), std::string_view("a\0\0b c\0d", 9)}) {
        family.lines.emplace_back(line);
    }
    family.lines.emplace_back(4097, 'q');
    family.lines.push_back(repeat("ab ", 1366));
    family.lines.push_back(
            cycle_to("Apache Doris是一个MPP分析型数据库，support full-text search. ", 5000));
    family.lines.push_back(han_to(8193));
    family.lines.push_back(
            cycle_to("the quick brown fox 跳过了 lazy dogs 和 小猫, 2026年 ", 20000));
    family.lines.push_back(repeat("xyz ", 1100) + std::string(1, '\0') + repeat("abc ", 100));
    return family;
}

} // namespace

std::vector<CorpusFamily> default_families(const CorpusSources& sources) {
    std::vector<CorpusFamily> families;
    families.push_back(latin_words());
    families.push_back(mixed_zh_en());
    families.push_back(cjk_text());
    families.push_back(japanese_text());
    families.push_back(invalid_utf8());
    families.push_back(length_edges());
    families.push_back(token_counts());
    families.push_back(chunk_boundaries());
    families.push_back(dictionary_words(sources));
    families.push_back(han_unknown());
    families.push_back(bmp_code_points());
    families.push_back(uax29_classes());
    families.push_back(edge_lines());
    return families;
}

CorpusFamily full_code_point_family() {
    std::vector<uint32_t> code_points(0x110000);
    for (uint32_t code_point = 0; code_point < code_points.size(); ++code_point) {
        code_points[code_point] = code_point;
    }
    return {.name = "all_code_points", .lines = code_point_lines(code_points)};
}

} // namespace doris::segment_v2::inverted_index::golden
