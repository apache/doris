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

#include "storage/index/inverted/analysis/golden/golden_driver.h"

#include <CLucene.h>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <map>
#include <stdexcept>
#include <thread>
#include <typeindex>
#include <utility>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/analysis_factory_mgr.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/char_filter/char_filter_factory.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/reader.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/token_filter/token_filter_factory.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"
#include "storage/index/inverted/clucene/lucene_backed_text_analyzer.h"
#include "util/hash_util.hpp"

namespace doris::segment_v2::inverted_index::golden {

const char* drive_mode_name(DriveMode mode) {
    return mode == DriveMode::FRESH ? "fresh" : "reused";
}

namespace {

using AnalyzerPtr = std::shared_ptr<lucene::analysis::Analyzer>;

void put_u32(std::string* out, uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
        out->push_back(static_cast<char>((value >> shift) & 0xFF));
    }
}

// The length of the valid UTF-8 sequence at `bytes[i]`, or 0 when the byte there starts none.
size_t utf8_sequence_length(std::string_view bytes, size_t i) {
    const auto at = [&](size_t k) { return static_cast<unsigned char>(bytes[i + k]); };
    const auto in = [](unsigned char c, unsigned char low, unsigned char high) {
        return c >= low && c <= high;
    };
    const size_t left = bytes.size() - i;
    const unsigned char b0 = at(0);
    if (b0 < 0x80) {
        return 1;
    }
    if (in(b0, 0xC2, 0xDF)) {
        return left >= 2 && in(at(1), 0x80, 0xBF) ? 2 : 0;
    }
    if (in(b0, 0xE0, 0xEF)) {
        const unsigned char low = b0 == 0xE0 ? 0xA0 : 0x80;
        const unsigned char high = b0 == 0xED ? 0x9F : 0xBF;
        return left >= 3 && in(at(1), low, high) && in(at(2), 0x80, 0xBF) ? 3 : 0;
    }
    if (in(b0, 0xF0, 0xF4)) {
        const unsigned char low = b0 == 0xF0 ? 0x90 : 0x80;
        const unsigned char high = b0 == 0xF4 ? 0x8F : 0xBF;
        return left >= 4 && in(at(1), low, high) && in(at(2), 0x80, 0xBF) && in(at(3), 0x80, 0xBF)
                       ? 4
                       : 0;
    }
    return 0;
}

void read_tokens(lucene::analysis::TokenStream* stream, lucene::analysis::Token* token,
                 GoldenLine* line) {
    while (stream->next(token) != nullptr) {
        line->tokens.push_back({std::string(token->termBuffer<char>(), token->termLength<char>()),
                                token->getPositionIncrement()});
    }
}

class LuceneDriver final : public GoldenDriver {
public:
    LuceneDriver(std::function<AnalyzerPtr()> make_analyzer, CharFilterMap char_filter_map)
            : _make_analyzer(std::move(make_analyzer)),
              _char_filter_map(std::move(char_filter_map)) {
        restart();
    }

    // As a query and the SNII writer read a value: a new stream on a new reader, without reset().
    GoldenLine fresh(std::string_view line) override {
        GoldenLine out;
        try {
            auto reader = InvertedIndexAnalyzer::create_reader(_char_filter_map);
            reader->init(line.data(), cast_set<int32_t>(line.size()), false);
            std::unique_ptr<lucene::analysis::TokenStream> stream(
                    _analyzer->tokenStream(L"", reader));
            lucene::analysis::Token token;
            read_tokens(stream.get(), &token, &out);
            stream->close();
        } catch (const CLuceneError&) {
            out = threw_line();
        } catch (const Exception&) {
            out = threw_line();
        }
        return out;
    }

    // As the V1 and V2 writers read a value: the analyzer's reusable stream on one reader, reset
    // before and closed after each value, with one token object for all values.
    GoldenLine reused(std::string_view line) override {
        GoldenLine out;
        try {
            _reader->init(line.data(), cast_set<int32_t>(line.size()), false);
            auto* stream = _analyzer->reusableTokenStream(L"", _reader);
            stream->reset();
            read_tokens(stream, _token.get(), &out);
            stream->close();
        } catch (const CLuceneError&) {
            out = threw_line();
        } catch (const Exception&) {
            out = threw_line();
        }
        if (out.threw) {
            // A writer gives up when a value throws, so nothing in production reads with a stream
            // after it threw: start over.
            restart();
        }
        return out;
    }

private:
    void restart() {
        _analyzer = _make_analyzer();
        _reader = InvertedIndexAnalyzer::create_reader(_char_filter_map);
        _token = std::make_unique<lucene::analysis::Token>();
    }

    std::function<AnalyzerPtr()> _make_analyzer;
    CharFilterMap _char_filter_map;
    AnalyzerPtr _analyzer;
    ReaderPtr _reader;
    std::unique_ptr<lucene::analysis::Token> _token;
};

class NormalizeDriver final : public GoldenDriver {
public:
    explicit NormalizeDriver(std::map<std::string, std::string> properties)
            : _properties(std::move(properties)) {}

    GoldenLine fresh(std::string_view line) override { return normalize(line); }
    GoldenLine reused(std::string_view line) override { return normalize(line); }

private:
    GoldenLine normalize(std::string_view line) const {
        try {
            return GoldenLine {.tokens = {{.bytes = InvertedIndexAnalyzer::normalize(
                                                   std::string(line), _properties),
                                           .position_increment = 1}}};
        } catch (const CLuceneError&) {
            return threw_line();
        } catch (const Exception&) {
            return threw_line();
        }
    }

    const std::map<std::string, std::string> _properties;
};

class TextDriver final : public GoldenDriver {
public:
    explicit TextDriver(TextAnalyzerPtr analyzer) : _analyzer(std::move(analyzer)) {}

    GoldenLine fresh(std::string_view line) override {
        std::unique_ptr<TextTokenStream> stream;
        return read(&stream, line);
    }

    GoldenLine reused(std::string_view line) override { return read(&_stream, line); }

private:
    // Reads `line` with `*stream`, made first if there is none. A stream is not used again after
    // it threw, so it is dropped then.
    GoldenLine read(std::unique_ptr<TextTokenStream>* stream, std::string_view line) {
        GoldenLine out;
        try {
            if (*stream == nullptr) {
                *stream = _analyzer->create_stream();
            }
            (*stream)->reset(line);
            while ((*stream)->next_batch(_batch)) {
                for (const TokenRef& token : _batch.tokens()) {
                    out.tokens.push_back({.bytes = std::string(token.term()),
                                          .position_increment = token.position_increment});
                }
            }
        } catch (const Exception&) {
            out = threw_line();
            stream->reset();
            _batch.clear();
        }
        _batch.release_oversized();
        return out;
    }

    const TextAnalyzerPtr _analyzer;
    std::unique_ptr<TextTokenStream> _stream;
    TokenBatch _batch;
};

// The analyzer configuration the writers read from `properties`, except that an unset lower_case
// stays unset, as on the query path; lower_case=true covers what the writers pass.
InvertedIndexAnalyzerConfig analyzer_config_of(
        const std::map<std::string, std::string>& properties) {
    InvertedIndexAnalyzerConfig analyzer_config;
    analyzer_config.analyzer_name = get_analyzer_name_from_properties(properties);
    analyzer_config.parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(properties));
    analyzer_config.parser_mode = get_parser_mode_string_from_properties(properties);
    analyzer_config.lower_case = get_parser_lowercase_from_properties(properties);
    analyzer_config.stop_words = get_parser_stopwords_from_properties(properties);
    analyzer_config.char_filter_map = get_parser_char_filter_map_from_properties(properties);
    return analyzer_config;
}

AnalyzerPtr build_analyzer(const InvertedIndexAnalyzerConfig& analyzer_config) {
    return InvertedIndexAnalyzer::create_analyzer_provider(&analyzer_config)->get_analyzer();
}

} // namespace

FamilyDigest digest_family(GoldenDriver& driver, DriveMode mode, const CorpusFamily& family,
                           std::string* dump) {
    FamilyDigest result;
    std::string encoded;
    for (size_t i = 0; i < family.lines.size(); ++i) {
        GoldenLine line;
        try {
            line = driver.drive(mode, family.lines[i]);
        } catch (const std::exception& e) {
            // Not an error production catches, so not a "threw" line: name the line and let the
            // caller report it.
            throw std::runtime_error(fmt::format("{} line {} \"{}\": {}", family.name, i,
                                                 escape_bytes(family.lines[i]), e.what()));
        }
        encoded.clear();
        encoded.push_back(line.threw ? '\1' : '\0');
        if (!line.threw) {
            put_u32(&encoded, cast_set<uint32_t>(line.tokens.size()));
            for (const GoldenToken& token : line.tokens) {
                put_u32(&encoded, cast_set<uint32_t>(token.bytes.size()));
                encoded += token.bytes;
                put_u32(&encoded, static_cast<uint32_t>(token.position_increment));
            }
            result.tokens += line.tokens.size();
        }
        result.digest = HashUtil::xxHash64WithSeed(encoded.data(), encoded.size(), result.digest);
        if (dump != nullptr) {
            *dump += fmt::format("{}\t{}\t{}\n", i, escape_bytes(family.lines[i]),
                                 format_line(line));
        }
    }
    return result;
}

std::string escape_bytes(std::string_view bytes) {
    std::string out;
    for (size_t i = 0; i < bytes.size();) {
        const auto c = static_cast<unsigned char>(bytes[i]);
        const size_t length = utf8_sequence_length(bytes, i);
        if (length > 1) {
            out.append(bytes.substr(i, length));
            i += length;
            continue;
        }
        if (c == '\\' || c == '"') {
            out += '\\';
            out += static_cast<char>(c);
        } else if (length == 1 && c >= 0x20 && c < 0x7F) {
            out += static_cast<char>(c);
        } else {
            out += fmt::format("\\x{:02X}", c);
        }
        ++i;
    }
    return out;
}

std::string format_line(const GoldenLine& line) {
    if (line.threw) {
        return "threw";
    }
    std::string out;
    for (const GoldenToken& token : line.tokens) {
        if (!out.empty()) {
            out += ' ';
        }
        out += fmt::format("+{}\"{}\"", token.position_increment, escape_bytes(token.bytes));
    }
    return out;
}

Status make_golden_driver(const GoldenConfig& config, std::unique_ptr<GoldenDriver>* driver) {
    if (config.kind == GoldenKind::NORMALIZE) {
        *driver = std::make_unique<NormalizeDriver>(config.properties);
        return Status::OK();
    }
    const InvertedIndexAnalyzerConfig analyzer_config = analyzer_config_of(config.properties);
    try {
        *driver = std::make_unique<LuceneDriver>(
                [analyzer_config]() { return build_analyzer(analyzer_config); },
                analyzer_config.char_filter_map);
    } catch (const CLuceneError& e) {
        return Status::InternalError("building {} failed: {}", config.name, e.what());
    } catch (const Exception& e) {
        return Status::InternalError("building {} failed: {}", config.name, e.what());
    }
    return Status::OK();
}

Status make_golden_text_analyzer(const GoldenConfig& config, TextAnalyzerPtr* analyzer) {
    DORIS_CHECK(config.kind == GoldenKind::ANALYZE) << config.name;
    const InvertedIndexAnalyzerConfig analyzer_config = analyzer_config_of(config.properties);
    try {
        *analyzer = std::make_shared<LuceneBackedTextAnalyzer>(build_analyzer(analyzer_config),
                                                               analyzer_config.char_filter_map);
    } catch (const CLuceneError& e) {
        return Status::InternalError("building {} failed: {}", config.name, e.what());
    } catch (const Exception& e) {
        return Status::InternalError("building {} failed: {}", config.name, e.what());
    }
    return Status::OK();
}

std::unique_ptr<GoldenDriver> make_text_driver(TextAnalyzerPtr analyzer) {
    return std::make_unique<TextDriver>(std::move(analyzer));
}

Status make_golden_text_driver(const GoldenConfig& config, std::unique_ptr<GoldenDriver>* driver) {
    TextAnalyzerPtr analyzer;
    RETURN_IF_ERROR(make_golden_text_analyzer(config, &analyzer));
    *driver = make_text_driver(std::move(analyzer));
    return Status::OK();
}

std::vector<std::pair<std::string, std::string>> registered_components() {
    auto& factories = AnalysisFactoryMgr::instance();
    factories.initialise();
    std::vector<std::pair<std::string, std::string>> components;
    for (const auto& [key, creator] : factories.registry_) {
        const std::type_index& type = key.first;
        DORIS_CHECK(type == typeid(TokenizerFactory) || type == typeid(TokenFilterFactory) ||
                    type == typeid(CharFilterFactory))
                << type.name();
        const char* kind = "char_filter";
        if (type == typeid(TokenizerFactory)) {
            kind = "tokenizer";
        } else if (type == typeid(TokenFilterFactory)) {
            kind = "token_filter";
        }
        components.emplace_back(kind, key.second);
    }
    return components;
}

std::unique_ptr<GoldenDriver> make_lucene_driver(std::function<AnalyzerPtr()> make_analyzer) {
    return std::make_unique<LuceneDriver>(std::move(make_analyzer), CharFilterMap {});
}

void parallel_for(size_t count, const std::function<void(size_t)>& body) {
    std::atomic<size_t> next {0};
    std::vector<std::thread> workers;
    const size_t threads = std::max<size_t>(1, std::min(golden_threads(), count));
    for (size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&] {
            for (size_t i = next.fetch_add(1); i < count; i = next.fetch_add(1)) {
                body(i);
            }
        });
    }
    for (std::thread& worker : workers) {
        worker.join();
    }
}

size_t golden_threads() {
    if (const char* threads = std::getenv("ANALYSIS_GOLDEN_THREADS"); threads != nullptr) {
        return std::max(1, std::atoi(threads));
    }
    return std::clamp<size_t>(std::thread::hardware_concurrency(), 1, 16);
}

} // namespace doris::segment_v2::inverted_index::golden
