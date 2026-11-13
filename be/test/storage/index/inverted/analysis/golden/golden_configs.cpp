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

#include "storage/index/inverted/analysis/golden/golden_configs.h"

#include <cctype>
#include <numeric>
#include <utility>

namespace doris::segment_v2::inverted_index::golden {

namespace {

using Properties = std::map<std::string, std::string>;

// A tokenizer, token filter or char filter, by type and settings. It has a constructor so that the
// matrix can write a component as {"lowercase"} or {"standard", {{"max_token_length", "5"}}}.
struct Component {
    Component(std::string type_name, Properties component_settings = {})
            : type(std::move(type_name)), settings(std::move(component_settings)) {}

    std::string type;
    Properties settings;
};

// Policy names are global in IndexPolicyMgr, so each configuration prefixes its own.
std::string policy_prefix(const std::string& config_name) {
    std::string prefix = "golden_";
    for (char c : config_name) {
        const auto u = static_cast<unsigned char>(c);
        prefix += std::isalnum(u) ? static_cast<char>(std::tolower(u)) : '_';
    }
    return prefix;
}

// The name an analyzer or normalizer policy gives a component: a policy of its own when the
// component has settings, otherwise the builtin component's name, as users write it.
std::string component_name(const Component& component, const std::string& policy_name,
                           TIndexPolicyType::type type, std::vector<GoldenPolicy>* policies) {
    if (component.settings.empty()) {
        return component.type;
    }
    GoldenPolicy policy {.name = policy_name, .type = type, .properties = component.settings};
    policy.properties["type"] = component.type;
    policies->push_back(std::move(policy));
    return policy_name;
}

std::string component_names(const std::vector<Component>& components, const std::string& prefix,
                            TIndexPolicyType::type type, std::vector<GoldenPolicy>* policies) {
    std::string names;
    for (size_t i = 0; i < components.size(); ++i) {
        if (i > 0) {
            names += ", ";
        }
        names += component_name(components[i], prefix + std::to_string(i), type, policies);
    }
    return names;
}

void add_filters(const std::vector<Component>& token_filters,
                 const std::vector<Component>& char_filters, const std::string& prefix,
                 GoldenPolicy* policy, std::vector<GoldenPolicy>* policies) {
    if (!token_filters.empty()) {
        policy->properties["token_filter"] = component_names(
                token_filters, prefix + "_token_filter", TIndexPolicyType::TOKEN_FILTER, policies);
    }
    if (!char_filters.empty()) {
        policy->properties["char_filter"] = component_names(
                char_filters, prefix + "_char_filter", TIndexPolicyType::CHAR_FILTER, policies);
    }
}

// A custom analyzer named after the configuration. `index_properties` adds index properties,
// such as the index's own character filter.
GoldenConfig custom(const std::string& name, const Component& tokenizer,
                    const std::vector<Component>& token_filters = {},
                    const std::vector<Component>& char_filters = {},
                    const Properties& index_properties = {},
                    GoldenKind kind = GoldenKind::ANALYZE) {
    GoldenConfig config {
            .name = name, .kind = kind, .properties = index_properties, .policies = {}};
    const std::string prefix = policy_prefix(name);
    GoldenPolicy analyzer {.name = prefix, .type = TIndexPolicyType::ANALYZER, .properties = {}};
    analyzer.properties["tokenizer"] = component_name(
            tokenizer, prefix + "_tokenizer", TIndexPolicyType::TOKENIZER, &config.policies);
    add_filters(token_filters, char_filters, prefix, &analyzer, &config.policies);
    config.policies.push_back(std::move(analyzer));
    config.properties["analyzer"] = prefix;
    return config;
}

// A custom normalizer named after the configuration.
GoldenConfig normalizer(const std::string& name, const std::vector<Component>& token_filters,
                        const std::vector<Component>& char_filters, GoldenKind kind,
                        const Properties& index_properties = {}) {
    GoldenConfig config {
            .name = name, .kind = kind, .properties = index_properties, .policies = {}};
    const std::string prefix = policy_prefix(name);
    GoldenPolicy policy {.name = prefix, .type = TIndexPolicyType::NORMALIZER, .properties = {}};
    add_filters(token_filters, char_filters, prefix, &policy, &config.policies);
    config.policies.push_back(std::move(policy));
    config.properties["normalizer"] = prefix;
    return config;
}

const Properties kIndexCharReplace = {{"char_filter_type", "char_replace"},
                                      {"char_filter_pattern", "-_"},
                                      {"char_filter_replacement", " "}};

Properties with(Properties properties, const Properties& more) {
    properties.insert(more.begin(), more.end());
    return properties;
}

struct Switch {
    const char* name;
    bool default_value;
};

constexpr Switch kPinyinSwitches[] = {
        {.name = "keep_first_letter", .default_value = true},
        {.name = "keep_separate_first_letter", .default_value = false},
        {.name = "keep_full_pinyin", .default_value = true},
        {.name = "keep_joined_full_pinyin", .default_value = false},
        {.name = "keep_none_chinese", .default_value = true},
        {.name = "keep_none_chinese_together", .default_value = true},
        {.name = "keep_none_chinese_in_first_letter", .default_value = true},
        {.name = "keep_none_chinese_in_joined_full_pinyin", .default_value = false},
        {.name = "none_chinese_pinyin_tokenize", .default_value = true},
        {.name = "keep_original", .default_value = false},
        {.name = "lowercase", .default_value = true},
        {.name = "trim_whitespace", .default_value = true},
        {.name = "remove_duplicated_term", .default_value = false},
        {.name = "ignore_pinyin_offset", .default_value = true}, // false is the strict mode
        {.name = "fixed_pinyin_offset", .default_value = false},
        {.name = "keep_separate_chinese", .default_value = false},
};

constexpr Switch kWordDelimiterSwitches[] = {
        {.name = "generate_word_parts", .default_value = true},
        {.name = "generate_number_parts", .default_value = true},
        {.name = "catenate_words", .default_value = false},
        {.name = "catenate_numbers", .default_value = false},
        {.name = "catenate_all", .default_value = false},
        {.name = "split_on_case_change", .default_value = true},
        {.name = "preserve_original", .default_value = false},
        {.name = "split_on_numerics", .default_value = true},
        {.name = "stem_english_possessive", .default_value = true},
};

// The settings of each row of a pairwise array over `switches`: the switches a row flips, set to
// the values that are not their defaults. Row 0 sets nothing.
template <size_t N>
std::vector<Properties> switch_rows(const Switch (&switches)[N]) {
    std::vector<Properties> rows;
    for (const std::vector<bool>& row : pairwise_rows(N)) {
        Properties settings;
        for (size_t i = 0; i < N; ++i) {
            if (row[i]) {
                settings[switches[i].name] = switches[i].default_value ? "false" : "true";
            }
        }
        rows.push_back(std::move(settings));
    }
    return rows;
}

void add_builtin_configs(std::vector<GoldenConfig>* configs) {
    struct Parser {
        const char* parser;
        const char* mode; // nullptr: not set
    };
    static constexpr Parser kParsers[] = {
            {.parser = "english", .mode = nullptr},
            {.parser = "standard", .mode = nullptr},
            {.parser = "unicode", .mode = nullptr},
            {.parser = "basic", .mode = nullptr},
            {.parser = "icu", .mode = nullptr},
            {.parser = "chinese", .mode = "coarse_grained"},
            {.parser = "chinese", .mode = "fine_grained"},
            {.parser = "ik", .mode = "ik_smart"},
            {.parser = "ik", .mode = "ik_max_word"},
            {.parser = "kuromoji", .mode = "search"},
            {.parser = "kuromoji", .mode = "normal"},
            {.parser = "kuromoji", .mode = "extended"},
    };
    static constexpr const char* kLowerCase[] = {nullptr, "true", "false"};
    // Pairwise over parser, lower_case and stopwords: every parser meets every lower_case value
    // and both stopwords values, and the alternation makes every lower_case value meet both
    // stopwords values.
    for (size_t p = 0; p < std::size(kParsers); ++p) {
        for (size_t l = 0; l < std::size(kLowerCase); ++l) {
            const bool stopwords_none = (p + l) % 2 == 1;
            GoldenConfig config;
            config.name = std::string("builtin/") + kParsers[p].parser;
            config.properties["parser"] = kParsers[p].parser;
            if (kParsers[p].mode != nullptr) {
                config.name += std::string("/") + kParsers[p].mode;
                config.properties["parser_mode"] = kParsers[p].mode;
            }
            config.name += std::string("/lower_case=") +
                           (kLowerCase[l] == nullptr ? "unset" : kLowerCase[l]);
            if (kLowerCase[l] != nullptr) {
                config.properties["lower_case"] = kLowerCase[l];
            }
            config.name += stopwords_none ? "/stopwords=none" : "/stopwords=unset";
            if (stopwords_none) {
                config.properties["stopwords"] = "none";
            }
            configs->push_back(std::move(config));
        }
    }
    // The index's own character filter in front of a builtin parser.
    for (const auto& [parser, mode] :
         std::vector<std::pair<std::string, std::string>> {{"english", ""},
                                                           {"standard", ""},
                                                           {"chinese", "fine_grained"},
                                                           {"ik", "ik_smart"}}) {
        GoldenConfig config {.name = "builtin/" + parser + "/index_char_replace",
                             .kind = GoldenKind::ANALYZE,
                             .properties = with({{"parser", parser}}, kIndexCharReplace),
                             .policies = {}};
        if (!mode.empty()) {
            config.properties["parser_mode"] = mode;
        }
        configs->push_back(std::move(config));
    }
}

void add_custom_tokenizer_configs(std::vector<GoldenConfig>* configs) {
    configs->push_back(custom("custom/standard", {"standard"}));
    for (int length : {1, 2, 3, 4, 5, 16, 255, 256, 1000, 16383}) {
        configs->push_back(custom("custom/standard/max_token_length=" + std::to_string(length),
                                  {"standard", {{"max_token_length", std::to_string(length)}}}));
    }
    configs->push_back(custom("custom/basic", {"basic"}));
    configs->push_back(custom("custom/basic/extra_chars=-_.", {"basic", {{"extra_chars", "-_."}}}));
    configs->push_back(custom("custom/basic/extra_chars=symbols",
                              {"basic", {{"extra_chars", "!@#$%^&*()_+-=[]{}|;:',.<>?/~`"}}}));
    configs->push_back(custom("custom/keyword", {"keyword"}));
    configs->push_back(
            custom("custom/keyword/buffer_size=16", {"keyword", {{"buffer_size", "16"}}}));
    configs->push_back(custom("custom/char_group/whitespace",
                              {"char_group", {{"tokenize_on_chars", "[whitespace]"}}}));
    configs->push_back(
            custom("custom/char_group/whitespace_punctuation",
                   {"char_group", {{"tokenize_on_chars", "[whitespace], [punctuation]"}}}));
    configs->push_back(custom("custom/char_group/letter_digit",
                              {"char_group", {{"tokenize_on_chars", "[letter], [digit]"}}}));
    configs->push_back(custom("custom/char_group/symbol_cjk",
                              {"char_group", {{"tokenize_on_chars", "[symbol], [cjk]"}}}));
    configs->push_back(custom("custom/char_group/escaped/max_token_length=5",
                              {"char_group",
                               {{"tokenize_on_chars", R"([-], [_], [\n], [\t], [\u3000])"},
                                {"max_token_length", "5"}}}));
    configs->push_back(custom("custom/ngram", {"ngram"}));
    configs->push_back(custom(
            "custom/ngram/2_3/letter_digit",
            {"ngram", {{"min_gram", "2"}, {"max_gram", "3"}, {"token_chars", "letter, digit"}}}));
    configs->push_back(
            custom("custom/ngram/1_1/custom_whitespace", {"ngram",
                                                          {{"min_gram", "1"},
                                                           {"max_gram", "1"},
                                                           {"token_chars", "custom, whitespace"},
                                                           {"custom_token_chars", "*-_"}}}));
    configs->push_back(custom(
            "custom/ngram/2_2/punctuation_symbol",
            {"ngram",
             {{"min_gram", "2"}, {"max_gram", "2"}, {"token_chars", "punctuation, symbol"}}}));
    configs->push_back(
            custom("custom/ngram/1_4/max_ngram_diff=3",
                   {"ngram", {{"min_gram", "1"}, {"max_gram", "4"}, {"max_ngram_diff", "3"}}}));
    configs->push_back(
            custom("custom/ngram/mode=dense",
                   {"ngram", {{"mode", "dense"}, {"min_gram", "2"}, {"max_gram", "4"}}}));
    configs->push_back(custom("custom/ngram/mode=sparse", {"ngram", {{"mode", "sparse"}}}));
    configs->push_back(
            custom("custom/ngram/mode=auto/density=0.5",
                   {"ngram",
                    {{"mode", "auto"}, {"min_gram", "3"}, {"max_gram", "8"}, {"density", "0.5"}}}));
    configs->push_back(custom("custom/ngram/mode=dense/lower_case=false",
                              {"ngram", {{"mode", "dense"}, {"lower_case", "false"}}}));
    configs->push_back(custom("custom/edge_ngram", {"edge_ngram"}));
    configs->push_back(custom("custom/edge_ngram/2_5",
                              {"edge_ngram", {{"min_gram", "2"}, {"max_gram", "5"}}}));
    configs->push_back(custom("custom/icu", {"icu"}));
    configs->push_back(custom("custom/ik_smart", {"ik_smart"}));
    configs->push_back(custom("custom/ik_max_word", {"ik_max_word"}));
    const std::vector<Properties> pinyin_rows = switch_rows(kPinyinSwitches);
    for (size_t row = 0; row < pinyin_rows.size(); ++row) {
        Properties settings = pinyin_rows[row];
        if (row % 2 == 1) {
            settings["limit_first_letter_length"] = "3";
        }
        configs->push_back(
                custom("custom/pinyin/row=" + std::to_string(row), {"pinyin", settings}));
    }
    configs->push_back(custom("custom/empty", {"empty"}));
}

void add_custom_filter_configs(std::vector<GoldenConfig>* configs) {
    configs->push_back(custom("custom/standard+lowercase", {"standard"}, {{"lowercase"}}));
    configs->push_back(custom("custom/icu+lowercase", {"icu"}, {{"lowercase"}}));
    configs->push_back(custom("custom/standard+asciifolding", {"standard"}, {{"asciifolding"}}));
    configs->push_back(custom("custom/standard+asciifolding/preserve_original", {"standard"},
                              {{"asciifolding", {{"preserve_original", "true"}}}}));
    configs->push_back(custom("custom/keyword+asciifolding/preserve_original+lowercase",
                              {"keyword"},
                              {{"asciifolding", {{"preserve_original", "true"}}}, {"lowercase"}}));
    const Component whitespace {"char_group", {{"tokenize_on_chars", "[whitespace]"}}};
    const std::vector<Properties> word_delimiter_rows = switch_rows(kWordDelimiterSwitches);
    for (size_t row = 0; row < word_delimiter_rows.size(); ++row) {
        configs->push_back(custom("custom/whitespace+word_delimiter/row=" + std::to_string(row),
                                  whitespace, {{"word_delimiter", word_delimiter_rows[row]}}));
    }
    configs->push_back(
            custom("custom/whitespace+word_delimiter/type_table", whitespace,
                   {{"word_delimiter",
                     {{"type_table",
                       "[- => ALPHA], [_ => ALPHANUM], [\\u002C => DIGIT], [' => LOWER]"}}}}));
    configs->push_back(custom("custom/whitespace+word_delimiter/protected_words", whitespace,
                              {{"word_delimiter",
                                {{"protected_words", "Wi-Fi, PowerShot, O'Neil's, x86_64"},
                                 {"catenate_all", "true"}}}}));
    configs->push_back(custom("custom/keyword+icu_normalizer", {"keyword"}, {{"icu_normalizer"}}));
    configs->push_back(custom("custom/keyword+icu_normalizer/nfc", {"keyword"},
                              {{"icu_normalizer", {{"name", "nfc"}}}}));
    configs->push_back(
            custom("custom/standard+icu_normalizer/nfkc/unicode_set_filter", {"standard"},
                   {{"icu_normalizer", {{"name", "nfkc"}, {"unicode_set_filter", "[^\\u00E5]"}}}}));
    const std::vector<Properties> pinyin_rows = switch_rows(kPinyinSwitches);
    for (size_t row = 0; row < pinyin_rows.size(); ++row) {
        Properties settings = pinyin_rows[row];
        if (row % 2 == 0) {
            settings["limit_first_letter_length"] = "5";
        }
        configs->push_back(custom("custom/keyword+pinyin/row=" + std::to_string(row), {"keyword"},
                                  {{"pinyin", settings}}));
    }
    configs->push_back(custom("custom/ik_max_word+pinyin", {"ik_max_word"}, {{"pinyin"}}));
    configs->push_back(custom("custom/standard+empty", {"standard"}, {{"empty"}}));
}

void add_char_filter_configs(std::vector<GoldenConfig>* configs) {
    configs->push_back(custom("custom/char_replace+standard", {"standard"}, {},
                              {{"char_replace", {{"pattern", "-_."}, {"replacement", " "}}}}));
    configs->push_back(custom("custom/char_replace_default+keyword", {"keyword"}, {},
                              {{"char_replace", {{"replacement", "|"}}}}));
    configs->push_back(
            custom("custom/icu_normalizer+keyword", {"keyword"}, {}, {{"icu_normalizer"}}));
    configs->push_back(custom("custom/icu_normalizer/nfkc_decompose+standard", {"standard"}, {},
                              {{"icu_normalizer", {{"name", "nfkc"}, {"mode", "decompose"}}}}));
    configs->push_back(custom("custom/empty+standard", {"standard"}, {}, {{"empty"}}));
    configs->push_back(custom("custom/standard+lowercase/index_char_replace", {"standard"},
                              {{"lowercase"}}, {}, kIndexCharReplace));
    configs->push_back(custom("custom/char_replace+standard/index_char_replace", {"standard"}, {},
                              {{"char_replace", {{"pattern", " "}, {"replacement", "-"}}}},
                              kIndexCharReplace));
    // Every kind of component in one chain; AnalysisWriteGolden writes it into the indexes too.
    configs->push_back(custom("custom/char_replace+standard+word_delimiter+asciifolding+lowercase",
                              {"standard"}, {{"word_delimiter"}, {"asciifolding"}, {"lowercase"}},
                              {{"char_replace", {{"pattern", "_"}, {"replacement", " "}}}}));
}

void add_normalizer_configs(std::vector<GoldenConfig>* configs) {
    const std::vector<Component> folding = {{"asciifolding"}, {"lowercase"}};
    const std::vector<Component> replace = {
            {"char_replace", {{"pattern", "-"}, {"replacement", "_"}}}};
    // A normalizer index writes what its normalizer makes of the whole value.
    configs->push_back({.name = "normalizer/lowercase",
                        .kind = GoldenKind::ANALYZE,
                        .properties = {{"normalizer", "lowercase"}},
                        .policies = {}});
    configs->push_back(normalizer("normalizer/char_replace+asciifolding+lowercase", folding,
                                  replace, GoldenKind::ANALYZE));
    configs->push_back(
            normalizer("normalizer/icu_normalizer", {{"icu_normalizer"}}, {}, GoldenKind::ANALYZE));
    configs->push_back(normalizer("normalizer/asciifolding+lowercase/index_char_replace", folding,
                                  {}, GoldenKind::ANALYZE, kIndexCharReplace));

    // The term a prefix or wildcard query normalizes its text to.
    static constexpr const char* kParsers[] = {"english", "standard", "unicode", "basic",
                                               "icu",     "chinese",  "ik",      "kuromoji"};
    for (const char* parser : kParsers) {
        for (const char* lower_case : {"unset", "true", "false"}) {
            GoldenConfig config {.name = std::string("normalize/builtin/") + parser +
                                         "/lower_case=" + lower_case,
                                 .kind = GoldenKind::NORMALIZE,
                                 .properties = {{"parser", parser}},
                                 .policies = {}};
            if (std::string_view(lower_case) != "unset") {
                config.properties["lower_case"] = lower_case;
            }
            configs->push_back(std::move(config));
        }
    }
    configs->push_back({.name = "normalize/builtin/standard/index_char_replace",
                        .kind = GoldenKind::NORMALIZE,
                        .properties = with({{"parser", "standard"}}, kIndexCharReplace),
                        .policies = {}});
    configs->push_back(
            {.name = "normalize/builtin/ik/index_char_replace",
             .kind = GoldenKind::NORMALIZE,
             .properties = with({{"parser", "ik"}, {"parser_mode", "ik_smart"}}, kIndexCharReplace),
             .policies = {}});
    configs->push_back({.name = "normalize/normalizer/lowercase",
                        .kind = GoldenKind::NORMALIZE,
                        .properties = {{"normalizer", "lowercase"}},
                        .policies = {}});
    configs->push_back(normalizer("normalize/normalizer/char_replace+asciifolding+lowercase",
                                  folding, replace, GoldenKind::NORMALIZE));
    configs->push_back(normalizer("normalize/normalizer/icu_normalizer", {{"icu_normalizer"}}, {},
                                  GoldenKind::NORMALIZE));
    configs->push_back(normalizer("normalize/normalizer/asciifolding+lowercase/index_char_replace",
                                  folding, {}, GoldenKind::NORMALIZE, kIndexCharReplace));
    // A custom analyzer normalizes with its per-character part only.
    configs->push_back(custom("normalize/custom/standard+word_delimiter+asciifolding+lowercase",
                              {"standard"}, {{"word_delimiter"}, {"asciifolding"}, {"lowercase"}},
                              replace, {}, GoldenKind::NORMALIZE));
    configs->push_back(
            custom("normalize/custom/standard", {"standard"}, {}, {}, {}, GoldenKind::NORMALIZE));
    configs->push_back(custom("normalize/custom/standard/index_char_replace", {"standard"}, {}, {},
                              kIndexCharReplace, GoldenKind::NORMALIZE));
}

size_t binomial(size_t n, size_t k) {
    size_t result = 1;
    for (size_t i = 1; i <= k; ++i) {
        result = result * (n - k + i) / i;
    }
    return result;
}

} // namespace

std::vector<std::vector<bool>> pairwise_rows(size_t factors) {
    // Each factor's column is a distinct subset of size m / 2 + 1 of the m rows after row 0. Two
    // such subsets always intersect, which gives (true, true), and neither contains the other,
    // which gives (true, false) and (false, true); row 0 gives (false, false).
    size_t m = 2;
    while (binomial(m, m / 2 + 1) < factors) {
        ++m;
    }
    const size_t weight = m / 2 + 1;
    std::vector<std::vector<bool>> rows(m + 1, std::vector<bool>(factors, false));
    std::vector<size_t> subset(weight);
    std::iota(subset.begin(), subset.end(), 0);
    for (size_t factor = 0; factor < factors; ++factor) {
        for (size_t row : subset) {
            rows[row + 1][factor] = true;
        }
        // The next subset in lexicographic order.
        size_t i = weight;
        while (i > 0 && subset[i - 1] == m - weight + i - 1) {
            --i;
        }
        if (i == 0) {
            break;
        }
        ++subset[i - 1];
        for (size_t j = i; j < weight; ++j) {
            subset[j] = subset[j - 1] + 1;
        }
    }
    return rows;
}

std::vector<GoldenConfig> golden_configs() {
    std::vector<GoldenConfig> configs;
    add_builtin_configs(&configs);
    add_custom_tokenizer_configs(&configs);
    add_custom_filter_configs(&configs);
    add_char_filter_configs(&configs);
    add_normalizer_configs(&configs);
    return configs;
}

std::string describe(const GoldenConfig& config) {
    std::string out = config.name;
    out += config.kind == GoldenKind::ANALYZE ? " (analyze)" : " (normalize)";
    out += "\n  properties:";
    for (const auto& [key, value] : config.properties) {
        out += " " + key + "=" + value;
    }
    for (const GoldenPolicy& policy : config.policies) {
        out += "\n  policy " + policy.name + " (" + to_string(policy.type) + "):";
        for (const auto& [key, value] : policy.properties) {
            out += " " + key + "=" + value;
        }
    }
    return out;
}

} // namespace doris::segment_v2::inverted_index::golden
