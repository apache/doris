// Derived from cppjieba's DictTrie.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/check.h"
#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/string_util.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

inline constexpr double MIN_DOUBLE = -3.14e+100;
inline constexpr size_t DICT_COLUMN_NUM = 3;

// The words of the main dictionary and the user dictionaries, with their weights, in a trie.
//
// A main dictionary line is "word frequency tag", three columns split at single spaces, with the
// frequency read by atof; any other line fails the load. A word's weight is log(frequency / the sum
// of all frequencies). A user dictionary line is "word", "word tag" or "word frequency tag"; the
// first two take the median weight of the main dictionary (upstream's default WordWeightMedian, the
// only option Jieba used), the third log(frequency / the main dictionary's sum), with the frequency
// read by atoi. A user line with more columns adds a unit with no word, and every user word of one
// rune is remembered for is_user_dict_single_chinese_word(), whatever its script.
class DictTrie {
public:
    // `user_dict_paths` lists the user dictionaries, separated by '|' or ';'.
    DictTrie(const std::string& dict_path, const std::string& user_dict_paths) {
        init(dict_path, user_dict_paths);
    }

    void find(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
              std::vector<Dag>& res, size_t max_word_len = MAX_WORD_LENGTH) const {
        _trie->find(begin, end, res, max_word_len);
    }

    bool is_user_dict_single_chinese_word(const Rune& word) const {
        return _user_dict_single_chinese_word.contains(word);
    }

    double get_min_weight() const { return _min_weight; }

private:
    void init(const std::string& dict_path, const std::string& user_dict_paths) {
        load_dict(dict_path);
        _freq_sum = calc_freq_sum(_static_node_infos);
        calculate_weight(_static_node_infos, _freq_sum);
        set_static_word_weights(dict_path);

        // Upstream skipped this for an empty list, which splits into no file anyway.
        load_user_dict(user_dict_paths);
        shrink(_static_node_infos);
        create_trie(_static_node_infos);
    }

    // The trie points into _static_node_infos, which no longer changes.
    void create_trie(const std::vector<DictUnit>& dict_units) {
        DORIS_CHECK(!dict_units.empty());
        std::vector<Unicode> words;
        std::vector<const DictUnit*> value_pointers;
        for (const DictUnit& dict_unit : dict_units) {
            words.push_back(dict_unit.word);
            value_pointers.push_back(&dict_unit);
        }

        _trie = std::make_unique<Trie>(words, value_pointers);
    }

    // Decodes `word` into the unit and sets its weight. A word that does not decode leaves the
    // unit with an empty word and its weight as it was, and returns false; no caller looks at the
    // result, as upstream's did not.
    static bool make_node_info(DictUnit& node_info, const std::string& word, double weight) {
        if (!decode_unicode(word, node_info.word)) {
            return false;
        }
        node_info.weight = weight;
        return true;
    }

    void load_dict(const std::string& file_path) {
        std::ifstream ifs(file_path);
        if (!ifs.is_open()) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "jieba main dictionary file not found: {}", file_path);
        }
        std::string line;
        std::vector<std::string> buf;

        // One unit for every line, as upstream: a line whose word does not decode adds a unit with
        // no word and the frequency of the line before it, which then counts twice in the sum of
        // the frequencies. (A first line that does not decode added an indeterminate frequency
        // upstream, and adds 0 here.)
        DictUnit node_info;
        while (std::getline(ifs, line)) {
            split(line, buf, " ");
            if (buf.size() != DICT_COLUMN_NUM) {
                throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                                "jieba main dictionary file {} has a line that is not {} columns "
                                "split at spaces: {}",
                                file_path, DICT_COLUMN_NUM, line);
            }
            make_node_info(node_info, buf[0], std::atof(buf[1].c_str()));
            _static_node_infos.push_back(node_info);
        }
    }

    // A user dictionary file that cannot be opened fails the load; an empty line is skipped.
    void load_user_dict(const std::string& file_paths) {
        const std::vector<std::string> files = split(file_paths, "|;");
        for (const std::string& file : files) {
            std::ifstream ifs(file);
            if (!ifs.is_open()) {
                throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                                "jieba user dictionary file not found: {}", file);
            }
            std::string line;
            while (std::getline(ifs, line)) {
                if (line.empty()) {
                    continue;
                }
                insert_user_dict_node(line);
            }
        }
    }

    // Upstream's InserUserDictNode.
    void insert_user_dict_node(const std::string& line) {
        std::vector<std::string> buf;
        DictUnit node_info;
        split(line, buf, " ");
        if (buf.size() == 1 || buf.size() == 2) {
            // The second column is the tag.
            make_node_info(node_info, buf[0], _user_word_default_weight);
        } else if (buf.size() == 3) {
            int freq = std::atoi(buf[1].c_str());
            DCHECK(_freq_sum > 0.0);
            double weight = std::log(1.0 * freq / _freq_sum);
            make_node_info(node_info, buf[0], weight);
        }
        _static_node_infos.push_back(node_info);
        if (node_info.word.size() == 1) {
            _user_dict_single_chinese_word.insert(node_info.word[0]);
        }
    }

    static bool weight_compare(const DictUnit& lhs, const DictUnit& rhs) {
        return lhs.weight < rhs.weight;
    }

    // Sorts a copy of the main dictionary's units by weight for the smallest and the median, as
    // upstream; the median is the default weight of a user word. Only these two order statistics
    // are read, which any sort gives alike; std::ranges::sort is libstdc++'s std::sort anyway.
    void set_static_word_weights(const std::string& dict_path) {
        if (_static_node_infos.empty()) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "jieba main dictionary file {} is empty", dict_path);
        }
        std::vector<DictUnit> x = _static_node_infos;
        std::ranges::sort(x, weight_compare);
        _min_weight = x[0].weight;
        _median_weight = x[x.size() / 2].weight;
        _user_word_default_weight = _median_weight;
    }

    // In the order of the file.
    static double calc_freq_sum(const std::vector<DictUnit>& node_infos) {
        double sum = 0.0;
        for (const DictUnit& node_info : node_infos) {
            sum += node_info.weight;
        }
        return sum;
    }

    // The DCHECKs are upstream's asserts. A debug build stops at a frequency, or a sum of them, of
    // 0 or less, as upstream's did, and so at an empty dictionary before set_static_word_weights()
    // can throw; a release build loads such a dictionary with weights of -inf or NaN, as upstream's
    // did, and throws for an empty one.
    static void calculate_weight(std::vector<DictUnit>& node_infos, double sum) {
        DCHECK(sum > 0.0);
        for (DictUnit& node_info : node_infos) {
            DCHECK(node_info.weight > 0.0);
            node_info.weight = std::log(node_info.weight / sum);
        }
    }

    static void shrink(std::vector<DictUnit>& units) { units.shrink_to_fit(); }

    std::vector<DictUnit> _static_node_infos;
    std::unique_ptr<Trie> _trie;

    double _freq_sum = 0.0;
    double _min_weight = 0.0;
    double _median_weight = 0.0;
    double _user_word_default_weight = 0.0;
    std::unordered_set<Rune> _user_dict_single_chinese_word;
};

} // namespace doris::segment_v2::inverted_index::jieba
