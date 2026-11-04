// Derived from cppjieba's HMMModel.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <array>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/string_util.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

using EmitProbMap = std::unordered_map<Rune, double>;

// The HMM of jieba's unknown-word segmentation. Every line of the model file is trimmed of white
// space, and blank lines and lines that start with '#' are skipped. The rest are one line of the
// 4 start probabilities, 4 lines of 4 transition probabilities, all split at single spaces and
// read by atof, and one line of emission probabilities for each of B, E, M and S:
// "rune:probability" pairs split at ','. A missing or illegal line fails the load; a later pair of
// the same rune replaces the earlier one, and what follows the S line is not read. Upstream's
// statMap, which only printing read, is gone, and so is GetEmitProb: the cuts read the model as
// FlatDict lays it out.
struct HMMModel {
    /*
     * STATUS:
     * 0: HMMModel::B, 1: HMMModel::E, 2: HMMModel::M, 3:HMMModel::S
     * */
    enum { B = 0, E = 1, M = 2, S = 3, STATUS_SUM = 4 };

    explicit HMMModel(const std::string& model_path) {
        emit_prob_vec.push_back(&emit_prob_b);
        emit_prob_vec.push_back(&emit_prob_e);
        emit_prob_vec.push_back(&emit_prob_m);
        emit_prob_vec.push_back(&emit_prob_s);
        load_model(model_path);
    }
    // emit_prob_vec points at the maps of this model.
    HMMModel(const HMMModel&) = delete;
    HMMModel& operator=(const HMMModel&) = delete;

    void load_model(const std::string& file_path) {
        std::ifstream ifile(file_path);
        if (!ifile.is_open()) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "jieba hmm model file not found: {}", file_path);
        }
        std::string line;
        std::vector<std::string> tmp;
        //Load start_prob
        if (!get_line(ifile, line)) {
            throw_illegal(file_path, "start probability");
        }
        split(line, tmp, " ");
        if (tmp.size() != STATUS_SUM) {
            throw_illegal(file_path, "start probability");
        }
        for (size_t j = 0; j < tmp.size(); j++) {
            start_prob[j] = std::atof(tmp[j].c_str());
        }

        //Load trans_prob
        for (size_t i = 0; i < STATUS_SUM; i++) {
            if (!get_line(ifile, line)) {
                throw_illegal(file_path, "transition probability");
            }
            split(line, tmp, " ");
            if (tmp.size() != STATUS_SUM) {
                throw_illegal(file_path, "transition probability");
            }
            for (size_t j = 0; j < STATUS_SUM; j++) {
                trans_prob[i][j] = std::atof(tmp[j].c_str());
            }
        }

        //Load emit_prob_b
        if (!get_line(ifile, line) || !load_emit_prob(line, emit_prob_b)) {
            throw_illegal(file_path, "B emission probability");
        }

        //Load emit_prob_e
        if (!get_line(ifile, line) || !load_emit_prob(line, emit_prob_e)) {
            throw_illegal(file_path, "E emission probability");
        }

        //Load emit_prob_m
        if (!get_line(ifile, line) || !load_emit_prob(line, emit_prob_m)) {
            throw_illegal(file_path, "M emission probability");
        }

        //Load emit_prob_s
        if (!get_line(ifile, line) || !load_emit_prob(line, emit_prob_s)) {
            throw_illegal(file_path, "S emission probability");
        }
    }

    // The next line that is neither blank nor a comment, trimmed.
    static bool get_line(std::ifstream& ifile, std::string& line) {
        while (std::getline(ifile, line)) {
            trim(line);
            if (line.empty()) {
                continue;
            }
            if (line.starts_with("#")) {
                continue;
            }
            return true;
        }
        return false;
    }

    // Fails on a pair that is not two pieces split at ':' and on a rune that does not decode to
    // exactly one code point. The pairs before it are in `mp` by then, which does not matter: the
    // load fails.
    static bool load_emit_prob(const std::string& line, EmitProbMap& mp) {
        // Upstream returned false for an empty line, which get_line() never returns.
        DCHECK(!line.empty());
        std::vector<std::string> tmp;
        std::vector<std::string> tmp2;
        Unicode unicode;
        split(line, tmp, ",");
        for (const std::string& item : tmp) {
            split(item, tmp2, ":");
            if (2 != tmp2.size()) {
                return false;
            }
            if (!decode_unicode(tmp2[0], unicode) || unicode.size() != 1) {
                return false;
            }
            mp[unicode[0]] = std::atof(tmp2[1].c_str());
        }
        return true;
    }

    std::array<double, STATUS_SUM> start_prob {};
    std::array<std::array<double, STATUS_SUM>, STATUS_SUM> trans_prob {};
    EmitProbMap emit_prob_b;
    EmitProbMap emit_prob_e;
    EmitProbMap emit_prob_m;
    EmitProbMap emit_prob_s;
    // Indexed by status.
    std::vector<EmitProbMap*> emit_prob_vec;

private:
    // A line of the model that upstream's checks failed the load on.
    [[noreturn]] static void throw_illegal(const std::string& file_path, std::string_view part) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "jieba hmm model file {} has no legal {} line", file_path, part);
    }
};

} // namespace doris::segment_v2::inverted_index::jieba
