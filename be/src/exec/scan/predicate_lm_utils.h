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

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/tablet/tablet_schema.h"
#include "util/string_util.h"

namespace doris {

namespace predicate_lm_utils_detail {

inline std::string to_lower_trimmed(std::string_view sv) {
    sv = doris::trim(sv);
    return doris::to_lower(std::string(sv));
}

inline std::string normalize_table_name_for_match(std::string_view table_name_sv) {
    table_name_sv = doris::trim(table_name_sv);
    std::string s(table_name_sv);

    // Strip rollup suffix: "tbl(rollup)" => "tbl".
    if (!s.empty() && s.back() == ')') {
        auto pos = s.find_last_of('(');
        if (pos != std::string::npos) {
            s.resize(pos);
        }
    }

    return to_lower_trimmed(std::string_view(s));
}

inline bool ends_with(std::string_view s, std::string_view suffix) {
    if (suffix.size() > s.size()) {
        return false;
    }
    return s.substr(s.size() - suffix.size()) == suffix;
}

inline std::string normalize_identifier_piece(std::string_view sv) {
    sv = doris::trim(sv);
    if (sv.size() >= 2 && sv.front() == '`' && sv.back() == '`') {
        sv = sv.substr(1, sv.size() - 2);
        sv = doris::trim(sv);
    }
    return std::string(sv);
}

inline bool qualifier_matches_current_table(const std::string& normalized_current_full_table_name,
                                           const std::string& normalized_current_table_only,
                                           const std::string& qualifier_raw) {
    std::string qualifier = to_lower_trimmed(std::string_view(qualifier_raw));
    if (qualifier.empty()) {
        return true;
    }

    // qualifier="tbl"
    if (qualifier.find('.') == std::string::npos) {
        return qualifier == normalized_current_table_only;
    }

    // qualifier="db.tbl" (also allow suffix match for catalog.db.tbl)
    if (qualifier == normalized_current_full_table_name) {
        return true;
    }
    std::string with_dot = "." + qualifier;
    return ends_with(normalized_current_full_table_name, with_dot);
}

} // namespace predicate_lm_utils_detail

inline Status parse_predicate_lm_stage1_cols_to_column_ids(const std::string& cols,
                                                          const TabletSchemaSPtr& tablet_schema,
                                                          std::string_view current_db_name,
                                                          std::string_view current_table_name,
                                                          std::vector<ColumnId>* column_ids) {
    column_ids->clear();
    if (cols.empty()) {
        return Status::OK();
    }

    const std::string normalized_db = predicate_lm_utils_detail::to_lower_trimmed(current_db_name);
    const std::string normalized_tbl =
            predicate_lm_utils_detail::normalize_table_name_for_match(current_table_name);
    const std::string normalized_current_table_only = normalized_tbl;

    std::string normalized_current_full_table_name;
    if (!normalized_db.empty()) {
        normalized_current_full_table_name.reserve(normalized_db.size() + 1 + normalized_tbl.size());
        normalized_current_full_table_name.append(normalized_db);
        normalized_current_full_table_name.push_back('.');
        normalized_current_full_table_name.append(normalized_tbl);
    } else {
        normalized_current_full_table_name = normalized_tbl;
    }

    std::vector<std::string> parts = doris::split(cols, ",");

    for (const auto& part : parts) {
        std::string_view token_sv = doris::trim(std::string_view(part));
        if (token_sv.empty()) {
            continue;
        }

        // Support qualified identifiers: tbl.col / db.tbl.col
        // (Backticks are supported on each identifier piece, e.g. `db`.`tbl`.`col`)
        std::vector<std::string> dot_parts = doris::split(std::string(token_sv), ".");
        std::vector<std::string> ident_parts;
        ident_parts.reserve(dot_parts.size());
        for (const auto& dot_part : dot_parts) {
            auto piece = predicate_lm_utils_detail::normalize_identifier_piece(std::string_view(dot_part));
            if (!piece.empty()) {
                ident_parts.emplace_back(std::move(piece));
            }
        }
        if (ident_parts.empty()) {
            continue;
        }

        std::string col_name = std::move(ident_parts.back());
        ident_parts.pop_back();

        std::string qualifier;
        if (!ident_parts.empty()) {
            qualifier.reserve(64);
            for (size_t i = 0; i < ident_parts.size(); ++i) {
                if (i > 0) {
                    qualifier.push_back('.');
                }
                qualifier.append(ident_parts[i]);
            }
        }

        if (!predicate_lm_utils_detail::qualifier_matches_current_table(
                    normalized_current_full_table_name, normalized_current_table_only, qualifier)) {
            continue;
        }

        int32_t cid = tablet_schema->field_index(col_name);
        if (cid < 0) {
            cid = tablet_schema->field_index(doris::to_lower(col_name));
        }

        // Ignore unknown columns (do not fail the query).
        if (cid < 0) {
            continue;
        }

        column_ids->push_back(static_cast<ColumnId>(cid));
    }

    std::sort(column_ids->begin(), column_ids->end());
    column_ids->erase(std::unique(column_ids->begin(), column_ids->end()), column_ids->end());
    return Status::OK();
}

} // namespace doris
