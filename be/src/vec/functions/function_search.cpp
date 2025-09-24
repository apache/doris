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

#include "vec/functions/function_search.h"

#include <CLucene/config/repl_wchar.h>
#include <CLucene/search/Scorer.h>
#include <glog/logging.h>

#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <sstream>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query/conjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/composite_reader.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_query.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"
#include "olap/rowset/segment_v2/inverted_index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/columns/column_const.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using doris::segment_v2::InvertedIndexQueryType;
using namespace doris::segment_v2;
using namespace doris::segment_v2::inverted_index;
using namespace doris::segment_v2::inverted_index::query_v2;
using namespace lucene::analysis;
using namespace lucene::index;
using namespace lucene::util;
using namespace lucene::search;

Status FunctionSearch::execute_impl(FunctionContext* /*context*/, Block& /*block*/,
                                    const ColumnNumbers& /*arguments*/, uint32_t /*result*/,
                                    size_t /*input_rows_count*/) const {
    return Status::RuntimeError("only inverted index queries are supported");
}

// Enhanced implementation: Handle new parameter structure (DSL + SlotReferences)
Status FunctionSearch::evaluate_inverted_index(
        const ColumnsWithTypeAndName& arguments,
        const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
        std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
        segment_v2::InvertedIndexResultBitmap& bitmap_result) const {
    return Status::OK();
}

// Helper function to build BooleanQuery from compound TSearchClause (AND/OR/NOT)
std::shared_ptr<BooleanQuery> FunctionSearch::build_query_from_clause(
        const TSearchClause& clause, const std::shared_ptr<segment_v2::IndexQueryContext>& context,
        const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                data_type_with_names,
        const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators) const {
    const std::string& clause_type = clause.clause_type;

    // This method should only be called for compound queries
    if (clause_type != "AND" && clause_type != "OR" && clause_type != "NOT") {
        LOG(WARNING) << "build_query_from_clause called with non-compound clause type: "
                     << clause_type;
        return nullptr;
    }

    // Determine operator type
    OperatorType op;
    if (clause_type == "AND") {
        op = OperatorType::OP_AND;
    } else if (clause_type == "OR") {
        op = OperatorType::OP_OR;
    } else { // NOT
        op = OperatorType::OP_NOT;
    }

    BooleanQuery::Builder builder(op);

    if (clause.__isset.children && !clause.children.empty()) {
        for (const auto& child_clause : clause.children) {
            const std::string& child_type = child_clause.clause_type;

            if (child_type == "AND" || child_type == "OR" || child_type == "NOT") {
                // Recursive call for compound child
                auto child_boolean_query = build_query_from_clause(child_clause, context,
                                                                   data_type_with_names, iterators);
                if (child_boolean_query) {
                    builder.add(std::static_pointer_cast<query_v2::Query>(child_boolean_query));
                }
            } else {
                // Leaf child - build appropriate leaf query with proper iterator access
                auto child_leaf_query =
                        build_leaf_query(child_clause, context, data_type_with_names, iterators);
                if (child_leaf_query) {
                    builder.add(child_leaf_query);
                }
            }
        }
    }

    return builder.build();
}

// Helper function to build leaf queries
std::shared_ptr<query_v2::Query> FunctionSearch::build_leaf_query(
        const TSearchClause& clause, const std::shared_ptr<segment_v2::IndexQueryContext>& context,
        const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                data_type_with_names,
        const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators) const {
    if (!clause.__isset.field_name || !clause.__isset.value) {
        LOG(WARNING) << "search: Leaf clause missing field_name or value";
        return nullptr;
    }

    const std::string& field_name = clause.field_name;
    const std::string& value = clause.value;
    const std::string& clause_type = clause.clause_type;

    // Find field in data_type_with_names
    auto field_iter = data_type_with_names.find(field_name);
    if (field_iter == data_type_with_names.end()) {
        LOG(WARNING) << "search: Field '" << field_name << "' not found in data_type_with_names";
        return nullptr;
    }

    const std::string& real_field_name = field_iter->second.first;
    std::wstring field_wstr = StringHelper::to_wstring(real_field_name);
    std::wstring value_wstr = StringHelper::to_wstring(value);

    // Build appropriate query based on clause type using DRY principle
    ClauseTypeCategory category = get_clause_type_category(clause_type);

    // Handle different clause types based on tokenization needs (most are TODO and use TermQuery for now)
    if (clause_type == "TERM") {
        // Check if field needs tokenization (text vs keyword type)
        auto index_properties = get_field_index_properties(field_name, iterators);
        bool should_analyze =
                inverted_index::InvertedIndexAnalyzer::should_analyzer(index_properties);

        if (should_analyze) {
            // Text type: use analyzer for tokenization and lowercase
            if (index_properties.empty()) {
                LOG(WARNING) << "Failed to get index properties for TERM query field: "
                             << field_name;
                return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
            }

            // Parse using proper tokenization
            std::vector<segment_v2::TermInfo> term_infos =
                    segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "No terms found after tokenization for TERM query: field="
                             << field_name << ", value='" << value << "'";
                return nullptr;
            }

            if (term_infos.size() == 1) {
                // Single term - use TermQuery directly
                std::wstring term_wstr = StringHelper::to_wstring(term_infos[0].get_single_term());
                return std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr);
            }

            // Multiple terms - create BooleanQuery with OR operation (any match)
            BooleanQuery::Builder builder(OperatorType::OP_OR);
            for (const auto& term_info : term_infos) {
                std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                auto term_query =
                        std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr);
                builder.add(std::static_pointer_cast<query_v2::Query>(term_query));
            }

            LOG(INFO) << "Built TERM query with " << term_infos.size()
                      << " OR conditions for text field: " << field_name;
            return std::static_pointer_cast<query_v2::Query>(builder.build());
        } else {
            // Keyword type: exact match without tokenization
            LOG(INFO) << "Using exact match for keyword field: " << field_name;
            return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
        }
    } else if (category == ClauseTypeCategory::TOKENIZED) {
        // Tokenized queries: PHRASE, MATCH, ANY, ALL - need tokenization
        if (clause_type == "PHRASE") {
            // TODO: Implement PhraseQuery
            LOG(INFO) << "TODO: PHRASE query type not yet implemented, using TermQuery";
            return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
        } else if (clause_type == "MATCH") {
            // TODO: Implement MatchQuery
            LOG(INFO) << "TODO: MATCH query type not yet implemented, using TermQuery";
            return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
        } else if (clause_type == "ANY") {
            // ANY query: field:ANY(value1 value2 ...) - any match (OR operation)
            auto index_properties = get_field_index_properties(field_name, iterators);
            if (index_properties.empty()) {
                LOG(WARNING) << "Failed to get index properties for ANY query field: "
                             << field_name;
                return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
            }

            // Parse using proper tokenization
            std::vector<segment_v2::TermInfo> term_infos =
                    segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "No terms found after tokenization for ANY query: field="
                             << field_name << ", value='" << value << "'";
                return nullptr;
            }

            if (term_infos.size() == 1) {
                // Single term - use TermQuery directly
                std::wstring term_wstr = StringHelper::to_wstring(term_infos[0].get_single_term());
                return std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr);
            }

            // Multiple terms - create BooleanQuery with OR operation
            BooleanQuery::Builder builder(OperatorType::OP_OR);
            for (const auto& term_info : term_infos) {
                std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                auto term_query =
                        std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr);
                builder.add(std::static_pointer_cast<query_v2::Query>(term_query));
            }

            LOG(INFO) << "Built ANY query with " << term_infos.size()
                      << " OR conditions for field: " << field_name;
            return std::static_pointer_cast<query_v2::Query>(builder.build());

        } else if (clause_type == "ALL") {
            // ALL query: field:ALL(value1 value2 ...) - all match (AND operation)
            auto index_properties = get_field_index_properties(field_name, iterators);
            if (index_properties.empty()) {
                LOG(WARNING) << "Failed to get index properties for ALL query field: "
                             << field_name;
                return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
            }

            // Parse using proper tokenization
            std::vector<segment_v2::TermInfo> term_infos =
                    segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "No terms found after tokenization for ALL query: field="
                             << field_name << ", value='" << value << "'";
                return nullptr;
            }

            if (term_infos.size() == 1) {
                // Single term - use TermQuery directly
                std::wstring term_wstr = StringHelper::to_wstring(term_infos[0].get_single_term());
                return std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr);
            }

            // Multiple terms - create BooleanQuery with AND operation
            BooleanQuery::Builder builder(OperatorType::OP_AND);
            for (const auto& term_info : term_infos) {
                std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                auto term_query =
                        std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr);
                builder.add(std::static_pointer_cast<query_v2::Query>(term_query));
            }

            LOG(INFO) << "Built ALL query with " << term_infos.size()
                      << " AND conditions for field: " << field_name;
            return std::static_pointer_cast<query_v2::Query>(builder.build());
        }
        return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
    } else if (category == ClauseTypeCategory::NON_TOKENIZED) {
        // Non-tokenized queries based on QueryParser.g4: exact/pattern matching, range, list operations
        if (clause_type == "PREFIX") {
            // TODO: Implement PrefixQuery
            LOG(INFO) << "TODO: PREFIX query type not yet implemented, using TermQuery";
        } else if (clause_type == "WILDCARD") {
            // TODO: Implement WildcardQuery
            LOG(INFO) << "TODO: WILDCARD query type not yet implemented, using TermQuery";
        } else if (clause_type == "REGEXP") {
            // TODO: Implement RegexpQuery
            LOG(INFO) << "TODO: REGEXP query type not yet implemented, using TermQuery";
        } else if (clause_type == "RANGE") {
            // TODO: Implement RangeQuery
            LOG(INFO) << "TODO: RANGE query type not yet implemented, using TermQuery";
        } else if (clause_type == "LIST") {
            // TODO: Implement ListQuery (IN operation)
            LOG(INFO) << "TODO: LIST query type not yet implemented, using TermQuery";
        }
        return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
    } else {
        // Default to TermQuery (should not happen due to get_clause_type_category logic)
        LOG(WARNING) << "Unexpected clause category for leaf query type '" << clause_type
                     << "', using TermQuery";
        return std::make_shared<query_v2::TermQuery>(context, field_wstr, value_wstr);
    }
}

Status FunctionSearch::evaluate_inverted_index_with_search_param(
        const TSearchParam& search_param,
        const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                data_type_with_names,
        std::unordered_map<std::string, segment_v2::IndexIterator*> iterators, uint32_t num_rows,
        segment_v2::InvertedIndexResultBitmap& bitmap_result) const {
    LOG(INFO) << "search: Processing structured query with DSL: " << search_param.original_dsl
              << ", available " << data_type_with_names.size() << " indexed columns, "
              << iterators.size() << " iterators";

    if (iterators.empty() || data_type_with_names.empty()) {
        LOG(INFO) << "No indexed columns or iterators available, returning empty result";
        return Status::OK();
    }

    // Create IndexQueryContext
    auto context = std::make_shared<segment_v2::IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    const std::string& root_clause_type = search_param.root.clause_type;
    std::shared_ptr<query_v2::Query> root_query = nullptr;

    // Check if root is compound node (AND/OR/NOT) or leaf node
    if (root_clause_type == "AND" || root_clause_type == "OR" || root_clause_type == "NOT") {
        // Compound node - build BooleanQuery
        auto boolean_query = build_query_from_clause(search_param.root, context,
                                                     data_type_with_names, iterators);
        if (!boolean_query) {
            LOG(WARNING) << "search: Failed to build BooleanQuery from compound clause";
            return Status::InternalError("Failed to build BooleanQuery from compound DSL clause");
        }
        root_query = std::static_pointer_cast<query_v2::Query>(boolean_query);
        LOG(INFO) << "search: Built BooleanQuery for compound clause type: " << root_clause_type;
    } else {
        // Leaf node - build appropriate leaf query directly
        root_query = build_leaf_query(search_param.root, context, data_type_with_names, iterators);
        if (!root_query) {
            LOG(WARNING) << "search: Failed to build leaf query for clause type: "
                         << root_clause_type;
            return Status::InternalError("Failed to build leaf query from DSL clause");
        }
        LOG(INFO) << "search: Built leaf query for clause type: " << root_clause_type;
    }
    // Initialize result bitmaps
    std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
    std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();

    try {
        // Create CompositeReader to manage multiple index readers
        auto composite_reader = std::make_unique<query_v2::CompositeReader>();

        // Build individual IndexReaders for each field
        std::vector<std::unique_ptr<lucene::index::IndexReader>> index_readers;
        index_readers.reserve(data_type_with_names.size());

        for (const auto& field_pair : data_type_with_names) {
            const std::string& field_name = field_pair.first;
            const std::string& real_field_name = field_pair.second.first;
            const vectorized::DataTypePtr& column_type = field_pair.second.second;
            std::wstring field_wstr = StringHelper::to_wstring(real_field_name);

            // Analyze query type specifically for this field
            InvertedIndexQueryType field_query_type =
                    analyze_field_query_type(field_name, search_param.root);
            if (field_query_type == InvertedIndexQueryType::UNKNOWN_QUERY) {
                LOG(WARNING) << "search: Unknown query type for field: " << field_name;
                return Status::InternalError("Unknown query type for field: " + field_name);
            }
            LOG(INFO) << "search: Field '" << field_name
                      << "' query type: " << segment_v2::query_type_to_string(field_query_type);

            // Use helper function to get inverted reader with field-specific query type
            auto inverted_reader =
                    get_field_inverted_reader(field_name, iterators, column_type, field_query_type);
            if (!inverted_reader) {
                LOG(ERROR) << "search: Could not get inverted reader for field: " << field_name;
                return Status::InternalError("Could not get inverted reader for field: " +
                                             field_name);
            }

            // Directly access the index file and create directory
            auto index_file_reader = inverted_reader->get_index_file_reader();
            if (!index_file_reader) {
                LOG(ERROR) << "search: No index file reader for field: " << field_name;
                return Status::InternalError("No index file reader for field: " + field_name);
            }

            // Initialize index file reader
            auto st = index_file_reader->init(config::inverted_index_read_buffer_size,
                                              context->io_ctx);
            if (!st.ok()) {
                LOG(ERROR) << "search: Failed to init index file reader for field: " << field_name
                           << ", error: " << st.to_string();
                return Status::InternalError("Failed to init index file reader for field: " +
                                             field_name + ", error: " + st.to_string());
            }

            // Open directory directly
            auto directory = DORIS_TRY(
                    index_file_reader->open(&inverted_reader->get_index_meta(), context->io_ctx));

            // Create lucene IndexReader for this field
            std::unique_ptr<lucene::index::IndexReader> index_reader;
            try {
                index_reader = std::unique_ptr<lucene::index::IndexReader>(
                        lucene::index::IndexReader::open(directory.get(),
                                                         config::inverted_index_read_buffer_size,
                                                         false)); // don't close directory
            } catch (const CLuceneError& e) {
                LOG(ERROR) << "search: Failed to open lucene IndexReader for field: " << field_name
                           << ", error: " << e.what();
                return Status::InternalError("Failed to open IndexReader for field: " + field_name +
                                             ", error: " + std::string(e.what()));
            }

            if (!index_reader) {
                LOG(ERROR) << "search: lucene IndexReader is null for field: " << field_name;
                return Status::InternalError("IndexReader is null for field: " + field_name);
            }

            // Set reader in composite reader and keep ownership
            composite_reader->set_reader(field_wstr, index_reader.get());
            index_readers.push_back(std::move(index_reader));

            LOG(INFO) << "search: Successfully added IndexReader for field: " << field_name;
        }

        if (index_readers.empty()) {
            LOG(WARNING) << "search: No valid IndexReaders created for any field";
            segment_v2::InvertedIndexResultBitmap result(roaring, null_bitmap);
            bitmap_result = result;
            return Status::OK();
        }

        LOG(INFO) << "search: Created CompositeReader with " << index_readers.size()
                  << " IndexReaders";

        // Execute the Boolean query using the CompositeReader-based approach
        auto weight = root_query->weight(false); // disable scoring for now
        if (!weight) {
            LOG(WARNING) << "search: Failed to create weight for Boolean query";
            segment_v2::InvertedIndexResultBitmap result(roaring, null_bitmap);
            bitmap_result = result;
            return Status::OK();
        }

        auto scorer = weight->scorer(composite_reader);
        if (!scorer) {
            LOG(WARNING) << "search: Failed to create scorer from weight";
            segment_v2::InvertedIndexResultBitmap result(roaring, null_bitmap);
            bitmap_result = result;
            return Status::OK();
        }

        // Collect documents from scorer
        uint32_t doc = scorer->doc();
        uint32_t matched_docs = 0;
        while (doc != query_v2::TERMINATED) {
            roaring->add(doc);
            matched_docs++;
            doc = scorer->advance();
        }

        LOG(INFO) << "search: Multi-value query completed, matched " << matched_docs
                  << " documents";

        segment_v2::InvertedIndexResultBitmap result(roaring, null_bitmap);
        bitmap_result = result;
        return Status::OK();

    } catch (const std::exception& e) {
        LOG(WARNING) << "search: Exception during direct Boolean query execution: " << e.what();
        return Status::InternalError("Exception during search query execution: " +
                                     std::string(e.what()));
    }
}

// Aligned with FE QsClauseType enum - uses enum.name() as clause_type
FunctionSearch::ClauseTypeCategory FunctionSearch::get_clause_type_category(
        const std::string& clause_type) const {
    if (clause_type == "AND" || clause_type == "OR" || clause_type == "NOT") {
        return ClauseTypeCategory::COMPOUND;
    } else if (clause_type == "TERM" || clause_type == "PREFIX" || clause_type == "WILDCARD" ||
               clause_type == "REGEXP" || clause_type == "RANGE" || clause_type == "LIST") {
        // Non-tokenized queries: exact matching, pattern matching, range, list operations
        return ClauseTypeCategory::NON_TOKENIZED;
    } else if (clause_type == "PHRASE" || clause_type == "MATCH" || clause_type == "ANY" ||
               clause_type == "ALL") {
        // Tokenized queries: phrase search, full-text search, multi-value matching
        // Note: ANY and ALL require tokenization of their input values
        return ClauseTypeCategory::TOKENIZED;
    } else {
        // Default to NON_TOKENIZED for unknown types
        LOG(WARNING) << "Unknown clause type '" << clause_type
                     << "', defaulting to NON_TOKENIZED category";
        return ClauseTypeCategory::NON_TOKENIZED;
    }
}

// Analyze query type for a specific field in the search clause
segment_v2::InvertedIndexQueryType FunctionSearch::analyze_field_query_type(
        const std::string& field_name, const TSearchClause& clause) const {
    const std::string& clause_type = clause.clause_type;
    ClauseTypeCategory category = get_clause_type_category(clause_type);

    // Handle leaf queries - use direct mapping
    if (category != ClauseTypeCategory::COMPOUND) {
        // Check if this clause targets the specific field
        if (clause.field_name == field_name) {
            // Use direct mapping from clause_type to InvertedIndexQueryType
            return clause_type_to_query_type(clause_type);
        }
    }

    // Handle boolean queries - recursively analyze children
    if (!clause.children.empty()) {
        for (const auto& child_clause : clause.children) {
            // Recursively analyze each child
            InvertedIndexQueryType child_type = analyze_field_query_type(field_name, child_clause);
            // If this child targets the field (not default EQUAL_QUERY), return its query type
            if (child_type != InvertedIndexQueryType::UNKNOWN_QUERY) {
                return child_type;
            }
        }
    }

    // If no children target this field, return UNKNOWN_QUERY as default
    return InvertedIndexQueryType::UNKNOWN_QUERY;
}

// Map clause_type string to InvertedIndexQueryType
segment_v2::InvertedIndexQueryType FunctionSearch::clause_type_to_query_type(
        const std::string& clause_type) const {
    // Use static map for better performance and maintainability
    static const std::unordered_map<std::string, segment_v2::InvertedIndexQueryType>
            clause_type_map = {
                    // Boolean operations
                    {"AND", segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY},
                    {"OR", segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY},
                    {"NOT", segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY},

                    // Non-tokenized queries (exact matching, pattern matching)
                    {"TERM", segment_v2::InvertedIndexQueryType::EQUAL_QUERY},
                    {"PREFIX", segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY},
                    {"WILDCARD", segment_v2::InvertedIndexQueryType::WILDCARD_QUERY},
                    {"REGEXP", segment_v2::InvertedIndexQueryType::MATCH_REGEXP_QUERY},
                    {"RANGE", segment_v2::InvertedIndexQueryType::RANGE_QUERY},
                    {"LIST", segment_v2::InvertedIndexQueryType::LIST_QUERY},

                    // Tokenized queries (full-text search, phrase search)
                    {"PHRASE", segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY},
                    {"MATCH", segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY},
                    {"ANY", segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY},
                    {"ALL", segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY},
            };

    auto it = clause_type_map.find(clause_type);
    if (it != clause_type_map.end()) {
        return it->second;
    }

    // Unknown clause type
    LOG(WARNING) << "Unknown clause type '" << clause_type << "', defaulting to EQUAL_QUERY";
    return segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
}

// Helper function to get InvertedIndexReader for a field
segment_v2::InvertedIndexReaderPtr FunctionSearch::get_field_inverted_reader(
        const std::string& field_name,
        const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators,
        const vectorized::DataTypePtr& column_type,
        segment_v2::InvertedIndexQueryType query_type) const {
    auto iter_it = iterators.find(field_name);
    if (iter_it == iterators.end() || !iter_it->second) {
        LOG(WARNING) << "No iterator found for field: " << field_name;
        return nullptr;
    }

    auto* field_iter = dynamic_cast<segment_v2::InvertedIndexIterator*>(iter_it->second);
    if (!field_iter) {
        LOG(WARNING) << "Iterator is not InvertedIndexIterator for field: " << field_name;
        return nullptr;
    }

    Result<segment_v2::InvertedIndexReaderPtr> reader_result;
    if (column_type) {
        reader_result = field_iter->select_best_reader(column_type, query_type);
    } else {
        reader_result = field_iter->select_best_reader();
    }

    if (!reader_result.has_value()) {
        LOG(WARNING) << "Failed to get reader for field: " << field_name
                     << ", error: " << reader_result.error().to_string();
        return nullptr;
    }

    auto reader = reader_result.value();
    if (!reader) {
        LOG(WARNING) << "Selected reader is null for field: " << field_name;
        return nullptr;
    }

    return reader;
}

// Helper function to get index properties for a field
std::map<std::string, std::string> FunctionSearch::get_field_index_properties(
        const std::string& field_name,
        const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators) const {
    std::map<std::string, std::string> empty_properties;

    auto reader = get_field_inverted_reader(field_name, iterators);
    if (!reader) {
        return empty_properties;
    }

    return reader->get_index_properties();
}

void register_function_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSearch>();
}

} // namespace doris::vectorized
