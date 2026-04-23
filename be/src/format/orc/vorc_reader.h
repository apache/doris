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

#include <cctz/time_zone.h>

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <orc/OrcFile.hh>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/column/column_array.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/column_type_convert.h"
#include "format/format_common.h"
#include "format/table/table_format_reader.h"
#include "format/table/table_schema_change_helper.h"
#include "format/table/transactional_hive_common.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/tracing_file_reader.h"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/sargs/Literal.hh"
#include "runtime/runtime_profile.h"

namespace doris {
class RuntimeState;
class TFileRangeDesc;
class TFileScanRangeParams;
namespace segment_v2 {
class RowIdColumnIteratorV2;
}
namespace io {
class FileSystem;
struct IOContext;
} // namespace io
class Block;
struct RowLineageColumns;
template <PrimitiveType T>
class ColumnVector;
template <PrimitiveType T>
class DataTypeDecimal;
template <DecimalNativeTypeConcept T>
struct Decimal;
} // namespace doris
namespace orc {
template <class T>
class DataBuffer;
} // namespace orc

namespace doris {
class ORCFileInputStream;

/// ORC-specific initialization context.
/// Extends ReaderInitContext with conjuncts and filter fields.
/// Note: ORC does NOT use slot_id_to_predicates (unlike Parquet).
struct OrcInitContext final : public ReaderInitContext {
    // Safe default for standalone readers (delete file readers) without conjuncts.
    static inline const VExprContextSPtrs EMPTY_CONJUNCTS {};

    const VExprContextSPtrs* conjuncts = &EMPTY_CONJUNCTS;
    const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;
};

struct LazyReadContext {
    VExprContextSPtrs conjuncts;
    bool can_lazy_read = false;
    // block->rows() returns the number of rows of the first column,
    // so we should check and resize the first column
    bool resize_first_column = true;
    std::list<std::string> all_read_columns;
    // include predicate_partition_columns & predicate_missing_columns
    std::vector<uint32_t> all_predicate_col_ids;
    // save slot_id to find dict filter column name, because expr column name may
    // be different with orc column name
    // std::pair<std::list<col_name>, std::vector<slot_id>>
    std::pair<std::list<std::string>, std::vector<int>> predicate_columns;
    // predicate orc file column names
    std::list<std::string> predicate_orc_columns;
    std::vector<std::string> lazy_read_columns;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            predicate_partition_columns;
    // lazy read partition columns or all partition columns
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> predicate_missing_columns;
    // lazy read missing columns or all missing columns
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;

    std::vector<std::string> partial_predicate_columns;

    // Record the number of rows filled in filter phase for lazy materialization
    // This is used to check if a column was already processed in filter phase
    size_t filter_phase_rows = 0;
};

class OrcReader : public TableFormatReader, public RowPositionProvider {
    ENABLE_FACTORY_CREATOR(OrcReader);

public:
    Status get_file_type(const orc::Type** root) {
        RETURN_IF_ERROR(_create_file_reader());
        *root = &(_reader->getType());
        return Status::OK();
    }

    struct Statistics {
        int64_t column_read_time = 0;
        int64_t get_batch_time = 0;
        int64_t create_reader_time = 0;
        int64_t init_column_time = 0;
        int64_t decode_value_time = 0;
        int64_t decode_null_map_time = 0;
        int64_t predicate_filter_time = 0;
        int64_t dict_filter_rewrite_time = 0;
        int64_t lazy_read_filtered_rows = 0;
        int64_t file_footer_read_calls = 0;
        int64_t file_footer_hit_cache = 0;
    };

    OrcReader(RuntimeProfile* profile, RuntimeState* state, const TFileScanRangeParams& params,
              const TFileRangeDesc& range, size_t batch_size, const std::string& ctz,
              io::IOContext* io_ctx, FileMetaCache* meta_cache = nullptr,
              bool enable_lazy_mat = true);

    OrcReader(RuntimeProfile* profile, RuntimeState* state, const TFileScanRangeParams& params,
              const TFileRangeDesc& range, size_t batch_size, const std::string& ctz,
              std::shared_ptr<io::IOContext> io_ctx_holder, FileMetaCache* meta_cache = nullptr,
              bool enable_lazy_mat = true);

    OrcReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
              const std::string& ctz, io::IOContext* io_ctx, FileMetaCache* meta_cache = nullptr,
              bool enable_lazy_mat = true);

    OrcReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
              const std::string& ctz, std::shared_ptr<io::IOContext> io_ctx_holder,
              FileMetaCache* meta_cache = nullptr, bool enable_lazy_mat = true);

    ~OrcReader() override = default;

    // Override to build table_info_node from ORC file type using by_orc_name.
    // Subclasses (HiveOrcReader, IcebergOrcReader) call GenericReader::on_before_init_reader
    // directly, so this OrcReader-level override only applies to plain OrcReader (TVF, load).
    Status on_before_init_reader(ReaderInitContext* ctx) override;

protected:
    // ---- Unified init_reader(ReaderInitContext*) overrides ----
    Status _open_file_reader(ReaderInitContext* ctx) override;
    Status _do_init_reader(ReaderInitContext* ctx) override;

public:
    int64_t size() const;

    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;

    Status init_schema_reader() override;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override;

    void set_position_delete_rowids(const std::vector<int64_t>* delete_rows) {
        _position_delete_ordered_rowids = delete_rows;
    }

    void set_delete_rows(const AcidRowIDSet* delete_rows) { _delete_rows = delete_rows; }

    Status filter(orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size, void* arg);

    Status fill_dict_filter_column_names(
            std::unique_ptr<orc::StripeInformation> current_strip_information,
            std::list<std::string>& column_names);

    Status on_string_dicts_loaded(
            std::unordered_map<std::string, orc::StringDictionary*>& column_name_to_dict_map,
            bool* is_stripe_filtered);

    DataTypePtr convert_to_doris_type(const orc::Type* orc_type);

    static std::string get_field_name_lower_case(const orc::Type* orc_type, int pos);

    void set_create_row_id_column_iterator_func(
            std::function<std::shared_ptr<segment_v2::RowIdColumnIteratorV2>()> create_func) {
        _create_topn_row_id_column_iterator = create_func;
    }

    Status fill_topn_row_id(
            std::shared_ptr<segment_v2::RowIdColumnIteratorV2> _row_id_column_iterator,
            std::string col_name, Block* block, size_t rows) {
        int col_pos = block->get_position_by_name(col_name);
        DCHECK(col_pos >= 0);
        if (col_pos < 0) {
            return Status::InternalError("Column {} not found in block", col_name);
        }
        auto col = block->get_by_position(col_pos).column->assume_mutable();
        const auto& row_ids = this->current_batch_row_positions();
        RETURN_IF_ERROR(
                _row_id_column_iterator->read_by_rowids(row_ids.data(), row_ids.size(), col));

        return Status::OK();
    }

    static bool inline is_hive1_col_name(const orc::Type* orc_type_ptr) {
        for (uint64_t idx = 0; idx < orc_type_ptr->getSubtypeCount(); idx++) {
            if (!_is_hive1_col_name(orc_type_ptr->getFieldName(idx))) {
                return false;
            }
        }
        return true;
    }
    static const orc::Type& remove_acid(const orc::Type& type);

    bool count_read_rows() override { return true; }

    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override {
        _condition_cache_ctx = std::move(ctx);
    }

    bool supports_count_pushdown() const override { return true; }

    int64_t get_total_rows() const override {
        return _row_reader ? _row_reader->getNumberOfRows() : 0;
    }

    bool has_delete_operations() const override {
        return (_position_delete_ordered_rowids != nullptr &&
                !_position_delete_ordered_rowids->empty()) ||
               (_delete_rows != nullptr && !_delete_rows->empty());
    }

    // RowPositionProvider implementation
    const std::vector<rowid_t>& current_batch_row_positions() const override {
        return _current_batch_row_positions;
    }

protected:
    void _collect_profile_before_close() override;
    void _filter_rows_by_condition_cache(size_t* read_rows, bool* eof);

    // Core block reading implementation
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    // ORC fills partition/missing columns per-batch internally,
    // so suppress TableFormatReader's default on_after_read_block fill.
    Status on_after_read_block(Block* /*block*/, size_t* /*read_rows*/) override {
        return Status::OK();
    }

    // Protected accessors so CRTP mixin subclasses can reach private members
    io::IOContext* get_io_ctx() const { return _io_ctx; }
    std::unordered_map<std::string, uint32_t>*& col_name_to_block_idx_ref() {
        return _col_name_to_block_idx;
    }
    RuntimeProfile* get_profile() const { return _profile; }
    RuntimeState* get_state() const { return _state; }
    const TFileScanRangeParams& get_scan_params() const { return _scan_params; }
    const TFileRangeDesc& get_scan_range() const { return _scan_range; }
    const TupleDescriptor* get_tuple_descriptor() const { return _tuple_descriptor; }
    const RowDescriptor* get_row_descriptor() const { return _row_descriptor; }

private:
    struct OrcProfile {
        RuntimeProfile::Counter* read_time = nullptr;
        RuntimeProfile::Counter* read_calls = nullptr;
        RuntimeProfile::Counter* read_bytes = nullptr;
        RuntimeProfile::Counter* column_read_time;
        RuntimeProfile::Counter* get_batch_time = nullptr;
        RuntimeProfile::Counter* create_reader_time = nullptr;
        RuntimeProfile::Counter* init_column_time = nullptr;
        RuntimeProfile::Counter* decode_value_time = nullptr;
        RuntimeProfile::Counter* decode_null_map_time = nullptr;
        RuntimeProfile::Counter* predicate_filter_time = nullptr;
        RuntimeProfile::Counter* dict_filter_rewrite_time = nullptr;
        RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
        RuntimeProfile::Counter* selected_row_group_count = nullptr;
        RuntimeProfile::Counter* evaluated_row_group_count = nullptr;
        RuntimeProfile::Counter* file_footer_read_calls = nullptr;
        RuntimeProfile::Counter* file_footer_hit_cache = nullptr;
    };

    class ORCFilterImpl : public orc::ORCFilter {
    public:
        ORCFilterImpl(OrcReader* orcReader) : _orcReader(orcReader) {}
        ~ORCFilterImpl() override = default;
        void filter(orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size,
                    void* arg) const override {
            THROW_IF_ERROR(_orcReader->filter(data, sel, size, arg));
        }

    private:
        OrcReader* _orcReader = nullptr;
    };

    class StringDictFilterImpl : public orc::StringDictFilter {
    public:
        StringDictFilterImpl(OrcReader* orc_reader) : _orc_reader(orc_reader) {}
        ~StringDictFilterImpl() override = default;

        void fillDictFilterColumnNames(
                std::unique_ptr<orc::StripeInformation> current_strip_information,
                std::list<std::string>& column_names) const override {
            THROW_IF_ERROR(_orc_reader->fill_dict_filter_column_names(
                    std::move(current_strip_information), column_names));
        }
        void onStringDictsLoaded(
                std::unordered_map<std::string, orc::StringDictionary*>& column_name_to_dict_map,
                bool* is_stripe_filtered) const override {
            THROW_IF_ERROR(_orc_reader->on_string_dicts_loaded(column_name_to_dict_map,
                                                               is_stripe_filtered));
        }

    private:
        OrcReader* _orc_reader = nullptr;
    };

    //class RowFilter : public orc::RowReader

    // Create inner orc file,
    // return EOF if file is empty
    // return EROOR if encounter error.
    Status _create_file_reader();

    void _init_profile();
    Status _init_read_columns();
    void _init_file_column_mapping();

    static bool _check_acid_schema(const orc::Type& type);

    // ---- set_fill_columns sub-functions ----
    // Collect predicate columns from conjuncts for lazy materialization.
    void _collect_predicate_columns_from_conjuncts(
            std::unordered_map<std::string, std::pair<uint32_t, int>>& predicate_table_columns);
    // Classify read/partition/missing columns into lazy vs predicate groups.
    void _classify_columns_for_lazy_read(
            const std::unordered_map<std::string, std::pair<uint32_t, int>>&
                    predicate_table_columns,
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns);
    // Create ORC row reader with proper options, tiny stripe optimization, and type map.
    Status _init_orc_row_reader();

    // functions for building search argument until _init_search_argument
    // Get predicate type from slot reference
    std::pair<bool, orc::PredicateDataType> _get_orc_predicate_type(const VSlotRef* slot_ref);

    // Make ORC literal from Doris literal
    std::pair<bool, orc::Literal> _make_orc_literal(const VSlotRef* slot_ref,
                                                    const VLiteral* literal);
    bool _check_slot_can_push_down(const VExprSPtr& expr);
    bool _check_literal_can_push_down(const VExprSPtr& expr, size_t child_id);
    bool _check_rest_children_can_push_down(const VExprSPtr& expr);
    bool _check_expr_can_push_down(const VExprSPtr& expr);
    void _build_less_than(const VExprSPtr& expr,
                          std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_less_than_equals(const VExprSPtr& expr,
                                 std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_equals(const VExprSPtr& expr, std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_filter_in(const VExprSPtr& expr,
                          std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_is_null(const VExprSPtr& expr,
                        std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    bool _build_search_argument(const VExprSPtr& expr,
                                std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    bool _init_search_argument(const VExprSPtrs& exprs);

    void _execute_filter_position_delete_rowids(IColumn::Filter& filter, int64_t start_row);
    void _fill_batch_vec(std::vector<orc::ColumnVectorBatch*>& result,
                         orc::ColumnVectorBatch* batch, int idx);

    void _build_delete_row_filter(const Block* block, size_t rows);
    Status _get_next_block_impl(Block* block, size_t* read_rows, bool* eof);
    void _init_system_properties();
    void _init_file_description();

    template <bool is_filter = false>
    Status _fill_doris_data_column(const std::string& col_name, MutableColumnPtr& data_column,
                                   const DataTypePtr& data_type,
                                   std::shared_ptr<TableSchemaChangeHelper::Node> root_node,
                                   const orc::Type* orc_column_type,
                                   const orc::ColumnVectorBatch* cvb, size_t num_values);

    template <bool is_filter = false>
    Status _orc_column_to_doris_column(const std::string& col_name, ColumnPtr& doris_column,
                                       const DataTypePtr& data_type,
                                       std::shared_ptr<TableSchemaChangeHelper::Node> root_node,
                                       const orc::Type* orc_column_type,
                                       const orc::ColumnVectorBatch* cvb, size_t num_values);

    template <PrimitiveType PType, typename OrcColumnType>
    Status _decode_flat_column(const std::string& col_name, const MutableColumnPtr& data_column,
                               const orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        auto* data = dynamic_cast<const OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for column '{}', expected {}", col_name,
                                         cvb->toString());
        }
        auto* cvb_data = data->data.data();
        auto& column_data = static_cast<ColumnVector<PType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        for (int i = 0; i < num_values; ++i) {
            column_data[origin_size + i] =
                    (typename PrimitiveTypeTraits<PType>::CppType)cvb_data[i];
        }
        return Status::OK();
    }

    template <PrimitiveType DecimalPrimitiveType>
    void _init_decimal_converter(const DataTypePtr& data_type, DecimalScaleParams& scale_params,
                                 const int32_t orc_decimal_scale) {
        if (scale_params.scale_type != DecimalScaleParams::NOT_INIT) {
            return;
        }
        auto* decimal_type = reinterpret_cast<const DataTypeDecimal<DecimalPrimitiveType>*>(
                remove_nullable(data_type).get());
        auto dest_scale = decimal_type->get_scale();
        if (dest_scale > orc_decimal_scale) {
            scale_params.scale_type = DecimalScaleParams::SCALE_UP;
            scale_params.scale_factor =
                    cast_set<int64_t>(DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(
                            dest_scale - orc_decimal_scale));
        } else if (dest_scale < orc_decimal_scale) {
            scale_params.scale_type = DecimalScaleParams::SCALE_DOWN;
            scale_params.scale_factor =
                    cast_set<int64_t>(DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(
                            orc_decimal_scale - dest_scale));
        } else {
            scale_params.scale_type = DecimalScaleParams::NO_SCALE;
            scale_params.scale_factor = 1;
        }
    }

    template <PrimitiveType DecimalPrimitiveType, typename OrcColumnType, bool is_filter>
    Status _decode_explicit_decimal_column(const std::string& col_name,
                                           const MutableColumnPtr& data_column,
                                           const DataTypePtr& data_type,
                                           const orc::ColumnVectorBatch* cvb, size_t num_values) {
        using DecimalType = typename PrimitiveTypeTraits<DecimalPrimitiveType>::CppType;
        auto* data = dynamic_cast<const OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for column '{}', expected {}", col_name,
                                         cvb->toString());
        }
        if (_decimal_scale_params_index >= _decimal_scale_params.size()) {
            DecimalScaleParams temp_scale_params;
            _init_decimal_converter<DecimalPrimitiveType>(data_type, temp_scale_params,
                                                          data->scale);
            _decimal_scale_params.emplace_back(temp_scale_params);
        }
        DecimalScaleParams& scale_params = _decimal_scale_params[_decimal_scale_params_index];
        ++_decimal_scale_params_index;

        auto* cvb_data = data->values.data();
        auto& column_data =
                static_cast<ColumnDecimal<DecimalPrimitiveType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);

        if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
            for (int i = 0; i < num_values; ++i) {
                int128_t value;
                if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
                    value = static_cast<int128_t>(cvb_data[i]);
                } else {
                    // cast data to non const, to use a third-party dependency method to obtain an integer
                    auto* non_const_data = const_cast<OrcColumnType*>(data);
                    uint64_t hi = non_const_data->values[i].getHighBits();
                    uint64_t lo = non_const_data->values[i].getLowBits();
                    value = (((int128_t)hi) << 64) | (int128_t)lo;
                }
                value *= scale_params.scale_factor;
                auto& v = reinterpret_cast<DecimalType&>(column_data[origin_size + i]);
                v = (DecimalType)value;
            }
        } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
            for (int i = 0; i < num_values; ++i) {
                int128_t value;
                if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
                    value = static_cast<int128_t>(cvb_data[i]);
                } else {
                    // cast data to non const, to use a third-party dependency method to obtain an integer
                    auto* non_const_data = const_cast<OrcColumnType*>(data);
                    uint64_t hi = non_const_data->values[i].getHighBits();
                    uint64_t lo = non_const_data->values[i].getLowBits();
                    value = (((int128_t)hi) << 64) | (int128_t)lo;
                }
                value /= scale_params.scale_factor;
                auto& v = reinterpret_cast<DecimalType&>(column_data[origin_size + i]);
                v = (DecimalType)value;
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                int128_t value;
                if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
                    value = static_cast<int128_t>(cvb_data[i]);
                } else {
                    // cast data to non const, to use a third-party dependency method to obtain an integer
                    auto* non_const_data = const_cast<OrcColumnType*>(data);
                    uint64_t hi = non_const_data->values[i].getHighBits();
                    uint64_t lo = non_const_data->values[i].getLowBits();
                    value = (((int128_t)hi) << 64) | (int128_t)lo;
                }
                auto& v = reinterpret_cast<DecimalType&>(column_data[origin_size + i]);
                v = (DecimalType)value;
            }
        }
        return Status::OK();
    }

    template <bool is_filter>
    Status _decode_int32_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                const orc::ColumnVectorBatch* cvb, size_t num_values);

    template <PrimitiveType DecimalPrimitiveType, bool is_filter>
    Status _decode_decimal_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                  const DataTypePtr& data_type, const orc::ColumnVectorBatch* cvb,
                                  size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        if (dynamic_cast<const orc::Decimal64VectorBatch*>(cvb) != nullptr) {
            return _decode_explicit_decimal_column<DecimalPrimitiveType, orc::Decimal64VectorBatch,
                                                   is_filter>(col_name, data_column, data_type, cvb,
                                                              num_values);
        } else {
            return _decode_explicit_decimal_column<DecimalPrimitiveType, orc::Decimal128VectorBatch,
                                                   is_filter>(col_name, data_column, data_type, cvb,
                                                              num_values);
        }
    }

    template <typename CppType, PrimitiveType DorisColumnType, typename OrcColumnType,
              bool is_filter>
    Status _decode_time_column(const std::string& col_name, const MutableColumnPtr& data_column,
                               const orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        auto* data = dynamic_cast<const OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for column '{}', expected {}", col_name,
                                         cvb->toString());
        }
        date_day_offset_dict& date_dict = date_day_offset_dict::get();
        auto& column_data = static_cast<ColumnVector<DorisColumnType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        UInt8* __restrict filter_data;
        if constexpr (is_filter) {
            filter_data = _filter->data();
        }
        for (int i = 0; i < num_values; ++i) {
            auto& v = reinterpret_cast<CppType&>(column_data[origin_size + i]);
            if constexpr (std::is_same_v<OrcColumnType, orc::LongVectorBatch>) { // date
                if constexpr (is_filter) {
                    if (!filter_data[i]) {
                        continue;
                    }
                }

                // ORC DATE stores a logical day count without time zone semantics.
                int32_t date_value = cast_set<int32_t>(data->data[i]);
                if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                    v.create_from_date_v2(date_dict[date_value], TIME_DATE);
                    // we should cast to date if using date v1.
                    v.cast_to_date();
                } else {
                    v = date_dict[date_value];
                }
            } else { // timestamp
                if constexpr (is_filter) {
                    if (!filter_data[i]) {
                        continue;
                    }
                }
                v.from_unixtime(data->data[i], _time_zone);
                if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    // nanoseconds will lose precision. only keep microseconds.
                    v.set_microsecond(data->nanoseconds[i] / 1000);
                }
            }
        }
        return Status::OK();
    }

    template <bool is_filter>
    Status _decode_timestamp_tz_column(const std::string& col_name,
                                       const MutableColumnPtr& data_column,
                                       const orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        const auto* data = dynamic_cast<const orc::TimestampVectorBatch*>(cvb);
        if (data == nullptr) {
            return Status::InternalError(
                    "Wrong data type for timestamp_tz column '{}', expected {}", col_name,
                    cvb->toString());
        }
        auto& column_data = assert_cast<ColumnTimeStampTz&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        UInt8* __restrict filter_data;
        if constexpr (is_filter) {
            filter_data = _filter->data();
        }
        static const cctz::time_zone utc_time_zone = cctz::utc_time_zone();
        for (int i = 0; i < num_values; ++i) {
            auto& tz = column_data[origin_size + i];
            if constexpr (is_filter) {
                if (!filter_data[i]) {
                    continue;
                }
            }
            tz.from_unixtime(data->data[i], utc_time_zone);
            // nanoseconds will lose precision. only keep microseconds.
            tz.set_microsecond(data->nanoseconds[i] / 1000);
        }
        return Status::OK();
    }

    template <bool is_filter>
    Status _decode_string_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                 const orc::TypeKind& type_kind, const orc::ColumnVectorBatch* cvb,
                                 size_t num_values);

    template <bool is_filter>
    Status _decode_string_non_dict_encoded_column(const MutableColumnPtr& data_column,
                                                  const orc::TypeKind& type_kind,
                                                  const orc::EncodedStringVectorBatch* cvb,
                                                  size_t num_values);

    template <bool is_filter>
    Status _decode_string_dict_encoded_column(const MutableColumnPtr& data_column,
                                              const orc::TypeKind& type_kind,
                                              const orc::EncodedStringVectorBatch* cvb,
                                              size_t num_values);

    Status _fill_doris_array_offsets(const std::string& col_name,
                                     ColumnArray::Offsets64& doris_offsets,
                                     const orc::DataBuffer<int64_t>& orc_offsets, size_t num_values,
                                     size_t* element_size);

    bool _can_filter_by_dict(int slot_id);

    Status _rewrite_dict_conjuncts(std::vector<int32_t>& dict_codes, int slot_id, bool is_nullable);

    Status _convert_dict_cols_to_string_cols(Block* block,
                                             const std::vector<orc::ColumnVectorBatch*>* batch_vec);

    MutableColumnPtr _convert_dict_column_to_string_column(const ColumnInt32* dict_column,
                                                           const NullMap* null_map,
                                                           orc::ColumnVectorBatch* cvb,
                                                           const orc::Type* orc_column_typ);
    int64_t get_remaining_rows() const { return _remaining_rows; }
    void set_remaining_rows(int64_t rows) { _remaining_rows = rows; }

    // check if the given name is like _col0, _col1, ...
    static bool inline _is_hive1_col_name(const std::string& name) {
        if (name.size() <= 4) {
            return false;
        }
        if (name.substr(0, 4) != "_col") {
            return false;
        }
        for (size_t i = 4; i < name.size(); ++i) {
            if (!isdigit(name[i])) {
                return false;
            }
        }
        return true;
    }

    bool _seek_to_read_one_line() {
        if (_read_by_rows) {
            if (_row_ids.empty()) {
                return false;
            }
            _row_reader->seekToRow(_row_ids.front());
            _row_ids.pop_front();
        }
        return true;
    }

    Status _set_read_one_line_impl() override {
        _batch_size = 1;
        // If the ORC row reader already exists, the batch was created earlier
        // (during _do_init_reader) with the original _batch_size (capped to
        // _MIN_BATCH_SIZE = 4064).  We must recreate it with the new size of 1
        // so that nextBatch() returns at most 1 row per call.
        if (_row_reader) {
            _batch = _row_reader->createRowBatch(_batch_size);
        }
        return Status::OK();
    }

    // This is only for count(*) short circuit read.
    // save the total number of rows in range
    int64_t _remaining_rows = 0;
    RuntimeProfile* _profile = nullptr;
    RuntimeState* _state = nullptr;
    const TFileScanRangeParams& _scan_params;
    const TFileRangeDesc& _scan_range;
    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;
    size_t _batch_size;
    // Bytes-per-row estimate from the previous batch, used to pre-shrink _batch_size
    // before reading so that oversized blocks are prevented from the current call onward.
    // Zero means no prior data (first batch).
    size_t _load_bytes_per_row = 0;
    int64_t _range_start_offset;

protected:
    size_t get_batch_size() const { return _batch_size; }

private:
    int64_t _range_size;
    std::string _ctz;

    cctz::time_zone _time_zone;

    // The columns of the table to be read (contain columns that do not exist)
    std::vector<std::string> _table_column_names;

    // The columns of the file to be read  (file column name)
    std::list<std::string> _read_file_cols;

    // The columns of the table to be read (table column name)
    std::list<std::string> _read_table_cols;

    // file column name to std::vector<orc::ColumnVectorBatch*> idx.
    std::unordered_map<std::string, int> _colname_to_idx;

    // file column name to orc type
    std::unordered_map<std::string, const orc::Type*> _type_map;

    // Column ID to file original type mapping for handling incomplete MAP type due to column pruning.
    std::unordered_map<uint64_t, const orc::Type*> _column_id_to_file_type;

    std::unique_ptr<ORCFileInputStream> _file_input_stream;
    Statistics _statistics;
    OrcProfile _orc_profile;
    orc::ReaderMetrics _reader_metrics;

    std::unique_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::Reader> _reader = nullptr;
    std::unique_ptr<orc::RowReader> _row_reader;

    // The absolute row number where the most recent batch started (set after nextBatch).
    // Used by the condition-cache code to map in-batch indices to granules.
    uint64_t _last_read_row_number = 0;
    // The absolute row number where the *next* nextBatch call will start reading.
    // Used by _filter_rows_by_condition_cache to decide which granule to seek to.
    uint64_t _current_read_position = 0;
    // The absolute row number of the first row in this scan range.
    // Used to convert absolute granule indices to cache-relative indices.
    uint64_t _first_row_in_range = 0;
    std::shared_ptr<ConditionCacheContext> _condition_cache_ctx;
    std::unique_ptr<ORCFilterImpl> _orc_filter;
    orc::RowReaderOptions _row_reader_options;

    std::shared_ptr<io::FileSystem> _file_system;

    io::IOContext* _io_ctx = nullptr;
    std::shared_ptr<io::IOContext> _io_ctx_holder;
    const TupleDescriptor* _tuple_descriptor = nullptr;
    const RowDescriptor* _row_descriptor = nullptr;
    bool _enable_lazy_mat = true;
    bool _enable_filter_by_min_max = true;

    std::vector<DecimalScaleParams> _decimal_scale_params;
    size_t _decimal_scale_params_index;

protected:
    bool _is_acid = false;
    // Protected so Iceberg subclasses can register synthesized columns
    // in on_before_init_reader.
    LazyReadContext _lazy_read_ctx;

    std::function<std::shared_ptr<segment_v2::RowIdColumnIteratorV2>()>
            _create_topn_row_id_column_iterator;

private:
    std::unique_ptr<IColumn::Filter> _filter;
    const AcidRowIDSet* _delete_rows = nullptr;
    std::unique_ptr<IColumn::Filter> _delete_rows_filter_ptr;

    VExprContextSPtrs _not_single_slot_filter_conjuncts;
    const std::unordered_map<int, VExprContextSPtrs>* _slot_id_to_filter_conjuncts = nullptr;
    VExprContextSPtrs _dict_filter_conjuncts;
    VExprContextSPtrs _non_dict_filter_conjuncts;
    VExprContextSPtrs _filter_conjuncts;
    bool _disable_dict_filter = false;
    // std::pair<col_name, slot_id>
    std::vector<std::pair<std::string, int>> _dict_filter_cols;
    std::unique_ptr<ObjectPool> _obj_pool;
    std::unique_ptr<StringDictFilterImpl> _string_dict_filter;
    bool _dict_cols_has_converted = false;

    // resolve schema type change
    std::unordered_map<std::string, std::unique_ptr<converter::ColumnTypeConverter>> _converters;

    //support iceberg position delete .
    const std::vector<int64_t>* _position_delete_ordered_rowids = nullptr;
    std::unordered_map<const VSlotRef*, orc::PredicateDataType>
            _vslot_ref_to_orc_predicate_data_type;
    std::unordered_map<const VLiteral*, orc::Literal> _vliteral_to_orc_literal;

    // If you set "orc_tiny_stripe_threshold_bytes" = 0, the use tiny stripes merge io optimization will not be used.
    int64_t _orc_tiny_stripe_threshold_bytes = 8L * 1024L * 1024L;
    int64_t _orc_once_max_read_bytes = 8L * 1024L * 1024L;
    int64_t _orc_max_merge_distance_bytes = 1L * 1024L * 1024L;

    std::vector<rowid_t> _current_batch_row_positions;

    // Through this node, you can find the file column based on the table column.
    std::shared_ptr<TableSchemaChangeHelper::Node> _table_info_node_ptr =
            TableSchemaChangeHelper::ConstNode::get_instance();

    std::set<uint64_t> _column_ids;
    std::set<uint64_t> _filter_column_ids;

    // Pointer to external column name to block index mapping (from FileScanner)
    std::unordered_map<std::string, uint32_t>* _col_name_to_block_idx = nullptr;

    VExprSPtrs _push_down_exprs;
};

class StripeStreamInputStream : public orc::InputStream, public ProfileCollector {
public:
    StripeStreamInputStream(const std::string& file_name, io::FileReaderSPtr inner_reader,
                            const io::IOContext* io_ctx, RuntimeProfile* profile)
            : _file_name(file_name),
              _inner_reader(std::move(inner_reader)),
              _io_ctx(io_ctx),
              _profile(profile) {}

    ~StripeStreamInputStream() override {
        if (_inner_reader != nullptr) {
            _inner_reader->collect_profile_before_close();
        }
    }

    uint64_t getLength() const override { return _inner_reader->size(); }

    uint64_t getNaturalReadSize() const override { return config::orc_natural_read_size_mb << 20; }

    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override { return _file_name; }

    RuntimeProfile* profile() const { return _profile; }

protected:
    void _collect_profile_at_runtime() override {};
    void _collect_profile_before_close() override {
        if (_inner_reader != nullptr) {
            _inner_reader->collect_profile_before_close();
        }
    };

private:
    const std::string& _file_name;
    io::FileReaderSPtr _inner_reader;
    // Owned by OrcReader
    const io::IOContext* _io_ctx = nullptr;
    RuntimeProfile* _profile = nullptr;
};

class ORCFileInputStream : public orc::InputStream, public ProfileCollector {
public:
    ORCFileInputStream(const std::string& file_name, io::FileReaderSPtr inner_reader,
                       const io::IOContext* io_ctx, RuntimeProfile* profile,
                       int64_t orc_once_max_read_bytes, int64_t orc_max_merge_distance_bytes)
            : _file_name(file_name),
              _inner_reader(inner_reader),
              _file_reader(inner_reader),
              _tracing_file_reader(io_ctx ? std::make_shared<io::TracingFileReader>(
                                                    _file_reader, io_ctx->file_reader_stats)
                                          : _file_reader),
              _orc_once_max_read_bytes(orc_once_max_read_bytes),
              _orc_max_merge_distance_bytes(orc_max_merge_distance_bytes),
              _io_ctx(io_ctx),
              _profile(profile) {}

    ~ORCFileInputStream() override { _collect_profile_before_close_file_stripe(); }

    uint64_t getLength() const override { return _tracing_file_reader->size(); }

    uint64_t getNaturalReadSize() const override { return config::orc_natural_read_size_mb << 20; }

    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override { return _file_name; }

    void beforeReadStripe(std::unique_ptr<orc::StripeInformation> current_strip_information,
                          const std::vector<bool>& selected_columns,
                          std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>&
                                  stripe_streams) override;

    void set_all_tiny_stripes() { _is_all_tiny_stripes = true; }

    io::FileReaderSPtr& get_file_reader() { return _file_reader; }

    io::FileReaderSPtr& get_inner_reader() { return _inner_reader; }

    io::FileReaderSPtr& get_tracing_file_reader() { return _tracing_file_reader; }

protected:
    void _collect_profile_at_runtime() override {};
    void _collect_profile_before_close() override { _collect_profile_before_close_file_stripe(); }

    void _collect_profile_before_close_file_stripe() {
        if (_file_reader != nullptr) {
            _file_reader->collect_profile_before_close();
        }
        for (const auto& stripe_stream : _stripe_streams) {
            if (stripe_stream != nullptr) {
                stripe_stream->collect_profile_before_close();
            }
        }
    }

private:
    void _build_input_stripe_streams(
            const std::unordered_map<orc::StreamId, io::PrefetchRange>& ranges,
            std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams);

    void _build_small_ranges_input_stripe_streams(
            const std::unordered_map<orc::StreamId, io::PrefetchRange>& ranges,
            std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams);

    void _build_large_ranges_input_stripe_streams(
            const std::unordered_map<orc::StreamId, io::PrefetchRange>& ranges,
            std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams);

    const std::string& _file_name;

    // _inner_reader is original file reader.
    // _file_reader == RangeCacheFileReader used by tiny stripe case, if not tiny stripe case,
    // _file_reader == _inner_reader.
    // _tracing_file_reader is tracing file reader with io context.
    // If io_ctx is null, _tracing_file_reader will be the same as _file_reader.
    io::FileReaderSPtr _inner_reader;
    io::FileReaderSPtr _file_reader;
    io::FileReaderSPtr _tracing_file_reader;

    bool _is_all_tiny_stripes = false;
    int64_t _orc_once_max_read_bytes;
    int64_t _orc_max_merge_distance_bytes;

    std::vector<std::shared_ptr<StripeStreamInputStream>> _stripe_streams;

    // Owned by OrcReader
    const io::IOContext* _io_ctx = nullptr;
    RuntimeProfile* _profile = nullptr;
};
} // namespace doris
