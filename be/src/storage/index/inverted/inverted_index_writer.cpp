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

#include "storage/index/inverted/inverted_index_writer.h"

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/key_coder.h"
#include "storage/storage_layout.h"
#include "storage/tablet/tablet_schema.h"
#include "util/faststring.h"

namespace doris::segment_v2 {

const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MERGE_FACTOR = 100000000;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;

template <FieldType field_type>
InvertedIndexColumnWriter<field_type>::InvertedIndexColumnWriter(const std::string& field_name,
                                                                 IndexFileWriter* index_file_writer,
                                                                 const TabletIndex* index_meta,
                                                                 const bool single_field)
        : _single_field(single_field),
          _index_meta(index_meta),
          _index_file_writer(index_file_writer) {
    _should_analyzer =
            inverted_index::InvertedIndexAnalyzer::should_analyzer(_index_meta->properties());
    _value_key_coder = get_key_coder(field_type);
    _field_name = StringUtil::string_to_wstring(field_name);
}

template <FieldType field_type>
InvertedIndexColumnWriter<field_type>::~InvertedIndexColumnWriter() {
    if (_index_writer != nullptr) {
        close_on_error();
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::init() {
    try {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::init_field_type_not_supported", {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "Field type not supported");
        })
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::init_inverted_index_writer_init_error",
                        { _CLTHROWA(CL_ERR_IO, "debug point: init index error"); })
        if constexpr (field_is_slice_type(field_type)) {
            return init_fulltext_index();
        } else if constexpr (field_is_numeric_type(field_type)) {
            return init_bkd_index();
        }
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Field type not supported");
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "Inverted index writer init error occurred: " << e.what();
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Inverted index writer init error occurred: {}", e.what());
    }
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::close_on_error() {
    try {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::close_on_error_throw_exception",
                        { _CLTHROWA(CL_ERR_IO, "debug point: close on error"); })
        // delete directory must be done before index_writer close
        // because index_writer will close the directory
        if (_dir) {
            _dir->deleteDirectory();
        }
        if (_index_writer) {
            _index_writer->close();
        }
    } catch (CLuceneError& e) {
        LOG(ERROR) << "InvertedIndexWriter close_on_error failure: " << e.what();
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::init_bkd_index() {
    size_t value_length = sizeof(CppType);
    // NOTE: initialize with 0, set to max_row_id when finished.
    int32_t max_doc = 0;
    int32_t total_point_count = std::numeric_limits<std::int32_t>::max();
    _bkd_writer = std::make_shared<lucene::util::bkd::bkd_writer>(
            max_doc, DIMS, DIMS, value_length, MAX_LEAF_COUNT, MAXMBSortInHeap, total_point_count,
            true, config::max_depth_in_bkd_tree);
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::init_bkd_index_throw_error",
                    { _CLTHROWA(CL_ERR_IllegalArgument, "debug point: create bkd_writer error"); })
    return open_index_directory();
}

template <FieldType field_type>
Result<ReaderPtr> InvertedIndexColumnWriter<field_type>::create_char_string_reader(
        CharFilterMap& char_filter_map) {
    try {
        return inverted_index::InvertedIndexAnalyzer::create_reader(char_filter_map);
    } catch (CLuceneError& e) {
        return ResultError(Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "inverted index create string reader failed: {}", e.what()));
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::open_index_directory() {
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::open_index_directory_error", {
        return Status::Error<ErrorCode::INTERNAL_ERROR>("debug point: open_index_directory_error");
    })
    _dir = DORIS_TRY(_index_file_writer->open(_index_meta));
    return Status::OK();
}

template <FieldType field_type>
std::unique_ptr<lucene::index::IndexWriter>
InvertedIndexColumnWriter<field_type>::create_index_writer() {
    bool create_index = true;
    bool close_dir_on_shutdown = true;
    auto index_writer = std::make_unique<lucene::index::IndexWriter>(
            _dir.get(), _analyzer.get(), create_index, close_dir_on_shutdown);
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_index_writer_setRAMBufferSizeMB_error",
                    { index_writer->setRAMBufferSizeMB(-100); })
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_index_writer_setMaxBufferedDocs_error",
                    { index_writer->setMaxBufferedDocs(1); })
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_index_writer_setMergeFactor_error",
                    { index_writer->setMergeFactor(1); })
    index_writer->setRAMBufferSizeMB(static_cast<float>(config::inverted_index_ram_buffer_size));
    index_writer->setMaxBufferedDocs(config::inverted_index_max_buffered_docs);
    index_writer->setMaxFieldLength(MAX_FIELD_LEN);
    index_writer->setMergeFactor(MERGE_FACTOR);
    index_writer->setUseCompoundFile(false);
    index_writer->setEnableCorrectTermWrite(config::enable_inverted_index_correct_term_write);
    index_writer->setSimilarity(_similarity.get());

    return index_writer;
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::create_field(lucene::document::Field** field) {
    int field_config =
            int(lucene::document::Field::STORE_NO) | int(lucene::document::Field::INDEX_NONORMS);
    field_config |= _should_analyzer ? int32_t(lucene::document::Field::INDEX_TOKENIZED)
                                     : int32_t(lucene::document::Field::INDEX_UNTOKENIZED);
    *field = new lucene::document::Field(_field_name.c_str(), field_config);
    (*field)->setOmitTermFreqAndPositions(
            !(get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
              INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES));
    if (_should_analyzer && should_write_index_norms(*_index_meta)) {
        (*field)->setOmitNorms(false);
    }

    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::always_omit_norms",
                    { (*field)->setOmitNorms(false); });

    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_field_v3", {
        if (_index_file_writer->get_storage_format() != InvertedIndexStorageFormatPB::V3) {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "debug point: InvertedIndexColumnWriter::create_field_v3 error");
        }
    })
    if (_index_file_writer->get_storage_format() >= InvertedIndexStorageFormatPB::V3) {
        (*field)->setIndexVersion(IndexVersion::kV4);
        // Only effective in v3
        std::string dict_compression =
                get_parser_dict_compression_from_properties(_index_meta->properties());
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_field_dic_compression", {
            if (dict_compression != INVERTED_INDEX_PARSER_TRUE) {
                return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "debug point: "
                        "InvertedIndexColumnWriter::create_field_dic_compression error");
            }
        })
        if (dict_compression == INVERTED_INDEX_PARSER_TRUE) {
            (*field)->updateFlag(FlagBits::DICT_COMPRESS);
        }
    }
    return Status::OK();
}

template <FieldType field_type>
Result<std::shared_ptr<lucene::analysis::Analyzer>>
InvertedIndexColumnWriter<field_type>::create_analyzer(
        const InvertedIndexAnalyzerConfig& analyzer_config) {
    try {
        return inverted_index::InvertedIndexAnalyzer::create_analyzer(&analyzer_config);
    } catch (CLuceneError& e) {
        return ResultError(Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "inverted index create analyzer failed: {}", e.what()));
    } catch (Exception& e) {
        return ResultError(Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "inverted index create analyzer failed: {}", e.what()));
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::init_fulltext_index() {
    _analyzer_config.analyzer_name = get_analyzer_name_from_properties(_index_meta->properties());
    _analyzer_config.parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(_index_meta->properties()));
    _analyzer_config.parser_mode =
            get_parser_mode_string_from_properties(_index_meta->properties());
    _analyzer_config.char_filter_map =
            get_parser_char_filter_map_from_properties(_index_meta->properties());
    _analyzer_config.lower_case =
            get_parser_lowercase_from_properties<true>(_index_meta->properties());
    _analyzer_config.stop_words = get_parser_stopwords_from_properties(_index_meta->properties());
    RETURN_IF_ERROR(open_index_directory());
    _char_string_reader = DORIS_TRY(create_char_string_reader(_analyzer_config.char_filter_map));
    if (_should_analyzer) {
        _analyzer = DORIS_TRY(create_analyzer(_analyzer_config));
    }
    _similarity = std::make_unique<lucene::search::LengthSimilarity>();
    _index_writer = create_index_writer();
    _doc = std::make_unique<lucene::document::Document>();
    if (_single_field) {
        RETURN_IF_ERROR(create_field(&_field));
        _doc->add(*_field);
    } else {
        // array's inverted index do need create field first
        _doc->setNeedResetFieldData(true);
    }
    auto ignore_above_value =
            get_parser_ignore_above_value_from_properties(_index_meta->properties());
    _ignore_above = std::stoi(ignore_above_value);
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_document() {
    DBUG_EXECUTE_IF("inverted_index_writer.add_document", { return Status::OK(); });

    try {
        _index_writer->addDocument(_doc.get());
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_document_throw_error",
                        { _CLTHROWA(CL_ERR_IO, "debug point: add_document io error"); })
    } catch (const CLuceneError& e) {
        close_on_error();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError add_document: {}", e.what());
    } catch (const Exception& e) {
        close_on_error();
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "Analyzer error while adding document: {}", e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_null_document() {
    try {
        _index_writer->addNullDocument(_doc.get());
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_null_document_throw_error",
                        { _CLTHROWA(CL_ERR_IO, "debug point: add_null_document io error"); })
    } catch (const CLuceneError& e) {
        close_on_error();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError add_null_document: {}", e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_nulls(uint32_t count) {
    _null_bitmap.addRange(_rid, _rid + count);
    _rid += count;
    if constexpr (field_is_slice_type(field_type)) {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_nulls_field_nullptr", { _field = nullptr; })
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_nulls_index_writer_nullptr",
                        { _index_writer = nullptr; })
        if (_field == nullptr || _index_writer == nullptr) {
            LOG(ERROR) << "field or index writer is null in inverted index writer.";
            return Status::InternalError("field or index writer is null in inverted index writer");
        }

        for (int i = 0; i < count; ++i) {
            RETURN_IF_ERROR(add_null_document());
        }
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_array_nulls(const uint8_t* null_map,
                                                              size_t num_rows) {
    DCHECK(_rid >= num_rows);
    if (num_rows == 0 || null_map == nullptr) {
        return Status::OK();
    }
    std::vector<uint32_t> null_indices;
    null_indices.reserve(num_rows / 8);

    // because _rid is the row id in block, not segment, and we add data before we add nulls,
    // so we need to subtract num_rows to get the row id in segment
    for (size_t i = 0; i < num_rows; i++) {
        if (null_map[i] == 1) {
            null_indices.push_back(cast_set<uint32_t>(_rid - num_rows + i));
        }
    }

    if (!null_indices.empty()) {
        _null_bitmap.addMany(null_indices.size(), null_indices.data());
    }

    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::new_inverted_index_field(const char* field_value_data,
                                                                       size_t field_value_size) {
    try {
        if (_should_analyzer) {
            new_char_token_stream(field_value_data, field_value_size, _field);
        } else {
            new_field_char_value(field_value_data, field_value_size, _field);
        }
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError create new index field error: {}", e.what());
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "Analyzer error while creating new index field: {}", e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::new_char_token_stream(const char* s, size_t len,
                                                                  lucene::document::Field* field) {
    _char_string_reader->init(s, cast_set<int32_t>(len), false);
    DBUG_EXECUTE_IF(
            "InvertedIndexColumnWriter::new_char_token_stream__char_string_reader_init_"
            "error",
            {
                _CLTHROWA(CL_ERR_UnsupportedOperation,
                          "UnsupportedOperationException: CLStream::init");
            })
    auto* stream = _analyzer->reusableTokenStream(field->name(), _char_string_reader);
    field->setValue(stream);
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::new_field_value(const char* s, size_t len,
                                                            lucene::document::Field* field) {
    auto* field_value = lucene::util::Misc::_charToWide(s, len);
    field->setValue(field_value, false);
    // setValue did not duplicate value, so we don't have to delete
    //_CLDELETE_ARRAY(field_value)
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::new_field_char_value(const char* s, size_t len,
                                                                 lucene::document::Field* field) {
    field->setValue((char*)s, len);
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add(const IColumn& column, size_t row_pos, size_t n) {
    if constexpr (field_is_slice_type(field_type)) {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_values_field_is_nullptr",
                        { _field = nullptr; })
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_values_index_writer_is_nullptr",
                        { _index_writer = nullptr; })
        if (_field == nullptr || _index_writer == nullptr) {
            LOG(ERROR) << "field or index writer is null in inverted index writer.";
            return Status::InternalError("field or index writer is null in inverted index writer");
        }
        // CHAR, VARCHAR and STRING all store the row's own bytes.
        const auto& strings = assert_cast<const ColumnString&>(column);
        for (size_t i = 0; i < n; ++i) {
            const StringRef value = strings.get_data_at(row_pos + i);
            // only ignore_above UNTOKENIZED strings and empty strings not tokenized
            if ((!_should_analyzer && value.size > _ignore_above) ||
                (_should_analyzer && value.size == 0)) {
                RETURN_IF_ERROR(add_null_document());
            } else {
                RETURN_IF_ERROR(new_inverted_index_field(value.data, value.size));
                RETURN_IF_ERROR(add_document());
            }
            _rid++;
        }
    } else if constexpr (field_is_numeric_type(field_type)) {
        const auto& data =
                assert_cast<const typename StorageLayout<field_type>::Column&>(column).get_data();
        for (size_t i = 0; i < n; ++i) {
            RETURN_IF_ERROR(add_value(StorageLayout<field_type>::to_storage(data[row_pos + i])));
            _rid++;
            _row_ids_seen_for_bkd++;
        }
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_array(const IColumn& items, size_t first_item,
                                                        const uint64_t* offsets, size_t num_rows) {
    if (num_rows == 0) {
        return Status::OK();
    }
    const uint8_t* nested_null_map = nullptr;
    const IColumn& nested = peel_nullable(items, first_item, &nested_null_map);
    if constexpr (field_is_slice_type(field_type)) {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_array_values_index_writer_is_nullptr",
                        { _index_writer = nullptr; })
        if (_index_writer == nullptr) {
            LOG(ERROR) << "index writer is null in inverted index writer.";
            return Status::InternalError("index writer is null in inverted index writer");
        }
        const auto& strings = assert_cast<const ColumnString&>(nested);
        size_t start_off = 0;
        std::vector<ReaderPtr> keep_readers;
        for (size_t i = 0; i < num_rows; ++i) {
            // The offsets are rebased to the segment (rebase_offsets), not to this
            // batch, while the elements are numbered from this batch's first one,
            // so only the per-row element count is read from them; walking every
            // row also keeps _rid in step.
            auto array_elem_size = offsets[i + 1] - offsets[i];
            // TODO(Amory).later we use object pool to avoid field creation
            std::unique_ptr<lucene::document::Field> new_field;
            CL_NS(analysis)::TokenStream* ts = nullptr;
            for (auto j = start_off; j < start_off + array_elem_size; ++j) {
                if (nested_null_map != nullptr && nested_null_map[j] == 1) {
                    continue;
                }
                const StringRef v = strings.get_data_at(first_item + j);
                if ((!_should_analyzer && v.size > _ignore_above) ||
                    (_should_analyzer && v.size == 0)) {
                    // is here a null value?
                    // TODO. Maybe here has performance problem for large size string.
                    continue;
                } else {
                    // now we temp create field . later make a pool
                    lucene::document::Field* tmp_field = nullptr;
                    Status st = create_field(&tmp_field);
                    new_field.reset(tmp_field);
                    DBUG_EXECUTE_IF(
                            "InvertedIndexColumnWriter::add_array_values_create_field_"
                            "error",
                            {
                                st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                        "debug point: add_array_values_create_field_error");
                            })
                    if (st != Status::OK()) {
                        LOG(ERROR) << "create field "
                                   << std::string(_field_name.begin(), _field_name.end())
                                   << " error:" << st;
                        return st;
                    }
                    if (_should_analyzer) {
                        try {
                            // Each array field needs an owned stream because streams cannot be
                            // reused across fields.
                            bool own_token_stream = true;
                            ReaderPtr char_string_reader = DORIS_TRY(
                                    create_char_string_reader(_analyzer_config.char_filter_map));
                            char_string_reader->init(v.data, cast_set<int32_t>(v.size), false);
                            ts = _analyzer->tokenStream(new_field->name(), char_string_reader);
                            new_field->setValue(ts, own_token_stream);
                            keep_readers.emplace_back(std::move(char_string_reader));
                        } catch (const CLuceneError& e) {
                            _doc->clear();
                            close_on_error();
                            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                                    "CLuceneError while creating array index field: {}", e.what());
                        } catch (const Exception& e) {
                            _doc->clear();
                            close_on_error();
                            return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                                    "Analyzer error while creating array index field: {}",
                                    e.what());
                        }
                    } else {
                        new_field_char_value(v.data, v.size, new_field.get());
                    }
                    // NOTE: new_field is managed by doc now, so we need to use release() to get the pointer
                    _doc->add(*new_field.release());
                }
            }
            start_off += array_elem_size;
            // here to make debug for array field with current doc which should has expected number of fields
            DBUG_EXECUTE_IF("array_inverted_index.write_index", {
                auto single_array_field_count =
                        DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                "array_inverted_index.write_index", "single_array_field_count", 0);
                if (single_array_field_count < 0) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "indexes count cannot be negative");
                }
                if (_doc->getFields()->size() != single_array_field_count) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "array field has fields count {} not equal to expected {}",
                            _doc->getFields()->size(), single_array_field_count);
                }
            })

            if (!_doc->getFields()->empty()) {
                // if this array is null, we just ignore to write inverted index
                RETURN_IF_ERROR(add_document());
                _doc->clear();
            } else {
                // avoid to add doc which without any field which may make threadState init skip
                // init fieldDataArray, then will make error with next doc with fields in
                // resetCurrentFieldData
                lucene::document::Field* tmp_field = nullptr;
                Status st = create_field(&tmp_field);
                new_field.reset(tmp_field);
                DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_array_values_create_field_error_2",
                                {
                                    st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                            "debug point: add_array_values_create_field_error_2");
                                })
                if (st != Status::OK()) {
                    LOG(ERROR) << "create field "
                               << std::string(_field_name.begin(), _field_name.end())
                               << " error:" << st;
                    return st;
                }
                _doc->add(*new_field.release());
                RETURN_IF_ERROR(add_null_document());
                _doc->clear();
            }
            _rid++;
            keep_readers.clear();
        }
    } else if constexpr (field_is_numeric_type(field_type)) {
        const auto& data =
                assert_cast<const typename StorageLayout<field_type>::Column&>(nested).get_data();
        size_t start_off = 0;
        for (size_t i = 0; i < num_rows; ++i) {
            auto array_elem_size = offsets[i + 1] - offsets[i];
            for (size_t j = start_off; j < start_off + array_elem_size; ++j) {
                if (nested_null_map != nullptr && nested_null_map[j] == 1) {
                    continue;
                }
                RETURN_IF_ERROR(
                        add_value(StorageLayout<field_type>::to_storage(data[first_item + j])));
            }
            start_off += array_elem_size;
            _row_ids_seen_for_bkd++;
            _rid++;
        }
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_value(const CppType& value) {
    try {
        std::string new_value;
        uint32_t value_length = sizeof(CppType);

        DBUG_EXECUTE_IF(
                "InvertedIndexColumnWriter::add_value_bkd_writer_add_throw_"
                "error",
                { _CLTHROWA(CL_ERR_IllegalArgument, ("packedValue should be length=xxx")); });

        _value_key_coder->full_encode_ascending(&value, &new_value);
        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError add_value: {}",
                                                                      e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
int64_t InvertedIndexColumnWriter<field_type>::size() const {
    //TODO: get memory size of inverted index
    return 0;
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::write_null_bitmap(
        lucene::store::IndexOutput* null_bitmap_out) {
    // write null_bitmap file
    _null_bitmap.runOptimize();
    size_t size = _null_bitmap.getSizeInBytes(false);
    if (size > 0) {
        faststring buf;
        buf.resize(size);
        _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
        null_bitmap_out->writeBytes(buf.data(), cast_set<int32_t>(size));
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::finish() {
    if (_dir != nullptr) {
        std::unique_ptr<lucene::store::IndexOutput> null_bitmap_out = nullptr;
        std::unique_ptr<lucene::store::IndexOutput> data_out = nullptr;
        std::unique_ptr<lucene::store::IndexOutput> index_out = nullptr;
        std::unique_ptr<lucene::store::IndexOutput> meta_out = nullptr;
        ErrorContext error_context;
        try {
            // write bkd file
            if constexpr (field_is_numeric_type(field_type)) {
                _bkd_writer->max_doc_ = _rid;
                _bkd_writer->docs_seen_ = _row_ids_seen_for_bkd;
                null_bitmap_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_null_bitmap_file_name()));
                data_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name()));
                meta_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name()));
                index_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_file_name()));
                write_null_bitmap(null_bitmap_out.get());

                DBUG_EXECUTE_IF("InvertedIndexWriter._set_bkd_data_out_nullptr",
                                { data_out = nullptr; });
                if (data_out != nullptr && meta_out != nullptr && index_out != nullptr) {
                    _bkd_writer->meta_finish(meta_out.get(),
                                             _bkd_writer->finish(data_out.get(), index_out.get()),
                                             int(field_type));
                } else {
                    LOG(WARNING) << "Inverted index writer create output error "
                                    "occurred: nullptr";
                    _CLTHROWA(CL_ERR_IO, "Create output error with nullptr");
                }
            } else if constexpr (field_is_slice_type(field_type)) {
                null_bitmap_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_null_bitmap_file_name()));
                write_null_bitmap(null_bitmap_out.get());
                DBUG_EXECUTE_IF(
                        "InvertedIndexWriter._throw_clucene_error_in_fulltext_"
                        "writer_close",
                        {
                            _CLTHROWA(CL_ERR_IO,
                                      "debug point: test throw error in fulltext "
                                      "index writer");
                        });
            }
        } catch (CLuceneError& e) {
            error_context.eptr = std::current_exception();
            error_context.err_msg.append("Inverted index writer finish error occurred: ");
            error_context.err_msg.append(e.what());
            LOG(ERROR) << error_context.err_msg;
        }
        FINALLY({
            FINALLY_CLOSE(null_bitmap_out);
            FINALLY_CLOSE(meta_out);
            FINALLY_CLOSE(data_out);
            FINALLY_CLOSE(index_out);
            if constexpr (field_is_numeric_type(field_type)) {
                FINALLY_CLOSE(_dir);
            } else if constexpr (field_is_slice_type(field_type)) {
                FINALLY_CLOSE(_index_writer);
                // After closing the _index_writer, it needs to be reset to null to prevent issues of not closing it or closing it multiple times.
                _index_writer.reset();
            }
        })

        return Status::OK();
    }
    LOG(WARNING) << "Inverted index writer finish error occurred: dir is nullptr";
    return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
            "Inverted index writer finish error occurred: dir is nullptr");
}

template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_CHAR>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_VARCHAR>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_STRING>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_TINYINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_SMALLINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_INT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_BIGINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_LARGEINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATE>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATETIME>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATEV2>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL32>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL64>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL256>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_BOOL>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_IPV4>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_IPV6>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_FLOAT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DOUBLE>;

} // namespace doris::segment_v2
