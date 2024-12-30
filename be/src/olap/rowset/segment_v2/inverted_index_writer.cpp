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

#include "olap/rowset/segment_v2/inverted_index_writer.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <glog/logging.h>

#include <limits>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif

#include "CLucene/analysis/standard95/StandardAnalyzer.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include "common/config.h"
#include "gutil/strings/strip.h"
#include "olap/field.h"
#include "olap/inverted_index_parser.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/collection_value.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/string_util.h"

#define FINALLY_CLOSE_OUTPUT(x)       \
    try {                             \
        if (x != nullptr) x->close(); \
    } catch (...) {                   \
    }
namespace doris::segment_v2 {
const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MERGE_FACTOR = 100000000;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;

bool InvertedIndexColumnWriter::check_support_inverted_index(const TabletColumn& column) {
    // bellow types are not supported in inverted index for extracted columns
    static std::set<FieldType> invalid_types = {
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            FieldType::OLAP_FIELD_TYPE_JSONB,
            FieldType::OLAP_FIELD_TYPE_ARRAY,
            FieldType::OLAP_FIELD_TYPE_FLOAT,
    };
    if (column.is_extracted_column() && (invalid_types.contains(column.type()))) {
        return false;
    }
    if (column.is_variant_type()) {
        return false;
    }
    return true;
}

template <FieldType field_type>
class InvertedIndexColumnWriterImpl : public InvertedIndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    explicit InvertedIndexColumnWriterImpl(const std::string& field_name,
                                           InvertedIndexFileWriter* index_file_writer,
                                           const TabletIndex* index_meta,
                                           const bool single_field = true)
            : _single_field(single_field),
              _index_meta(index_meta),
              _index_file_writer(index_file_writer) {
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta->properties()));
        _value_key_coder = get_key_coder(field_type);
        _field_name = StringUtil::string_to_wstring(field_name);
    }

    ~InvertedIndexColumnWriterImpl() override {
        if (_index_writer != nullptr) {
            close_on_error();
        }
    }

    Status init() override {
        try {
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
                    "Inverted index writer init error occurred");
        }
    }

    void close() {
        if (_index_writer) {
            _index_writer->close();
            _index_writer.reset();
        }
    }

    void close_on_error() override {
        try {
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

    Status init_bkd_index() {
        size_t value_length = sizeof(CppType);
        // NOTE: initialize with 0, set to max_row_id when finished.
        int32_t max_doc = 0;
        int32_t total_point_count = std::numeric_limits<std::int32_t>::max();
        _bkd_writer = std::make_shared<lucene::util::bkd::bkd_writer>(
                max_doc, DIMS, DIMS, value_length, MAX_LEAF_COUNT, MAXMBSortInHeap,
                total_point_count, true, config::max_depth_in_bkd_tree);
        return open_index_directory();
    }

    std::unique_ptr<lucene::analysis::Analyzer> create_chinese_analyzer() {
        auto chinese_analyzer = std::make_unique<lucene::analysis::LanguageBasedAnalyzer>();
        chinese_analyzer->setLanguage(L"chinese");
        chinese_analyzer->initDict(config::inverted_index_dict_path);

        auto mode = get_parser_mode_string_from_properties(_index_meta->properties());
        if (mode == INVERTED_INDEX_PARSER_FINE_GRANULARITY) {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::All);
        } else {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
        }

        return chinese_analyzer;
    }

    Status create_char_string_reader(std::unique_ptr<lucene::util::Reader>& string_reader) {
        CharFilterMap char_filter_map =
                get_parser_char_filter_map_from_properties(_index_meta->properties());
        if (!char_filter_map.empty()) {
            string_reader = std::unique_ptr<lucene::util::Reader>(CharFilterFactory::create(
                    char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE],
                    new lucene::util::SStringReader<char>(),
                    char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN],
                    char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT]));
        } else {
            string_reader = std::make_unique<lucene::util::SStringReader<char>>();
        }
        return Status::OK();
    }

    Status open_index_directory() {
        _dir = DORIS_TRY(_index_file_writer->open(_index_meta));
        return Status::OK();
    }

    Status create_index_writer(std::unique_ptr<lucene::index::IndexWriter>& index_writer) {
        bool create_index = true;
        bool close_dir_on_shutdown = true;
        index_writer = std::make_unique<lucene::index::IndexWriter>(
                _dir, _analyzer.get(), create_index, close_dir_on_shutdown);
        index_writer->setRAMBufferSizeMB(config::inverted_index_ram_buffer_size);
        index_writer->setMaxBufferedDocs(config::inverted_index_max_buffered_docs);
        index_writer->setMaxFieldLength(MAX_FIELD_LEN);
        index_writer->setMergeFactor(MERGE_FACTOR);
        index_writer->setUseCompoundFile(false);

        return Status::OK();
    }

    Status create_field(lucene::document::Field** field) {
        int field_config = int(lucene::document::Field::STORE_NO) |
                           int(lucene::document::Field::INDEX_NONORMS);
        field_config |= (_parser_type == InvertedIndexParserType::PARSER_NONE)
                                ? int(lucene::document::Field::INDEX_UNTOKENIZED)
                                : int(lucene::document::Field::INDEX_TOKENIZED);
        *field = new lucene::document::Field(_field_name.c_str(), field_config);
        (*field)->setOmitTermFreqAndPositions(
                !(get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
                  INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES));
        return Status::OK();
    }

    Status create_analyzer(std::unique_ptr<lucene::analysis::Analyzer>& analyzer) {
        try {
            switch (_parser_type) {
            case InvertedIndexParserType::PARSER_STANDARD:
            case InvertedIndexParserType::PARSER_UNICODE:
                analyzer = std::make_unique<lucene::analysis::standard95::StandardAnalyzer>();
                break;
            case InvertedIndexParserType::PARSER_ENGLISH:
                analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
                break;
            case InvertedIndexParserType::PARSER_CHINESE:
                analyzer = create_chinese_analyzer();
                break;
            default:
                analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
                break;
            }
            setup_analyzer_lowercase(analyzer);
            setup_analyzer_use_stopwords(analyzer);
            return Status::OK();
        } catch (CLuceneError& e) {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "inverted index create analyzer failed: {}", e.what());
        }
    }

    void setup_analyzer_lowercase(std::unique_ptr<lucene::analysis::Analyzer>& analyzer) {
        auto lowercase = get_parser_lowercase_from_properties<true>(_index_meta->properties());
        if (lowercase == INVERTED_INDEX_PARSER_TRUE) {
            analyzer->set_lowercase(true);
        } else if (lowercase == INVERTED_INDEX_PARSER_FALSE) {
            analyzer->set_lowercase(false);
        }
    }

    void setup_analyzer_use_stopwords(std::unique_ptr<lucene::analysis::Analyzer>& analyzer) {
        auto stop_words = get_parser_stopwords_from_properties(_index_meta->properties());
        if (stop_words == "none") {
            analyzer->set_stopwords(nullptr);
        } else {
            analyzer->set_stopwords(&lucene::analysis::standard95::stop_words);
        }
    }

    Status init_fulltext_index() {
        RETURN_IF_ERROR(open_index_directory());
        RETURN_IF_ERROR(create_char_string_reader(_char_string_reader));
        RETURN_IF_ERROR(create_analyzer(_analyzer));
        RETURN_IF_ERROR(create_index_writer(_index_writer));
        _doc = std::make_unique<lucene::document::Document>();
        if (_single_field) {
            RETURN_IF_ERROR(create_field(&_field));
            _doc->add(*_field);
        } else {
            // array's inverted index do need create field first
            _doc->setNeedResetFieldData(true);
        }
        return Status::OK();
    }

    Status add_document() {
        try {
            _index_writer->addDocument(_doc.get());
        } catch (const CLuceneError& e) {
            close_on_error();
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError add_document: {}", e.what());
        }
        return Status::OK();
    }

    Status add_null_document() {
        try {
            _index_writer->addNullDocument(_doc.get());
        } catch (const CLuceneError& e) {
            close_on_error();
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError add_null_document: {}", e.what());
        }
        return Status::OK();
    }

    Status add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr || _index_writer == nullptr) {
                LOG(ERROR) << "field or index writer is null in inverted index writer.";
                return Status::InternalError(
                        "field or index writer is null in inverted index writer");
            }

            for (int i = 0; i < count; ++i) {
                RETURN_IF_ERROR(add_null_document());
            }
        }
        return Status::OK();
    }

    Status add_array_nulls(uint32_t row_id) override {
        _null_bitmap.add(row_id);
        return Status::OK();
    }

    void new_inverted_index_field(const char* field_value_data, size_t field_value_size) {
        if (_parser_type != InvertedIndexParserType::PARSER_UNKNOWN &&
            _parser_type != InvertedIndexParserType::PARSER_NONE) {
            new_char_token_stream(field_value_data, field_value_size, _field);
        } else {
            new_field_char_value(field_value_data, field_value_size, _field);
        }
    }

    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field) {
        _char_string_reader->init(s, len, false);
        auto* stream = _analyzer->reusableTokenStream(field->name(), _char_string_reader.get());
        field->setValue(stream);
    }

    void new_field_value(const char* s, size_t len, lucene::document::Field* field) {
        auto* field_value = lucene::util::Misc::_charToWide(s, len);
        field->setValue(field_value, false);
        // setValue did not duplicate value, so we don't have to delete
        //_CLDELETE_ARRAY(field_value)
    }

    void new_field_char_value(const char* s, size_t len, lucene::document::Field* field) {
        field->setValue((char*)s, len);
    }

    Status add_values(const std::string fn, const void* values, size_t count) override {
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr || _index_writer == nullptr) {
                LOG(ERROR) << "field or index writer is null in inverted index writer.";
                return Status::InternalError(
                        "field or index writer is null in inverted index writer");
            }
            auto* v = (Slice*)values;
            auto ignore_above_value =
                    get_parser_ignore_above_value_from_properties(_index_meta->properties());
            auto ignore_above = std::stoi(ignore_above_value);
            for (int i = 0; i < count; ++i) {
                // only ignore_above UNTOKENIZED strings and empty strings not tokenized
                if ((_parser_type == InvertedIndexParserType::PARSER_NONE &&
                     v->get_size() > ignore_above) ||
                    (_parser_type != InvertedIndexParserType::PARSER_NONE && v->empty())) {
                    RETURN_IF_ERROR(add_null_document());
                } else {
                    new_inverted_index_field(v->get_data(), v->get_size());
                    RETURN_IF_ERROR(add_document());
                }
                ++v;
                _rid++;
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            RETURN_IF_ERROR(add_numeric_values(values, count));
        }
        return Status::OK();
    }

    Status add_array_values(size_t field_size, const void* value_ptr, const uint8_t* null_map,
                            const uint8_t* offsets_ptr, size_t count) override {
        if (count == 0) {
            // no values to add inverted index
            return Status::OK();
        }
        const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);
        if constexpr (field_is_slice_type(field_type)) {
            if (_index_writer == nullptr) {
                LOG(ERROR) << "index writer is null in inverted index writer.";
                return Status::InternalError("index writer is null in inverted index writer");
            }
            auto ignore_above_value =
                    get_parser_ignore_above_value_from_properties(_index_meta->properties());
            auto ignore_above = std::stoi(ignore_above_value);
            size_t start_off = 0;
            for (int i = 0; i < count; ++i) {
                // nullmap & value ptr-array may not from offsets[i] because olap_convertor make offsets accumulate from _base_offset which may not is 0, but nullmap & value in this segment is from 0, we only need
                // every single array row element size to go through the nullmap & value ptr-array, and also can go through the every row in array to keep with _rid++
                auto array_elem_size = offsets[i + 1] - offsets[i];
                // TODO(Amory).later we use object pool to avoid field creation
                lucene::document::Field* new_field = nullptr;
                CL_NS(analysis)::TokenStream* ts = nullptr;
                for (auto j = start_off; j < start_off + array_elem_size; ++j) {
                    if (null_map[j] == 1) {
                        continue;
                    }
                    auto* v = (Slice*)((const uint8_t*)value_ptr + j * field_size);
                    if ((_parser_type == InvertedIndexParserType::PARSER_NONE &&
                         v->get_size() > ignore_above) ||
                        (_parser_type != InvertedIndexParserType::PARSER_NONE && v->empty())) {
                        // is here a null value?
                        // TODO. Maybe here has performance problem for large size string.
                        continue;
                    } else {
                        // now we temp create field . later make a pool
                        if (Status st = create_field(&new_field); st != Status::OK()) {
                            LOG(ERROR) << "create field "
                                       << string(_field_name.begin(), _field_name.end())
                                       << " error:" << st;
                            return st;
                        }
                        if (_parser_type != InvertedIndexParserType::PARSER_UNKNOWN &&
                            _parser_type != InvertedIndexParserType::PARSER_NONE) {
                            // in this case stream need to delete after add_document, because the
                            // stream can not reuse for different field
                            bool own_token_stream = true;
                            bool own_reader = true;
                            std::unique_ptr<lucene::util::Reader> char_string_reader = nullptr;
                            RETURN_IF_ERROR(create_char_string_reader(char_string_reader));
                            char_string_reader->init(v->get_data(), v->get_size(), false);
                            _analyzer->set_ownReader(own_reader);
                            ts = _analyzer->tokenStream(new_field->name(),
                                                        char_string_reader.release());
                            new_field->setValue(ts, own_token_stream);
                        } else {
                            new_field_char_value(v->get_data(), v->get_size(), new_field);
                        }
                        _doc->add(*new_field);
                    }
                }
                start_off += array_elem_size;
                if (!_doc->getFields()->empty()) {
                    // if this array is null, we just ignore to write inverted index
                    RETURN_IF_ERROR(add_document());
                    _doc->clear();
                } else {
                    // avoid to add doc which without any field which may make threadState init skip
                    // init fieldDataArray, then will make error with next doc with fields in
                    // resetCurrentFieldData
                    if (Status st = create_field(&new_field); st != Status::OK()) {
                        LOG(ERROR)
                                << "create field " << string(_field_name.begin(), _field_name.end())
                                << " error:" << st;
                        return st;
                    }
                    _doc->add(*new_field);
                    RETURN_IF_ERROR(add_null_document());
                    _doc->clear();
                }
                _rid++;
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            size_t start_off = 0;
            for (int i = 0; i < count; ++i) {
                auto array_elem_size = offsets[i + 1] - offsets[i];
                for (size_t j = start_off; j < start_off + array_elem_size; ++j) {
                    if (null_map[j] == 1) {
                        continue;
                    }
                    const CppType* p = &reinterpret_cast<const CppType*>(value_ptr)[j];
                    RETURN_IF_ERROR(add_value(*p));
                }
                start_off += array_elem_size;
                _row_ids_seen_for_bkd++;
                _rid++;
            }
        }
        return Status::OK();
    }

    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override {
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr || _index_writer == nullptr) {
                LOG(ERROR) << "field or index writer is null in inverted index writer.";
                return Status::InternalError(
                        "field or index writer is null in inverted index writer");
            }
            for (int i = 0; i < count; ++i) {
                auto* item_data_ptr = const_cast<CollectionValue*>(values)->mutable_data();
                std::vector<std::string> strings;

                for (size_t j = 0; j < values->length(); ++j) {
                    auto* v = (Slice*)item_data_ptr;

                    if (!values->is_null_at(j)) {
                        strings.emplace_back(v->get_data(), v->get_size());
                    }
                    item_data_ptr = (uint8_t*)item_data_ptr + field_size;
                }
                auto value = join(strings, " ");
                new_inverted_index_field(value.c_str(), value.length());
                _rid++;
                RETURN_IF_ERROR(add_document());
                values++;
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            for (int i = 0; i < count; ++i) {
                auto* item_data_ptr = const_cast<CollectionValue*>(values)->mutable_data();

                for (size_t j = 0; j < values->length(); ++j) {
                    const auto* p = reinterpret_cast<const CppType*>(item_data_ptr);
                    if (values->is_null_at(j)) {
                        // bkd do not index null values, so we do nothing here.
                    } else {
                        RETURN_IF_ERROR(add_value(*p));
                    }
                    item_data_ptr = (uint8_t*)item_data_ptr + field_size;
                }
                _row_ids_seen_for_bkd++;
                _rid++;
                values++;
            }
        }
        return Status::OK();
    }

    Status add_numeric_values(const void* values, size_t count) {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            RETURN_IF_ERROR(add_value(*p));
            _rid++;
            p++;
            _row_ids_seen_for_bkd++;
        }
        return Status::OK();
    }

    Status add_value(const CppType& value) {
        try {
            std::string new_value;
            size_t value_length = sizeof(CppType);

            DBUG_EXECUTE_IF("InvertedIndexColumnWriterImpl::add_value_bkd_writer_add_throw_error", {
                _CLTHROWA(CL_ERR_IllegalArgument, ("packedValue should be length=xxx"));
            });

            _value_key_coder->full_encode_ascending(&value, &new_value);
            _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);
        } catch (const CLuceneError& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError add_value: {}", e.what());
        }
        return Status::OK();
    }

    int64_t size() const override {
        //TODO: get memory size of inverted index
        return 0;
    }

    int64_t file_size() const override { return _dir->getCompoundFileSize(); }

    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out) {
        // write null_bitmap file
        _null_bitmap.runOptimize();
        size_t size = _null_bitmap.getSizeInBytes(false);
        if (size > 0) {
            faststring buf;
            buf.resize(size);
            _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap_out->writeBytes(buf.data(), size);
            null_bitmap_out->close();
        }
    }

    Status finish() override {
        if (_dir != nullptr) {
            std::unique_ptr<lucene::store::IndexOutput> null_bitmap_out = nullptr;
            std::unique_ptr<lucene::store::IndexOutput> data_out = nullptr;
            std::unique_ptr<lucene::store::IndexOutput> index_out = nullptr;
            std::unique_ptr<lucene::store::IndexOutput> meta_out = nullptr;
            try {
                // write bkd file
                if constexpr (field_is_numeric_type(field_type)) {
                    _bkd_writer->max_doc_ = _rid;
                    _bkd_writer->docs_seen_ = _row_ids_seen_for_bkd;
                    null_bitmap_out =
                            std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                                    InvertedIndexDescriptor::get_temporary_null_bitmap_file_name()
                                            .c_str()));
                    data_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                            InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name()
                                    .c_str()));
                    meta_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                            InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name()
                                    .c_str()));
                    index_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                            InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str()));
                    write_null_bitmap(null_bitmap_out.get());

                    DBUG_EXECUTE_IF("InvertedIndexWriter._set_bkd_data_out_nullptr",
                                    { data_out = nullptr; });
                    if (data_out != nullptr && meta_out != nullptr && index_out != nullptr) {
                        _bkd_writer->meta_finish(
                                meta_out.get(),
                                _bkd_writer->finish(data_out.get(), index_out.get()),
                                int(field_type));
                    } else {
                        LOG(WARNING)
                                << "Inverted index writer create output error occurred: nullptr";
                        _CLTHROWA(CL_ERR_IO, "Create output error with nullptr");
                    }
                    meta_out->close();
                    data_out->close();
                    index_out->close();
                    _dir->close();
                } else if constexpr (field_is_slice_type(field_type)) {
                    null_bitmap_out =
                            std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                                    InvertedIndexDescriptor::get_temporary_null_bitmap_file_name()
                                            .c_str()));
                    write_null_bitmap(null_bitmap_out.get());
                    close();
                    DBUG_EXECUTE_IF(
                            "InvertedIndexWriter._throw_clucene_error_in_fulltext_writer_close", {
                                _CLTHROWA(CL_ERR_IO,
                                          "debug point: test throw error in fulltext index writer");
                            });
                }
            } catch (CLuceneError& e) {
                FINALLY_CLOSE_OUTPUT(null_bitmap_out)
                FINALLY_CLOSE_OUTPUT(meta_out)
                FINALLY_CLOSE_OUTPUT(data_out)
                FINALLY_CLOSE_OUTPUT(index_out)
                if constexpr (field_is_numeric_type(field_type)) {
                    FINALLY_CLOSE_OUTPUT(_dir)
                } else if constexpr (field_is_slice_type(field_type)) {
                    FINALLY_CLOSE_OUTPUT(_index_writer);
                }
                LOG(WARNING) << "Inverted index writer finish error occurred: " << e.what();
                return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "Inverted index writer finish error occurred:{}", e.what());
            }

            return Status::OK();
        }
        LOG(WARNING) << "Inverted index writer finish error occurred: dir is nullptr";
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Inverted index writer finish error occurred: dir is nullptr");
    }

private:
    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    roaring::Roaring _null_bitmap;
    uint64_t _reverted_index_size;

    std::unique_ptr<lucene::document::Document> _doc = nullptr;
    lucene::document::Field* _field = nullptr;
    bool _single_field = true;
    // Since _index_writer's write.lock is created by _dir.lockFactory,
    // _dir must destruct after _index_writer, so _dir must be defined before _index_writer.
    DorisFSDirectory* _dir = nullptr;
    std::unique_ptr<lucene::index::IndexWriter> _index_writer = nullptr;
    std::unique_ptr<lucene::analysis::Analyzer> _analyzer = nullptr;
    std::unique_ptr<lucene::util::Reader> _char_string_reader = nullptr;
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer = nullptr;
    const KeyCoder* _value_key_coder;
    const TabletIndex* _index_meta;
    InvertedIndexParserType _parser_type;
    std::wstring _field_name;
    InvertedIndexFileWriter* _index_file_writer;
};

Status InvertedIndexColumnWriter::create(const Field* field,
                                         std::unique_ptr<InvertedIndexColumnWriter>* res,
                                         InvertedIndexFileWriter* index_file_writer,
                                         const TabletIndex* index_meta) {
    const auto* typeinfo = field->type_info();
    FieldType type = typeinfo->type();
    std::string field_name;
    auto storage_format = index_file_writer->get_storage_format();
    if (storage_format == InvertedIndexStorageFormatPB::V1) {
        field_name = field->name();
    } else {
        if (field->is_extracted_column()) {
            // variant sub col
            // field_name format: parent_unique_id.sub_col_name
            field_name = std::to_string(field->parent_unique_id()) + "." + field->name();
        } else {
            field_name = std::to_string(field->unique_id());
        }
    }
    bool single_field = true;
    if (type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        const auto* array_typeinfo = dynamic_cast<const ArrayTypeInfo*>(typeinfo);
        if (array_typeinfo != nullptr) {
            typeinfo = array_typeinfo->item_type_info();
            type = typeinfo->type();
            single_field = false;
        } else {
            return Status::NotSupported("unsupported array type for inverted index: " +
                                        std::to_string(int(type)));
        }
    }

    switch (type) {
#define M(TYPE)                                                           \
    case TYPE:                                                            \
        *res = std::make_unique<InvertedIndexColumnWriterImpl<TYPE>>(     \
                field_name, index_file_writer, index_meta, single_field); \
        break;
        M(FieldType::OLAP_FIELD_TYPE_TINYINT)
        M(FieldType::OLAP_FIELD_TYPE_SMALLINT)
        M(FieldType::OLAP_FIELD_TYPE_INT)
        M(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT)
        M(FieldType::OLAP_FIELD_TYPE_BIGINT)
        M(FieldType::OLAP_FIELD_TYPE_LARGEINT)
        M(FieldType::OLAP_FIELD_TYPE_CHAR)
        M(FieldType::OLAP_FIELD_TYPE_VARCHAR)
        M(FieldType::OLAP_FIELD_TYPE_STRING)
        M(FieldType::OLAP_FIELD_TYPE_DATE)
        M(FieldType::OLAP_FIELD_TYPE_DATETIME)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL)
        M(FieldType::OLAP_FIELD_TYPE_DATEV2)
        M(FieldType::OLAP_FIELD_TYPE_DATETIMEV2)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL32)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL64)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL128I)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL256)
        M(FieldType::OLAP_FIELD_TYPE_BOOL)
        M(FieldType::OLAP_FIELD_TYPE_IPV4)
        M(FieldType::OLAP_FIELD_TYPE_IPV6)
#undef M
    default:
        return Status::NotSupported("unsupported type for inverted index: " +
                                    std::to_string(int(type)));
    }
    if (*res != nullptr) {
        auto st = (*res)->init();
        if (!st.ok()) {
            (*res)->close_on_error();
            return st;
        }
    }
    return Status::OK();
}
} // namespace doris::segment_v2
