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

#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>

#include <memory>

#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet_schema.h"
#include "util/string_util.h"

#define FINALIZE_OUTPUT(x) \
    if (x != nullptr) {    \
        x->close();        \
        _CLDELETE(x);      \
    }
#define FINALLY_FINALIZE_OUTPUT(x) \
    try {                          \
        FINALIZE_OUTPUT(x)         \
    } catch (...) {                \
    }

namespace doris::segment_v2 {
const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MAX_BUFFER_DOCS = 100000000;
const int32_t MERGE_FACTOR = 100000000;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;
const std::string empty_value;

template <FieldType field_type>
class InvertedIndexColumnWriterImpl : public InvertedIndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    explicit InvertedIndexColumnWriterImpl(const std::string& field_name,
                                           const std::string& segment_file_name,
                                           const std::string& dir, const io::FileSystemSPtr& fs,
                                           const TabletIndex* index_meta)
            : _segment_file_name(segment_file_name),
              _directory(dir),
              _fs(fs),
              _index_meta(index_meta) {
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta->properties()));
        _value_key_coder = get_key_coder(field_type);
        _field_name = std::wstring(field_name.begin(), field_name.end());
    }

    ~InvertedIndexColumnWriterImpl() override = default;

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
            if (config::enable_write_index_searcher_cache) {
                // open index searcher into cache
                auto index_file_name = InvertedIndexDescriptor::get_index_file_name(
                        _segment_file_name, _index_meta->index_id());
                InvertedIndexSearcherCache::instance()->insert(_fs, _directory, index_file_name);
            }
            _CLLDELETE(_index_writer)
            _index_writer = nullptr;
        }

        if (_doc) {
            _CLLDELETE(_doc)
            _doc = nullptr;
        }

        if (_analyzer) {
            _CLLDELETE(_analyzer)
            _analyzer = nullptr;
        }

        if (_char_string_reader) {
            _CLDELETE(_char_string_reader)
            _char_string_reader = nullptr;
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
        return Status::OK();
    }

    Status init_fulltext_index() {
        bool create = true;

        auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                _directory + "/" + _segment_file_name, _index_meta->index_id());

        // LOG(INFO) << "inverted index path: " << index_path;

        if (lucene::index::IndexReader::indexExists(index_path.c_str())) {
            create = false;
            if (lucene::index::IndexReader::isLocked(index_path.c_str())) {
                LOG(WARNING) << ("Lucene Index was locked... unlocking it.\n");
                lucene::index::IndexReader::unlock(index_path.c_str());
            }
        }

        _char_string_reader = _CLNEW lucene::util::SStringReader<char>;
        _doc = _CLNEW lucene::document::Document();
        _dir.reset(DorisCompoundDirectory::getDirectory(_fs, index_path.c_str(), true));

        if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
            _analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
        } else if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            _analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>();
        } else if (_parser_type == InvertedIndexParserType::PARSER_CHINESE) {
            auto chinese_analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
            chinese_analyzer->setLanguage(L"chinese");
            chinese_analyzer->initDict(config::inverted_index_dict_path);
            _analyzer = chinese_analyzer;
        } else {
            // ANALYSER_NOT_SET, ANALYSER_NONE use default SimpleAnalyzer
            _analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<TCHAR>();
        }
        _index_writer = _CLNEW lucene::index::IndexWriter(_dir.get(), _analyzer, create, true);
        _index_writer->setMaxBufferedDocs(MAX_BUFFER_DOCS);
        _index_writer->setRAMBufferSizeMB(config::inverted_index_ram_buffer_size);
        _index_writer->setMaxFieldLength(MAX_FIELD_LEN);
        _index_writer->setMergeFactor(MERGE_FACTOR);
        _index_writer->setUseCompoundFile(false);
        _doc->clear();

        int field_config =
                lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_NONORMS;
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
        } else {
            field_config |= lucene::document::Field::INDEX_TOKENIZED;
        }
        _field = _CLNEW lucene::document::Field(_field_name.c_str(), field_config);
        _doc->add(*_field);
        return Status::OK();
    }

    Status add_nulls(uint32_t count) override {
        _rid += count;
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr || _index_writer == nullptr) {
                LOG(ERROR) << "field or index writer is null in inverted index writer.";
                return Status::InternalError(
                        "field or index writer is null in inverted index writer");
            }

            for (int i = 0; i < count; ++i) {
                new_fulltext_field(empty_value.c_str(), 0);
                _index_writer->addDocument(_doc);
            }
        }
        return Status::OK();
    }

    void new_fulltext_field(const char* field_value_data, size_t field_value_size) {
        if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            new_char_token_stream(field_value_data, field_value_size, _field);
        } else if (_parser_type == InvertedIndexParserType::PARSER_CHINESE) {
            auto stringReader = _CLNEW lucene::util::SimpleInputStreamReader(
                    new lucene::util::AStringReader(field_value_data, field_value_size),
                    lucene::util::SimpleInputStreamReader::UTF8);
            _field->setValue(stringReader);
        } else {
            new_field_value(field_value_data, field_value_size, _field);
        }
    }

    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field) {
        _char_string_reader->init(s, len, false);
        auto stream = _analyzer->reusableTokenStream(field->name(), _char_string_reader);
        field->setValue(stream);
    }

    void new_field_value(const char* s, size_t len, lucene::document::Field* field) {
        auto field_value = lucene::util::Misc::_charToWide(s, len);
        field->setValue(field_value, false);
        // setValue did not duplicate value, so we don't have to delete
        //_CLDELETE_ARRAY(field_value)
    }

    Status add_values(const std::string fn, const void* values, size_t count) override {
        if constexpr (field_is_slice_type(field_type)) {
            if (_field == nullptr || _index_writer == nullptr) {
                LOG(ERROR) << "field or index writer is null in inverted index writer.";
                return Status::InternalError(
                        "field or index writer is null in inverted index writer");
            }
            auto* v = (Slice*)values;
            for (int i = 0; i < count; ++i) {
                new_fulltext_field(v->get_data(), v->get_size());
                _index_writer->addDocument(_doc);
                ++v;
                _rid++;
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            add_numeric_values(values, count);
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
                        strings.emplace_back(std::string(v->get_data(), v->get_size()));
                    }
                    item_data_ptr = (uint8_t*)item_data_ptr + field_size;
                }
                auto value = join(strings, " ");
                new_fulltext_field(value.c_str(), value.length());
                _rid++;
                _index_writer->addDocument(_doc);
                values++;
            }
        } else if constexpr (field_is_numeric_type(field_type)) {
            for (int i = 0; i < count; ++i) {
                auto* item_data_ptr = const_cast<CollectionValue*>(values)->mutable_data();

                for (size_t j = 0; j < values->length(); ++j) {
                    const CppType* p = reinterpret_cast<const CppType*>(item_data_ptr);
                    if (values->is_null_at(j)) {
                        // bkd do not index null values, so we do nothing here.
                    } else {
                        std::string new_value;
                        size_t value_length = sizeof(CppType);

                        _value_key_coder->full_encode_ascending(p, &new_value);
                        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);
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

    void add_numeric_values(const void* values, size_t count) {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value(*p);
            p++;
            _row_ids_seen_for_bkd++;
        }
    }

    void add_value(const CppType& value) {
        std::string new_value;
        size_t value_length = sizeof(CppType);

        _value_key_coder->full_encode_ascending(&value, &new_value);
        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);

        _rid++;
    }

    uint64_t size() const override {
        //TODO: get size of inverted index
        return 0;
    }

    Status finish() override {
        lucene::store::Directory* dir = nullptr;
        lucene::store::IndexOutput* data_out = nullptr;
        lucene::store::IndexOutput* index_out = nullptr;
        lucene::store::IndexOutput* meta_out = nullptr;
        try {
            if constexpr (field_is_numeric_type(field_type)) {
                auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                        _directory + "/" + _segment_file_name, _index_meta->index_id());
                dir = DorisCompoundDirectory::getDirectory(_fs, index_path.c_str(), true);
                _bkd_writer->max_doc_ = _rid;
                _bkd_writer->docs_seen_ = _row_ids_seen_for_bkd;
                data_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name().c_str());
                meta_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name().c_str());
                index_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str());
                if (data_out != nullptr && meta_out != nullptr && index_out != nullptr) {
                    _bkd_writer->meta_finish(meta_out, _bkd_writer->finish(data_out, index_out),
                                             field_type);
                }
                FINALIZE_OUTPUT(meta_out)
                FINALIZE_OUTPUT(data_out)
                FINALIZE_OUTPUT(index_out)
                FINALIZE_OUTPUT(dir)
            } else if constexpr (field_is_slice_type(field_type)) {
                close();
            }
        } catch (CLuceneError& e) {
            FINALLY_FINALIZE_OUTPUT(meta_out)
            FINALLY_FINALIZE_OUTPUT(data_out)
            FINALLY_FINALIZE_OUTPUT(index_out)
            FINALLY_FINALIZE_OUTPUT(dir)
            LOG(WARNING) << "Inverted index writer finish error occurred: " << e.what();
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "Inverted index writer finish error occurred");
        }

        return Status::OK();
    }

private:
    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    roaring::Roaring _null_bitmap;
    uint64_t _reverted_index_size;

    lucene::document::Document* _doc {};
    lucene::document::Field* _field {};
    lucene::index::IndexWriter* _index_writer {};
    lucene::analysis::Analyzer* _analyzer {};
    lucene::util::SStringReader<char>* _char_string_reader {};
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer;
    std::string _segment_file_name;
    std::string _directory;
    io::FileSystemSPtr _fs;
    const KeyCoder* _value_key_coder;
    const TabletIndex* _index_meta;
    InvertedIndexParserType _parser_type;
    std::wstring _field_name;
    std::unique_ptr<DorisCompoundDirectory> _dir;
};

Status InvertedIndexColumnWriter::create(const Field* field,
                                         std::unique_ptr<InvertedIndexColumnWriter>* res,
                                         const std::string& segment_file_name,
                                         const std::string& dir, const TabletIndex* index_meta,
                                         const io::FileSystemSPtr& fs) {
    auto typeinfo = field->type_info();
    FieldType type = typeinfo->type();
    std::string field_name = field->name();
    if (type == OLAP_FIELD_TYPE_ARRAY) {
        const auto array_typeinfo = dynamic_cast<const ArrayTypeInfo*>(typeinfo);
        typeinfo = array_typeinfo->item_type_info();
        type = typeinfo->type();
    }

    switch (type) {
    case OLAP_FIELD_TYPE_CHAR: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_CHAR>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_VARCHAR: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_VARCHAR>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_STRING: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_STRING>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATETIME>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DATE: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATE>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DATETIMEV2: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATETIMEV2>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DATEV2: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DATEV2>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_TINYINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_TINYINT>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_SMALLINT>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_UNSIGNED_INT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_INT>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_LARGEINT>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DECIMAL>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL32: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DECIMAL32>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL64: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DECIMAL64>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL128I: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DECIMAL128I>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_BOOL: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_BOOL>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_DOUBLE>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_FLOAT>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        *res = std::make_unique<InvertedIndexColumnWriterImpl<OLAP_FIELD_TYPE_BIGINT>>(
                field_name, segment_file_name, dir, fs, index_meta);
        break;
    }
    default:
        return Status::NotSupported("unsupported type for inverted index: " + std::to_string(type));
    }
    if (*res != nullptr) {
        RETURN_IF_ERROR((*res)->init());
    }
    return Status::OK();
}
} // namespace doris::segment_v2