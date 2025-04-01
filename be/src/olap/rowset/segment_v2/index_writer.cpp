#include "olap/rowset/segment_v2/index_writer.h"

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

#include "io/fs/local_file_system.h"

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
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/x_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/collection_value.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/string_util.h"

#include "vector/vector_index.h"
#include "vector/diskann_vector_index.h"

#include "olap/rowset/segment_v2/ann_index_writer.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"

namespace doris {
namespace segment_v2 {

bool IndexColumnWriter::check_support_inverted_index(const TabletColumn& column){
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

bool IndexColumnWriter::check_support_ann_index(const TabletColumn& column){
    // bellow types are not supported in inverted index for extracted columns
    return column.is_array_type();
}


Status IndexColumnWriter::create(const Field* field,
                                         std::unique_ptr<IndexColumnWriter>* res,
                                         XIndexFileWriter* index_file_writer,
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
        DBUG_EXECUTE_IF("IndexColumnWriter::create_array_typeinfo_is_nullptr",
                        { array_typeinfo = nullptr; })
        if (array_typeinfo != nullptr) {
            typeinfo = array_typeinfo->item_type_info();
            type = typeinfo->type();
            single_field = false;
        } else {
            return Status::NotSupported("unsupported array type for inverted index: " +
                                        std::to_string(int(type)));
        }
    }

    if(index_meta->index_type() == IndexType::ANN){
        *res = std::make_unique<AnnIndexColumnWriter>(
                field_name, index_file_writer, index_meta, single_field);
        RETURN_IF_ERROR((*res)->init());
        return Status::OK();
    }

    DBUG_EXECUTE_IF("IndexColumnWriter::create_unsupported_type_for_inverted_index",
                    { type = FieldType::OLAP_FIELD_TYPE_FLOAT; })
    switch (type) {
#define M(TYPE)                                                           \
    case TYPE:                                                            \
        *res = std::make_unique<InvertedIndexColumnWriter<TYPE>>(     \
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


}
}