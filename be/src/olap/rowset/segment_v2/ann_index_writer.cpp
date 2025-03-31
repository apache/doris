#include "olap/rowset/segment_v2/ann_index_writer.h"

namespace doris::segment_v2 {

AnnIndexColumnWriter::AnnIndexColumnWriter(const std::string& field_name,
                                           XIndexFileWriter* index_file_writer,
                                           const TabletIndex* index_meta, const bool single_field)
        : _index_file_writer(index_file_writer), _index_meta(index_meta) {
    _field_name = StringUtil::string_to_wstring(field_name);
}

AnnIndexColumnWriter::~AnnIndexColumnWriter() {}

Status AnnIndexColumnWriter::init() {
    RETURN_IF_ERROR(open_index_directory());
    RETURN_IF_ERROR(init_ann_index());
    return Status::OK();
}

std::string get_or_default(const std::map<std::string, std::string>& properties,
                           const std::string& key, const std::string& default_value) {
    auto it = properties.find(key);
    if (it != properties.end()) {
        return it->second;
    }
    return default_value;
}

Status AnnIndexColumnWriter::init_ann_index() {
    // if(get_or_default(_index_meta->properties(), INDEX_TYPE, "")=="diskann"){
    //     _vector_index_writer = std::make_shared<DiskannVectorIndex>(_dir);
    //     std::shared_ptr<DiskannBuilderParameter>  builderParameterPtr = std::make_shared<DiskannBuilderParameter>();
    //     builderParameterPtr->with_dim(std::stoi(get_or_default(_index_meta->properties(), DIM,"")))
    //                         .with_L(std::stoi(get_or_default(_index_meta->properties(), DISKANN_SEARCH_LIST,"")))
    //                         .with_R(std::stoi(get_or_default(_index_meta->properties(), DISKANN_MAX_DEGREE,"")))
    //                         .with_build_num_threads(8)
    //                         .with_sample_rate(1)
    //                         .with_indexing_ram_budget_mb(10*1024)
    //                         .with_search_ram_budget_mb(30)
    //                         .with_mertic_type(VectorIndex::string_to_metric(get_or_default(_index_meta->properties(), METRIC_TYPE,"")));
    //     _vector_index_writer->set_build_params(std::static_pointer_cast<BuilderParameter>(builderParameterPtr));
    //     return Status::OK();
    // }else{
    return Status::NotSupported("ANN index is not support for now.");
    // }
}

Status AnnIndexColumnWriter::open_index_directory() {
    _dir = DORIS_TRY(_index_file_writer->open(_index_meta));
    return Status::OK();
}

Status AnnIndexColumnWriter::add_values(const std::string fn, const void* values, size_t count) {
    return Status::OK();
}

void AnnIndexColumnWriter::close_on_error() {}

Status AnnIndexColumnWriter::add_array_values(size_t field_size, const void* value_ptr,
                                              const uint8_t* null_map, const uint8_t* offsets_ptr,
                                              size_t count) {
    if (count == 0) {
        return Status::OK();
    }
    const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);
    size_t start_off = 0;
    for (int i = 0; i < count; ++i) {
        auto array_elem_size = offsets[i + 1] - offsets[i];
        const float* p = &reinterpret_cast<const float*>(value_ptr)[start_off];
        RETURN_IF_ERROR(_vector_index_writer->add(1, p));
        start_off += array_elem_size;
        _rid++;
    }
    return Status::OK();
}

Status AnnIndexColumnWriter::add_array_values(size_t field_size, const CollectionValue* values,
                                              size_t count) {
    return Status::OK();
}

Status AnnIndexColumnWriter::add_nulls(uint32_t count) {
    // 实现逻辑
    return Status::OK();
}

Status AnnIndexColumnWriter::add_array_nulls(const uint8_t* null_map, size_t row_id) {
    // 实现逻辑
    return Status::OK();
}

int64_t AnnIndexColumnWriter::size() const {
    return 0; // TODO: 获取倒排索引的内存大小
}

Status AnnIndexColumnWriter::finish() {
    return _vector_index_writer->save();
}

} // namespace doris::segment_v2
