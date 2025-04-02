#include "olap/rowset/segment_v2/ann_index_writer.h"

#ifdef BUILD_FAISS
#include "vector/faiss_vector_index.h"
#endif

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
    _vector_index_writer = nullptr;
    std::string index_type = get_or_default(_index_meta->properties(), INDEX_TYPE, "");
    if (index_type == "hnsw") {
#ifdef BUILD_FAISS
        std::shared_ptr<FaissVectorIndex> faiss_index_writer =
                std::make_shared<FaissVectorIndex>(_dir);

        FaissBuildParameter builderParameter;
        builderParameter.index_type = FaissBuildParameter::string_to_index_type("hnsw");
        builderParameter.d = std::stoi(get_or_default(_index_meta->properties(), DIM, "512"));
        builderParameter.m = std::stoi(get_or_default(_index_meta->properties(), MAX_DEGREE, "32"));
        builderParameter.quantilizer = FaissBuildParameter::string_to_quantilizer(
                get_or_default(_index_meta->properties(), QUANTILIZER, "flat"));
        faiss_index_writer->set_build_params(builderParameter);
        _vector_index_writer = faiss_index_writer;
#else
        return Status::NotSupported("Faiss index is not supported, please build doris with faiss");
#endif
    }
    if (_vector_index_writer == nullptr) {
        return Status::NotSupported("Unsupported index type: " + index_type);
    } else {
        return Status::OK();
    }
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
