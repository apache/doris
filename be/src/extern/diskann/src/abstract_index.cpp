#include "common_includes.h"
#include "windows_customizations.h"
#include "abstract_index.h"

namespace diskann
{

template <typename data_type, typename tag_type>
void AbstractIndex::build(const data_type *data, const size_t num_points_to_load, const std::vector<tag_type> &tags)
{
    auto any_data = std::any(data);
    auto any_tags_vec = TagVector(tags);
    this->_build(any_data, num_points_to_load, any_tags_vec);
}

template <typename data_type, typename IDType>
std::pair<uint32_t, uint32_t> AbstractIndex::search(const data_type *query, const size_t K, const uint32_t L,
                                                    IDType *indices, float *distances)
{
    auto any_indices = std::any(indices);
    auto any_query = std::any(query);
    return _search(any_query, K, L, any_indices, distances);
}

template <typename data_type, typename tag_type>
size_t AbstractIndex::search_with_tags(const data_type *query, const uint64_t K, const uint32_t L, tag_type *tags,
                                       float *distances, std::vector<data_type *> &res_vectors, bool use_filters,
                                       const std::string filter_label)
{
    auto any_query = std::any(query);
    auto any_tags = std::any(tags);
    auto any_res_vectors = DataVector(res_vectors);
    return this->_search_with_tags(any_query, K, L, any_tags, distances, any_res_vectors, use_filters, filter_label);
}

template <typename IndexType>
std::pair<uint32_t, uint32_t> AbstractIndex::search_with_filters(const DataType &query, const std::string &raw_label,
                                                                 const size_t K, const uint32_t L, IndexType *indices,
                                                                 float *distances)
{
    auto any_indices = std::any(indices);
    return _search_with_filters(query, raw_label, K, L, any_indices, distances);
}

template <typename data_type>
void AbstractIndex::search_with_optimized_layout(const data_type *query, size_t K, size_t L, uint32_t *indices)
{
    auto any_query = std::any(query);
    this->_search_with_optimized_layout(any_query, K, L, indices);
}

template <typename data_type, typename tag_type>
int AbstractIndex::insert_point(const data_type *point, const tag_type tag)
{
    auto any_point = std::any(point);
    auto any_tag = std::any(tag);
    return this->_insert_point(any_point, any_tag);
}

template <typename data_type, typename tag_type, typename label_type>
int AbstractIndex::insert_point(const data_type *point, const tag_type tag, const std::vector<label_type> &labels)
{
    auto any_point = std::any(point);
    auto any_tag = std::any(tag);
    auto any_labels = Labelvector(labels);
    return this->_insert_point(any_point, any_tag, any_labels);
}

template <typename tag_type> int AbstractIndex::lazy_delete(const tag_type &tag)
{
    auto any_tag = std::any(tag);
    return this->_lazy_delete(any_tag);
}

template <typename tag_type>
void AbstractIndex::lazy_delete(const std::vector<tag_type> &tags, std::vector<tag_type> &failed_tags)
{
    auto any_tags = TagVector(tags);
    auto any_failed_tags = TagVector(failed_tags);
    this->_lazy_delete(any_tags, any_failed_tags);
}

template <typename tag_type> void AbstractIndex::get_active_tags(tsl::robin_set<tag_type> &active_tags)
{
    auto any_active_tags = TagRobinSet(active_tags);
    this->_get_active_tags(any_active_tags);
}

template <typename data_type> void AbstractIndex::set_start_points_at_random(data_type radius, uint32_t random_seed)
{
    auto any_radius = std::any(radius);
    this->_set_start_points_at_random(any_radius, random_seed);
}

template <typename tag_type, typename data_type> int AbstractIndex::get_vector_by_tag(tag_type &tag, data_type *vec)
{
    auto any_tag = std::any(tag);
    auto any_data_ptr = std::any(vec);
    return this->_get_vector_by_tag(any_tag, any_data_ptr);
}

template <typename label_type> void AbstractIndex::set_universal_label(const label_type universal_label)
{
    auto any_label = std::any(universal_label);
    this->_set_universal_label(any_label);
}

// exports
template DISKANN_DLLEXPORT void AbstractIndex::build<float, int32_t>(const float *data, const size_t num_points_to_load,
                                                                     const std::vector<int32_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<int8_t, int32_t>(const int8_t *data,
                                                                      const size_t num_points_to_load,
                                                                      const std::vector<int32_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<uint8_t, int32_t>(const uint8_t *data,
                                                                       const size_t num_points_to_load,
                                                                       const std::vector<int32_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<float, uint32_t>(const float *data,
                                                                      const size_t num_points_to_load,
                                                                      const std::vector<uint32_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<int8_t, uint32_t>(const int8_t *data,
                                                                       const size_t num_points_to_load,
                                                                       const std::vector<uint32_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<uint8_t, uint32_t>(const uint8_t *data,
                                                                        const size_t num_points_to_load,
                                                                        const std::vector<uint32_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<float, int64_t>(const float *data, const size_t num_points_to_load,
                                                                     const std::vector<int64_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<int8_t, int64_t>(const int8_t *data,
                                                                      const size_t num_points_to_load,
                                                                      const std::vector<int64_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<uint8_t, int64_t>(const uint8_t *data,
                                                                       const size_t num_points_to_load,
                                                                       const std::vector<int64_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<float, uint64_t>(const float *data,
                                                                      const size_t num_points_to_load,
                                                                      const std::vector<uint64_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<int8_t, uint64_t>(const int8_t *data,
                                                                       const size_t num_points_to_load,
                                                                       const std::vector<uint64_t> &tags);
template DISKANN_DLLEXPORT void AbstractIndex::build<uint8_t, uint64_t>(const uint8_t *data,
                                                                        const size_t num_points_to_load,
                                                                        const std::vector<uint64_t> &tags);

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search<float, uint32_t>(
    const float *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search<uint8_t, uint32_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search<int8_t, uint32_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search<float, uint64_t>(
    const float *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search<uint8_t, uint64_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search<int8_t, uint64_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search_with_filters<uint32_t>(
    const DataType &query, const std::string &raw_label, const size_t K, const uint32_t L, uint32_t *indices,
    float *distances);

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> AbstractIndex::search_with_filters<uint64_t>(
    const DataType &query, const std::string &raw_label, const size_t K, const uint32_t L, uint64_t *indices,
    float *distances);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<float, int32_t>(
    const float *query, const uint64_t K, const uint32_t L, int32_t *tags, float *distances,
    std::vector<float *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<uint8_t, int32_t>(
    const uint8_t *query, const uint64_t K, const uint32_t L, int32_t *tags, float *distances,
    std::vector<uint8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<int8_t, int32_t>(
    const int8_t *query, const uint64_t K, const uint32_t L, int32_t *tags, float *distances,
    std::vector<int8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<float, uint32_t>(
    const float *query, const uint64_t K, const uint32_t L, uint32_t *tags, float *distances,
    std::vector<float *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<uint8_t, uint32_t>(
    const uint8_t *query, const uint64_t K, const uint32_t L, uint32_t *tags, float *distances,
    std::vector<uint8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<int8_t, uint32_t>(
    const int8_t *query, const uint64_t K, const uint32_t L, uint32_t *tags, float *distances,
    std::vector<int8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<float, int64_t>(
    const float *query, const uint64_t K, const uint32_t L, int64_t *tags, float *distances,
    std::vector<float *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<uint8_t, int64_t>(
    const uint8_t *query, const uint64_t K, const uint32_t L, int64_t *tags, float *distances,
    std::vector<uint8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<int8_t, int64_t>(
    const int8_t *query, const uint64_t K, const uint32_t L, int64_t *tags, float *distances,
    std::vector<int8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<float, uint64_t>(
    const float *query, const uint64_t K, const uint32_t L, uint64_t *tags, float *distances,
    std::vector<float *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<uint8_t, uint64_t>(
    const uint8_t *query, const uint64_t K, const uint32_t L, uint64_t *tags, float *distances,
    std::vector<uint8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT size_t AbstractIndex::search_with_tags<int8_t, uint64_t>(
    const int8_t *query, const uint64_t K, const uint32_t L, uint64_t *tags, float *distances,
    std::vector<int8_t *> &res_vectors, bool use_filters, const std::string filter_label);

template DISKANN_DLLEXPORT void AbstractIndex::search_with_optimized_layout<float>(const float *query, size_t K,
                                                                                   size_t L, uint32_t *indices);
template DISKANN_DLLEXPORT void AbstractIndex::search_with_optimized_layout<uint8_t>(const uint8_t *query, size_t K,
                                                                                     size_t L, uint32_t *indices);
template DISKANN_DLLEXPORT void AbstractIndex::search_with_optimized_layout<int8_t>(const int8_t *query, size_t K,
                                                                                    size_t L, uint32_t *indices);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, int32_t>(const float *point, const int32_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, int32_t>(const uint8_t *point, const int32_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, int32_t>(const int8_t *point, const int32_t tag);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, uint32_t>(const float *point, const uint32_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, uint32_t>(const uint8_t *point, const uint32_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, uint32_t>(const int8_t *point, const uint32_t tag);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, int64_t>(const float *point, const int64_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, int64_t>(const uint8_t *point, const int64_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, int64_t>(const int8_t *point, const int64_t tag);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, uint64_t>(const float *point, const uint64_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, uint64_t>(const uint8_t *point, const uint64_t tag);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, uint64_t>(const int8_t *point, const uint64_t tag);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, int32_t, uint16_t>(
    const float *point, const int32_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, int32_t, uint16_t>(
    const uint8_t *point, const int32_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, int32_t, uint16_t>(
    const int8_t *point, const int32_t tag, const std::vector<uint16_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, uint32_t, uint16_t>(
    const float *point, const uint32_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, uint32_t, uint16_t>(
    const uint8_t *point, const uint32_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, uint32_t, uint16_t>(
    const int8_t *point, const uint32_t tag, const std::vector<uint16_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, int64_t, uint16_t>(
    const float *point, const int64_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, int64_t, uint16_t>(
    const uint8_t *point, const int64_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, int64_t, uint16_t>(
    const int8_t *point, const int64_t tag, const std::vector<uint16_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, uint64_t, uint16_t>(
    const float *point, const uint64_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, uint64_t, uint16_t>(
    const uint8_t *point, const uint64_t tag, const std::vector<uint16_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, uint64_t, uint16_t>(
    const int8_t *point, const uint64_t tag, const std::vector<uint16_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, int32_t, uint32_t>(
    const float *point, const int32_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, int32_t, uint32_t>(
    const uint8_t *point, const int32_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, int32_t, uint32_t>(
    const int8_t *point, const int32_t tag, const std::vector<uint32_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, uint32_t, uint32_t>(
    const float *point, const uint32_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, uint32_t, uint32_t>(
    const uint8_t *point, const uint32_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, uint32_t, uint32_t>(
    const int8_t *point, const uint32_t tag, const std::vector<uint32_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, int64_t, uint32_t>(
    const float *point, const int64_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, int64_t, uint32_t>(
    const uint8_t *point, const int64_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, int64_t, uint32_t>(
    const int8_t *point, const int64_t tag, const std::vector<uint32_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::insert_point<float, uint64_t, uint32_t>(
    const float *point, const uint64_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<uint8_t, uint64_t, uint32_t>(
    const uint8_t *point, const uint64_t tag, const std::vector<uint32_t> &labels);
template DISKANN_DLLEXPORT int AbstractIndex::insert_point<int8_t, uint64_t, uint32_t>(
    const int8_t *point, const uint64_t tag, const std::vector<uint32_t> &labels);

template DISKANN_DLLEXPORT int AbstractIndex::lazy_delete<int32_t>(const int32_t &tag);
template DISKANN_DLLEXPORT int AbstractIndex::lazy_delete<uint32_t>(const uint32_t &tag);
template DISKANN_DLLEXPORT int AbstractIndex::lazy_delete<int64_t>(const int64_t &tag);
template DISKANN_DLLEXPORT int AbstractIndex::lazy_delete<uint64_t>(const uint64_t &tag);

template DISKANN_DLLEXPORT void AbstractIndex::lazy_delete<int32_t>(const std::vector<int32_t> &tags,
                                                                    std::vector<int32_t> &failed_tags);
template DISKANN_DLLEXPORT void AbstractIndex::lazy_delete<uint32_t>(const std::vector<uint32_t> &tags,
                                                                     std::vector<uint32_t> &failed_tags);
template DISKANN_DLLEXPORT void AbstractIndex::lazy_delete<int64_t>(const std::vector<int64_t> &tags,
                                                                    std::vector<int64_t> &failed_tags);
template DISKANN_DLLEXPORT void AbstractIndex::lazy_delete<uint64_t>(const std::vector<uint64_t> &tags,
                                                                     std::vector<uint64_t> &failed_tags);

template DISKANN_DLLEXPORT void AbstractIndex::get_active_tags<int32_t>(tsl::robin_set<int32_t> &active_tags);
template DISKANN_DLLEXPORT void AbstractIndex::get_active_tags<uint32_t>(tsl::robin_set<uint32_t> &active_tags);
template DISKANN_DLLEXPORT void AbstractIndex::get_active_tags<int64_t>(tsl::robin_set<int64_t> &active_tags);
template DISKANN_DLLEXPORT void AbstractIndex::get_active_tags<uint64_t>(tsl::robin_set<uint64_t> &active_tags);

template DISKANN_DLLEXPORT void AbstractIndex::set_start_points_at_random<float>(float radius, uint32_t random_seed);
template DISKANN_DLLEXPORT void AbstractIndex::set_start_points_at_random<uint8_t>(uint8_t radius,
                                                                                   uint32_t random_seed);
template DISKANN_DLLEXPORT void AbstractIndex::set_start_points_at_random<int8_t>(int8_t radius, uint32_t random_seed);

template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<int32_t, float>(int32_t &tag, float *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<int32_t, uint8_t>(int32_t &tag, uint8_t *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<int32_t, int8_t>(int32_t &tag, int8_t *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<uint32_t, float>(uint32_t &tag, float *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<uint32_t, uint8_t>(uint32_t &tag, uint8_t *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<uint32_t, int8_t>(uint32_t &tag, int8_t *vec);

template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<int64_t, float>(int64_t &tag, float *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<int64_t, uint8_t>(int64_t &tag, uint8_t *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<int64_t, int8_t>(int64_t &tag, int8_t *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<uint64_t, float>(uint64_t &tag, float *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<uint64_t, uint8_t>(uint64_t &tag, uint8_t *vec);
template DISKANN_DLLEXPORT int AbstractIndex::get_vector_by_tag<uint64_t, int8_t>(uint64_t &tag, int8_t *vec);

template DISKANN_DLLEXPORT void AbstractIndex::set_universal_label<uint16_t>(const uint16_t label);
template DISKANN_DLLEXPORT void AbstractIndex::set_universal_label<uint32_t>(const uint32_t label);

} // namespace diskann
