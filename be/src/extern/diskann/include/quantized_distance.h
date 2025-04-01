#pragma once
#include <memory>
#include <string>
#include <vector>
#include "abstract_scratch.h"

namespace diskann
{
template <typename data_t> class PQScratch;

template <typename data_t> class QuantizedDistance
{
  public:
    QuantizedDistance() = default;
    QuantizedDistance(const QuantizedDistance &) = delete;
    QuantizedDistance &operator=(const QuantizedDistance &) = delete;
    virtual ~QuantizedDistance() = default;

    virtual bool is_opq() const = 0;
    virtual std::string get_quantized_vectors_filename(const std::string &prefix) const = 0;
    virtual std::string get_pivot_data_filename(const std::string &prefix) const = 0;
    virtual std::string get_rotation_matrix_suffix(const std::string &pq_pivots_filename) const = 0;

    // Loading the PQ centroid table need not be part of the abstract class.
    // However, we want to indicate that this function will change once we have a
    // file reader hierarchy, so leave it here as-is.
#ifdef EXEC_ENV_OLS
    virtual void load_pivot_data(MemoryMappedFiles &files, const std::String &pq_table_file, size_t num_chunks) = 0;
#else
    virtual void load_pivot_data(const std::string &pq_table_file, size_t num_chunks) = 0;
#endif

    // Number of chunks in the PQ table. Depends on the compression level used.
    // Has to be < ndim
    virtual uint32_t get_num_chunks() const = 0;

    // Preprocess the query by computing chunk distances from the query vector to
    // various centroids. Since we don't want this class to do scratch management,
    // we will take a PQScratch object which can come either from Index class or
    // PQFlashIndex class.
    virtual void preprocess_query(const data_t *query_vec, uint32_t query_dim, PQScratch<data_t> &pq_scratch) = 0;

    // Workhorse
    // This function must be called after preprocess_query
    virtual void preprocessed_distance(PQScratch<data_t> &pq_scratch, const uint32_t id_count, float *dists_out) = 0;

    // Same as above, but convenience function for index.cpp.
    virtual void preprocessed_distance(PQScratch<data_t> &pq_scratch, const uint32_t n_ids,
                                       std::vector<float> &dists_out) = 0;

    // Currently this function is required for DiskPQ. However, it too can be subsumed
    // under preprocessed_distance if we add the appropriate scratch variables to
    // PQScratch and initialize them in pq_flash_index.cpp::disk_iterate_to_fixed_point()
    virtual float brute_force_distance(const float *query_vec, uint8_t *base_vec) = 0;
};
} // namespace diskann
