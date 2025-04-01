#pragma once
#include "quantized_distance.h"

namespace diskann
{
template <typename data_t> class PQL2Distance : public QuantizedDistance<data_t>
{
  public:
    // REFACTOR TODO: We could take a file prefix here and load the
    // PQ pivots file, so that the distance object is initialized
    // immediately after construction. But this would not work well
    // with our data store concept where the store is created first
    // and data populated after.
    // REFACTOR TODO: Ideally, we should only read the num_chunks from
    // the pivots file. However, we read the pivots file only later, but
    // clients can call functions like get_<xxx>_filename without calling
    // load_pivot_data. Hence this. The TODO is whether we should check
    // that the num_chunks from the file is the same as this one.

    PQL2Distance(uint32_t num_chunks, bool use_opq = false);

    virtual ~PQL2Distance() override;

    virtual bool is_opq() const override;

    virtual std::string get_quantized_vectors_filename(const std::string &prefix) const override;
    virtual std::string get_pivot_data_filename(const std::string &prefix) const override;
    virtual std::string get_rotation_matrix_suffix(const std::string &pq_pivots_filename) const override;

#ifdef EXEC_ENV_OLS
    virtual void load_pivot_data(MemoryMappedFiles &files, const std::string &pq_table_file,
                                 size_t num_chunks) override;
#else
    virtual void load_pivot_data(const std::string &pq_table_file, size_t num_chunks) override;
#endif

    // Number of chunks in the PQ table. Depends on the compression level used.
    // Has to be < ndim
    virtual uint32_t get_num_chunks() const override;

    // Preprocess the query by computing chunk distances from the query vector to
    // various centroids. Since we don't want this class to do scratch management,
    // we will take a PQScratch object which can come either from Index class or
    // PQFlashIndex class.
    virtual void preprocess_query(const data_t *aligned_query, uint32_t original_dim,
                                  PQScratch<data_t> &pq_scratch) override;

    // Distance function used for graph traversal. This function must be called
    // after
    // preprocess_query. The reason we do not call preprocess ourselves is because
    // that function has to be called once per query, while this function is
    // called at each iteration of the graph walk. NOTE: This function expects
    // 1. the query to be preprocessed using preprocess_query()
    // 2. the scratch object to contain the quantized vectors corresponding to ids
    // in aligned_pq_coord_scratch. Done by calling aggregate_coords()
    //
    virtual void preprocessed_distance(PQScratch<data_t> &pq_scratch, const uint32_t id_count,
                                       float *dists_out) override;

    // Same as above, but returns the distances in a vector instead of an array.
    // Convenience function for index.cpp.
    virtual void preprocessed_distance(PQScratch<data_t> &pq_scratch, const uint32_t n_ids,
                                       std::vector<float> &dists_out) override;

    // Currently this function is required for DiskPQ. However, it too can be
    // subsumed under preprocessed_distance if we add the appropriate scratch
    // variables to PQScratch and initialize them in
    // pq_flash_index.cpp::disk_iterate_to_fixed_point()
    virtual float brute_force_distance(const float *query_vec, uint8_t *base_vec) override;

  protected:
    // assumes pre-processed query
    virtual void prepopulate_chunkwise_distances(const float *query_vec, float *dist_vec);

    // assumes no rotation is involved
    // virtual void inflate_vector(uint8_t *base_vec, float *out_vec);

    float *_tables = nullptr; // pq_tables = float array of size [256 * ndims]
    uint64_t _ndims = 0;      // ndims = true dimension of vectors
    uint64_t _num_chunks = 0;
    bool _is_opq = false;
    uint32_t *_chunk_offsets = nullptr;
    float *_centroid = nullptr;
    float *_tables_tr = nullptr; // same as pq_tables, but col-major
    float *_rotmat_tr = nullptr;
};
} // namespace diskann
