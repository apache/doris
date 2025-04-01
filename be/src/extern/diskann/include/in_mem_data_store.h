// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
#pragma once

#include <shared_mutex>
#include <memory>

#include "tsl/robin_map.h"
#include "tsl/robin_set.h"
#include "tsl/sparse_map.h"
// #include "boost/dynamic_bitset.hpp"

#include "abstract_data_store.h"

#include "distance.h"
#include "natural_number_map.h"
#include "natural_number_set.h"
#include "aligned_file_reader.h"

namespace diskann
{
template <typename data_t> class InMemDataStore : public AbstractDataStore<data_t>
{
  public:
    InMemDataStore(const location_t capacity, const size_t dim, std::unique_ptr<Distance<data_t>> distance_fn);
    virtual ~InMemDataStore();

    virtual location_t load(const std::string &filename) override;
    virtual size_t save(const std::string &filename, const location_t num_points) override;

    virtual size_t get_aligned_dim() const override;

    // Populate internal data from unaligned data while doing alignment and any
    // normalization that is required.
    virtual void populate_data(const data_t *vectors, const location_t num_pts) override;
    virtual void populate_data(const std::string &filename, const size_t offset) override;

    virtual void extract_data_to_bin(const std::string &filename, const location_t num_pts) override;

    virtual void get_vector(const location_t i, data_t *target) const override;
    virtual void set_vector(const location_t i, const data_t *const vector) override;
    virtual void prefetch_vector(const location_t loc) override;

    virtual void move_vectors(const location_t old_location_start, const location_t new_location_start,
                              const location_t num_points) override;
    virtual void copy_vectors(const location_t from_loc, const location_t to_loc, const location_t num_points) override;

    virtual void preprocess_query(const data_t *query, AbstractScratch<data_t> *query_scratch) const override;

    virtual float get_distance(const data_t *preprocessed_query, const location_t loc) const override;
    virtual float get_distance(const location_t loc1, const location_t loc2) const override;

    virtual void get_distance(const data_t *preprocessed_query, const location_t *locations,
                              const uint32_t location_count, float *distances,
                              AbstractScratch<data_t> *scratch) const override;
    virtual void get_distance(const data_t *preprocessed_query, const std::vector<location_t> &ids,
                              std::vector<float> &distances, AbstractScratch<data_t> *scratch_space) const override;

    virtual location_t calculate_medoid() const override;

    virtual Distance<data_t> *get_dist_fn() const override;

    virtual size_t get_alignment_factor() const override;

  protected:
    virtual location_t expand(const location_t new_size) override;
    virtual location_t shrink(const location_t new_size) override;

    virtual location_t load_impl(const std::string &filename);
#ifdef EXEC_ENV_OLS
    virtual location_t load_impl(AlignedFileReader &reader);
#endif

  private:
    data_t *_data = nullptr;

    size_t _aligned_dim;

    // It may seem weird to put distance metric along with the data store class,
    // but this gives us perf benefits as the datastore can do distance
    // computations during search and compute norms of vectors internally without
    // have to copy data back and forth.
    std::unique_ptr<Distance<data_t>> _distance_fn;

    // in case we need to save vector norms for optimization
    std::shared_ptr<float[]> _pre_computed_norms;
};

} // namespace diskann