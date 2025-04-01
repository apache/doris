// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <vector>
#include <string>

#include "types.h"
#include "windows_customizations.h"
#include "distance.h"

namespace diskann
{

template <typename data_t> class AbstractScratch;

template <typename data_t> class AbstractDataStore
{
  public:
    AbstractDataStore(const location_t capacity, const size_t dim);

    virtual ~AbstractDataStore() = default;

    // Return number of points returned
    virtual location_t load(const std::string &filename) = 0;

    // Why does store take num_pts? Since store only has capacity, but we allow
    // resizing we can end up in a situation where the store has spare capacity.
    // To optimize disk utilization, we pass the number of points that are "true"
    // points, so that the store can discard the empty locations before saving.
    virtual size_t save(const std::string &filename, const location_t num_pts) = 0;

    DISKANN_DLLEXPORT virtual location_t capacity() const;

    DISKANN_DLLEXPORT virtual size_t get_dims() const;

    // Implementers can choose to return _dim if they are not
    // concerned about memory alignment.
    // Some distance metrics (like l2) need data vectors to be aligned, so we
    // align the dimension by padding zeros.
    virtual size_t get_aligned_dim() const = 0;

    // populate the store with vectors (either from a pointer or bin file),
    // potentially after pre-processing the vectors if the metric deems so
    // e.g., normalizing vectors for cosine distance over floating-point vectors
    // useful for bulk or static index building.
    virtual void populate_data(const data_t *vectors, const location_t num_pts) = 0;
    virtual void populate_data(const std::string &filename, const size_t offset) = 0;

    // save the first num_pts many vectors back to bin file
    // note: cannot undo the pre-processing done in populate data
    virtual void extract_data_to_bin(const std::string &filename, const location_t num_pts) = 0;

    // Returns the updated capacity of the datastore. Clients should check
    // if resize actually changed the capacity to new_num_points before
    // proceeding with operations. See the code below:
    //  auto new_capcity = data_store->resize(new_num_points);
    //  if ( new_capacity >= new_num_points) {
    //   //PROCEED
    //  else
    //    //ERROR.
    virtual location_t resize(const location_t new_num_points);

    // operations on vectors
    // like populate_data function, but over one vector at a time useful for
    // streaming setting
    virtual void get_vector(const location_t i, data_t *dest) const = 0;
    virtual void set_vector(const location_t i, const data_t *const vector) = 0;
    virtual void prefetch_vector(const location_t loc) = 0;

    // internal shuffle operations to move around vectors
    // will bulk-move all the vectors in [old_start_loc, old_start_loc +
    // num_points) to [new_start_loc, new_start_loc + num_points) and set the old
    // positions to zero vectors.
    virtual void move_vectors(const location_t old_start_loc, const location_t new_start_loc,
                              const location_t num_points) = 0;

    // same as above, without resetting the vectors in [from_loc, from_loc +
    // num_points) to zero
    virtual void copy_vectors(const location_t from_loc, const location_t to_loc, const location_t num_points) = 0;

    // With the PQ Data Store PR, we have also changed iterate_to_fixed_point to NOT take the query
    // from the scratch object. Therefore every data store has to implement preprocess_query which
    // at the least will be to copy the query into the scratch object. So making this pure virtual.
    virtual void preprocess_query(const data_t *aligned_query,
                                  AbstractScratch<data_t> *query_scratch = nullptr) const = 0;
    // distance functions.
    virtual float get_distance(const data_t *query, const location_t loc) const = 0;
    virtual void get_distance(const data_t *query, const location_t *locations, const uint32_t location_count,
                              float *distances, AbstractScratch<data_t> *scratch_space = nullptr) const = 0;
    // Specific overload for index.cpp.
    virtual void get_distance(const data_t *preprocessed_query, const std::vector<location_t> &ids,
                              std::vector<float> &distances, AbstractScratch<data_t> *scratch_space) const = 0;
    virtual float get_distance(const location_t loc1, const location_t loc2) const = 0;

    // stats of the data stored in store
    // Returns the point in the dataset that is closest to the mean of all points
    // in the dataset
    virtual location_t calculate_medoid() const = 0;

    // REFACTOR PQ TODO: Each data store knows about its distance function, so this is
    // redundant. However, we don't have an OptmizedDataStore yet, and to preserve code
    // compability, we are exposing this function.
    virtual Distance<data_t> *get_dist_fn() const = 0;

    // search helpers
    // if the base data is aligned per the request of the metric, this will tell
    // how to align the query vector in a consistent manner
    virtual size_t get_alignment_factor() const = 0;

  protected:
    // Expand the datastore to new_num_points. Returns the new capacity created,
    // which should be == new_num_points in the normal case. Implementers can also
    // return _capacity to indicate that there are not implementing this method.
    virtual location_t expand(const location_t new_num_points) = 0;

    // Shrink the datastore to new_num_points. It is NOT an error if shrink
    // doesn't reduce the capacity so callers need to check this correctly. See
    // also for "default" implementation
    virtual location_t shrink(const location_t new_num_points) = 0;

    location_t _capacity;
    size_t _dim;
};

} // namespace diskann
