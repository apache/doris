// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "mkl.h"
#if defined(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS) && defined(DISKANN_BUILD)
#include "gperftools/malloc_extension.h"
#endif
#include "pq.h"
#include "partition.h"
#include "math_utils.h"
#include "tsl/robin_map.h"

#include "vector/stream_wrapper.h"

// block size for reading/processing large files and matrices in blocks
#define BLOCK_SIZE 5000000

namespace diskann
{
FixedChunkPQTable::FixedChunkPQTable()
{
}

FixedChunkPQTable::~FixedChunkPQTable()
{
#ifndef EXEC_ENV_OLS
    if (tables != nullptr)
        delete[] tables;
    if (tables_tr != nullptr)
        delete[] tables_tr;
    if (chunk_offsets != nullptr)
        delete[] chunk_offsets;
    if (centroid != nullptr)
        delete[] centroid;
    if (rotmat_tr != nullptr)
        delete[] rotmat_tr;
#endif
}

#ifdef EXEC_ENV_OLS
void FixedChunkPQTable::load_pq_centroid_bin(MemoryMappedFiles &files, const char *pq_table_file, size_t num_chunks)
{
#else
void FixedChunkPQTable::load_pq_centroid_bin(const char *pq_table_file, size_t num_chunks)
{
#endif

    uint64_t nr, nc;
    std::string rotmat_file = std::string(pq_table_file) + "_rotation_matrix.bin";

#ifdef EXEC_ENV_OLS
    size_t *file_offset_data; // since load_bin only sets the pointer, no need
                              // to delete.
    diskann::load_bin<size_t>(files, pq_table_file, file_offset_data, nr, nc);
#else
    std::unique_ptr<size_t[]> file_offset_data;
    diskann::load_bin<size_t>(pq_table_file, file_offset_data, nr, nc);
#endif

    bool use_old_filetype = false;

    if (nr != 4 && nr != 5)
    {
        diskann::cout << "Error reading pq_pivots file " << pq_table_file
                      << ". Offsets dont contain correct metadata, # offsets = " << nr << ", but expecting " << 4
                      << " or " << 5;
        throw diskann::ANNException("Error reading pq_pivots file at offsets data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    if (nr == 4)
    {
        diskann::cout << "Offsets: " << file_offset_data[0] << " " << file_offset_data[1] << " " << file_offset_data[2]
                      << " " << file_offset_data[3] << std::endl;
    }
    else if (nr == 5)
    {
        use_old_filetype = true;
        diskann::cout << "Offsets: " << file_offset_data[0] << " " << file_offset_data[1] << " " << file_offset_data[2]
                      << " " << file_offset_data[3] << file_offset_data[4] << std::endl;
    }
    else
    {
        throw diskann::ANNException("Wrong number of offsets in pq_pivots", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

#ifdef EXEC_ENV_OLS

    diskann::load_bin<float>(files, pq_table_file, tables, nr, nc, file_offset_data[0]);
#else
    diskann::load_bin<float>(pq_table_file, tables, nr, nc, file_offset_data[0]);
#endif

    if ((nr != NUM_PQ_CENTROIDS))
    {
        diskann::cout << "Error reading pq_pivots file " << pq_table_file << ". file_num_centers  = " << nr
                      << " but expecting " << NUM_PQ_CENTROIDS << " centers";
        throw diskann::ANNException("Error reading pq_pivots file at pivots data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    this->ndims = nc;

#ifdef EXEC_ENV_OLS
    diskann::load_bin<float>(files, pq_table_file, centroid, nr, nc, file_offset_data[1]);
#else
    diskann::load_bin<float>(pq_table_file, centroid, nr, nc, file_offset_data[1]);
#endif

    if ((nr != this->ndims) || (nc != 1))
    {
        diskann::cerr << "Error reading centroids from pq_pivots file " << pq_table_file << ". file_dim  = " << nr
                      << ", file_cols = " << nc << " but expecting " << this->ndims << " entries in 1 dimension.";
        throw diskann::ANNException("Error reading pq_pivots file at centroid data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    int chunk_offsets_index = 2;
    if (use_old_filetype)
    {
        chunk_offsets_index = 3;
    }
#ifdef EXEC_ENV_OLS
    diskann::load_bin<uint32_t>(files, pq_table_file, chunk_offsets, nr, nc, file_offset_data[chunk_offsets_index]);
#else
    diskann::load_bin<uint32_t>(pq_table_file, chunk_offsets, nr, nc, file_offset_data[chunk_offsets_index]);
#endif

    if (nc != 1 || (nr != num_chunks + 1 && num_chunks != 0))
    {
        diskann::cerr << "Error loading chunk offsets file. numc: " << nc << " (should be 1). numr: " << nr
                      << " (should be " << num_chunks + 1 << " or 0 if we need to infer)" << std::endl;
        throw diskann::ANNException("Error loading chunk offsets file", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    this->n_chunks = nr - 1;
    diskann::cout << "Loaded PQ Pivots: #ctrs: " << NUM_PQ_CENTROIDS << ", #dims: " << this->ndims
                  << ", #chunks: " << this->n_chunks << std::endl;

#ifdef EXEC_ENV_OLS
    if (files.fileExists(rotmat_file))
    {
        diskann::load_bin<float>(files, rotmat_file, (float *&)rotmat_tr, nr, nc);
#else
    if (file_exists(rotmat_file))
    {
        diskann::load_bin<float>(rotmat_file, rotmat_tr, nr, nc);
#endif
        if (nr != this->ndims || nc != this->ndims)
        {
            diskann::cerr << "Error loading rotation matrix file" << std::endl;
            throw diskann::ANNException("Error loading rotation matrix file", -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        use_rotation = true;
    }

    // alloc and compute transpose
    tables_tr = new float[256 * this->ndims];
    for (size_t i = 0; i < 256; i++)
    {
        for (size_t j = 0; j < this->ndims; j++)
        {
            tables_tr[j * 256 + i] = tables[i * this->ndims + j];
        }
    }
}

void FixedChunkPQTable::load_pq_centroid_bin(IReaderWrapperSPtr reader, size_t num_chunks)
{

    uint64_t nr, nc;
    size_t *file_offset_data;
    diskann::load_bin<size_t>(reader, file_offset_data, nr, nc);
    std::unique_ptr<size_t[]> tmp(file_offset_data);
    diskann::load_bin<float>(reader, tables, nr, nc, file_offset_data[0]);

    if ((nr != NUM_PQ_CENTROIDS))
    {
        diskann::cout << "Error reading pq_pivots file " << ". file_num_centers  = " << nr
                      << " but expecting " << NUM_PQ_CENTROIDS << " centers";
        throw diskann::ANNException("Error reading pq_pivots file at pivots data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    this->ndims = nc;
    diskann::load_bin<float>(reader, centroid, nr, nc, file_offset_data[1]);
    if ((nr != this->ndims) || (nc != 1))
    {
        diskann::cerr << "Error reading centroids from pq_pivots file " << ". file_dim  = " << nr
                      << ", file_cols = " << nc << " but expecting " << this->ndims << " entries in 1 dimension.";
        throw diskann::ANNException("Error reading pq_pivots file at centroid data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    diskann::load_bin<uint32_t>(reader, chunk_offsets, nr, nc, file_offset_data[2]);
    if (nc != 1 || (nr != num_chunks + 1 && num_chunks != 0))
    {
        diskann::cerr << "Error loading chunk offsets file. numc: " << nc << " (should be 1). numr: " << nr
                      << " (should be " << num_chunks + 1 << " or 0 if we need to infer)" << std::endl;
        throw diskann::ANNException("Error loading chunk offsets file", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    this->n_chunks = nr - 1;
    diskann::cout << "Loaded PQ Pivots: #ctrs: " << NUM_PQ_CENTROIDS << ", #dims: " << this->ndims
                  << ", #chunks: " << this->n_chunks << std::endl;

    // alloc and compute transpose
    tables_tr = new float[256 * this->ndims];
    for (size_t i = 0; i < 256; i++)
    {
        for (size_t j = 0; j < this->ndims; j++)
        {
            tables_tr[j * 256 + i] = tables[i * this->ndims + j];
            //std::cout << tables_tr[j * 256 + i] << ",";
        }
        //std::cout << std::endl;
    }
}

uint32_t FixedChunkPQTable::get_num_chunks()
{
    return static_cast<uint32_t>(n_chunks);
}

void FixedChunkPQTable::preprocess_query(float *query_vec)
{
    for (uint32_t d = 0; d < ndims; d++)
    {
        query_vec[d] -= centroid[d];
    }
    std::vector<float> tmp(ndims, 0);
    if (use_rotation)
    {
        for (uint32_t d = 0; d < ndims; d++)
        {
            for (uint32_t d1 = 0; d1 < ndims; d1++)
            {
                tmp[d] += query_vec[d1] * rotmat_tr[d1 * ndims + d];
            }
        }
        std::memcpy(query_vec, tmp.data(), ndims * sizeof(float));
    }
}

// assumes pre-processed query
void FixedChunkPQTable::populate_chunk_distances(const float *query_vec, float *dist_vec)
{
    memset(dist_vec, 0, 256 * n_chunks * sizeof(float));
    // chunk wise distance computation
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        // sum (q-c)^2 for the dimensions associated with this chunk
        float *chunk_dists = dist_vec + (256 * chunk);
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            for (size_t idx = 0; idx < 256; idx++)
            {
                double diff = centers_dim_vec[idx] - (query_vec[j]);
                chunk_dists[idx] += (float)(diff * diff);
            }
        }
    }
}

float FixedChunkPQTable::l2_distance(const float *query_vec, uint8_t *base_vec)
{
    float res = 0;
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            float diff = centers_dim_vec[base_vec[chunk]] - (query_vec[j]);
            res += diff * diff;
        }
    }
    return res;
}

float FixedChunkPQTable::inner_product(const float *query_vec, uint8_t *base_vec)
{
    float res = 0;
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            float diff = centers_dim_vec[base_vec[chunk]] * query_vec[j]; // assumes centroid is 0 to
                                                                          // prevent translation errors
            res += diff;
        }
    }
    return -res; // returns negative value to simulate distances (max -> min
                 // conversion)
}

// assumes no rotation is involved
void FixedChunkPQTable::inflate_vector(uint8_t *base_vec, float *out_vec)
{
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            out_vec[j] = centers_dim_vec[base_vec[chunk]] + centroid[j];
        }
    }
}

void FixedChunkPQTable::populate_chunk_inner_products(const float *query_vec, float *dist_vec)
{
    memset(dist_vec, 0, 256 * n_chunks * sizeof(float));
    // chunk wise distance computation
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        // sum (q-c)^2 for the dimensions associated with this chunk
        float *chunk_dists = dist_vec + (256 * chunk);
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            for (size_t idx = 0; idx < 256; idx++)
            {
                double prod = centers_dim_vec[idx] * query_vec[j]; // assumes that we are not
                                                                   // shifting the vectors to
                                                                   // mean zero, i.e., centroid
                                                                   // array should be all zeros
                chunk_dists[idx] -= (float)prod;                   // returning negative to keep the search code
                                                                   // clean (max inner product vs min distance)
            }
        }
    }
}

void aggregate_coords(const std::vector<uint32_t> &ids, const uint8_t *all_coords, const size_t ndims, uint8_t *out)
{
    for (size_t i = 0; i < ids.size(); i++)
    {
        memcpy(out + i * ndims, all_coords + ids[i] * ndims, ndims * sizeof(uint8_t));
    }
}

void pq_dist_lookup(const uint8_t *pq_ids, const size_t n_pts, const size_t pq_nchunks, const float *pq_dists,
                    std::vector<float> &dists_out)
{
    //_mm_prefetch((char*) dists_out, _MM_HINT_T0);
    _mm_prefetch((char *)pq_ids, _MM_HINT_T0);
    _mm_prefetch((char *)(pq_ids + 64), _MM_HINT_T0);
    _mm_prefetch((char *)(pq_ids + 128), _MM_HINT_T0);
    dists_out.clear();
    dists_out.resize(n_pts, 0);
    for (size_t chunk = 0; chunk < pq_nchunks; chunk++)
    {
        const float *chunk_dists = pq_dists + 256 * chunk;
        if (chunk < pq_nchunks - 1)
        {
            _mm_prefetch((char *)(chunk_dists + 256), _MM_HINT_T0);
        }
        for (size_t idx = 0; idx < n_pts; idx++)
        {
            uint8_t pq_centerid = pq_ids[pq_nchunks * idx + chunk];
            dists_out[idx] += chunk_dists[pq_centerid];
        }
    }
}

// Need to replace calls to these functions with calls to vector& based
// functions above
void aggregate_coords(const uint32_t *ids, const size_t n_ids, const uint8_t *all_coords, const size_t ndims,
                      uint8_t *out)
{
    for (size_t i = 0; i < n_ids; i++)
    {
        memcpy(out + i * ndims, all_coords + ids[i] * ndims, ndims * sizeof(uint8_t));
    }
}

void pq_dist_lookup(const uint8_t *pq_ids, const size_t n_pts, const size_t pq_nchunks, const float *pq_dists,
                    float *dists_out)
{
    _mm_prefetch((char *)dists_out, _MM_HINT_T0);
    _mm_prefetch((char *)pq_ids, _MM_HINT_T0);
    _mm_prefetch((char *)(pq_ids + 64), _MM_HINT_T0);
    _mm_prefetch((char *)(pq_ids + 128), _MM_HINT_T0);
    memset(dists_out, 0, n_pts * sizeof(float));
    for (size_t chunk = 0; chunk < pq_nchunks; chunk++)
    {
        const float *chunk_dists = pq_dists + 256 * chunk;
        if (chunk < pq_nchunks - 1)
        {
            _mm_prefetch((char *)(chunk_dists + 256), _MM_HINT_T0);
        }
        for (size_t idx = 0; idx < n_pts; idx++)
        {
            uint8_t pq_centerid = pq_ids[pq_nchunks * idx + chunk];
            dists_out[idx] += chunk_dists[pq_centerid];
        }
    }
}

// generate_pq_pivots_simplified is a simplified version of generate_pq_pivots.
// Input is provided in the in-memory buffer train_data.
// Output is stored in the in-memory buffer pivot_data_vector.
// Simplification is based on the following assumptions:
//   dim % num_pq_chunks == 0
//   num_centers == 256 by default
//   KMEANS_ITERS_FOR_PQ == 15 by default
//   make_zero_mean is false by default.
// These assumptions allow to make the function much simpler and avoid storing
// array of chunk_offsets and centroids.
// The compiler pragma for multi-threading support is removed from this implementation
// for the purpose of integration into systems that strictly control resource allocation.
int generate_pq_pivots_simplified(const float *train_data, size_t num_train, size_t dim, size_t num_pq_chunks,
                                  std::vector<float> &pivot_data_vector)
{
    if (num_pq_chunks > dim || dim % num_pq_chunks != 0)
    {
        return -1;
    }

    const size_t num_centers = 256;
    const size_t cur_chunk_size = dim / num_pq_chunks;
    const uint32_t KMEANS_ITERS_FOR_PQ = 15;

    pivot_data_vector.resize(num_centers * dim);
    std::vector<float> cur_pivot_data_vector(num_centers * cur_chunk_size);
    std::vector<float> cur_data_vector(num_train * cur_chunk_size);
    std::vector<uint32_t> closest_center_vector(num_train);

    float *pivot_data = &pivot_data_vector[0];
    float *cur_pivot_data = &cur_pivot_data_vector[0];
    float *cur_data = &cur_data_vector[0];
    uint32_t *closest_center = &closest_center_vector[0];

    for (size_t i = 0; i < num_pq_chunks; i++)
    {
        size_t chunk_offset = cur_chunk_size * i;

        for (int32_t j = 0; j < num_train; j++)
        {
            std::memcpy(cur_data + j * cur_chunk_size, train_data + j * dim + chunk_offset,
                        cur_chunk_size * sizeof(float));
        }

        kmeans::kmeanspp_selecting_pivots(cur_data, num_train, cur_chunk_size, cur_pivot_data, num_centers);

        kmeans::run_lloyds(cur_data, num_train, cur_chunk_size, cur_pivot_data, num_centers, KMEANS_ITERS_FOR_PQ, NULL,
                           closest_center);

        for (uint64_t j = 0; j < num_centers; j++)
        {
            std::memcpy(pivot_data + j * dim + chunk_offset, cur_pivot_data + j * cur_chunk_size,
                        cur_chunk_size * sizeof(float));
        }
    }

    return 0;
}

// given training data in train_data of dimensions num_train * dim, generate
// PQ pivots using k-means algorithm to partition the co-ordinates into
// num_pq_chunks (if it divides dimension, else rounded) chunks, and runs
// k-means in each chunk to compute the PQ pivots and stores in bin format in
// file pq_pivots_path as a s num_centers*dim floating point binary file
int generate_pq_pivots(const float *const passed_train_data, size_t num_train, uint32_t dim, uint32_t num_centers,
                       uint32_t num_pq_chunks, uint32_t max_k_means_reps, std::string pq_pivots_path,
                       bool make_zero_mean)
{
    if (num_pq_chunks > dim)
    {
        diskann::cout << " Error: number of chunks more than dimension" << std::endl;
        return -1;
    }

    std::unique_ptr<float[]> train_data = std::make_unique<float[]>(num_train * dim);
    std::memcpy(train_data.get(), passed_train_data, num_train * dim * sizeof(float));

    std::unique_ptr<float[]> full_pivot_data;

    if (file_exists(pq_pivots_path))
    {
        size_t file_dim, file_num_centers;
        diskann::load_bin<float>(pq_pivots_path, full_pivot_data, file_num_centers, file_dim, METADATA_SIZE);
        if (file_dim == dim && file_num_centers == num_centers)
        {
            diskann::cout << "PQ pivot file exists. Not generating again" << std::endl;
            return -1;
        }
    }

    // Calculate centroid and center the training data
    std::unique_ptr<float[]> centroid = std::make_unique<float[]>(dim);
    for (uint64_t d = 0; d < dim; d++)
    {
        centroid[d] = 0;
    }
    if (make_zero_mean)
    { // If we use L2 distance, there is an option to
      // translate all vectors to make them centered and
      // then compute PQ. This needs to be set to false
      // when using PQ for MIPS as such translations dont
      // preserve inner products.
        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                centroid[d] += train_data[p * dim + d];
            }
            centroid[d] /= num_train;
        }

        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                train_data[p * dim + d] -= centroid[d];
            }
        }
    }

    std::vector<uint32_t> chunk_offsets;

    size_t low_val = (size_t)std::floor((double)dim / (double)num_pq_chunks);
    size_t high_val = (size_t)std::ceil((double)dim / (double)num_pq_chunks);
    size_t max_num_high = dim - (low_val * num_pq_chunks);
    size_t cur_num_high = 0;
    size_t cur_bin_threshold = high_val;

    std::vector<std::vector<uint32_t>> bin_to_dims(num_pq_chunks);
    tsl::robin_map<uint32_t, uint32_t> dim_to_bin;
    std::vector<float> bin_loads(num_pq_chunks, 0);

    // Process dimensions not inserted by previous loop
    for (uint32_t d = 0; d < dim; d++)
    {
        if (dim_to_bin.find(d) != dim_to_bin.end())
            continue;
        auto cur_best = num_pq_chunks + 1;
        float cur_best_load = std::numeric_limits<float>::max();
        for (uint32_t b = 0; b < num_pq_chunks; b++)
        {
            if (bin_loads[b] < cur_best_load && bin_to_dims[b].size() < cur_bin_threshold)
            {
                cur_best = b;
                cur_best_load = bin_loads[b];
            }
        }
        bin_to_dims[cur_best].push_back(d);
        if (bin_to_dims[cur_best].size() == high_val)
        {
            cur_num_high++;
            if (cur_num_high == max_num_high)
                cur_bin_threshold = low_val;
        }
    }

    chunk_offsets.clear();
    chunk_offsets.push_back(0);

    for (uint32_t b = 0; b < num_pq_chunks; b++)
    {
        if (b > 0)
            chunk_offsets.push_back(chunk_offsets[b - 1] + (uint32_t)bin_to_dims[b - 1].size());
    }
    chunk_offsets.push_back(dim);

    full_pivot_data.reset(new float[num_centers * dim]);

    for (size_t i = 0; i < num_pq_chunks; i++)
    {
        size_t cur_chunk_size = chunk_offsets[i + 1] - chunk_offsets[i];

        if (cur_chunk_size == 0)
            continue;
        std::unique_ptr<float[]> cur_pivot_data = std::make_unique<float[]>(num_centers * cur_chunk_size);
        std::unique_ptr<float[]> cur_data = std::make_unique<float[]>(num_train * cur_chunk_size);
        std::unique_ptr<uint32_t[]> closest_center = std::make_unique<uint32_t[]>(num_train);

        diskann::cout << "Processing chunk " << i << " with dimensions [" << chunk_offsets[i] << ", "
                      << chunk_offsets[i + 1] << ")" << std::endl;

#pragma omp parallel for schedule(static, 65536)
        for (int64_t j = 0; j < (int64_t)num_train; j++)
        {
            std::memcpy(cur_data.get() + j * cur_chunk_size, train_data.get() + j * dim + chunk_offsets[i],
                        cur_chunk_size * sizeof(float));
        }

        kmeans::kmeanspp_selecting_pivots(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(), num_centers);

        kmeans::run_lloyds(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(), num_centers,
                           max_k_means_reps, NULL, closest_center.get());

        for (uint64_t j = 0; j < num_centers; j++)
        {
            std::memcpy(full_pivot_data.get() + j * dim + chunk_offsets[i], cur_pivot_data.get() + j * cur_chunk_size,
                        cur_chunk_size * sizeof(float));
        }
    }

    std::vector<size_t> cumul_bytes(4, 0);
    cumul_bytes[0] = METADATA_SIZE;
    cumul_bytes[1] = cumul_bytes[0] + diskann::save_bin<float>(pq_pivots_path.c_str(), full_pivot_data.get(),
                                                               (size_t)num_centers, dim, cumul_bytes[0]);
    cumul_bytes[2] = cumul_bytes[1] +
                     diskann::save_bin<float>(pq_pivots_path.c_str(), centroid.get(), (size_t)dim, 1, cumul_bytes[1]);
    cumul_bytes[3] = cumul_bytes[2] + diskann::save_bin<uint32_t>(pq_pivots_path.c_str(), chunk_offsets.data(),
                                                                  chunk_offsets.size(), 1, cumul_bytes[2]);
    diskann::save_bin<size_t>(pq_pivots_path.c_str(), cumul_bytes.data(), cumul_bytes.size(), 1, 0);

    diskann::cout << "Saved pq pivot data to " << pq_pivots_path << " of size " << cumul_bytes[cumul_bytes.size() - 1]
                  << "B." << std::endl;

    return 0;
}


int generate_pq_pivots(const float *const passed_train_data, size_t num_train, uint32_t dim, uint32_t num_centers,
                       uint32_t num_pq_chunks, uint32_t max_k_means_reps, std::stringstream &pq_pivots_stream,
                       bool make_zero_mean)
{
    if (num_pq_chunks > dim)
    {
        diskann::cout << " Error: number of chunks more than dimension" << std::endl;
        return -1;
    }

    std::unique_ptr<float[]> train_data = std::make_unique<float[]>(num_train * dim);
    std::memcpy(train_data.get(), passed_train_data, num_train * dim * sizeof(float));

    std::unique_ptr<float[]> full_pivot_data;

    // Calculate centroid and center the training data
    std::unique_ptr<float[]> centroid = std::make_unique<float[]>(dim);
    for (uint64_t d = 0; d < dim; d++)
    {
        centroid[d] = 0;
    }
    if (make_zero_mean)
    { // If we use L2 distance, there is an option to
      // translate all vectors to make them centered and
      // then compute PQ. This needs to be set to false
      // when using PQ for MIPS as such translations dont
      // preserve inner products.
        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                centroid[d] += train_data[p * dim + d];
            }
            centroid[d] /= num_train;
        }

        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                train_data[p * dim + d] -= centroid[d];
            }
        }
    }

    std::vector<uint32_t> chunk_offsets;

    size_t low_val = (size_t)std::floor((double)dim / (double)num_pq_chunks);
    size_t high_val = (size_t)std::ceil((double)dim / (double)num_pq_chunks);
    size_t max_num_high = dim - (low_val * num_pq_chunks);
    size_t cur_num_high = 0;
    size_t cur_bin_threshold = high_val;

    std::vector<std::vector<uint32_t>> bin_to_dims(num_pq_chunks);
    tsl::robin_map<uint32_t, uint32_t> dim_to_bin;
    std::vector<float> bin_loads(num_pq_chunks, 0);

    // Process dimensions not inserted by previous loop
    for (uint32_t d = 0; d < dim; d++)
    {
        if (dim_to_bin.find(d) != dim_to_bin.end())
            continue;
        auto cur_best = num_pq_chunks + 1;
        float cur_best_load = std::numeric_limits<float>::max();
        for (uint32_t b = 0; b < num_pq_chunks; b++)
        {
            if (bin_loads[b] < cur_best_load && bin_to_dims[b].size() < cur_bin_threshold)
            {
                cur_best = b;
                cur_best_load = bin_loads[b];
            }
        }
        bin_to_dims[cur_best].push_back(d);
        if (bin_to_dims[cur_best].size() == high_val)
        {
            cur_num_high++;
            if (cur_num_high == max_num_high)
                cur_bin_threshold = low_val;
        }
    }

    chunk_offsets.clear();
    chunk_offsets.push_back(0);

    for (uint32_t b = 0; b < num_pq_chunks; b++)
    {
        if (b > 0)
            chunk_offsets.push_back(chunk_offsets[b - 1] + (uint32_t)bin_to_dims[b - 1].size());
    }
    chunk_offsets.push_back(dim);

    full_pivot_data.reset(new float[num_centers * dim]);

    for (size_t i = 0; i < num_pq_chunks; i++)
    {
        size_t cur_chunk_size = chunk_offsets[i + 1] - chunk_offsets[i];

        if (cur_chunk_size == 0)
            continue;
        std::unique_ptr<float[]> cur_pivot_data = std::make_unique<float[]>(num_centers * cur_chunk_size);
        std::unique_ptr<float[]> cur_data = std::make_unique<float[]>(num_train * cur_chunk_size);
        std::unique_ptr<uint32_t[]> closest_center = std::make_unique<uint32_t[]>(num_train);

        diskann::cout << "Processing chunk " << i << " with dimensions [" << chunk_offsets[i] << ", "
                      << chunk_offsets[i + 1] << ")" << std::endl;

#pragma omp parallel for schedule(static, 65536)
        for (int64_t j = 0; j < (int64_t)num_train; j++)
        {
            std::memcpy(cur_data.get() + j * cur_chunk_size, train_data.get() + j * dim + chunk_offsets[i],
                        cur_chunk_size * sizeof(float));
        }

        kmeans::kmeanspp_selecting_pivots(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(), num_centers);

        kmeans::run_lloyds(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(), num_centers,
                           max_k_means_reps, NULL, closest_center.get());

        for (uint64_t j = 0; j < num_centers; j++)
        {
            std::memcpy(full_pivot_data.get() + j * dim + chunk_offsets[i], cur_pivot_data.get() + j * cur_chunk_size,
                        cur_chunk_size * sizeof(float));
        }
    }
    std::vector<size_t> cumul_bytes(4, 0);
    cumul_bytes[0] = cumul_bytes.size() * sizeof(size_t) + + 2 * sizeof(uint32_t);
    cumul_bytes[1] = cumul_bytes[0] + (size_t)num_centers * dim * sizeof(float) + + 2 * sizeof(uint32_t);
    cumul_bytes[2] = cumul_bytes[1] + (size_t)dim * 1 * sizeof(float) + + 2 * sizeof(uint32_t);
    cumul_bytes[3] = cumul_bytes[2] + (size_t)chunk_offsets.size() * 1 * sizeof(uint32_t) + + 2 * sizeof(uint32_t);
    diskann::save_bin<size_t>(pq_pivots_stream, cumul_bytes.data(), cumul_bytes.size(), 1, 0);
    diskann::save_bin<float>(pq_pivots_stream, full_pivot_data.get(),
                                                               (size_t)num_centers, dim, cumul_bytes[0]);
    diskann::save_bin<float>(pq_pivots_stream, centroid.get(), (size_t)dim, 1, cumul_bytes[1]);
    diskann::save_bin<uint32_t>(pq_pivots_stream, chunk_offsets.data(),
                                                                  chunk_offsets.size(), 1, cumul_bytes[2]);
    pq_pivots_stream.seekp(0, pq_pivots_stream.beg);
    return 0;
}

int generate_opq_pivots(const float *passed_train_data, size_t num_train, uint32_t dim, uint32_t num_centers,
                        uint32_t num_pq_chunks, std::string opq_pivots_path, bool make_zero_mean)
{
    if (num_pq_chunks > dim)
    {
        diskann::cout << " Error: number of chunks more than dimension" << std::endl;
        return -1;
    }

    std::unique_ptr<float[]> train_data = std::make_unique<float[]>(num_train * dim);
    std::memcpy(train_data.get(), passed_train_data, num_train * dim * sizeof(float));

    std::unique_ptr<float[]> rotated_train_data = std::make_unique<float[]>(num_train * dim);
    std::unique_ptr<float[]> rotated_and_quantized_train_data = std::make_unique<float[]>(num_train * dim);

    std::unique_ptr<float[]> full_pivot_data;

    // rotation matrix for OPQ
    std::unique_ptr<float[]> rotmat_tr;

    // matrices for SVD
    std::unique_ptr<float[]> Umat = std::make_unique<float[]>(dim * dim);
    std::unique_ptr<float[]> Vmat_T = std::make_unique<float[]>(dim * dim);
    std::unique_ptr<float[]> singular_values = std::make_unique<float[]>(dim);
    std::unique_ptr<float[]> correlation_matrix = std::make_unique<float[]>(dim * dim);

    // Calculate centroid and center the training data
    std::unique_ptr<float[]> centroid = std::make_unique<float[]>(dim);
    for (uint64_t d = 0; d < dim; d++)
    {
        centroid[d] = 0;
    }
    if (make_zero_mean)
    { // If we use L2 distance, there is an option to
      // translate all vectors to make them centered and
      // then compute PQ. This needs to be set to false
      // when using PQ for MIPS as such translations dont
      // preserve inner products.
        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                centroid[d] += train_data[p * dim + d];
            }
            centroid[d] /= num_train;
        }
        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                train_data[p * dim + d] -= centroid[d];
            }
        }
    }

    std::vector<uint32_t> chunk_offsets;

    size_t low_val = (size_t)std::floor((double)dim / (double)num_pq_chunks);
    size_t high_val = (size_t)std::ceil((double)dim / (double)num_pq_chunks);
    size_t max_num_high = dim - (low_val * num_pq_chunks);
    size_t cur_num_high = 0;
    size_t cur_bin_threshold = high_val;

    std::vector<std::vector<uint32_t>> bin_to_dims(num_pq_chunks);
    tsl::robin_map<uint32_t, uint32_t> dim_to_bin;
    std::vector<float> bin_loads(num_pq_chunks, 0);

    // Process dimensions not inserted by previous loop
    for (uint32_t d = 0; d < dim; d++)
    {
        if (dim_to_bin.find(d) != dim_to_bin.end())
            continue;
        auto cur_best = num_pq_chunks + 1;
        float cur_best_load = std::numeric_limits<float>::max();
        for (uint32_t b = 0; b < num_pq_chunks; b++)
        {
            if (bin_loads[b] < cur_best_load && bin_to_dims[b].size() < cur_bin_threshold)
            {
                cur_best = b;
                cur_best_load = bin_loads[b];
            }
        }
        bin_to_dims[cur_best].push_back(d);
        if (bin_to_dims[cur_best].size() == high_val)
        {
            cur_num_high++;
            if (cur_num_high == max_num_high)
                cur_bin_threshold = low_val;
        }
    }

    chunk_offsets.clear();
    chunk_offsets.push_back(0);

    for (uint32_t b = 0; b < num_pq_chunks; b++)
    {
        if (b > 0)
            chunk_offsets.push_back(chunk_offsets[b - 1] + (uint32_t)bin_to_dims[b - 1].size());
    }
    chunk_offsets.push_back(dim);

    full_pivot_data.reset(new float[num_centers * dim]);
    rotmat_tr.reset(new float[dim * dim]);

    std::memset(rotmat_tr.get(), 0, dim * dim * sizeof(float));
    for (uint32_t d1 = 0; d1 < dim; d1++)
        *(rotmat_tr.get() + d1 * dim + d1) = 1;

    for (uint32_t rnd = 0; rnd < MAX_OPQ_ITERS; rnd++)
    {
        // rotate the training data using the current rotation matrix
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (MKL_INT)num_train, (MKL_INT)dim, (MKL_INT)dim, 1.0f,
                    train_data.get(), (MKL_INT)dim, rotmat_tr.get(), (MKL_INT)dim, 0.0f, rotated_train_data.get(),
                    (MKL_INT)dim);

        // compute the PQ pivots on the rotated space
        for (size_t i = 0; i < num_pq_chunks; i++)
        {
            size_t cur_chunk_size = chunk_offsets[i + 1] - chunk_offsets[i];

            if (cur_chunk_size == 0)
                continue;
            std::unique_ptr<float[]> cur_pivot_data = std::make_unique<float[]>(num_centers * cur_chunk_size);
            std::unique_ptr<float[]> cur_data = std::make_unique<float[]>(num_train * cur_chunk_size);
            std::unique_ptr<uint32_t[]> closest_center = std::make_unique<uint32_t[]>(num_train);

            diskann::cout << "Processing chunk " << i << " with dimensions [" << chunk_offsets[i] << ", "
                          << chunk_offsets[i + 1] << ")" << std::endl;

#pragma omp parallel for schedule(static, 65536)
            for (int64_t j = 0; j < (int64_t)num_train; j++)
            {
                std::memcpy(cur_data.get() + j * cur_chunk_size, rotated_train_data.get() + j * dim + chunk_offsets[i],
                            cur_chunk_size * sizeof(float));
            }

            if (rnd == 0)
            {
                kmeans::kmeanspp_selecting_pivots(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(),
                                                  num_centers);
            }
            else
            {
                for (uint64_t j = 0; j < num_centers; j++)
                {
                    std::memcpy(cur_pivot_data.get() + j * cur_chunk_size,
                                full_pivot_data.get() + j * dim + chunk_offsets[i], cur_chunk_size * sizeof(float));
                }
            }

            uint32_t num_lloyds_iters = 8;
            kmeans::run_lloyds(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(), num_centers,
                               num_lloyds_iters, NULL, closest_center.get());

            for (uint64_t j = 0; j < num_centers; j++)
            {
                std::memcpy(full_pivot_data.get() + j * dim + chunk_offsets[i],
                            cur_pivot_data.get() + j * cur_chunk_size, cur_chunk_size * sizeof(float));
            }

            for (size_t j = 0; j < num_train; j++)
            {
                std::memcpy(rotated_and_quantized_train_data.get() + j * dim + chunk_offsets[i],
                            cur_pivot_data.get() + (size_t)closest_center[j] * cur_chunk_size,
                            cur_chunk_size * sizeof(float));
            }
        }

        // compute the correlation matrix between the original data and the
        // quantized data to compute the new rotation
        cblas_sgemm(CblasRowMajor, CblasTrans, CblasNoTrans, (MKL_INT)dim, (MKL_INT)dim, (MKL_INT)num_train, 1.0f,
                    train_data.get(), (MKL_INT)dim, rotated_and_quantized_train_data.get(), (MKL_INT)dim, 0.0f,
                    correlation_matrix.get(), (MKL_INT)dim);

        // compute the SVD of the correlation matrix to help determine the new
        // rotation matrix
        uint32_t errcode = (uint32_t)LAPACKE_sgesdd(LAPACK_ROW_MAJOR, 'A', (MKL_INT)dim, (MKL_INT)dim,
                                                    correlation_matrix.get(), (MKL_INT)dim, singular_values.get(),
                                                    Umat.get(), (MKL_INT)dim, Vmat_T.get(), (MKL_INT)dim);

        if (errcode > 0)
        {
            std::cout << "SVD failed to converge." << std::endl;
            exit(-1);
        }

        // compute the new rotation matrix from the singular vectors as R^T = U
        // V^T
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (MKL_INT)dim, (MKL_INT)dim, (MKL_INT)dim, 1.0f,
                    Umat.get(), (MKL_INT)dim, Vmat_T.get(), (MKL_INT)dim, 0.0f, rotmat_tr.get(), (MKL_INT)dim);
    }

    std::vector<size_t> cumul_bytes(4, 0);
    cumul_bytes[0] = METADATA_SIZE;
    cumul_bytes[1] = cumul_bytes[0] + diskann::save_bin<float>(opq_pivots_path.c_str(), full_pivot_data.get(),
                                                               (size_t)num_centers, dim, cumul_bytes[0]);
    cumul_bytes[2] = cumul_bytes[1] +
                     diskann::save_bin<float>(opq_pivots_path.c_str(), centroid.get(), (size_t)dim, 1, cumul_bytes[1]);
    cumul_bytes[3] = cumul_bytes[2] + diskann::save_bin<uint32_t>(opq_pivots_path.c_str(), chunk_offsets.data(),
                                                                  chunk_offsets.size(), 1, cumul_bytes[2]);
    diskann::save_bin<size_t>(opq_pivots_path.c_str(), cumul_bytes.data(), cumul_bytes.size(), 1, 0);

    diskann::cout << "Saved opq pivot data to " << opq_pivots_path << " of size " << cumul_bytes[cumul_bytes.size() - 1]
                  << "B." << std::endl;

    std::string rotmat_path = opq_pivots_path + "_rotation_matrix.bin";
    diskann::save_bin<float>(rotmat_path.c_str(), rotmat_tr.get(), dim, dim);

    return 0;
}



int generate_opq_pivots(const float *passed_train_data, size_t num_train, uint32_t dim, uint32_t num_centers,
                        uint32_t num_pq_chunks, std::stringstream opq_pivots_stream, bool make_zero_mean)
{
    if (num_pq_chunks > dim)
    {
        diskann::cout << " Error: number of chunks more than dimension" << std::endl;
        return -1;
    }

    std::unique_ptr<float[]> train_data = std::make_unique<float[]>(num_train * dim);
    std::memcpy(train_data.get(), passed_train_data, num_train * dim * sizeof(float));

    std::unique_ptr<float[]> rotated_train_data = std::make_unique<float[]>(num_train * dim);
    std::unique_ptr<float[]> rotated_and_quantized_train_data = std::make_unique<float[]>(num_train * dim);

    std::unique_ptr<float[]> full_pivot_data;

    // rotation matrix for OPQ
    std::unique_ptr<float[]> rotmat_tr;

    // matrices for SVD
    std::unique_ptr<float[]> Umat = std::make_unique<float[]>(dim * dim);
    std::unique_ptr<float[]> Vmat_T = std::make_unique<float[]>(dim * dim);
    std::unique_ptr<float[]> singular_values = std::make_unique<float[]>(dim);
    std::unique_ptr<float[]> correlation_matrix = std::make_unique<float[]>(dim * dim);

    // Calculate centroid and center the training data
    std::unique_ptr<float[]> centroid = std::make_unique<float[]>(dim);
    for (uint64_t d = 0; d < dim; d++)
    {
        centroid[d] = 0;
    }
    if (make_zero_mean)
    { // If we use L2 distance, there is an option to
      // translate all vectors to make them centered and
      // then compute PQ. This needs to be set to false
      // when using PQ for MIPS as such translations dont
      // preserve inner products.
        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                centroid[d] += train_data[p * dim + d];
            }
            centroid[d] /= num_train;
        }
        for (uint64_t d = 0; d < dim; d++)
        {
            for (uint64_t p = 0; p < num_train; p++)
            {
                train_data[p * dim + d] -= centroid[d];
            }
        }
    }

    std::vector<uint32_t> chunk_offsets;

    size_t low_val = (size_t)std::floor((double)dim / (double)num_pq_chunks);
    size_t high_val = (size_t)std::ceil((double)dim / (double)num_pq_chunks);
    size_t max_num_high = dim - (low_val * num_pq_chunks);
    size_t cur_num_high = 0;
    size_t cur_bin_threshold = high_val;

    std::vector<std::vector<uint32_t>> bin_to_dims(num_pq_chunks);
    tsl::robin_map<uint32_t, uint32_t> dim_to_bin;
    std::vector<float> bin_loads(num_pq_chunks, 0);

    // Process dimensions not inserted by previous loop
    for (uint32_t d = 0; d < dim; d++)
    {
        if (dim_to_bin.find(d) != dim_to_bin.end())
            continue;
        auto cur_best = num_pq_chunks + 1;
        float cur_best_load = std::numeric_limits<float>::max();
        for (uint32_t b = 0; b < num_pq_chunks; b++)
        {
            if (bin_loads[b] < cur_best_load && bin_to_dims[b].size() < cur_bin_threshold)
            {
                cur_best = b;
                cur_best_load = bin_loads[b];
            }
        }
        bin_to_dims[cur_best].push_back(d);
        if (bin_to_dims[cur_best].size() == high_val)
        {
            cur_num_high++;
            if (cur_num_high == max_num_high)
                cur_bin_threshold = low_val;
        }
    }

    chunk_offsets.clear();
    chunk_offsets.push_back(0);

    for (uint32_t b = 0; b < num_pq_chunks; b++)
    {
        if (b > 0)
            chunk_offsets.push_back(chunk_offsets[b - 1] + (uint32_t)bin_to_dims[b - 1].size());
    }
    chunk_offsets.push_back(dim);

    full_pivot_data.reset(new float[num_centers * dim]);
    rotmat_tr.reset(new float[dim * dim]);

    std::memset(rotmat_tr.get(), 0, dim * dim * sizeof(float));
    for (uint32_t d1 = 0; d1 < dim; d1++)
        *(rotmat_tr.get() + d1 * dim + d1) = 1;

    for (uint32_t rnd = 0; rnd < MAX_OPQ_ITERS; rnd++)
    {
        // rotate the training data using the current rotation matrix
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (MKL_INT)num_train, (MKL_INT)dim, (MKL_INT)dim, 1.0f,
                    train_data.get(), (MKL_INT)dim, rotmat_tr.get(), (MKL_INT)dim, 0.0f, rotated_train_data.get(),
                    (MKL_INT)dim);

        // compute the PQ pivots on the rotated space
        for (size_t i = 0; i < num_pq_chunks; i++)
        {
            size_t cur_chunk_size = chunk_offsets[i + 1] - chunk_offsets[i];

            if (cur_chunk_size == 0)
                continue;
            std::unique_ptr<float[]> cur_pivot_data = std::make_unique<float[]>(num_centers * cur_chunk_size);
            std::unique_ptr<float[]> cur_data = std::make_unique<float[]>(num_train * cur_chunk_size);
            std::unique_ptr<uint32_t[]> closest_center = std::make_unique<uint32_t[]>(num_train);

            diskann::cout << "Processing chunk " << i << " with dimensions [" << chunk_offsets[i] << ", "
                          << chunk_offsets[i + 1] << ")" << std::endl;

#pragma omp parallel for schedule(static, 65536)
            for (int64_t j = 0; j < (int64_t)num_train; j++)
            {
                std::memcpy(cur_data.get() + j * cur_chunk_size, rotated_train_data.get() + j * dim + chunk_offsets[i],
                            cur_chunk_size * sizeof(float));
            }

            if (rnd == 0)
            {
                kmeans::kmeanspp_selecting_pivots(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(),
                                                  num_centers);
            }
            else
            {
                for (uint64_t j = 0; j < num_centers; j++)
                {
                    std::memcpy(cur_pivot_data.get() + j * cur_chunk_size,
                                full_pivot_data.get() + j * dim + chunk_offsets[i], cur_chunk_size * sizeof(float));
                }
            }

            uint32_t num_lloyds_iters = 8;
            kmeans::run_lloyds(cur_data.get(), num_train, cur_chunk_size, cur_pivot_data.get(), num_centers,
                               num_lloyds_iters, NULL, closest_center.get());

            for (uint64_t j = 0; j < num_centers; j++)
            {
                std::memcpy(full_pivot_data.get() + j * dim + chunk_offsets[i],
                            cur_pivot_data.get() + j * cur_chunk_size, cur_chunk_size * sizeof(float));
            }

            for (size_t j = 0; j < num_train; j++)
            {
                std::memcpy(rotated_and_quantized_train_data.get() + j * dim + chunk_offsets[i],
                            cur_pivot_data.get() + (size_t)closest_center[j] * cur_chunk_size,
                            cur_chunk_size * sizeof(float));
            }
        }

        // compute the correlation matrix between the original data and the
        // quantized data to compute the new rotation
        cblas_sgemm(CblasRowMajor, CblasTrans, CblasNoTrans, (MKL_INT)dim, (MKL_INT)dim, (MKL_INT)num_train, 1.0f,
                    train_data.get(), (MKL_INT)dim, rotated_and_quantized_train_data.get(), (MKL_INT)dim, 0.0f,
                    correlation_matrix.get(), (MKL_INT)dim);

        // compute the SVD of the correlation matrix to help determine the new
        // rotation matrix
        uint32_t errcode = (uint32_t)LAPACKE_sgesdd(LAPACK_ROW_MAJOR, 'A', (MKL_INT)dim, (MKL_INT)dim,
                                                    correlation_matrix.get(), (MKL_INT)dim, singular_values.get(),
                                                    Umat.get(), (MKL_INT)dim, Vmat_T.get(), (MKL_INT)dim);

        if (errcode > 0)
        {
            std::cout << "SVD failed to converge." << std::endl;
            exit(-1);
        }

        // compute the new rotation matrix from the singular vectors as R^T = U
        // V^T
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (MKL_INT)dim, (MKL_INT)dim, (MKL_INT)dim, 1.0f,
                    Umat.get(), (MKL_INT)dim, Vmat_T.get(), (MKL_INT)dim, 0.0f, rotmat_tr.get(), (MKL_INT)dim);
    }

    std::vector<size_t> cumul_bytes(4, 0);
    cumul_bytes[0] = METADATA_SIZE;
    cumul_bytes[1] = cumul_bytes[0] + diskann::save_bin<float>(opq_pivots_stream, full_pivot_data.get(),
                                                               (size_t)num_centers, dim, cumul_bytes[0]);
    cumul_bytes[2] = cumul_bytes[1] +
                     diskann::save_bin<float>(opq_pivots_stream, centroid.get(), (size_t)dim, 1, cumul_bytes[1]);
    cumul_bytes[3] = cumul_bytes[2] + diskann::save_bin<uint32_t>(opq_pivots_stream, chunk_offsets.data(),
                                                                  chunk_offsets.size(), 1, cumul_bytes[2]);
    diskann::save_bin<size_t>(opq_pivots_stream, cumul_bytes.data(), cumul_bytes.size(), 1, 0);

    // diskann::cout << "Saved opq pivot data to " << opq_pivots_path << " of size " << cumul_bytes[cumul_bytes.size() - 1]
    //               << "B." << std::endl;

    // std::string rotmat_path = opq_pivots_path + "_rotation_matrix.bin";
    // diskann::save_bin<float>(rotmat_path.c_str(), rotmat_tr.get(), dim, dim);

    return 0;
}

// generate_pq_data_from_pivots_simplified is a simplified version of generate_pq_data_from_pivots.
// Input is provided in the in-memory buffers data and pivot_data.
// Output is stored in the in-memory buffer pq.
// Simplification is based on the following assumptions:
//   supporting only float data type
//   dim % num_pq_chunks == 0, which results in a fixed chunk_size
//   num_centers == 256 by default
//   make_zero_mean is false by default.
// These assumptions allow to make the function much simpler and avoid using
// array of chunk_offsets and centroids.
// The compiler pragma for multi-threading support is removed from this implementation
// for the purpose of integration into systems that strictly control resource allocation.
int generate_pq_data_from_pivots_simplified(const float *data, const size_t num, const float *pivot_data,
                                            const size_t pivots_num, const size_t dim, const size_t num_pq_chunks,
                                            std::vector<uint8_t> &pq)
{
    if (num_pq_chunks == 0 || num_pq_chunks > dim || dim % num_pq_chunks != 0)
    {
        return -1;
    }

    const size_t num_centers = 256;
    const size_t chunk_size = dim / num_pq_chunks;

    if (pivots_num != num_centers * dim)
    {
        return -1;
    }

    pq.resize(num * num_pq_chunks);

    std::vector<float> cur_pivot_vector(num_centers * chunk_size);
    std::vector<float> cur_data_vector(num * chunk_size);
    std::vector<uint32_t> closest_center_vector(num);

    float *cur_pivot_data = &cur_pivot_vector[0];
    float *cur_data = &cur_data_vector[0];
    uint32_t *closest_center = &closest_center_vector[0];

    for (size_t i = 0; i < num_pq_chunks; i++)
    {
        const size_t chunk_offset = chunk_size * i;

        for (int j = 0; j < num_centers; j++)
        {
            std::memcpy(cur_pivot_data + j * chunk_size, pivot_data + j * dim + chunk_offset,
                        chunk_size * sizeof(float));
        }

        for (int j = 0; j < num; j++)
        {
            for (size_t k = 0; k < chunk_size; k++)
            {
                cur_data[j * chunk_size + k] = data[j * dim + chunk_offset + k];
            }
        }

        math_utils::compute_closest_centers(cur_data, num, chunk_size, cur_pivot_data, num_centers, 1, closest_center);

        for (int j = 0; j < num; j++)
        {
            assert(closest_center[j] < num_centers);
            pq[j * num_pq_chunks + i] = closest_center[j];
        }
    }

    return 0;
}

// streams the base file (data_file), and computes the closest centers in each
// chunk to generate the compressed data_file and stores it in
// pq_compressed_vectors_path.
// If the numbber of centers is < 256, it stores as byte vector, else as
// 4-byte vector in binary format.
template <typename T>
int generate_pq_data_from_pivots(const std::string &data_file, uint32_t num_centers, uint32_t num_pq_chunks,
                                 const std::string &pq_pivots_path, const std::string &pq_compressed_vectors_path,
                                 bool use_opq)
{
    size_t read_blk_size = 64 * 1024 * 1024;
    cached_ifstream base_reader(data_file, read_blk_size);
    uint32_t npts32;
    uint32_t basedim32;
    base_reader.read((char *)&npts32, sizeof(uint32_t));
    base_reader.read((char *)&basedim32, sizeof(uint32_t));
    size_t num_points = npts32;
    size_t dim = basedim32;

    std::unique_ptr<float[]> full_pivot_data;
    std::unique_ptr<float[]> rotmat_tr;
    std::unique_ptr<float[]> centroid;
    std::unique_ptr<uint32_t[]> chunk_offsets;

    std::string inflated_pq_file = pq_compressed_vectors_path + "_inflated.bin";

    if (!file_exists(pq_pivots_path))
    {
        std::cout << "ERROR: PQ k-means pivot file not found" << std::endl;
        throw diskann::ANNException("PQ k-means pivot file not found", -1);
    }
    else
    {
        size_t nr, nc;
        std::unique_ptr<size_t[]> file_offset_data;

        diskann::load_bin<size_t>(pq_pivots_path.c_str(), file_offset_data, nr, nc, 0);

        if (nr != 4)
        {
            diskann::cout << "Error reading pq_pivots file " << pq_pivots_path
                          << ". Offsets dont contain correct metadata, # offsets = " << nr << ", but expecting 4.";
            throw diskann::ANNException("Error reading pq_pivots file at offsets data.", -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
        }

        diskann::load_bin<float>(pq_pivots_path.c_str(), full_pivot_data, nr, nc, file_offset_data[0]);

        if ((nr != num_centers) || (nc != dim))
        {
            diskann::cout << "Error reading pq_pivots file " << pq_pivots_path << ". file_num_centers  = " << nr
                          << ", file_dim = " << nc << " but expecting " << num_centers << " centers in " << dim
                          << " dimensions.";
            throw diskann::ANNException("Error reading pq_pivots file at pivots data.", -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
        }

        diskann::load_bin<float>(pq_pivots_path.c_str(), centroid, nr, nc, file_offset_data[1]);

        if ((nr != dim) || (nc != 1))
        {
            diskann::cout << "Error reading pq_pivots file " << pq_pivots_path << ". file_dim  = " << nr
                          << ", file_cols = " << nc << " but expecting " << dim << " entries in 1 dimension.";
            throw diskann::ANNException("Error reading pq_pivots file at centroid data.", -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
        }

        diskann::load_bin<uint32_t>(pq_pivots_path.c_str(), chunk_offsets, nr, nc, file_offset_data[2]);

        if (nr != (uint64_t)num_pq_chunks + 1 || nc != 1)
        {
            diskann::cout << "Error reading pq_pivots file at chunk offsets; file has nr=" << nr << ",nc=" << nc
                          << ", expecting nr=" << num_pq_chunks + 1 << ", nc=1." << std::endl;
            throw diskann::ANNException("Error reading pq_pivots file at chunk offsets.", -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
        }

        if (use_opq)
        {
            std::string rotmat_path = pq_pivots_path + "_rotation_matrix.bin";
            diskann::load_bin<float>(rotmat_path.c_str(), rotmat_tr, nr, nc);
            if (nr != (uint64_t)dim || nc != dim)
            {
                diskann::cout << "Error reading rotation matrix file." << std::endl;
                throw diskann::ANNException("Error reading rotation matrix file.", -1, __FUNCSIG__, __FILE__, __LINE__);
            }
        }

        diskann::cout << "Loaded PQ pivot information" << std::endl;
    }

    std::ofstream compressed_file_writer(pq_compressed_vectors_path, std::ios::binary);
    uint32_t num_pq_chunks_u32 = num_pq_chunks;

    compressed_file_writer.write((char *)&num_points, sizeof(uint32_t));
    compressed_file_writer.write((char *)&num_pq_chunks_u32, sizeof(uint32_t));

    size_t block_size = num_points <= BLOCK_SIZE ? num_points : BLOCK_SIZE;

#ifdef SAVE_INFLATED_PQ
    std::ofstream inflated_file_writer(inflated_pq_file, std::ios::binary);
    inflated_file_writer.write((char *)&num_points, sizeof(uint32_t));
    inflated_file_writer.write((char *)&basedim32, sizeof(uint32_t));

    std::unique_ptr<float[]> block_inflated_base = std::make_unique<float[]>(block_size * dim);
    std::memset(block_inflated_base.get(), 0, block_size * dim * sizeof(float));
#endif

    std::unique_ptr<uint32_t[]> block_compressed_base =
        std::make_unique<uint32_t[]>(block_size * (size_t)num_pq_chunks);
    std::memset(block_compressed_base.get(), 0, block_size * (size_t)num_pq_chunks * sizeof(uint32_t));

    std::unique_ptr<T[]> block_data_T = std::make_unique<T[]>(block_size * dim);
    std::unique_ptr<float[]> block_data_float = std::make_unique<float[]>(block_size * dim);
    std::unique_ptr<float[]> block_data_tmp = std::make_unique<float[]>(block_size * dim);

    size_t num_blocks = DIV_ROUND_UP(num_points, block_size);

    for (size_t block = 0; block < num_blocks; block++)
    {
        size_t start_id = block * block_size;
        size_t end_id = (std::min)((block + 1) * block_size, num_points);
        size_t cur_blk_size = end_id - start_id;

        base_reader.read((char *)(block_data_T.get()), sizeof(T) * (cur_blk_size * dim));
        diskann::convert_types<T, float>(block_data_T.get(), block_data_tmp.get(), cur_blk_size, dim);

        diskann::cout << "Processing points  [" << start_id << ", " << end_id << ").." << std::flush;

        for (size_t p = 0; p < cur_blk_size; p++)
        {
            for (uint64_t d = 0; d < dim; d++)
            {
                block_data_tmp[p * dim + d] -= centroid[d];
            }
        }

        for (size_t p = 0; p < cur_blk_size; p++)
        {
            for (uint64_t d = 0; d < dim; d++)
            {
                block_data_float[p * dim + d] = block_data_tmp[p * dim + d];
            }
        }

        if (use_opq)
        {
            // rotate the current block with the trained rotation matrix before
            // PQ
            cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (MKL_INT)cur_blk_size, (MKL_INT)dim, (MKL_INT)dim,
                        1.0f, block_data_float.get(), (MKL_INT)dim, rotmat_tr.get(), (MKL_INT)dim, 0.0f,
                        block_data_tmp.get(), (MKL_INT)dim);
            std::memcpy(block_data_float.get(), block_data_tmp.get(), cur_blk_size * dim * sizeof(float));
        }

        for (size_t i = 0; i < num_pq_chunks; i++)
        {
            size_t cur_chunk_size = chunk_offsets[i + 1] - chunk_offsets[i];
            if (cur_chunk_size == 0)
                continue;

            std::unique_ptr<float[]> cur_pivot_data = std::make_unique<float[]>(num_centers * cur_chunk_size);
            std::unique_ptr<float[]> cur_data = std::make_unique<float[]>(cur_blk_size * cur_chunk_size);
            std::unique_ptr<uint32_t[]> closest_center = std::make_unique<uint32_t[]>(cur_blk_size);

#pragma omp parallel for schedule(static, 8192)
            for (int64_t j = 0; j < (int64_t)cur_blk_size; j++)
            {
                for (size_t k = 0; k < cur_chunk_size; k++)
                    cur_data[j * cur_chunk_size + k] = block_data_float[j * dim + chunk_offsets[i] + k];
            }

#pragma omp parallel for schedule(static, 1)
            for (int64_t j = 0; j < (int64_t)num_centers; j++)
            {
                std::memcpy(cur_pivot_data.get() + j * cur_chunk_size,
                            full_pivot_data.get() + j * dim + chunk_offsets[i], cur_chunk_size * sizeof(float));
            }

            math_utils::compute_closest_centers(cur_data.get(), cur_blk_size, cur_chunk_size, cur_pivot_data.get(),
                                                num_centers, 1, closest_center.get());

#pragma omp parallel for schedule(static, 8192)
            for (int64_t j = 0; j < (int64_t)cur_blk_size; j++)
            {
                block_compressed_base[j * num_pq_chunks + i] = closest_center[j];
#ifdef SAVE_INFLATED_PQ
                for (size_t k = 0; k < cur_chunk_size; k++)
                    block_inflated_base[j * dim + chunk_offsets[i] + k] =
                        cur_pivot_data[closest_center[j] * cur_chunk_size + k] + centroid[chunk_offsets[i] + k];
#endif
            }
        }

        if (num_centers > 256)
        {
            compressed_file_writer.write((char *)(block_compressed_base.get()),
                                         cur_blk_size * num_pq_chunks * sizeof(uint32_t));
        }
        else
        {
            std::unique_ptr<uint8_t[]> pVec = std::make_unique<uint8_t[]>(cur_blk_size * num_pq_chunks);
            diskann::convert_types<uint32_t, uint8_t>(block_compressed_base.get(), pVec.get(), cur_blk_size,
                                                      num_pq_chunks);
            compressed_file_writer.write((char *)(pVec.get()), cur_blk_size * num_pq_chunks * sizeof(uint8_t));
        }
#ifdef SAVE_INFLATED_PQ
        inflated_file_writer.write((char *)(block_inflated_base.get()), cur_blk_size * dim * sizeof(float));
#endif
        diskann::cout << ".done." << std::endl;
    }
// Gopal. Splitting diskann_dll into separate DLLs for search and build.
// This code should only be available in the "build" DLL.
#if defined(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS) && defined(DISKANN_BUILD)
    MallocExtension::instance()->ReleaseFreeMemory();
#endif
    compressed_file_writer.close();
#ifdef SAVE_INFLATED_PQ
    inflated_file_writer.close();
#endif
    return 0;
}


template <typename T>
int generate_pq_data_from_pivots(std::stringstream &data_stream, unsigned num_centers, unsigned num_pq_chunks,
                                 std::stringstream &pq_pivots_stream, std::stringstream &pq_compressed_stream,
                                 bool use_opq)
{
    uint32_t npts32;
    uint32_t basedim32;
    data_stream.seekg(0, data_stream.beg);
    data_stream.read((char *)&npts32, sizeof(uint32_t));
    data_stream.read((char *)&basedim32, sizeof(uint32_t));
    size_t num_points = npts32;
    size_t dim = basedim32;

    std::unique_ptr<float[]> full_pivot_data;
    std::unique_ptr<float[]> rotmat_tr;
    std::unique_ptr<float[]> centroid;
    std::unique_ptr<uint32_t[]> chunk_offsets;
        
    size_t nr, nc;
    std::unique_ptr<size_t[]> file_offset_data;

    //SampleStringStreamReaderWrapperSPtr pq_pivots(new SampleStringStreamReaderWrapper(pq_pivots_stream));
    diskann::load_bin<size_t>(pq_pivots_stream, file_offset_data, nr, nc, 0);


    if (nr != 4)
    {
        diskann::cout << "Error reading pq_pivots file "
                        << ". Offsets dont contain correct metadata, # offsets = " << nr << ", but expecting 4.";
        throw diskann::ANNException("Error reading pq_pivots file at offsets data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    diskann::load_bin<float>(pq_pivots_stream, full_pivot_data, nr, nc, file_offset_data[0]);

    if ((nr != num_centers) || (nc != dim))
    {
        diskann::cout << "Error reading pq_pivots. file_num_centers  = " << nr
                        << ", file_dim = " << nc << " but expecting " << num_centers << " centers in " << dim
                        << " dimensions.";
        throw diskann::ANNException("Error reading pq_pivots file at pivots data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    diskann::load_bin<float>(pq_pivots_stream, centroid, nr, nc, file_offset_data[1]);

    if ((nr != dim) || (nc != 1))
    {
        diskann::cout << "Error reading pq_pivots file file_dim  = " << nr
                        << ", file_cols = " << nc << " but expecting " << dim << " entries in 1 dimension.";
        throw diskann::ANNException("Error reading pq_pivots file at centroid data.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    diskann::load_bin<uint32_t>(pq_pivots_stream, chunk_offsets, nr, nc, file_offset_data[2]);

    if (nr != (uint64_t)num_pq_chunks + 1 || nc != 1)
    {
        diskann::cout << "Error reading pq_pivots file at chunk offsets; file has nr=" << nr << ",nc=" << nc
                        << ", expecting nr=" << num_pq_chunks + 1 << ", nc=1." << std::endl;
        throw diskann::ANNException("Error reading pq_pivots file at chunk offsets.", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    diskann::cout << "Loaded PQ pivot information" << std::endl;


    uint32_t num_pq_chunks_u32 = num_pq_chunks;

    pq_compressed_stream.write((char *)&num_points, sizeof(uint32_t));
    pq_compressed_stream.write((char *)&num_pq_chunks_u32, sizeof(uint32_t));

    size_t block_size = num_points <= BLOCK_SIZE ? num_points : BLOCK_SIZE;

    std::unique_ptr<uint32_t[]> block_compressed_base =
        std::make_unique<uint32_t[]>(block_size * (size_t)num_pq_chunks);
    std::memset(block_compressed_base.get(), 0, block_size * (size_t)num_pq_chunks * sizeof(uint32_t));

    std::unique_ptr<T[]> block_data_T = std::make_unique<T[]>(block_size * dim);
    std::unique_ptr<float[]> block_data_float = std::make_unique<float[]>(block_size * dim);
    std::unique_ptr<float[]> block_data_tmp = std::make_unique<float[]>(block_size * dim);

    size_t num_blocks = DIV_ROUND_UP(num_points, block_size);

    for (size_t block = 0; block < num_blocks; block++)
    {
        size_t start_id = block * block_size;
        size_t end_id = (std::min)((block + 1) * block_size, num_points);
        size_t cur_blk_size = end_id - start_id;

        data_stream.read((char *)(block_data_T.get()), sizeof(T) * (cur_blk_size * dim));
        diskann::convert_types<T, float>(block_data_T.get(), block_data_tmp.get(), cur_blk_size, dim);

        diskann::cout << "Processing points  [" << start_id << ", " << end_id << ").." << std::flush;

        for (size_t p = 0; p < cur_blk_size; p++)
        {
            for (uint64_t d = 0; d < dim; d++)
            {
                block_data_tmp[p * dim + d] -= centroid[d];
            }
        }

        for (size_t p = 0; p < cur_blk_size; p++)
        {
            for (uint64_t d = 0; d < dim; d++)
            {
                block_data_float[p * dim + d] = block_data_tmp[p * dim + d];
            }
        }

        if (use_opq)
        {
            // rotate the current block with the trained rotation matrix before
            // PQ
            cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (MKL_INT)cur_blk_size, (MKL_INT)dim, (MKL_INT)dim,
                        1.0f, block_data_float.get(), (MKL_INT)dim, rotmat_tr.get(), (MKL_INT)dim, 0.0f,
                        block_data_tmp.get(), (MKL_INT)dim);
            std::memcpy(block_data_float.get(), block_data_tmp.get(), cur_blk_size * dim * sizeof(float));
        }

        for (size_t i = 0; i < num_pq_chunks; i++)
        {
            size_t cur_chunk_size = chunk_offsets[i + 1] - chunk_offsets[i];
            if (cur_chunk_size == 0)
                continue;

            std::unique_ptr<float[]> cur_pivot_data = std::make_unique<float[]>(num_centers * cur_chunk_size);
            std::unique_ptr<float[]> cur_data = std::make_unique<float[]>(cur_blk_size * cur_chunk_size);
            std::unique_ptr<uint32_t[]> closest_center = std::make_unique<uint32_t[]>(cur_blk_size);

#pragma omp parallel for schedule(static, 8192)
            for (int64_t j = 0; j < (int64_t)cur_blk_size; j++)
            {
                for (size_t k = 0; k < cur_chunk_size; k++)
                    cur_data[j * cur_chunk_size + k] = block_data_float[j * dim + chunk_offsets[i] + k];
            }

#pragma omp parallel for schedule(static, 1)
            for (int64_t j = 0; j < (int64_t)num_centers; j++)
            {
                std::memcpy(cur_pivot_data.get() + j * cur_chunk_size,
                            full_pivot_data.get() + j * dim + chunk_offsets[i], cur_chunk_size * sizeof(float));
            }

            math_utils::compute_closest_centers(cur_data.get(), cur_blk_size, cur_chunk_size, cur_pivot_data.get(),
                                                num_centers, 1, closest_center.get());

//#pragma omp parallel for schedule(static, 8192)
            for (int64_t j = 0; j < (int64_t)cur_blk_size; j++)
            {
                block_compressed_base[j * num_pq_chunks + i] = closest_center[j];
            }
        }

        if (num_centers > 256)
        {
            pq_compressed_stream.write((char *)(block_compressed_base.get()),
                                         cur_blk_size * num_pq_chunks * sizeof(uint32_t));
        }
        else
        {
            std::unique_ptr<uint8_t[]> pVec = std::make_unique<uint8_t[]>(cur_blk_size * num_pq_chunks);
            diskann::convert_types<uint32_t, uint8_t>(block_compressed_base.get(), pVec.get(), cur_blk_size,
                                                      num_pq_chunks);
            pq_compressed_stream.write((char *)(pVec.get()), cur_blk_size * num_pq_chunks * sizeof(uint8_t));
        }
        diskann::cout << ".done." << std::endl;
    }
// Gopal. Splitting diskann_dll into separate DLLs for search and build.
// This code should only be available in the "build" DLL.
#if defined(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS) && defined(DISKANN_BUILD)
    MallocExtension::instance()->ReleaseFreeMemory();
#endif
    return 0;
}


template <typename T>
void generate_disk_quantized_data(const std::string &data_file_to_use, const std::string &disk_pq_pivots_path,
                                  const std::string &disk_pq_compressed_vectors_path, diskann::Metric compareMetric,
                                  const double p_val, size_t &disk_pq_dims)
{
    size_t train_size, train_dim;
    float *train_data;

    // instantiates train_data with random sample updates train_size
    gen_random_slice<T>(data_file_to_use.c_str(), p_val, train_data, train_size, train_dim);
    diskann::cout << "Training data with " << train_size << " samples loaded1." << std::endl;

    if (disk_pq_dims > train_dim)
        disk_pq_dims = train_dim;

    std::cout << "Compressing base for disk-PQ into " << disk_pq_dims << " chunks " << std::endl;
    generate_pq_pivots(train_data, train_size, (uint32_t)train_dim, 256, (uint32_t)disk_pq_dims, NUM_KMEANS_REPS_PQ,
                       disk_pq_pivots_path, false);
    if (compareMetric == diskann::Metric::INNER_PRODUCT)
        generate_pq_data_from_pivots<float>(data_file_to_use, 256, (uint32_t)disk_pq_dims, disk_pq_pivots_path,
                                            disk_pq_compressed_vectors_path);
    else
        generate_pq_data_from_pivots<T>(data_file_to_use, 256, (uint32_t)disk_pq_dims, disk_pq_pivots_path,
                                        disk_pq_compressed_vectors_path);

    delete[] train_data;
}

template <typename T>
void generate_quantized_data(const std::string &data_file_to_use, const std::string &pq_pivots_path,
                             const std::string &pq_compressed_vectors_path, diskann::Metric compareMetric,
                             const double p_val, const size_t num_pq_chunks, const bool use_opq,
                             const std::string &codebook_prefix)
{
    size_t train_size, train_dim;
    float *train_data;
    if (!file_exists(codebook_prefix))
    {
        // instantiates train_data with random sample updates train_size
        gen_random_slice<T>(data_file_to_use.c_str(), p_val, train_data, train_size, train_dim);
        diskann::cout << "Training data with " << train_size << " samples loaded2." << ",p_val:" <<p_val << std::endl;

        bool make_zero_mean = true;
        if (compareMetric == diskann::Metric::INNER_PRODUCT)
            make_zero_mean = false;
        if (use_opq) // we also do not center the data for OPQ
            make_zero_mean = false;

        if (!use_opq)
        {
            generate_pq_pivots(train_data, train_size, (uint32_t)train_dim, NUM_PQ_CENTROIDS, (uint32_t)num_pq_chunks,
                               NUM_KMEANS_REPS_PQ, pq_pivots_path, make_zero_mean);
        }
        else
        {
            generate_opq_pivots(train_data, train_size, (uint32_t)train_dim, NUM_PQ_CENTROIDS, (uint32_t)num_pq_chunks,
                                pq_pivots_path, make_zero_mean);
        }
        delete[] train_data;
    }
    else
    {
        diskann::cout << "Skip Training with predefined pivots in: " << pq_pivots_path << std::endl;
    }
    generate_pq_data_from_pivots<T>(data_file_to_use, NUM_PQ_CENTROIDS, (uint32_t)num_pq_chunks, pq_pivots_path,
                                    pq_compressed_vectors_path, use_opq);
}

template <typename T>
void generate_quantized_data(std::stringstream &data_stream, std::stringstream &pq_pivots_stream,
                             std::stringstream &pq_compressed_stream, diskann::Metric compareMetric,
                             const double p_val, const size_t num_pq_chunks, const bool use_opq,
                             const std::string &codebook_prefix)
{
    size_t train_size, train_dim;
    float *train_data;
    // instantiates train_data with random sample updates train_size
    gen_random_slice<T>(data_stream, p_val, train_data, train_size, train_dim);
    
    diskann::cout << "Training data with " << train_size << " samples loaded2." << ",p_val:" <<p_val << std::endl;

    bool make_zero_mean = true;
    if (compareMetric == diskann::Metric::INNER_PRODUCT)
        make_zero_mean = false;
    if (use_opq) // we also do not center the data for OPQ
        make_zero_mean = false;

    // if (!use_opq)
    // {
        generate_pq_pivots(train_data, train_size, (uint32_t)train_dim, (uint32_t)NUM_PQ_CENTROIDS, (uint32_t)num_pq_chunks,
                            (uint32_t)NUM_KMEANS_REPS_PQ, pq_pivots_stream, make_zero_mean);
    // }
    // else
    // {
    //     generate_opq_pivots(train_data, train_size, (uint32_t)train_dim, (uint32_t)NUM_PQ_CENTROIDS, (uint32_t)num_pq_chunks,
    //                         pq_pivots_stream, make_zero_mean);
    // }
    delete[] train_data;
    generate_pq_data_from_pivots<T>(data_stream, NUM_PQ_CENTROIDS, (uint32_t)num_pq_chunks, pq_pivots_stream,
                                    pq_compressed_stream, use_opq);
}

// Instantations of supported templates


template DISKANN_DLLEXPORT void generate_quantized_data<float>(std::stringstream &  data_stream, std::stringstream &pq_pivots_stream,
                             std::stringstream &pq_compressed_stream, diskann::Metric compareMetric,
                             const double p_val, const size_t num_pq_chunks, const bool use_opq,
                             const std::string &codebook_prefix);

template DISKANN_DLLEXPORT int generate_pq_data_from_pivots<float>(std::stringstream &  data_stream, unsigned num_centers, unsigned num_pq_chunks,
                                 std::stringstream &pq_pivots_stream, std::stringstream &pq_compressed_stream,
                                 bool use_opq);

template DISKANN_DLLEXPORT int generate_pq_data_from_pivots<int8_t>(const std::string &data_file, uint32_t num_centers,
                                                                    uint32_t num_pq_chunks,
                                                                    const std::string &pq_pivots_path,
                                                                    const std::string &pq_compressed_vectors_path,
                                                                    bool use_opq);
template DISKANN_DLLEXPORT int generate_pq_data_from_pivots<uint8_t>(const std::string &data_file, uint32_t num_centers,
                                                                     uint32_t num_pq_chunks,
                                                                     const std::string &pq_pivots_path,
                                                                     const std::string &pq_compressed_vectors_path,
                                                                     bool use_opq);
template DISKANN_DLLEXPORT int generate_pq_data_from_pivots<float>(const std::string &data_file, uint32_t num_centers,
                                                                   uint32_t num_pq_chunks,
                                                                   const std::string &pq_pivots_path,
                                                                   const std::string &pq_compressed_vectors_path,
                                                                   bool use_opq);

template DISKANN_DLLEXPORT void generate_disk_quantized_data<int8_t>(const std::string &data_file_to_use,
                                                                     const std::string &disk_pq_pivots_path,
                                                                     const std::string &disk_pq_compressed_vectors_path,
                                                                     diskann::Metric compareMetric, const double p_val,
                                                                     size_t &disk_pq_dims);

template DISKANN_DLLEXPORT void generate_disk_quantized_data<uint8_t>(
    const std::string &data_file_to_use, const std::string &disk_pq_pivots_path,
    const std::string &disk_pq_compressed_vectors_path, diskann::Metric compareMetric, const double p_val,
    size_t &disk_pq_dims);



template DISKANN_DLLEXPORT void generate_disk_quantized_data<float>(const std::string &data_file_to_use,
                                                                    const std::string &disk_pq_pivots_path,
                                                                    const std::string &disk_pq_compressed_vectors_path,
                                                                    diskann::Metric compareMetric, const double p_val,
                                                                    size_t &disk_pq_dims);

template DISKANN_DLLEXPORT void generate_quantized_data<int8_t>(const std::string &data_file_to_use,
                                                                const std::string &pq_pivots_path,
                                                                const std::string &pq_compressed_vectors_path,
                                                                diskann::Metric compareMetric, const double p_val,
                                                                const size_t num_pq_chunks, const bool use_opq,
                                                                const std::string &codebook_prefix);

template DISKANN_DLLEXPORT void generate_quantized_data<uint8_t>(const std::string &data_file_to_use,
                                                                 const std::string &pq_pivots_path,
                                                                 const std::string &pq_compressed_vectors_path,
                                                                 diskann::Metric compareMetric, const double p_val,
                                                                 const size_t num_pq_chunks, const bool use_opq,
                                                                 const std::string &codebook_prefix);

template DISKANN_DLLEXPORT void generate_quantized_data<float>(const std::string &data_file_to_use,
                                                               const std::string &pq_pivots_path,
                                                               const std::string &pq_compressed_vectors_path,
                                                               diskann::Metric compareMetric, const double p_val,
                                                               const size_t num_pq_chunks, const bool use_opq,
                                                               const std::string &codebook_prefix);
} // namespace diskann
