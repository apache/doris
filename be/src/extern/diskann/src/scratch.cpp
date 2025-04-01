// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <vector>
#include <boost/dynamic_bitset.hpp>

#include "scratch.h"
#include "pq_scratch.h"

namespace diskann
{
//
// Functions to manage scratch space for in-memory index based search
//
template <typename T>
InMemQueryScratch<T>::InMemQueryScratch(uint32_t search_l, uint32_t indexing_l, uint32_t r, uint32_t maxc, size_t dim,
                                        size_t aligned_dim, size_t alignment_factor, bool init_pq_scratch)
    : _L(0), _R(r), _maxc(maxc)
{
    if (search_l == 0 || indexing_l == 0 || r == 0 || dim == 0)
    {
        std::stringstream ss;
        ss << "In InMemQueryScratch, one of search_l = " << search_l << ", indexing_l = " << indexing_l
           << ", dim = " << dim << " or r = " << r << " is zero." << std::endl;
        throw diskann::ANNException(ss.str(), -1);
    }

    alloc_aligned(((void **)&this->_aligned_query_T), aligned_dim * sizeof(T), alignment_factor * sizeof(T));
    memset(this->_aligned_query_T, 0, aligned_dim * sizeof(T));

    if (init_pq_scratch)
        this->_pq_scratch = new PQScratch<T>(defaults::MAX_GRAPH_DEGREE, aligned_dim);
    else
        this->_pq_scratch = nullptr;

    _occlude_factor.reserve(maxc);
    _inserted_into_pool_bs = new boost::dynamic_bitset<>();
    _id_scratch.reserve((size_t)std::ceil(1.5 * defaults::GRAPH_SLACK_FACTOR * _R));
    _dist_scratch.reserve((size_t)std::ceil(1.5 * defaults::GRAPH_SLACK_FACTOR * _R));

    resize_for_new_L(std::max(search_l, indexing_l));
}

template <typename T> void InMemQueryScratch<T>::clear()
{
    _pool.clear();
    _best_l_nodes.clear();
    _occlude_factor.clear();

    _inserted_into_pool_rs.clear();
    _inserted_into_pool_bs->reset();

    _id_scratch.clear();
    _dist_scratch.clear();

    _expanded_nodes_set.clear();
    _expanded_nghrs_vec.clear();
    _occlude_list_output.clear();
}

template <typename T> void InMemQueryScratch<T>::resize_for_new_L(uint32_t new_l)
{
    if (new_l > _L)
    {
        _L = new_l;
        _pool.reserve(3 * _L + _R);
        _best_l_nodes.reserve(_L);

        _inserted_into_pool_rs.reserve(20 * _L);
    }
}

template <typename T> InMemQueryScratch<T>::~InMemQueryScratch()
{
    if (this->_aligned_query_T != nullptr)
    {
        aligned_free(this->_aligned_query_T);
        this->_aligned_query_T = nullptr;
    }

    delete this->_pq_scratch;
    delete _inserted_into_pool_bs;
}

//
// Functions to manage scratch space for SSD based search
//
template <typename T> void SSDQueryScratch<T>::reset()
{
    sector_idx = 0;
    visited.clear();
    retset.clear();
    full_retset.clear();
}

template <typename T> SSDQueryScratch<T>::SSDQueryScratch(size_t aligned_dim, size_t visited_reserve)
{
    size_t coord_alloc_size = ROUND_UP(sizeof(T) * aligned_dim, 256);

    diskann::alloc_aligned((void **)&coord_scratch, coord_alloc_size, 256);
    diskann::alloc_aligned((void **)&sector_scratch, defaults::MAX_N_SECTOR_READS * defaults::SECTOR_LEN,
                           defaults::SECTOR_LEN);
    diskann::alloc_aligned((void **)&this->_aligned_query_T, aligned_dim * sizeof(T), 8 * sizeof(T));
    this->_pq_scratch = new PQScratch<T>(defaults::MAX_GRAPH_DEGREE, aligned_dim);

    memset(coord_scratch, 0, coord_alloc_size);
    memset(this->_aligned_query_T, 0, aligned_dim * sizeof(T));

    visited.reserve(visited_reserve);
    full_retset.reserve(visited_reserve);
}

template <typename T> SSDQueryScratch<T>::~SSDQueryScratch()
{
    diskann::aligned_free((void *)coord_scratch);
    diskann::aligned_free((void *)sector_scratch);
    diskann::aligned_free((void *)this->_aligned_query_T);

    delete this->_pq_scratch;
}

template <typename T>
SSDThreadData<T>::SSDThreadData(size_t aligned_dim, size_t visited_reserve) : scratch(aligned_dim, visited_reserve)
{
}

template <typename T> void SSDThreadData<T>::clear()
{
    scratch.reset();
}

template <typename T> PQScratch<T>::PQScratch(size_t graph_degree, size_t aligned_dim)
{
    diskann::alloc_aligned((void **)&aligned_pq_coord_scratch,
                           (size_t)graph_degree * (size_t)MAX_PQ_CHUNKS * sizeof(uint8_t), 256);
    diskann::alloc_aligned((void **)&aligned_pqtable_dist_scratch, 256 * (size_t)MAX_PQ_CHUNKS * sizeof(float), 256);
    diskann::alloc_aligned((void **)&aligned_dist_scratch, (size_t)graph_degree * sizeof(float), 256);
    diskann::alloc_aligned((void **)&aligned_query_float, aligned_dim * sizeof(float), 8 * sizeof(float));
    diskann::alloc_aligned((void **)&rotated_query, aligned_dim * sizeof(float), 8 * sizeof(float));

    memset(aligned_query_float, 0, aligned_dim * sizeof(float));
    memset(rotated_query, 0, aligned_dim * sizeof(float));
}

template <typename T> PQScratch<T>::~PQScratch()
{
    diskann::aligned_free((void *)aligned_pq_coord_scratch);
    diskann::aligned_free((void *)aligned_pqtable_dist_scratch);
    diskann::aligned_free((void *)aligned_dist_scratch);
    diskann::aligned_free((void *)aligned_query_float);
    diskann::aligned_free((void *)rotated_query);
}

template <typename T> void PQScratch<T>::initialize(size_t dim, const T *query, const float norm)
{
    for (size_t d = 0; d < dim; ++d)
    {
        if (norm != 1.0f)
            rotated_query[d] = aligned_query_float[d] = static_cast<float>(query[d]) / norm;
        else
            rotated_query[d] = aligned_query_float[d] = static_cast<float>(query[d]);
    }
}

template DISKANN_DLLEXPORT class InMemQueryScratch<int8_t>;
template DISKANN_DLLEXPORT class InMemQueryScratch<uint8_t>;
template DISKANN_DLLEXPORT class InMemQueryScratch<float>;

template DISKANN_DLLEXPORT class SSDQueryScratch<int8_t>;
template DISKANN_DLLEXPORT class SSDQueryScratch<uint8_t>;
template DISKANN_DLLEXPORT class SSDQueryScratch<float>;

template DISKANN_DLLEXPORT class PQScratch<int8_t>;
template DISKANN_DLLEXPORT class PQScratch<uint8_t>;
template DISKANN_DLLEXPORT class PQScratch<float>;

template DISKANN_DLLEXPORT class SSDThreadData<int8_t>;
template DISKANN_DLLEXPORT class SSDThreadData<uint8_t>;
template DISKANN_DLLEXPORT class SSDThreadData<float>;

} // namespace diskann
