#pragma once
#include <cstdint>
#include "pq_common.h"
#include "utils.h"

namespace diskann
{

template <typename T> class PQScratch
{
  public:
    float *aligned_pqtable_dist_scratch = nullptr; // MUST BE AT LEAST [256 * NCHUNKS]
    float *aligned_dist_scratch = nullptr;         // MUST BE AT LEAST diskann MAX_DEGREE
    uint8_t *aligned_pq_coord_scratch = nullptr;   // AT LEAST  [N_CHUNKS * MAX_DEGREE]
    float *rotated_query = nullptr;
    float *aligned_query_float = nullptr;

    PQScratch(size_t graph_degree, size_t aligned_dim);
    void initialize(size_t dim, const T *query, const float norm = 1.0f);
    virtual ~PQScratch();
};

} // namespace diskann