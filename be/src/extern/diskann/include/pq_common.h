#pragma once

#include <string>
#include <sstream>

#define NUM_PQ_BITS 8
#define NUM_PQ_CENTROIDS (1 << NUM_PQ_BITS)
#define MAX_OPQ_ITERS 20
#define NUM_KMEANS_REPS_PQ 12
#define MAX_PQ_TRAINING_SET_SIZE 256000
#define MAX_PQ_CHUNKS 512

namespace diskann
{
inline std::string get_quantized_vectors_filename(const std::string &prefix, bool use_opq, uint32_t num_chunks)
{
    return prefix + (use_opq ? "_opq" : "pq") + std::to_string(num_chunks) + "_compressed.bin";
}

inline std::string get_pivot_data_filename(const std::string &prefix, bool use_opq, uint32_t num_chunks)
{
    return prefix + (use_opq ? "_opq" : "pq") + std::to_string(num_chunks) + "_pivots.bin";
}

inline std::string get_rotation_matrix_suffix(const std::string &pivot_data_filename)
{
    return pivot_data_filename + "_rotation_matrix.bin";
}

} // namespace diskann
