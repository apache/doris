// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <algorithm>
#include <fcntl.h>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#ifdef __APPLE__
#else
#include <malloc.h>
#endif

#ifdef _WINDOWS
#include <Windows.h>
typedef HANDLE FileHandle;
#else
#include <unistd.h>
typedef int FileHandle;
#endif

#include "cached_io.h"
#include "common_includes.h"

#include "utils.h"
#include "windows_customizations.h"

namespace diskann
{
const size_t MAX_SAMPLE_POINTS_FOR_WARMUP = 100000;
const double PQ_TRAINING_SET_FRACTION = 0.1;
const double SPACE_FOR_CACHED_NODES_IN_GB = 0.25;
const double THRESHOLD_FOR_CACHING_IN_GB = 1.0;
const uint32_t NUM_NODES_TO_CACHE = 250000;
const uint32_t WARMUP_L = 20;
const uint32_t NUM_KMEANS_REPS = 12;

const std::string PQ_PIVOTS = "pq_pivots";
const std::string PQ_COMPRESSED = "pq_compressed";
const std::string MEM_INDEX = "mem_index";
const std::string DISK_INDEX_PATH = "disk_index_path";
const std::string MEM_INDEX_DATA = "mem_index_data";

template <typename T, typename LabelT> class PQFlashIndex;

DISKANN_DLLEXPORT double get_memory_budget(const std::string &mem_budget_str);
DISKANN_DLLEXPORT double get_memory_budget(double search_ram_budget_in_gb);
DISKANN_DLLEXPORT void add_new_file_to_single_index(std::string index_file, std::string new_file);

DISKANN_DLLEXPORT size_t calculate_num_pq_chunks(double final_index_ram_limit, size_t points_num, uint32_t dim);

DISKANN_DLLEXPORT void read_idmap(const std::string &fname, std::vector<uint32_t> &ivecs);

#ifdef EXEC_ENV_OLS
template <typename T>
DISKANN_DLLEXPORT T *load_warmup(MemoryMappedFiles &files, const std::string &cache_warmup_file, uint64_t &warmup_num,
                                 uint64_t warmup_dim, uint64_t warmup_aligned_dim);
#else
template <typename T>
DISKANN_DLLEXPORT T *load_warmup(const std::string &cache_warmup_file, uint64_t &warmup_num, uint64_t warmup_dim,
                                 uint64_t warmup_aligned_dim);
#endif

DISKANN_DLLEXPORT int merge_shards(const std::string &vamana_prefix, const std::string &vamana_suffix,
                                   const std::string &idmaps_prefix, const std::string &idmaps_suffix,
                                   const uint64_t nshards, uint32_t max_degree, const std::string &output_vamana,
                                   const std::string &medoids_file, bool use_filters = false,
                                   const std::string &labels_to_medoids_file = std::string(""));

DISKANN_DLLEXPORT void extract_shard_labels(const std::string &in_label_file, const std::string &shard_ids_bin,
                                            const std::string &shard_label_file);

template <typename T>
DISKANN_DLLEXPORT std::string preprocess_base_file(const std::string &infile, const std::string &indexPrefix,
                                                   diskann::Metric &distMetric);

template <typename T, typename LabelT = uint32_t>
DISKANN_DLLEXPORT int build_merged_vamana_index(std::string base_file, diskann::Metric _compareMetric, uint32_t L,
                                                uint32_t R, double sampling_rate, double ram_budget,
                                                std::string mem_index_path, std::string medoids_file,
                                                std::string centroids_file, size_t build_pq_bytes, bool use_opq,
                                                uint32_t num_threads, bool use_filters = false,
                                                const std::string &label_file = std::string(""),
                                                const std::string &labels_to_medoids_file = std::string(""),
                                                const std::string &universal_label = "", const uint32_t Lf = 0);

template <typename T, typename LabelT = uint32_t>
DISKANN_DLLEXPORT int build_merged_vamana_index(std::stringstream & data_stream, diskann::Metric _compareMetric, uint32_t L,
                                                uint32_t R, double sampling_rate, double ram_budget,
                                                std::stringstream &index_stream, std::string medoids_file,
                                                std::string centroids_file, size_t build_pq_bytes, bool use_opq,
                                                uint32_t num_threads, bool use_filters = false,
                                                const std::string &label_file = std::string(""),
                                                const std::string &labels_to_medoids_file = std::string(""),
                                                const std::string &universal_label = "", const uint32_t Lf = 0);


template <typename T, typename LabelT>
DISKANN_DLLEXPORT uint32_t optimize_beamwidth(std::unique_ptr<diskann::PQFlashIndex<T, LabelT>> &_pFlashIndex,
                                              T *tuning_sample, uint64_t tuning_sample_num,
                                              uint64_t tuning_sample_aligned_dim, uint32_t L, uint32_t nthreads,
                                              uint32_t start_bw = 2);

template <typename T, typename LabelT = uint32_t>
DISKANN_DLLEXPORT int build_disk_index(
    const char *dataFilePath, const char *indexFilePath, const char *indexBuildParameters,
    diskann::Metric _compareMetric, bool use_opq = false,
    const std::string &codebook_prefix = "", // default is empty for no codebook pass in
    bool use_filters = false,
    const std::string &label_file = std::string(""), // default is empty string for no label_file
    const std::string &universal_label = "", const uint32_t filter_threshold = 0,
    const uint32_t Lf = 0); // default is empty string for no universal label

template <typename T>
DISKANN_DLLEXPORT void create_disk_layout(const std::string base_file, const std::string mem_index_file,
                                          const std::string output_file,
                                          const std::string reorder_data_file = std::string(""));

template <typename T>
DISKANN_DLLEXPORT void create_disk_layout(const T &data, const std::string mem_index_file,
                                          const std::string output_file,
                                          const std::string reorder_data_file = std::string(""));

template <typename T>
DISKANN_DLLEXPORT void create_disk_layout(std::stringstream &  data_stream, std::stringstream &index_stream,
                                          std::stringstream &disklayout_stream,
                                          const std::string reorder_data_file = std::string(""));

} // namespace diskann
