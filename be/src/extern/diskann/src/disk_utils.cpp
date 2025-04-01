// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "common_includes.h"

#if defined(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS) && defined(DISKANN_BUILD)
#include "gperftools/malloc_extension.h"
#endif

#include "logger.h"
#include "disk_utils.h"
#include "cached_io.h"
#include "index.h"
#include "mkl.h"
#include "omp.h"
#include "percentile_stats.h"
#include "partition.h"
#include "pq_flash_index.h"
#include "timer.h"
#include "tsl/robin_set.h"

namespace diskann
{

void add_new_file_to_single_index(std::string index_file, std::string new_file)
{
    std::unique_ptr<uint64_t[]> metadata;
    uint64_t nr, nc;
    diskann::load_bin<uint64_t>(index_file, metadata, nr, nc);
    if (nc != 1)
    {
        std::stringstream stream;
        stream << "Error, index file specified does not have correct metadata. " << std::endl;
        throw diskann::ANNException(stream.str(), -1);
    }
    size_t index_ending_offset = metadata[nr - 1];
    size_t read_blk_size = 64 * 1024 * 1024;
    cached_ofstream writer(index_file, read_blk_size);
    size_t check_file_size = get_file_size(index_file);
    if (check_file_size != index_ending_offset)
    {
        std::stringstream stream;
        stream << "Error, index file specified does not have correct metadata "
                  "(last entry must match the filesize). "
               << std::endl;
        throw diskann::ANNException(stream.str(), -1);
    }

    cached_ifstream reader(new_file, read_blk_size);
    size_t fsize = reader.get_file_size();
    if (fsize == 0)
    {
        std::stringstream stream;
        stream << "Error, new file specified is empty. Not appending. " << std::endl;
        throw diskann::ANNException(stream.str(), -1);
    }

    size_t num_blocks = DIV_ROUND_UP(fsize, read_blk_size);
    char *dump = new char[read_blk_size];
    for (uint64_t i = 0; i < num_blocks; i++)
    {
        size_t cur_block_size =
            read_blk_size > fsize - (i * read_blk_size) ? fsize - (i * read_blk_size) : read_blk_size;
        reader.read(dump, cur_block_size);
        writer.write(dump, cur_block_size);
    }
    //    reader.close();
    //    writer.close();

    delete[] dump;
    std::vector<uint64_t> new_meta;
    for (uint64_t i = 0; i < nr; i++)
        new_meta.push_back(metadata[i]);
    new_meta.push_back(metadata[nr - 1] + fsize);

    diskann::save_bin<uint64_t>(index_file, new_meta.data(), new_meta.size(), 1);
}

double get_memory_budget(double search_ram_budget)
{
    double final_index_ram_limit = search_ram_budget;
    if (search_ram_budget - SPACE_FOR_CACHED_NODES_IN_GB > THRESHOLD_FOR_CACHING_IN_GB)
    { // slack for space used by cached
      // nodes
        final_index_ram_limit = search_ram_budget - SPACE_FOR_CACHED_NODES_IN_GB;
    }
    return final_index_ram_limit * 1024 * 1024 * 1024;
}

double get_memory_budget(const std::string &mem_budget_str)
{
    double search_ram_budget = atof(mem_budget_str.c_str());
    return get_memory_budget(search_ram_budget);
}

size_t calculate_num_pq_chunks(double final_index_ram_limit, size_t points_num, uint32_t dim,
                               const std::vector<std::string> &param_list)
{
    size_t num_pq_chunks = (size_t)(std::floor)(uint64_t(final_index_ram_limit / (double)points_num));
    diskann::cout << "Calculated num_pq_chunks :" << num_pq_chunks << std::endl;
    if (param_list.size() >= 6)
    {
        float compress_ratio = (float)atof(param_list[5].c_str());
        if (compress_ratio > 0 && compress_ratio <= 1)
        {
            size_t chunks_by_cr = (size_t)(std::floor)(compress_ratio * dim);

            if (chunks_by_cr > 0 && chunks_by_cr < num_pq_chunks)
            {
                diskann::cout << "Compress ratio:" << compress_ratio << " new #pq_chunks:" << chunks_by_cr << std::endl;
                num_pq_chunks = chunks_by_cr;
            }
            else
            {
                diskann::cout << "Compress ratio: " << compress_ratio << " #new pq_chunks: " << chunks_by_cr
                              << " is either zero or greater than num_pq_chunks: " << num_pq_chunks
                              << ". num_pq_chunks is unchanged. " << std::endl;
            }
        }
        else
        {
            diskann::cerr << "Compression ratio: " << compress_ratio << " should be in (0,1]" << std::endl;
        }
    }

    num_pq_chunks = num_pq_chunks <= 0 ? 1 : num_pq_chunks;
    num_pq_chunks = num_pq_chunks > dim ? dim : num_pq_chunks;
    num_pq_chunks = num_pq_chunks > MAX_PQ_CHUNKS ? MAX_PQ_CHUNKS : num_pq_chunks;

    diskann::cout << "Compressing " << dim << "-dimensional data into " << num_pq_chunks << " bytes per vector."
                  << std::endl;
    return num_pq_chunks;
}


size_t calculate_num_pq_chunks(double final_index_ram_limit, size_t points_num, uint32_t dim)
{
    size_t num_pq_chunks = (size_t)(std::floor)(uint64_t(final_index_ram_limit / (double)points_num));
    diskann::cout << "Calculated num_pq_chunks :" << num_pq_chunks << std::endl;
    num_pq_chunks = num_pq_chunks <= 0 ? 1 : num_pq_chunks;
    num_pq_chunks = num_pq_chunks > dim ? dim : num_pq_chunks;
    num_pq_chunks = num_pq_chunks > MAX_PQ_CHUNKS ? MAX_PQ_CHUNKS : num_pq_chunks;

    diskann::cout << "Compressing " << dim << "-dimensional data into " << num_pq_chunks << " bytes per vector."
                  << std::endl;
    return num_pq_chunks;
}

template <typename T> T *generateRandomWarmup(uint64_t warmup_num, uint64_t warmup_dim, uint64_t warmup_aligned_dim)
{
    T *warmup = nullptr;
    warmup_num = 100000;
    diskann::cout << "Generating random warmup file with dim " << warmup_dim << " and aligned dim "
                  << warmup_aligned_dim << std::flush;
    diskann::alloc_aligned(((void **)&warmup), warmup_num * warmup_aligned_dim * sizeof(T), 8 * sizeof(T));
    std::memset(warmup, 0, warmup_num * warmup_aligned_dim * sizeof(T));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(-128, 127);
    for (uint32_t i = 0; i < warmup_num; i++)
    {
        for (uint32_t d = 0; d < warmup_dim; d++)
        {
            warmup[i * warmup_aligned_dim + d] = (T)dis(gen);
        }
    }
    diskann::cout << "..done" << std::endl;
    return warmup;
}

#ifdef EXEC_ENV_OLS
template <typename T>
T *load_warmup(MemoryMappedFiles &files, const std::string &cache_warmup_file, uint64_t &warmup_num,
               uint64_t warmup_dim, uint64_t warmup_aligned_dim)
{
    T *warmup = nullptr;
    uint64_t file_dim, file_aligned_dim;

    if (files.fileExists(cache_warmup_file))
    {
        diskann::load_aligned_bin<T>(files, cache_warmup_file, warmup, warmup_num, file_dim, file_aligned_dim);
        diskann::cout << "In the warmup file: " << cache_warmup_file << " File dim: " << file_dim
                      << " File aligned dim: " << file_aligned_dim << " Expected dim: " << warmup_dim
                      << " Expected aligned dim: " << warmup_aligned_dim << std::endl;

        if (file_dim != warmup_dim || file_aligned_dim != warmup_aligned_dim)
        {
            std::stringstream stream;
            stream << "Mismatched dimensions in sample file. file_dim = " << file_dim
                   << " file_aligned_dim: " << file_aligned_dim << " index_dim: " << warmup_dim
                   << " index_aligned_dim: " << warmup_aligned_dim << std::endl;
            diskann::cerr << stream.str();
            throw diskann::ANNException(stream.str(), -1);
        }
    }
    else
    {
        warmup = generateRandomWarmup<T>(warmup_num, warmup_dim, warmup_aligned_dim);
    }
    return warmup;
}
#endif

template <typename T>
T *load_warmup(const std::string &cache_warmup_file, uint64_t &warmup_num, uint64_t warmup_dim,
               uint64_t warmup_aligned_dim)
{
    T *warmup = nullptr;
    uint64_t file_dim, file_aligned_dim;

    if (file_exists(cache_warmup_file))
    {
        diskann::load_aligned_bin<T>(cache_warmup_file, warmup, warmup_num, file_dim, file_aligned_dim);
        if (file_dim != warmup_dim || file_aligned_dim != warmup_aligned_dim)
        {
            std::stringstream stream;
            stream << "Mismatched dimensions in sample file. file_dim = " << file_dim
                   << " file_aligned_dim: " << file_aligned_dim << " index_dim: " << warmup_dim
                   << " index_aligned_dim: " << warmup_aligned_dim << std::endl;
            throw diskann::ANNException(stream.str(), -1);
        }
    }
    else
    {
        warmup = generateRandomWarmup<T>(warmup_num, warmup_dim, warmup_aligned_dim);
    }
    return warmup;
}

/***************************************************
    Support for Merging Many Vamana Indices
 ***************************************************/

void read_idmap(const std::string &fname, std::vector<uint32_t> &ivecs)
{
    uint32_t npts32, dim;
    size_t actual_file_size = get_file_size(fname);
    std::ifstream reader(fname.c_str(), std::ios::binary);
    reader.read((char *)&npts32, sizeof(uint32_t));
    reader.read((char *)&dim, sizeof(uint32_t));
    if (dim != 1 || actual_file_size != ((size_t)npts32) * sizeof(uint32_t) + 2 * sizeof(uint32_t))
    {
        std::stringstream stream;
        stream << "Error reading idmap file. Check if the file is bin file with "
                  "1 dimensional data. Actual: "
               << actual_file_size << ", expected: " << (size_t)npts32 + 2 * sizeof(uint32_t) << std::endl;

        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    ivecs.resize(npts32);
    reader.read((char *)ivecs.data(), ((size_t)npts32) * sizeof(uint32_t));
    reader.close();
}

int merge_shards(const std::string &vamana_prefix, const std::string &vamana_suffix, const std::string &idmaps_prefix,
                 const std::string &idmaps_suffix, const uint64_t nshards, uint32_t max_degree,
                 const std::string &output_vamana, const std::string &medoids_file, bool use_filters,
                 const std::string &labels_to_medoids_file)
{
    // Read ID maps
    std::vector<std::string> vamana_names(nshards);
    std::vector<std::vector<uint32_t>> idmaps(nshards);
    for (uint64_t shard = 0; shard < nshards; shard++)
    {
        vamana_names[shard] = vamana_prefix + std::to_string(shard) + vamana_suffix;
        read_idmap(idmaps_prefix + std::to_string(shard) + idmaps_suffix, idmaps[shard]);
    }

    // find max node id
    size_t nnodes = 0;
    size_t nelems = 0;
    for (auto &idmap : idmaps)
    {
        for (auto &id : idmap)
        {
            nnodes = std::max(nnodes, (size_t)id);
        }
        nelems += idmap.size();
    }
    nnodes++;
    diskann::cout << "# nodes: " << nnodes << ", max. degree: " << max_degree << std::endl;

    // compute inverse map: node -> shards
    std::vector<std::pair<uint32_t, uint32_t>> node_shard;
    node_shard.reserve(nelems);
    for (size_t shard = 0; shard < nshards; shard++)
    {
        diskann::cout << "Creating inverse map -- shard #" << shard << std::endl;
        for (size_t idx = 0; idx < idmaps[shard].size(); idx++)
        {
            size_t node_id = idmaps[shard][idx];
            node_shard.push_back(std::make_pair((uint32_t)node_id, (uint32_t)shard));
        }
    }
    std::sort(node_shard.begin(), node_shard.end(), [](const auto &left, const auto &right) {
        return left.first < right.first || (left.first == right.first && left.second < right.second);
    });
    diskann::cout << "Finished computing node -> shards map" << std::endl;

    // will merge all the labels to medoids files of each shard into one
    // combined file
    if (use_filters)
    {
        std::unordered_map<uint32_t, std::vector<uint32_t>> global_label_to_medoids;

        for (size_t i = 0; i < nshards; i++)
        {
            std::ifstream mapping_reader;
            std::string map_file = vamana_names[i] + "_labels_to_medoids.txt";
            mapping_reader.open(map_file);

            std::string line, token;
            uint32_t line_cnt = 0;

            while (std::getline(mapping_reader, line))
            {
                std::istringstream iss(line);
                uint32_t cnt = 0;
                uint32_t medoid = 0;
                uint32_t label = 0;
                while (std::getline(iss, token, ','))
                {
                    token.erase(std::remove(token.begin(), token.end(), '\n'), token.end());
                    token.erase(std::remove(token.begin(), token.end(), '\r'), token.end());

                    uint32_t token_as_num = std::stoul(token);

                    if (cnt == 0)
                        label = token_as_num;
                    else
                        medoid = token_as_num;
                    cnt++;
                }
                global_label_to_medoids[label].push_back(idmaps[i][medoid]);
                line_cnt++;
            }
            mapping_reader.close();
        }

        std::ofstream mapping_writer(labels_to_medoids_file);
        assert(mapping_writer.is_open());
        for (auto iter : global_label_to_medoids)
        {
            mapping_writer << iter.first << ", ";
            auto &vec = iter.second;
            for (uint32_t idx = 0; idx < vec.size() - 1; idx++)
            {
                mapping_writer << vec[idx] << ", ";
            }
            mapping_writer << vec[vec.size() - 1] << std::endl;
        }
        mapping_writer.close();
    }

    // create cached vamana readers
    std::vector<cached_ifstream> vamana_readers(nshards);
    for (size_t i = 0; i < nshards; i++)
    {
        vamana_readers[i].open(vamana_names[i], BUFFER_SIZE_FOR_CACHED_IO);
        size_t expected_file_size;
        vamana_readers[i].read((char *)&expected_file_size, sizeof(uint64_t));
    }

    size_t vamana_metadata_size =
        sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t); // expected file size + max degree +
                                                                                   // medoid_id + frozen_point info

    // create cached vamana writers
    cached_ofstream merged_vamana_writer(output_vamana, BUFFER_SIZE_FOR_CACHED_IO);

    size_t merged_index_size = vamana_metadata_size; // we initialize the size of the merged index to
                                                     // the metadata size
    size_t merged_index_frozen = 0;
    merged_vamana_writer.write((char *)&merged_index_size,
                               sizeof(uint64_t)); // we will overwrite the index size at the end

    uint32_t output_width = max_degree;
    uint32_t max_input_width = 0;
    // read width from each vamana to advance buffer by sizeof(uint32_t) bytes
    for (auto &reader : vamana_readers)
    {
        uint32_t input_width;
        reader.read((char *)&input_width, sizeof(uint32_t));
        max_input_width = input_width > max_input_width ? input_width : max_input_width;
    }

    diskann::cout << "Max input width: " << max_input_width << ", output width: " << output_width << std::endl;

    merged_vamana_writer.write((char *)&output_width, sizeof(uint32_t));
    std::ofstream medoid_writer(medoids_file.c_str(), std::ios::binary);
    uint32_t nshards_u32 = (uint32_t)nshards;
    uint32_t one_val = 1;
    medoid_writer.write((char *)&nshards_u32, sizeof(uint32_t));
    medoid_writer.write((char *)&one_val, sizeof(uint32_t));

    uint64_t vamana_index_frozen = 0; // as of now the functionality to merge many overlapping vamana
                                      // indices is supported only for bulk indices without frozen point.
                                      // Hence the final index will also not have any frozen points.
    for (uint64_t shard = 0; shard < nshards; shard++)
    {
        uint32_t medoid;
        // read medoid
        vamana_readers[shard].read((char *)&medoid, sizeof(uint32_t));
        vamana_readers[shard].read((char *)&vamana_index_frozen, sizeof(uint64_t));
        assert(vamana_index_frozen == false);
        // rename medoid
        medoid = idmaps[shard][medoid];

        medoid_writer.write((char *)&medoid, sizeof(uint32_t));
        // write renamed medoid
        if (shard == (nshards - 1)) //--> uncomment if running hierarchical
            merged_vamana_writer.write((char *)&medoid, sizeof(uint32_t));
    }
    merged_vamana_writer.write((char *)&merged_index_frozen, sizeof(uint64_t));
    medoid_writer.close();

    diskann::cout << "Starting merge" << std::endl;

    // Gopal. random_shuffle() is deprecated.
    std::random_device rng;
    std::mt19937 urng(rng());

    std::vector<bool> nhood_set(nnodes, 0);
    std::vector<uint32_t> final_nhood;

    uint32_t nnbrs = 0, shard_nnbrs = 0;
    uint32_t cur_id = 0;
    for (const auto &id_shard : node_shard)
    {
        uint32_t node_id = id_shard.first;
        uint32_t shard_id = id_shard.second;
        if (cur_id < node_id)
        {
            // Gopal. random_shuffle() is deprecated.
            std::shuffle(final_nhood.begin(), final_nhood.end(), urng);
            nnbrs = (uint32_t)(std::min)(final_nhood.size(), (uint64_t)max_degree);
            // write into merged ofstream
            merged_vamana_writer.write((char *)&nnbrs, sizeof(uint32_t));
            merged_vamana_writer.write((char *)final_nhood.data(), nnbrs * sizeof(uint32_t));
            merged_index_size += (sizeof(uint32_t) + nnbrs * sizeof(uint32_t));
            if (cur_id % 499999 == 1)
            {
                diskann::cout << "." << std::flush;
            }
            cur_id = node_id;
            nnbrs = 0;
            for (auto &p : final_nhood)
                nhood_set[p] = 0;
            final_nhood.clear();
        }
        // read from shard_id ifstream
        vamana_readers[shard_id].read((char *)&shard_nnbrs, sizeof(uint32_t));

        if (shard_nnbrs == 0)
        {
            diskann::cout << "WARNING: shard #" << shard_id << ", node_id " << node_id << " has 0 nbrs" << std::endl;
        }

        std::vector<uint32_t> shard_nhood(shard_nnbrs);
        if (shard_nnbrs > 0)
            vamana_readers[shard_id].read((char *)shard_nhood.data(), shard_nnbrs * sizeof(uint32_t));
        // rename nodes
        for (uint64_t j = 0; j < shard_nnbrs; j++)
        {
            if (nhood_set[idmaps[shard_id][shard_nhood[j]]] == 0)
            {
                nhood_set[idmaps[shard_id][shard_nhood[j]]] = 1;
                final_nhood.emplace_back(idmaps[shard_id][shard_nhood[j]]);
            }
        }
    }

    // Gopal. random_shuffle() is deprecated.
    std::shuffle(final_nhood.begin(), final_nhood.end(), urng);
    nnbrs = (uint32_t)(std::min)(final_nhood.size(), (uint64_t)max_degree);
    // write into merged ofstream
    merged_vamana_writer.write((char *)&nnbrs, sizeof(uint32_t));
    if (nnbrs > 0)
    {
        merged_vamana_writer.write((char *)final_nhood.data(), nnbrs * sizeof(uint32_t));
    }
    merged_index_size += (sizeof(uint32_t) + nnbrs * sizeof(uint32_t));
    for (auto &p : final_nhood)
        nhood_set[p] = 0;
    final_nhood.clear();

    diskann::cout << "Expected size: " << merged_index_size << std::endl;

    merged_vamana_writer.reset();
    merged_vamana_writer.write((char *)&merged_index_size, sizeof(uint64_t));

    diskann::cout << "Finished merge" << std::endl;
    return 0;
}

// TODO: Make this a streaming implementation to avoid exceeding the memory
// budget
/* If the number of filters per point N exceeds the graph degree R,
  then it is difficult to have edges to all labels from this point.
  This function break up such dense points to have only a threshold of maximum
  T labels per point  It divides one graph nodes to multiple nodes and append
  the new nodes at the end. The dummy map contains the real graph id of the
  new nodes added to the graph */
template <typename T>
void breakup_dense_points(const std::string data_file, const std::string labels_file, uint32_t density,
                          const std::string out_data_file, const std::string out_labels_file,
                          const std::string out_metadata_file)
{
    std::string token, line;
    std::ifstream labels_stream(labels_file);
    T *data;
    uint64_t npts, ndims;
    diskann::load_bin<T>(data_file, data, npts, ndims);

    std::unordered_map<uint32_t, uint32_t> dummy_pt_ids;
    uint32_t next_dummy_id = (uint32_t)npts;

    uint32_t point_cnt = 0;

    std::vector<std::vector<uint32_t>> labels_per_point;
    labels_per_point.resize(npts);

    uint32_t dense_pts = 0;
    if (labels_stream.is_open())
    {
        while (getline(labels_stream, line))
        {
            std::stringstream iss(line);
            uint32_t lbl_cnt = 0;
            uint32_t label_host = point_cnt;
            while (getline(iss, token, ','))
            {
                if (lbl_cnt == density)
                {
                    if (label_host == point_cnt)
                        dense_pts++;
                    label_host = next_dummy_id;
                    labels_per_point.resize(next_dummy_id + 1);
                    dummy_pt_ids[next_dummy_id] = (uint32_t)point_cnt;
                    next_dummy_id++;
                    lbl_cnt = 0;
                }
                token.erase(std::remove(token.begin(), token.end(), '\n'), token.end());
                token.erase(std::remove(token.begin(), token.end(), '\r'), token.end());
                uint32_t token_as_num = std::stoul(token);
                labels_per_point[label_host].push_back(token_as_num);
                lbl_cnt++;
            }
            point_cnt++;
        }
    }
    diskann::cout << "fraction of dense points with >= " << density << " labels = " << (float)dense_pts / (float)npts
                  << std::endl;

    if (labels_per_point.size() != 0)
    {
        diskann::cout << labels_per_point.size() << " is the new number of points" << std::endl;
        std::ofstream label_writer(out_labels_file);
        assert(label_writer.is_open());
        for (uint32_t i = 0; i < labels_per_point.size(); i++)
        {
            for (uint32_t j = 0; j < (labels_per_point[i].size() - 1); j++)
            {
                label_writer << labels_per_point[i][j] << ",";
            }
            if (labels_per_point[i].size() != 0)
                label_writer << labels_per_point[i][labels_per_point[i].size() - 1];
            label_writer << std::endl;
        }
        label_writer.close();
    }

    if (dummy_pt_ids.size() != 0)
    {
        diskann::cout << dummy_pt_ids.size() << " is the number of dummy points created" << std::endl;

        T *ptr = (T *)std::realloc((void *)data, labels_per_point.size() * ndims * sizeof(T));
        if (ptr == nullptr)
        {
            diskann::cerr << "Realloc failed while creating dummy points" << std::endl;
            free(data);
            data = nullptr;
            throw new diskann::ANNException("Realloc failed while expanding data.", -1, __FUNCTION__, __FILE__,
                                            __LINE__);
        }
        else
        {
            data = ptr;
        }

        std::ofstream dummy_writer(out_metadata_file);
        assert(dummy_writer.is_open());
        for (auto i = dummy_pt_ids.begin(); i != dummy_pt_ids.end(); i++)
        {
            dummy_writer << i->first << "," << i->second << std::endl;
            std::memcpy(data + i->first * ndims, data + i->second * ndims, ndims * sizeof(T));
        }
        dummy_writer.close();
    }

    diskann::save_bin<T>(out_data_file, data, labels_per_point.size(), ndims);
}

void extract_shard_labels(const std::string &in_label_file, const std::string &shard_ids_bin,
                          const std::string &shard_label_file)
{ // assumes ith row is for ith
  // point in labels file
    diskann::cout << "Extracting labels for shard" << std::endl;

    uint32_t *ids = nullptr;
    uint64_t num_ids, tmp_dim;
    diskann::load_bin(shard_ids_bin, ids, num_ids, tmp_dim);

    uint32_t counter = 0, shard_counter = 0;
    std::string cur_line;

    std::ifstream label_reader(in_label_file);
    std::ofstream label_writer(shard_label_file);
    assert(label_reader.is_open());
    assert(label_reader.is_open());
    if (label_reader && label_writer)
    {
        while (std::getline(label_reader, cur_line))
        {
            if (shard_counter >= num_ids)
            {
                break;
            }
            if (counter == ids[shard_counter])
            {
                label_writer << cur_line << "\n";
                shard_counter++;
            }
            counter++;
        }
    }
    if (ids != nullptr)
        delete[] ids;
}

template <typename T, typename LabelT>
int build_merged_vamana_index(std::string base_file, diskann::Metric compareMetric, uint32_t L, uint32_t R,
                              double sampling_rate, double ram_budget, std::string mem_index_path,
                              std::string medoids_file, std::string centroids_file, size_t build_pq_bytes, bool use_opq,
                              uint32_t num_threads, bool use_filters, const std::string &label_file,
                              const std::string &labels_to_medoids_file, const std::string &universal_label,
                              const uint32_t Lf)
{
    size_t base_num, base_dim;
    diskann::get_bin_metadata(base_file, base_num, base_dim);

    double full_index_ram = estimate_ram_usage(base_num, (uint32_t)base_dim, sizeof(T), R);

    // TODO: Make this honest when there is filter support
    if (full_index_ram < ram_budget * 1024 * 1024 * 1024)
    {
        diskann::cout << "Full index fits in RAM budget, should consume at most "
                      << full_index_ram / (1024 * 1024 * 1024) << "GiBs, so building in one shot" << std::endl;

        diskann::IndexWriteParameters paras = diskann::IndexWriteParametersBuilder(L, R)
                                                  .with_filter_list_size(Lf)
                                                  .with_saturate_graph(!use_filters)
                                                  .with_num_threads(num_threads)
                                                  .build();
        using TagT = uint32_t;
        diskann::Index<T, TagT, LabelT> _index(compareMetric, base_dim, base_num,
                                               std::make_shared<diskann::IndexWriteParameters>(paras), nullptr,
                                               defaults::NUM_FROZEN_POINTS_STATIC, false, false, false,
                                               build_pq_bytes > 0, build_pq_bytes, use_opq, use_filters);
       
            _index.build(base_file.c_str(), base_num);
       
        _index.save(mem_index_path.c_str());

        if (use_filters)
        {
            // need to copy the labels_to_medoids file to the specified input
            // file
            std::remove(labels_to_medoids_file.c_str());
            std::string mem_labels_to_medoid_file = mem_index_path + "_labels_to_medoids.txt";
            copy_file(mem_labels_to_medoid_file, labels_to_medoids_file);
            std::remove(mem_labels_to_medoid_file.c_str());
        }

        std::remove(medoids_file.c_str());
        std::remove(centroids_file.c_str());
        return 0;
    }

    // where the universal label is to be saved in the final graph
    std::string final_index_universal_label_file = mem_index_path + "_universal_label.txt";

    std::string merged_index_prefix = mem_index_path + "_tempFiles";

    Timer timer;
    int num_parts =
        partition_with_ram_budget<T>(base_file, sampling_rate, ram_budget, 2 * R / 3, merged_index_prefix, 2);
    diskann::cout << timer.elapsed_seconds_for_step("partitioning data ") << std::endl;

    std::string cur_centroid_filepath = merged_index_prefix + "_centroids.bin";
    std::rename(cur_centroid_filepath.c_str(), centroids_file.c_str());

    timer.reset();
    for (int p = 0; p < num_parts; p++)
    {
#if defined(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS) && defined(DISKANN_BUILD)
        MallocExtension::instance()->ReleaseFreeMemory();
#endif

        std::string shard_base_file = merged_index_prefix + "_subshard-" + std::to_string(p) + ".bin";

        std::string shard_ids_file = merged_index_prefix + "_subshard-" + std::to_string(p) + "_ids_uint32.bin";

        std::string shard_labels_file = merged_index_prefix + "_subshard-" + std::to_string(p) + "_labels.txt";

        retrieve_shard_data_from_ids<T>(base_file, shard_ids_file, shard_base_file);

        std::string shard_index_file = merged_index_prefix + "_subshard-" + std::to_string(p) + "_mem.index";

        diskann::IndexWriteParameters low_degree_params = diskann::IndexWriteParametersBuilder(L, 2 * R / 3)
                                                              .with_filter_list_size(Lf)
                                                              .with_saturate_graph(false)
                                                              .with_num_threads(num_threads)
                                                              .build();

        uint64_t shard_base_dim, shard_base_pts;
        get_bin_metadata(shard_base_file, shard_base_pts, shard_base_dim);

        diskann::Index<T> _index(compareMetric, shard_base_dim, shard_base_pts,
                                 std::make_shared<diskann::IndexWriteParameters>(low_degree_params), nullptr,
                                 defaults::NUM_FROZEN_POINTS_STATIC, false, false, false, build_pq_bytes > 0,
                                 build_pq_bytes, use_opq);
        if (!use_filters)
        {
            _index.build(shard_base_file.c_str(), shard_base_pts);
        }
        else
        {
            diskann::extract_shard_labels(label_file, shard_ids_file, shard_labels_file);
            if (universal_label != "")
            { //  indicates no universal label
                LabelT unv_label_as_num = 0;
                _index.set_universal_label(unv_label_as_num);
            }
            _index.build_filtered_index(shard_base_file.c_str(), shard_labels_file, shard_base_pts);
        }
        _index.save(shard_index_file.c_str());
        // copy universal label file from first shard to the final destination
        // index, since all shards anyway share the universal label
        if (p == 0)
        {
            std::string shard_universal_label_file = shard_index_file + "_universal_label.txt";
            if (universal_label != "")
            {
                copy_file(shard_universal_label_file, final_index_universal_label_file);
            }
        }

        std::remove(shard_base_file.c_str());
    }
    diskann::cout << timer.elapsed_seconds_for_step("building indices on shards") << std::endl;

    timer.reset();
    diskann::merge_shards(merged_index_prefix + "_subshard-", "_mem.index", merged_index_prefix + "_subshard-",
                          "_ids_uint32.bin", num_parts, R, mem_index_path, medoids_file, use_filters,
                          labels_to_medoids_file);
    diskann::cout << timer.elapsed_seconds_for_step("merging indices") << std::endl;

    // delete tempFiles
    for (int p = 0; p < num_parts; p++)
    {
        std::string shard_base_file = merged_index_prefix + "_subshard-" + std::to_string(p) + ".bin";
        std::string shard_id_file = merged_index_prefix + "_subshard-" + std::to_string(p) + "_ids_uint32.bin";
        std::string shard_labels_file = merged_index_prefix + "_subshard-" + std::to_string(p) + "_labels.txt";
        std::string shard_index_file = merged_index_prefix + "_subshard-" + std::to_string(p) + "_mem.index";
        std::string shard_index_file_data = shard_index_file + ".data";

        std::remove(shard_base_file.c_str());
        std::remove(shard_id_file.c_str());
        std::remove(shard_index_file.c_str());
        std::remove(shard_index_file_data.c_str());
        if (use_filters)
        {
            std::string shard_index_label_file = shard_index_file + "_labels.txt";
            std::string shard_index_univ_label_file = shard_index_file + "_universal_label.txt";
            std::string shard_index_label_map_file = shard_index_file + "_labels_to_medoids.txt";
            std::remove(shard_labels_file.c_str());
            std::remove(shard_index_label_file.c_str());
            std::remove(shard_index_label_map_file.c_str());
            std::remove(shard_index_univ_label_file.c_str());
        }
    }
    return 0;
}

void print_opts_and_dim(std::stringstream &data_stream, const std::string &point) {
    int32_t nrows_32 = 0, ncols_32 = 0;
    data_stream.seekg(0, std::ios::beg);
    if (!data_stream.read(reinterpret_cast<char*>(&nrows_32), sizeof(int32_t)) ||
        !data_stream.read(reinterpret_cast<char*>(&ncols_32), sizeof(int32_t))) {
        std::cerr << "Error: Failed to read nrows_32 and ncols_32 from data_stream." << std::endl;
        return;
    }
    for(int i=0;i<nrows_32;i++){
        std::cout << "vec" << i <<"->";
        for(int j=0;j<ncols_32;j++){
            float vec;
            data_stream.read(reinterpret_cast<char*>(&vec), 4);
            std::cout << vec << ",";
        }
        std::cout << "\n";
    }
    // 恢复流状态，防止影响后续操作
    data_stream.clear();
    data_stream.seekg(0, std::ios::beg);
}

template <typename T, typename LabelT>
int build_merged_vamana_index(std::stringstream &data_stream, diskann::Metric compareMetric, uint32_t L, uint32_t R,
                              double sampling_rate, double ram_budget, std::stringstream &mem_index_stream,
                              std::string medoids_file, std::string centroids_file, size_t build_pq_bytes, bool use_opq,
                              uint32_t num_threads, bool use_filters, const std::string &label_file,
                              const std::string &labels_to_medoids_file, const std::string &universal_label,
                              const uint32_t Lf)
{
    size_t base_num, base_dim;
    data_stream.seekg(0, data_stream.beg);
    diskann::get_bin_metadata(data_stream, base_num, base_dim);
    data_stream.seekg(0, data_stream.beg);

    double full_index_ram = estimate_ram_usage(base_num, (uint32_t)base_dim, sizeof(T), R);
    diskann::cout << "Full index fits in RAM budget, should consume at most "
                    << full_index_ram / (1024 * 1024 * 1024) << "GiBs, so building in one shot" << std::endl;

    diskann::IndexWriteParameters paras = diskann::IndexWriteParametersBuilder(L, R)
                                                .with_filter_list_size(Lf)
                                                .with_saturate_graph(!use_filters)
                                                .with_num_threads(num_threads)
                                                .build();
    using TagT = uint32_t;
    diskann::Index<T, TagT, LabelT> _index(compareMetric, base_dim, base_num,
                                            std::make_shared<diskann::IndexWriteParameters>(paras), nullptr,
                                            defaults::NUM_FROZEN_POINTS_STATIC, false, false, false,
                                            build_pq_bytes > 0, build_pq_bytes, use_opq, use_filters);
    
    

    //需要优化
    float* train_data = new float[base_num * base_dim];  // 使用裸指针和 `new` 分配内存
    data_stream.seekg(8, data_stream.beg);
    data_stream.read(reinterpret_cast<char*>(train_data), base_num * base_dim * sizeof(float)); 

    std::vector<TagT> tags;
    _index.build(static_cast<const float*>(train_data), base_num, tags);
    _index.save(mem_index_stream);
    delete[] train_data;
    return 0;
}

// General purpose support for DiskANN interface

// optimizes the beamwidth to maximize QPS for a given L_search subject to
// 99.9 latency not blowing up
template <typename T, typename LabelT>
uint32_t optimize_beamwidth(std::unique_ptr<diskann::PQFlashIndex<T, LabelT>> &pFlashIndex, T *tuning_sample,
                            uint64_t tuning_sample_num, uint64_t tuning_sample_aligned_dim, uint32_t L,
                            uint32_t nthreads, uint32_t start_bw)
{
    uint32_t cur_bw = start_bw;
    double max_qps = 0;
    uint32_t best_bw = start_bw;
    bool stop_flag = false;

    while (!stop_flag)
    {
        std::vector<uint64_t> tuning_sample_result_ids_64(tuning_sample_num, 0);
        std::vector<float> tuning_sample_result_dists(tuning_sample_num, 0);
        diskann::QueryStats *stats = new diskann::QueryStats[tuning_sample_num];

        auto s = std::chrono::high_resolution_clock::now();
#pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
        for (int64_t i = 0; i < (int64_t)tuning_sample_num; i++)
        {
            pFlashIndex->cached_beam_search(tuning_sample + (i * tuning_sample_aligned_dim), 1, L,
                                            tuning_sample_result_ids_64.data() + (i * 1),
                                            tuning_sample_result_dists.data() + (i * 1), cur_bw, nullptr, stats + i);
        }
        auto e = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = e - s;
        double qps = (1.0f * (float)tuning_sample_num) / (1.0f * (float)diff.count());

        double lat_999 = diskann::get_percentile_stats<float>(
            stats, tuning_sample_num, 0.999f, [](const diskann::QueryStats &stats) { return stats.total_us; });

        double mean_latency = diskann::get_mean_stats<float>(
            stats, tuning_sample_num, [](const diskann::QueryStats &stats) { return stats.total_us; });

        if (qps > max_qps && lat_999 < (15000) + mean_latency * 2)
        {
            max_qps = qps;
            best_bw = cur_bw;
            cur_bw = (uint32_t)(std::ceil)((float)cur_bw * 1.1f);
        }
        else
        {
            stop_flag = true;
        }
        if (cur_bw > 64)
            stop_flag = true;

        delete[] stats;
    }
    return best_bw;
}

template <typename T>
void create_disk_layout(const std::string base_file, const std::string mem_index_file, const std::string output_file,
                        const std::string reorder_data_file)
{
    uint32_t npts, ndims;

    // amount to read or write in one shot
    size_t read_blk_size = 64 * 1024 * 1024;
    size_t write_blk_size = read_blk_size;
    cached_ifstream base_reader(base_file, read_blk_size);
    base_reader.read((char *)&npts, sizeof(uint32_t));
    base_reader.read((char *)&ndims, sizeof(uint32_t));

    size_t npts_64, ndims_64;
    npts_64 = npts;
    ndims_64 = ndims;

    // Check if we need to append data for re-ordering
    bool append_reorder_data = false;
    std::ifstream reorder_data_reader;

    uint32_t npts_reorder_file = 0, ndims_reorder_file = 0;
    if (reorder_data_file != std::string(""))
    {
        append_reorder_data = true;
        size_t reorder_data_file_size = get_file_size(reorder_data_file);
        reorder_data_reader.exceptions(std::ofstream::failbit | std::ofstream::badbit);

        try
        {
            reorder_data_reader.open(reorder_data_file, std::ios::binary);
            reorder_data_reader.read((char *)&npts_reorder_file, sizeof(uint32_t));
            reorder_data_reader.read((char *)&ndims_reorder_file, sizeof(uint32_t));
            if (npts_reorder_file != npts)
                throw ANNException("Mismatch in num_points between reorder "
                                   "data file and base file",
                                   -1, __FUNCSIG__, __FILE__, __LINE__);
            if (reorder_data_file_size != 8 + sizeof(float) * (size_t)npts_reorder_file * (size_t)ndims_reorder_file)
                throw ANNException("Discrepancy in reorder data file size ", -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        catch (std::system_error &e)
        {
            throw FileException(reorder_data_file, e, __FUNCSIG__, __FILE__, __LINE__);
        }
    }

    // create cached reader + writer
    size_t actual_file_size = get_file_size(mem_index_file);
    diskann::cout << "Vamana index file size=" << actual_file_size << std::endl;
    std::ifstream vamana_reader(mem_index_file, std::ios::binary);
    cached_ofstream diskann_writer(output_file, write_blk_size);

    // metadata: width, medoid
    uint32_t width_u32, medoid_u32;
    size_t index_file_size;

    vamana_reader.read((char *)&index_file_size, sizeof(uint64_t));
    if (index_file_size != actual_file_size)
    {
        std::stringstream stream;
        stream << "Vamana Index file size does not match expected size per "
                  "meta-data."
               << " file size from file: " << index_file_size << " actual file size: " << actual_file_size << std::endl;

        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    uint64_t vamana_frozen_num = false, vamana_frozen_loc = 0;

    vamana_reader.read((char *)&width_u32, sizeof(uint32_t));
    vamana_reader.read((char *)&medoid_u32, sizeof(uint32_t));
    vamana_reader.read((char *)&vamana_frozen_num, sizeof(uint64_t));
    // compute
    uint64_t medoid, max_node_len, nnodes_per_sector;
    npts_64 = (uint64_t)npts;
    medoid = (uint64_t)medoid_u32;
    if (vamana_frozen_num == 1)
        vamana_frozen_loc = medoid;
    max_node_len = (((uint64_t)width_u32 + 1) * sizeof(uint32_t)) + (ndims_64 * sizeof(T));
    nnodes_per_sector = defaults::SECTOR_LEN / max_node_len; // 0 if max_node_len > SECTOR_LEN

    diskann::cout << "medoid: " << medoid << "B" << std::endl;
    diskann::cout << "max_node_len: " << max_node_len << "B" << std::endl;
    diskann::cout << "nnodes_per_sector: " << nnodes_per_sector << "B" << std::endl;

    // defaults::SECTOR_LEN buffer for each sector
    std::unique_ptr<char[]> sector_buf = std::make_unique<char[]>(defaults::SECTOR_LEN);
    std::unique_ptr<char[]> multisector_buf = std::make_unique<char[]>(ROUND_UP(max_node_len, defaults::SECTOR_LEN));
    std::unique_ptr<char[]> node_buf = std::make_unique<char[]>(max_node_len);
    uint32_t &nnbrs = *(uint32_t *)(node_buf.get() + ndims_64 * sizeof(T));
    uint32_t *nhood_buf = (uint32_t *)(node_buf.get() + (ndims_64 * sizeof(T)) + sizeof(uint32_t));

    // number of sectors (1 for meta data)
    uint64_t n_sectors = nnodes_per_sector > 0 ? ROUND_UP(npts_64, nnodes_per_sector) / nnodes_per_sector
                                               : npts_64 * DIV_ROUND_UP(max_node_len, defaults::SECTOR_LEN);
    uint64_t n_reorder_sectors = 0;
    uint64_t n_data_nodes_per_sector = 0;

    if (append_reorder_data)
    {
        n_data_nodes_per_sector = defaults::SECTOR_LEN / (ndims_reorder_file * sizeof(float));
        n_reorder_sectors = ROUND_UP(npts_64, n_data_nodes_per_sector) / n_data_nodes_per_sector;
    }
    uint64_t disk_index_file_size = (n_sectors + n_reorder_sectors + 1) * defaults::SECTOR_LEN;

    std::vector<uint64_t> output_file_meta;
    output_file_meta.push_back(npts_64);
    output_file_meta.push_back(ndims_64);
    output_file_meta.push_back(medoid);
    output_file_meta.push_back(max_node_len);
    output_file_meta.push_back(nnodes_per_sector);
    output_file_meta.push_back(vamana_frozen_num);
    output_file_meta.push_back(vamana_frozen_loc);
    output_file_meta.push_back((uint64_t)append_reorder_data);
    if (append_reorder_data)
    {
        output_file_meta.push_back(n_sectors + 1);
        output_file_meta.push_back(ndims_reorder_file);
        output_file_meta.push_back(n_data_nodes_per_sector);
    }
    output_file_meta.push_back(disk_index_file_size);

    diskann_writer.write(sector_buf.get(), defaults::SECTOR_LEN);

    std::unique_ptr<T[]> cur_node_coords = std::make_unique<T[]>(ndims_64);
    diskann::cout << "# sectors: " << n_sectors << std::endl;
    uint64_t cur_node_id = 0;

    if (nnodes_per_sector > 0)
    { // Write multiple nodes per sector
        for (uint64_t sector = 0; sector < n_sectors; sector++)
        {
            if (sector % 100000 == 0)
            {
                diskann::cout << "Sector #" << sector << "written" << std::endl;
            }
            memset(sector_buf.get(), 0, defaults::SECTOR_LEN);
            for (uint64_t sector_node_id = 0; sector_node_id < nnodes_per_sector && cur_node_id < npts_64;
                 sector_node_id++)
            {
                memset(node_buf.get(), 0, max_node_len);
                // read cur node's nnbrs
                vamana_reader.read((char *)&nnbrs, sizeof(uint32_t));

                // sanity checks on nnbrs
                assert(nnbrs > 0);
                assert(nnbrs <= width_u32);

                // read node's nhood
                vamana_reader.read((char *)nhood_buf, (std::min)(nnbrs, width_u32) * sizeof(uint32_t));
                if (nnbrs > width_u32)
                {
                    vamana_reader.seekg((nnbrs - width_u32) * sizeof(uint32_t), vamana_reader.cur);
                }

                // write coords of node first
                //  T *node_coords = data + ((uint64_t) ndims_64 * cur_node_id);
                base_reader.read((char *)cur_node_coords.get(), sizeof(T) * ndims_64);
                memcpy(node_buf.get(), cur_node_coords.get(), ndims_64 * sizeof(T));

                // write nnbrs
                *(uint32_t *)(node_buf.get() + ndims_64 * sizeof(T)) = (std::min)(nnbrs, width_u32);

                // write nhood next
                memcpy(node_buf.get() + ndims_64 * sizeof(T) + sizeof(uint32_t), nhood_buf,
                       (std::min)(nnbrs, width_u32) * sizeof(uint32_t));

                // get offset into sector_buf
                char *sector_node_buf = sector_buf.get() + (sector_node_id * max_node_len);

                // copy node buf into sector_node_buf
                memcpy(sector_node_buf, node_buf.get(), max_node_len);
                cur_node_id++;
            }
            // flush sector to disk
            diskann_writer.write(sector_buf.get(), defaults::SECTOR_LEN);
        }
    }
    else
    { // Write multi-sector nodes
        uint64_t nsectors_per_node = DIV_ROUND_UP(max_node_len, defaults::SECTOR_LEN);
        for (uint64_t i = 0; i < npts_64; i++)
        {
            if ((i * nsectors_per_node) % 100000 == 0)
            {
                diskann::cout << "Sector #" << i * nsectors_per_node << "written" << std::endl;
            }
            memset(multisector_buf.get(), 0, nsectors_per_node * defaults::SECTOR_LEN);

            memset(node_buf.get(), 0, max_node_len);
            // read cur node's nnbrs
            vamana_reader.read((char *)&nnbrs, sizeof(uint32_t));

            // sanity checks on nnbrs
            assert(nnbrs > 0);
            assert(nnbrs <= width_u32);

            // read node's nhood
            vamana_reader.read((char *)nhood_buf, (std::min)(nnbrs, width_u32) * sizeof(uint32_t));
            if (nnbrs > width_u32)
            {
                vamana_reader.seekg((nnbrs - width_u32) * sizeof(uint32_t), vamana_reader.cur);
            }

            // write coords of node first
            //  T *node_coords = data + ((uint64_t) ndims_64 * cur_node_id);
            base_reader.read((char *)cur_node_coords.get(), sizeof(T) * ndims_64);
            memcpy(multisector_buf.get(), cur_node_coords.get(), ndims_64 * sizeof(T));

            // write nnbrs
            *(uint32_t *)(multisector_buf.get() + ndims_64 * sizeof(T)) = (std::min)(nnbrs, width_u32);

            // write nhood next
            memcpy(multisector_buf.get() + ndims_64 * sizeof(T) + sizeof(uint32_t), nhood_buf,
                   (std::min)(nnbrs, width_u32) * sizeof(uint32_t));

            // flush sector to disk
            diskann_writer.write(multisector_buf.get(), nsectors_per_node * defaults::SECTOR_LEN);
        }
    }

    if (append_reorder_data)
    {
        diskann::cout << "Index written. Appending reorder data..." << std::endl;

        auto vec_len = ndims_reorder_file * sizeof(float);
        std::unique_ptr<char[]> vec_buf = std::make_unique<char[]>(vec_len);

        for (uint64_t sector = 0; sector < n_reorder_sectors; sector++)
        {
            if (sector % 100000 == 0)
            {
                diskann::cout << "Reorder data Sector #" << sector << "written" << std::endl;
            }

            memset(sector_buf.get(), 0, defaults::SECTOR_LEN);

            for (uint64_t sector_node_id = 0; sector_node_id < n_data_nodes_per_sector && sector_node_id < npts_64;
                 sector_node_id++)
            {
                memset(vec_buf.get(), 0, vec_len);
                reorder_data_reader.read(vec_buf.get(), vec_len);

                // copy node buf into sector_node_buf
                memcpy(sector_buf.get() + (sector_node_id * vec_len), vec_buf.get(), vec_len);
            }
            // flush sector to disk
            diskann_writer.write(sector_buf.get(), defaults::SECTOR_LEN);
        }
    }
    diskann_writer.close();
    diskann::save_bin<uint64_t>(output_file, output_file_meta.data(), output_file_meta.size(), 1, 0);
    diskann::cout << "Output disk index file written to " << output_file << std::endl;
}

template <typename T>
void create_disk_layout(std::stringstream &_data_stream, std::stringstream &vamana_index_stream, std::stringstream &disklayout_stream,
                        const std::string reorder_data_file)
{
    uint32_t npts, ndims;

    // amount to read or write in one shot
    size_t read_blk_size = 64 * 1024 * 1024;
    size_t write_blk_size = read_blk_size;
    _data_stream.read((char *)&npts, sizeof(uint32_t));
    _data_stream.read((char *)&ndims, sizeof(uint32_t));

    size_t npts_64, ndims_64;
    npts_64 = npts;
    ndims_64 = ndims;

    // Check if we need to append data for re-ordering
    bool append_reorder_data = false;
    std::ifstream reorder_data_reader;

    uint32_t npts_reorder_file = 0, ndims_reorder_file = 0;
    // if (reorder_data_file != std::string(""))
    // {
    //     append_reorder_data = true;
    //     size_t reorder_data_file_size = get_file_size(reorder_data_file);
    //     reorder_data_reader.exceptions(std::ofstream::failbit | std::ofstream::badbit);

    //     try
    //     {
    //         reorder_data_reader.open(reorder_data_file, std::ios::binary);
    //         reorder_data_reader.read((char *)&npts_reorder_file, sizeof(uint32_t));
    //         reorder_data_reader.read((char *)&ndims_reorder_file, sizeof(uint32_t));
    //         if (npts_reorder_file != npts)
    //             throw ANNException("Mismatch in num_points between reorder "
    //                                "data file and base file",
    //                                -1, __FUNCSIG__, __FILE__, __LINE__);
    //         if (reorder_data_file_size != 8 + sizeof(float) * (size_t)npts_reorder_file * (size_t)ndims_reorder_file)
    //             throw ANNException("Discrepancy in reorder data file size ", -1, __FUNCSIG__, __FILE__, __LINE__);
    //     }
    //     catch (std::system_error &e)
    //     {
    //         throw FileException(reorder_data_file, e, __FUNCSIG__, __FILE__, __LINE__);
    //     }
    // }

    // create cached reader + writer
    // size_t actual_file_size = get_file_size(mem_index_file);
    // diskann::cout << "Vamana index file size=" << actual_file_size << std::endl;
    // std::ifstream vamana_reader(mem_index_file, std::ios::binary);
    // cached_ofstream diskann_writer(output_file, write_blk_size);

    // metadata: width, medoid
    uint32_t width_u32, medoid_u32;
    size_t index_file_size;

    // vamana_reader.read((char *)&index_file_size, sizeof(uint64_t));
    // if (index_file_size != actual_file_size)
    // {
    //     std::stringstream stream;
    //     stream << "Vamana Index file size does not match expected size per "
    //               "meta-data."
    //            << " file size from file: " << index_file_size << " actual file size: " << actual_file_size << std::endl;

    //     throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    // }
    uint64_t vamana_frozen_num = false, vamana_frozen_loc = 0;
    vamana_index_stream.read((char *)&index_file_size, sizeof(size_t));
    vamana_index_stream.read((char *)&width_u32, sizeof(uint32_t));
    vamana_index_stream.read((char *)&medoid_u32, sizeof(uint32_t));
    vamana_index_stream.read((char *)&vamana_frozen_num, sizeof(uint64_t));
    // compute
    uint64_t medoid, max_node_len, nnodes_per_sector;
    npts_64 = (uint64_t)npts;
    medoid = (uint64_t)medoid_u32;
    if (vamana_frozen_num == 1)
        vamana_frozen_loc = medoid;
    max_node_len = (((uint64_t)width_u32 + 1) * sizeof(uint32_t)) + (ndims_64 * sizeof(T));
    nnodes_per_sector = defaults::SECTOR_LEN / max_node_len; // 0 if max_node_len > SECTOR_LEN

    diskann::cout << "medoid: " << medoid << "B" << std::endl;
    diskann::cout << "max_node_len: " << max_node_len << "B" << std::endl;
    diskann::cout << "nnodes_per_sector: " << nnodes_per_sector << "B" << std::endl;

    // defaults::SECTOR_LEN buffer for each sector
    std::unique_ptr<char[]> sector_buf = std::make_unique<char[]>(defaults::SECTOR_LEN);
    std::unique_ptr<char[]> multisector_buf = std::make_unique<char[]>(ROUND_UP(max_node_len, defaults::SECTOR_LEN));
    std::unique_ptr<char[]> node_buf = std::make_unique<char[]>(max_node_len);
    uint32_t &nnbrs = *(uint32_t *)(node_buf.get() + ndims_64 * sizeof(T));
    uint32_t *nhood_buf = (uint32_t *)(node_buf.get() + (ndims_64 * sizeof(T)) + sizeof(uint32_t));

    // number of sectors (1 for meta data)
    uint64_t n_sectors = nnodes_per_sector > 0 ? ROUND_UP(npts_64, nnodes_per_sector) / nnodes_per_sector
                                               : npts_64 * DIV_ROUND_UP(max_node_len, defaults::SECTOR_LEN);
    uint64_t n_reorder_sectors = 0;
    uint64_t n_data_nodes_per_sector = 0;

    if (append_reorder_data)
    {
        n_data_nodes_per_sector = defaults::SECTOR_LEN / (ndims_reorder_file * sizeof(float));
        n_reorder_sectors = ROUND_UP(npts_64, n_data_nodes_per_sector) / n_data_nodes_per_sector;
    }
    uint64_t disk_index_file_size = (n_sectors + n_reorder_sectors + 1) * defaults::SECTOR_LEN;

    std::vector<uint64_t> output_file_meta;
    output_file_meta.push_back(npts_64);
    output_file_meta.push_back(ndims_64);
    output_file_meta.push_back(medoid);
    output_file_meta.push_back(max_node_len);
    output_file_meta.push_back(nnodes_per_sector);
    output_file_meta.push_back(vamana_frozen_num);
    output_file_meta.push_back(vamana_frozen_loc);
    output_file_meta.push_back((uint64_t)append_reorder_data);
    if (append_reorder_data)
    {
        output_file_meta.push_back(n_sectors + 1);
        output_file_meta.push_back(ndims_reorder_file);
        output_file_meta.push_back(n_data_nodes_per_sector);
    }
    output_file_meta.push_back(disk_index_file_size);

    disklayout_stream.write(sector_buf.get(), defaults::SECTOR_LEN);

    std::unique_ptr<T[]> cur_node_coords = std::make_unique<T[]>(ndims_64);
    diskann::cout << "# sectors: " << n_sectors << std::endl;
    uint64_t cur_node_id = 0;

    if (nnodes_per_sector > 0)
    { // Write multiple nodes per sector
        for (uint64_t sector = 0; sector < n_sectors; sector++)
        {
            if (sector % 100000 == 0)
            {
                diskann::cout << "Sector #" << sector << "written" << std::endl;
            }
            memset(sector_buf.get(), 0, defaults::SECTOR_LEN);
            for (uint64_t sector_node_id = 0; sector_node_id < nnodes_per_sector && cur_node_id < npts_64;
                 sector_node_id++)
            {
                memset(node_buf.get(), 0, max_node_len);
                // read cur node's nnbrs
                vamana_index_stream.read((char *)&nnbrs, sizeof(uint32_t));

                // sanity checks on nnbrs
                assert(nnbrs > 0);
                assert(nnbrs <= width_u32);

                // read node's nhood
                vamana_index_stream.read((char *)nhood_buf, (std::min)(nnbrs, width_u32) * sizeof(uint32_t));
                if (nnbrs > width_u32)
                {
                    vamana_index_stream.seekg((nnbrs - width_u32) * sizeof(uint32_t), vamana_index_stream.cur);
                }

                // write coords of node first
                //  T *node_coords = data + ((uint64_t) ndims_64 * cur_node_id);
                _data_stream.read((char *)cur_node_coords.get(), sizeof(T) * ndims_64);
                memcpy(node_buf.get(), cur_node_coords.get(), ndims_64 * sizeof(T));

                // write nnbrs
                *(uint32_t *)(node_buf.get() + ndims_64 * sizeof(T)) = (std::min)(nnbrs, width_u32);

                // write nhood next
                memcpy(node_buf.get() + ndims_64 * sizeof(T) + sizeof(uint32_t), nhood_buf,
                       (std::min)(nnbrs, width_u32) * sizeof(uint32_t));

                // get offset into sector_buf
                char *sector_node_buf = sector_buf.get() + (sector_node_id * max_node_len);

                // copy node buf into sector_node_buf
                memcpy(sector_node_buf, node_buf.get(), max_node_len);
                cur_node_id++;
            }
            // flush sector to disk
            disklayout_stream.write(sector_buf.get(), defaults::SECTOR_LEN);
        }
    }
    else
    { // Write multi-sector nodes
        uint64_t nsectors_per_node = DIV_ROUND_UP(max_node_len, defaults::SECTOR_LEN);
        for (uint64_t i = 0; i < npts_64; i++)
        {
            if ((i * nsectors_per_node) % 100000 == 0)
            {
                diskann::cout << "Sector #" << i * nsectors_per_node << "written" << std::endl;
            }
            memset(multisector_buf.get(), 0, nsectors_per_node * defaults::SECTOR_LEN);

            memset(node_buf.get(), 0, max_node_len);
            // read cur node's nnbrs
            vamana_index_stream.read((char *)&nnbrs, sizeof(uint32_t));

            // sanity checks on nnbrs
            assert(nnbrs > 0);
            assert(nnbrs <= width_u32);

            // read node's nhood
            vamana_index_stream.read((char *)nhood_buf, (std::min)(nnbrs, width_u32) * sizeof(uint32_t));
            if (nnbrs > width_u32)
            {
                vamana_index_stream.seekg((nnbrs - width_u32) * sizeof(uint32_t), vamana_index_stream.cur);
            }

            // write coords of node first
            //  T *node_coords = data + ((uint64_t) ndims_64 * cur_node_id);
            _data_stream.read((char *)cur_node_coords.get(), sizeof(T) * ndims_64);
            memcpy(multisector_buf.get(), cur_node_coords.get(), ndims_64 * sizeof(T));

            // write nnbrs
            *(uint32_t *)(multisector_buf.get() + ndims_64 * sizeof(T)) = (std::min)(nnbrs, width_u32);

            // write nhood next
            memcpy(multisector_buf.get() + ndims_64 * sizeof(T) + sizeof(uint32_t), nhood_buf,
                   (std::min)(nnbrs, width_u32) * sizeof(uint32_t));

            // flush sector to disk
            disklayout_stream.write(multisector_buf.get(), nsectors_per_node * defaults::SECTOR_LEN);
        }
    }

    if (append_reorder_data)
    {
        diskann::cout << "Index written. Appending reorder data..." << std::endl;

        auto vec_len = ndims_reorder_file * sizeof(float);
        std::unique_ptr<char[]> vec_buf = std::make_unique<char[]>(vec_len);

        for (uint64_t sector = 0; sector < n_reorder_sectors; sector++)
        {
            if (sector % 100000 == 0)
            {
                diskann::cout << "Reorder data Sector #" << sector << "written" << std::endl;
            }

            memset(sector_buf.get(), 0, defaults::SECTOR_LEN);

            for (uint64_t sector_node_id = 0; sector_node_id < n_data_nodes_per_sector && sector_node_id < npts_64;
                 sector_node_id++)
            {
                memset(vec_buf.get(), 0, vec_len);
                reorder_data_reader.read(vec_buf.get(), vec_len);

                // copy node buf into sector_node_buf
                memcpy(sector_buf.get() + (sector_node_id * vec_len), vec_buf.get(), vec_len);
            }
            // flush sector to disk
            disklayout_stream.write(sector_buf.get(), defaults::SECTOR_LEN);
        }
    }
    // diskann_writer.close();
    diskann::save_bin<uint64_t>(disklayout_stream, output_file_meta.data(), output_file_meta.size(), 1, 0);
    //diskann::cout << "Output disk index file written to " << output_file << std::endl;
}

template <typename T, typename LabelT>
int build_disk_index(const char *dataFilePath, const char *indexFilePath, const char *indexBuildParameters,
                     diskann::Metric compareMetric, bool use_opq, const std::string &codebook_prefix, bool use_filters,
                     const std::string &label_file, const std::string &universal_label, const uint32_t filter_threshold,
                     const uint32_t Lf)
{
    std::stringstream parser;
    parser << std::string(indexBuildParameters);
    std::string cur_param;
    std::vector<std::string> param_list;
    while (parser >> cur_param)
    {
        param_list.push_back(cur_param);
    }
    if (param_list.size() < 5 || param_list.size() > 9)
    {
        diskann::cout << "Correct usage of parameters is R (max degree)\n"
                         "L (indexing list size, better if >= R)\n"
                         "B (RAM limit of final index in GB)\n"
                         "M (memory limit while indexing)\n"
                         "T (number of threads for indexing)\n"
                         "B' (PQ bytes for disk index: optional parameter for "
                         "very large dimensional data)\n"
                         "reorder (set true to include full precision in data file"
                         ": optional paramter, use only when using disk PQ\n"
                         "build_PQ_byte (number of PQ bytes for inde build; set 0 to use "
                         "full precision vectors)\n"
                         "QD Quantized Dimension to overwrite the derived dim from B "
                      << std::endl;
        return -1;
    }

    if (!std::is_same<T, float>::value &&
        (compareMetric == diskann::Metric::INNER_PRODUCT || compareMetric == diskann::Metric::COSINE))
    {
        std::stringstream stream;
        stream << "Disk-index build currently only supports floating point data for Max "
                  "Inner Product Search/ cosine similarity. "
               << std::endl;
        throw diskann::ANNException(stream.str(), -1);
    }

    size_t disk_pq_dims = 0;
    bool use_disk_pq = false;
    size_t build_pq_bytes = 0;

    // if there is a 6th parameter, it means we compress the disk index
    // vectors also using PQ data (for very large dimensionality data). If the
    // provided parameter is 0, it means we store full vectors.
    if (param_list.size() > 5)
    {
        disk_pq_dims = atoi(param_list[5].c_str());
        use_disk_pq = true;
        if (disk_pq_dims == 0)
            use_disk_pq = false;
    }

    bool reorder_data = false;
    if (param_list.size() >= 7)
    {
        if (1 == atoi(param_list[6].c_str()))
        {
            reorder_data = true;
        }
    }

    if (param_list.size() >= 8)
    {
        build_pq_bytes = atoi(param_list[7].c_str());
    }

    std::string base_file(dataFilePath);
    std::string data_file_to_use = base_file;
    std::string labels_file_original = label_file;
    std::string index_prefix_path(indexFilePath);
    std::string labels_file_to_use = index_prefix_path + "_label_formatted.txt";
    std::string pq_pivots_path_base = codebook_prefix;
    std::string pq_pivots_path = file_exists(pq_pivots_path_base) ? pq_pivots_path_base + "_pq_pivots.bin"
                                                                  : index_prefix_path + "_pq_pivots.bin";
    std::string pq_compressed_vectors_path = index_prefix_path + "_pq_compressed.bin";
    std::string mem_index_path = index_prefix_path + "_mem.index";
    std::string disk_index_path = index_prefix_path + "_disk.index";
    std::string medoids_path = disk_index_path + "_medoids.bin";
    std::string centroids_path = disk_index_path + "_centroids.bin";

    std::string labels_to_medoids_path = disk_index_path + "_labels_to_medoids.txt";
    std::string mem_labels_file = mem_index_path + "_labels.txt";
    std::string disk_labels_file = disk_index_path + "_labels.txt";
    std::string mem_univ_label_file = mem_index_path + "_universal_label.txt";
    std::string disk_univ_label_file = disk_index_path + "_universal_label.txt";
    std::string disk_labels_int_map_file = disk_index_path + "_labels_map.txt";
    std::string dummy_remap_file = disk_index_path + "_dummy_map.txt"; // remap will be used if we break-up points of
                                                                       // high label-density to create copies

    std::string sample_base_prefix = index_prefix_path + "_sample";
    // optional, used if disk index file must store pq data
    std::string disk_pq_pivots_path = index_prefix_path + "_disk.index_pq_pivots.bin";
    // optional, used if disk index must store pq data
    std::string disk_pq_compressed_vectors_path = index_prefix_path + "_disk.index_pq_compressed.bin";
    std::string prepped_base =
        index_prefix_path +
        "_prepped_base.bin"; // temp file for storing pre-processed base file for cosine/ mips metrics
    bool created_temp_file_for_processed_data = false;

    // output a new base file which contains extra dimension with sqrt(1 -
    // ||x||^2/M^2) for every x, M is max norm of all points. Extra space on
    // disk needed!
    if (compareMetric == diskann::Metric::INNER_PRODUCT)
    {
        Timer timer;
        std::cout << "Using Inner Product search, so need to pre-process base "
                     "data into temp file. Please ensure there is additional "
                     "(n*(d+1)*4) bytes for storing pre-processed base vectors, "
                     "apart from the interim indices created by DiskANN and the final index."
                  << std::endl;
        data_file_to_use = prepped_base;
        float max_norm_of_base = diskann::prepare_base_for_inner_products<T>(base_file, prepped_base);
        std::string norm_file = disk_index_path + "_max_base_norm.bin";
        diskann::save_bin<float>(norm_file, &max_norm_of_base, 1, 1);
        diskann::cout << timer.elapsed_seconds_for_step("preprocessing data for inner product") << std::endl;
        created_temp_file_for_processed_data = true;
    }
    else if (compareMetric == diskann::Metric::COSINE)
    {
        Timer timer;
        std::cout << "Normalizing data for cosine to temporary file, please ensure there is additional "
                     "(n*d*4) bytes for storing normalized base vectors, "
                     "apart from the interim indices created by DiskANN and the final index."
                  << std::endl;
        data_file_to_use = prepped_base;
        diskann::normalize_data_file(base_file, prepped_base);
        diskann::cout << timer.elapsed_seconds_for_step("preprocessing data for cosine") << std::endl;
        created_temp_file_for_processed_data = true;
    }

    uint32_t R = (uint32_t)atoi(param_list[0].c_str());
    uint32_t L = (uint32_t)atoi(param_list[1].c_str());

    double final_index_ram_limit = get_memory_budget(param_list[2]);
    if (final_index_ram_limit <= 0)
    {
        std::cerr << "Insufficient memory budget (or string was not in right "
                     "format). Should be > 0."
                  << std::endl;
        return -1;
    }
    double indexing_ram_budget = (float)atof(param_list[3].c_str());
    if (indexing_ram_budget <= 0)
    {
        std::cerr << "Not building index. Please provide more RAM budget" << std::endl;
        return -1;
    }
    uint32_t num_threads = (uint32_t)atoi(param_list[4].c_str());

    if (num_threads != 0)
    {
        omp_set_num_threads(num_threads);
        mkl_set_num_threads(num_threads);
    }

    diskann::cout << "Starting index build: R=" << R << " L=" << L << " Query RAM budget: " << final_index_ram_limit
                  << " Indexing ram budget: " << indexing_ram_budget << " T: " << num_threads << std::endl;

    auto s = std::chrono::high_resolution_clock::now();

    // If there is filter support, we break-up points which have too many labels
    // into replica dummy points which evenly distribute the filters. The rest
    // of index build happens on the augmented base and labels
    std::string augmented_data_file, augmented_labels_file;
    if (use_filters)
    {
        convert_labels_string_to_int(labels_file_original, labels_file_to_use, disk_labels_int_map_file,
                                     universal_label);
        augmented_data_file = index_prefix_path + "_augmented_data.bin";
        augmented_labels_file = index_prefix_path + "_augmented_labels.txt";
        if (filter_threshold != 0)
        {
            breakup_dense_points<T>(data_file_to_use, labels_file_to_use, filter_threshold, augmented_data_file,
                                    augmented_labels_file,
                                    dummy_remap_file); // RKNOTE: This has large memory footprint,
                                                       // need to make this streaming
            data_file_to_use = augmented_data_file;
            labels_file_to_use = augmented_labels_file;
        }
    }

    size_t points_num, dim;

    Timer timer;
    diskann::get_bin_metadata(data_file_to_use.c_str(), points_num, dim);
    const double p_val = ((double)MAX_PQ_TRAINING_SET_SIZE / (double)points_num);

    if (use_disk_pq)
    {
        generate_disk_quantized_data<T>(data_file_to_use, disk_pq_pivots_path, disk_pq_compressed_vectors_path,
                                        compareMetric, p_val, disk_pq_dims);
    }
    size_t num_pq_chunks = (size_t)(std::floor)(uint64_t(final_index_ram_limit / points_num));

    num_pq_chunks = num_pq_chunks <= 0 ? 1 : num_pq_chunks;
    num_pq_chunks = num_pq_chunks > dim ? dim : num_pq_chunks;
    num_pq_chunks = num_pq_chunks > MAX_PQ_CHUNKS ? MAX_PQ_CHUNKS : num_pq_chunks;

    if (param_list.size() >= 9 && atoi(param_list[8].c_str()) <= MAX_PQ_CHUNKS && atoi(param_list[8].c_str()) > 0)
    {
        std::cout << "Use quantized dimension (QD) to overwrite derived quantized "
                     "dimension from search_DRAM_budget (B)"
                  << std::endl;
        num_pq_chunks = atoi(param_list[8].c_str());
    }

    diskann::cout << "Compressing " << dim << "-dimensional data into " << num_pq_chunks << " bytes per vector."
                  << std::endl;

    generate_quantized_data<T>(data_file_to_use, pq_pivots_path, pq_compressed_vectors_path, compareMetric, p_val,
                               num_pq_chunks, use_opq, codebook_prefix);
    diskann::cout << timer.elapsed_seconds_for_step("generating quantized data") << std::endl;

// Gopal. Splitting diskann_dll into separate DLLs for search and build.
// This code should only be available in the "build" DLL.
#if defined(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS) && defined(DISKANN_BUILD)
    MallocExtension::instance()->ReleaseFreeMemory();
#endif
    // Whether it is cosine or inner product, we still L2 metric due to the pre-processing.
    timer.reset();
    diskann::build_merged_vamana_index<T, LabelT>(data_file_to_use.c_str(), diskann::Metric::L2, L, R, p_val,
                                                  indexing_ram_budget, mem_index_path, medoids_path, centroids_path,
                                                  build_pq_bytes, use_opq, num_threads, use_filters, labels_file_to_use,
                                                  labels_to_medoids_path, universal_label, Lf);
    diskann::cout << timer.elapsed_seconds_for_step("building merged vamana index") << std::endl;

    timer.reset();
    if (!use_disk_pq)
    {
        diskann::create_disk_layout<T>(data_file_to_use.c_str(), mem_index_path, disk_index_path);
    }
    else
    {
        if (!reorder_data)
            diskann::create_disk_layout<uint8_t>(disk_pq_compressed_vectors_path, mem_index_path, disk_index_path);
        else
            diskann::create_disk_layout<uint8_t>(disk_pq_compressed_vectors_path, mem_index_path, disk_index_path,
                                                 data_file_to_use.c_str());
    }
    diskann::cout << timer.elapsed_seconds_for_step("generating disk layout") << std::endl;

    double ten_percent_points = std::ceil(points_num * 0.1);
    double num_sample_points =
        ten_percent_points > MAX_SAMPLE_POINTS_FOR_WARMUP ? MAX_SAMPLE_POINTS_FOR_WARMUP : ten_percent_points;
    double sample_sampling_rate = num_sample_points / points_num;
    gen_random_slice<T>(data_file_to_use.c_str(), sample_base_prefix, sample_sampling_rate);
    if (use_filters)
    {
        copy_file(labels_file_to_use, disk_labels_file);
        std::remove(mem_labels_file.c_str());
        if (universal_label != "")
        {
            copy_file(mem_univ_label_file, disk_univ_label_file);
            std::remove(mem_univ_label_file.c_str());
        }
        std::remove(augmented_data_file.c_str());
        std::remove(augmented_labels_file.c_str());
        std::remove(labels_file_to_use.c_str());
    }
    if (created_temp_file_for_processed_data)
        std::remove(prepped_base.c_str());
    std::remove(mem_index_path.c_str());
    std::remove((mem_index_path + ".data").c_str());
    std::remove((mem_index_path + ".tags").c_str());
    if (use_disk_pq)
        std::remove(disk_pq_compressed_vectors_path.c_str());

    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = e - s;
    diskann::cout << "Indexing time: " << diff.count() << std::endl;

    return 0;
}

template DISKANN_DLLEXPORT void create_disk_layout<int8_t>(const std::string base_file,
                                                           const std::string mem_index_file,
                                                           const std::string output_file,
                                                           const std::string reorder_data_file);
template DISKANN_DLLEXPORT void create_disk_layout<uint8_t>(const std::string base_file,
                                                            const std::string mem_index_file,
                                                            const std::string output_file,
                                                            const std::string reorder_data_file);
template DISKANN_DLLEXPORT void create_disk_layout<float>(const std::string base_file, const std::string mem_index_file,
                                                          const std::string output_file,
                                                          const std::string reorder_data_file);


template DISKANN_DLLEXPORT void create_disk_layout<float>(std::stringstream &  data_stream, std::stringstream &index_stream,
                                          std::stringstream &disklayout_stream,
                                          const std::string reorder_data_file = std::string(""));

template DISKANN_DLLEXPORT int8_t *load_warmup<int8_t>(const std::string &cache_warmup_file, uint64_t &warmup_num,
                                                       uint64_t warmup_dim, uint64_t warmup_aligned_dim);
template DISKANN_DLLEXPORT uint8_t *load_warmup<uint8_t>(const std::string &cache_warmup_file, uint64_t &warmup_num,
                                                         uint64_t warmup_dim, uint64_t warmup_aligned_dim);
template DISKANN_DLLEXPORT float *load_warmup<float>(const std::string &cache_warmup_file, uint64_t &warmup_num,
                                                     uint64_t warmup_dim, uint64_t warmup_aligned_dim);

#ifdef EXEC_ENV_OLS
template DISKANN_DLLEXPORT int8_t *load_warmup<int8_t>(MemoryMappedFiles &files, const std::string &cache_warmup_file,
                                                       uint64_t &warmup_num, uint64_t warmup_dim,
                                                       uint64_t warmup_aligned_dim);
template DISKANN_DLLEXPORT uint8_t *load_warmup<uint8_t>(MemoryMappedFiles &files, const std::string &cache_warmup_file,
                                                         uint64_t &warmup_num, uint64_t warmup_dim,
                                                         uint64_t warmup_aligned_dim);
template DISKANN_DLLEXPORT float *load_warmup<float>(MemoryMappedFiles &files, const std::string &cache_warmup_file,
                                                     uint64_t &warmup_num, uint64_t warmup_dim,
                                                     uint64_t warmup_aligned_dim);
#endif

template DISKANN_DLLEXPORT uint32_t optimize_beamwidth<int8_t, uint32_t>(
    std::unique_ptr<diskann::PQFlashIndex<int8_t, uint32_t>> &pFlashIndex, int8_t *tuning_sample,
    uint64_t tuning_sample_num, uint64_t tuning_sample_aligned_dim, uint32_t L, uint32_t nthreads, uint32_t start_bw);
template DISKANN_DLLEXPORT uint32_t optimize_beamwidth<uint8_t, uint32_t>(
    std::unique_ptr<diskann::PQFlashIndex<uint8_t, uint32_t>> &pFlashIndex, uint8_t *tuning_sample,
    uint64_t tuning_sample_num, uint64_t tuning_sample_aligned_dim, uint32_t L, uint32_t nthreads, uint32_t start_bw);
template DISKANN_DLLEXPORT uint32_t optimize_beamwidth<float, uint32_t>(
    std::unique_ptr<diskann::PQFlashIndex<float, uint32_t>> &pFlashIndex, float *tuning_sample,
    uint64_t tuning_sample_num, uint64_t tuning_sample_aligned_dim, uint32_t L, uint32_t nthreads, uint32_t start_bw);

template DISKANN_DLLEXPORT uint32_t optimize_beamwidth<int8_t, uint16_t>(
    std::unique_ptr<diskann::PQFlashIndex<int8_t, uint16_t>> &pFlashIndex, int8_t *tuning_sample,
    uint64_t tuning_sample_num, uint64_t tuning_sample_aligned_dim, uint32_t L, uint32_t nthreads, uint32_t start_bw);
template DISKANN_DLLEXPORT uint32_t optimize_beamwidth<uint8_t, uint16_t>(
    std::unique_ptr<diskann::PQFlashIndex<uint8_t, uint16_t>> &pFlashIndex, uint8_t *tuning_sample,
    uint64_t tuning_sample_num, uint64_t tuning_sample_aligned_dim, uint32_t L, uint32_t nthreads, uint32_t start_bw);
template DISKANN_DLLEXPORT uint32_t optimize_beamwidth<float, uint16_t>(
    std::unique_ptr<diskann::PQFlashIndex<float, uint16_t>> &pFlashIndex, float *tuning_sample,
    uint64_t tuning_sample_num, uint64_t tuning_sample_aligned_dim, uint32_t L, uint32_t nthreads, uint32_t start_bw);

template DISKANN_DLLEXPORT int build_disk_index<int8_t, uint32_t>(const char *dataFilePath, const char *indexFilePath,
                                                                  const char *indexBuildParameters,
                                                                  diskann::Metric compareMetric, bool use_opq,
                                                                  const std::string &codebook_prefix, bool use_filters,
                                                                  const std::string &label_file,
                                                                  const std::string &universal_label,
                                                                  const uint32_t filter_threshold, const uint32_t Lf);
template DISKANN_DLLEXPORT int build_disk_index<uint8_t, uint32_t>(const char *dataFilePath, const char *indexFilePath,
                                                                   const char *indexBuildParameters,
                                                                   diskann::Metric compareMetric, bool use_opq,
                                                                   const std::string &codebook_prefix, bool use_filters,
                                                                   const std::string &label_file,
                                                                   const std::string &universal_label,
                                                                   const uint32_t filter_threshold, const uint32_t Lf);
template DISKANN_DLLEXPORT int build_disk_index<float, uint32_t>(const char *dataFilePath, const char *indexFilePath,
                                                                 const char *indexBuildParameters,
                                                                 diskann::Metric compareMetric, bool use_opq,
                                                                 const std::string &codebook_prefix, bool use_filters,
                                                                 const std::string &label_file,
                                                                 const std::string &universal_label,
                                                                 const uint32_t filter_threshold, const uint32_t Lf);
// LabelT = uint16
template DISKANN_DLLEXPORT int build_disk_index<int8_t, uint16_t>(const char *dataFilePath, const char *indexFilePath,
                                                                  const char *indexBuildParameters,
                                                                  diskann::Metric compareMetric, bool use_opq,
                                                                  const std::string &codebook_prefix, bool use_filters,
                                                                  const std::string &label_file,
                                                                  const std::string &universal_label,
                                                                  const uint32_t filter_threshold, const uint32_t Lf);
template DISKANN_DLLEXPORT int build_disk_index<uint8_t, uint16_t>(const char *dataFilePath, const char *indexFilePath,
                                                                   const char *indexBuildParameters,
                                                                   diskann::Metric compareMetric, bool use_opq,
                                                                   const std::string &codebook_prefix, bool use_filters,
                                                                   const std::string &label_file,
                                                                   const std::string &universal_label,
                                                                   const uint32_t filter_threshold, const uint32_t Lf);
template DISKANN_DLLEXPORT int build_disk_index<float, uint16_t>(const char *dataFilePath, const char *indexFilePath,
                                                                 const char *indexBuildParameters,
                                                                 diskann::Metric compareMetric, bool use_opq,
                                                                 const std::string &codebook_prefix, bool use_filters,
                                                                 const std::string &label_file,
                                                                 const std::string &universal_label,
                                                                 const uint32_t filter_threshold, const uint32_t Lf);

template DISKANN_DLLEXPORT int build_merged_vamana_index<int8_t, uint32_t>(
    std::string base_file, diskann::Metric compareMetric, uint32_t L, uint32_t R, double sampling_rate,
    double ram_budget, std::string mem_index_path, std::string medoids_path, std::string centroids_file,
    size_t build_pq_bytes, bool use_opq, uint32_t num_threads, bool use_filters, const std::string &label_file,
    const std::string &labels_to_medoids_file, const std::string &universal_label, const uint32_t Lf);

template DISKANN_DLLEXPORT int build_merged_vamana_index<float, uint32_t>(std::stringstream &  data_stream, diskann::Metric compareMetric, uint32_t L, uint32_t R,
                              double sampling_rate, double ram_budget, std::stringstream &mem_index_stream,
                              std::string medoids_file, std::string centroids_file, size_t build_pq_bytes, bool use_opq,
                              uint32_t num_threads, bool use_filters, const std::string &label_file,
                              const std::string &labels_to_medoids_file, const std::string &universal_label,
                              const uint32_t Lf);

template DISKANN_DLLEXPORT int build_merged_vamana_index<float, uint32_t>(
    std::string base_file, diskann::Metric compareMetric, uint32_t L, uint32_t R, double sampling_rate,
    double ram_budget, std::string mem_index_path, std::string medoids_path, std::string centroids_file,
    size_t build_pq_bytes, bool use_opq, uint32_t num_threads, bool use_filters, const std::string &label_file,
    const std::string &labels_to_medoids_file, const std::string &universal_label, const uint32_t Lf);
template DISKANN_DLLEXPORT int build_merged_vamana_index<uint8_t, uint32_t>(
    std::string base_file, diskann::Metric compareMetric, uint32_t L, uint32_t R, double sampling_rate,
    double ram_budget, std::string mem_index_path, std::string medoids_path, std::string centroids_file,
    size_t build_pq_bytes, bool use_opq, uint32_t num_threads, bool use_filters, const std::string &label_file,
    const std::string &labels_to_medoids_file, const std::string &universal_label, const uint32_t Lf);
// Label=16_t
template DISKANN_DLLEXPORT int build_merged_vamana_index<int8_t, uint16_t>(
    std::string base_file, diskann::Metric compareMetric, uint32_t L, uint32_t R, double sampling_rate,
    double ram_budget, std::string mem_index_path, std::string medoids_path, std::string centroids_file,
    size_t build_pq_bytes, bool use_opq, uint32_t num_threads, bool use_filters, const std::string &label_file,
    const std::string &labels_to_medoids_file, const std::string &universal_label, const uint32_t Lf);
template DISKANN_DLLEXPORT int build_merged_vamana_index<float, uint16_t>(
    std::string base_file, diskann::Metric compareMetric, uint32_t L, uint32_t R, double sampling_rate,
    double ram_budget, std::string mem_index_path, std::string medoids_path, std::string centroids_file,
    size_t build_pq_bytes, bool use_opq, uint32_t num_threads, bool use_filters, const std::string &label_file,
    const std::string &labels_to_medoids_file, const std::string &universal_label, const uint32_t Lf);
template DISKANN_DLLEXPORT int build_merged_vamana_index<uint8_t, uint16_t>(
    std::string base_file, diskann::Metric compareMetric, uint32_t L, uint32_t R, double sampling_rate,
    double ram_budget, std::string mem_index_path, std::string medoids_path, std::string centroids_file,
    size_t build_pq_bytes, bool use_opq, uint32_t num_threads, bool use_filters, const std::string &label_file,
    const std::string &labels_to_medoids_file, const std::string &universal_label, const uint32_t Lf);
}; // namespace diskann
