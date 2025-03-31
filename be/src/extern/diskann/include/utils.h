// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once



#include <errno.h>

#include "common_includes.h"

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

#include "distance.h"
#include "logger.h"
#include "cached_io.h"
#include "ann_exception.h"
#include "windows_customizations.h"
#include "tsl/robin_set.h"
#include "types.h"
#include "tag_uint128.h"
#include <any>

#ifdef EXEC_ENV_OLS
#include "content_buf.h"
#include "memory_mapped_files.h"
#endif
#include "combined_file.h"
#include "ThreadPool.h"

#include <immintrin.h>

#include "vector/stream_wrapper.h"

// taken from
// https://github.com/Microsoft/BLAS-on-flash/blob/master/include/utils.h
// round up X to the nearest multiple of Y
#define ROUND_UP(X, Y) ((((uint64_t)(X) / (Y)) + ((uint64_t)(X) % (Y) != 0)) * (Y))

#define DIV_ROUND_UP(X, Y) (((uint64_t)(X) / (Y)) + ((uint64_t)(X) % (Y) != 0))

// round down X to the nearest multiple of Y
#define ROUND_DOWN(X, Y) (((uint64_t)(X) / (Y)) * (Y))

// alignment tests
#define IS_ALIGNED(X, Y) ((uint64_t)(X) % (uint64_t)(Y) == 0)
#define IS_512_ALIGNED(X) IS_ALIGNED(X, 512)
#define IS_4096_ALIGNED(X) IS_ALIGNED(X, 4096)
#define METADATA_SIZE                                                                                                  \
    4096 // all metadata of individual sub-component files is written in first
         // 4KB for unified files

#define BUFFER_SIZE_FOR_CACHED_IO (size_t)1024 * (size_t)1048576

#define PBSTR "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
#define PBWIDTH 60

inline bool file_exists_impl(const std::string &name, bool dirCheck = false)
{
    int val;
#ifndef _WINDOWS
    struct stat buffer;
    val = stat(name.c_str(), &buffer);
#else
    // It is the 21st century but Windows API still thinks in 32-bit terms.
    // Turns out calling stat() on a file > 4GB results in errno = 132
    // (OVERFLOW). How silly is this!? So calling _stat64()
    struct _stat64 buffer;
    val = _stat64(name.c_str(), &buffer);
#endif

    if (val != 0)
    {
        switch (errno)
        {
        case EINVAL:
            diskann::cout << "Invalid argument passed to stat()" << std::endl;
            break;
        case ENOENT:
            // file is not existing, not an issue, so we won't cout anything.
            break;
        default:
            diskann::cout << "Unexpected error in stat():" << errno << std::endl;
            break;
        }
        return false;
    }
    else
    {
        // the file entry exists. If reqd, check if this is a directory.
        return dirCheck ? buffer.st_mode & S_IFDIR : true;
    }
}

inline bool file_exists(const std::string &name, bool dirCheck = false)
{
#ifdef EXEC_ENV_OLS
    bool exists = file_exists_impl(name, dirCheck);
    if (exists)
    {
        return true;
    }
    if (!dirCheck)
    {
        // try with .enc extension
        std::string enc_name = name + ENCRYPTED_EXTENSION;
        return file_exists_impl(enc_name, dirCheck);
    }
    else
    {
        return exists;
    }
#else
    return file_exists_impl(name, dirCheck);
#endif
}

inline void open_file_to_write(std::ofstream &writer, const std::string &filename)
{
    writer.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    if (!file_exists(filename))
        writer.open(filename, std::ios::binary | std::ios::out);
    else
        writer.open(filename, std::ios::binary | std::ios::in | std::ios::out);

    if (writer.fail())
    {
        char buff[1024];
#ifdef _WINDOWS
        auto ret = std::to_string(strerror_s(buff, 1024, errno));
#else
        auto ret = std::string(strerror_r(errno, buff, 1024));
#endif
        auto message = std::string("Failed to open file") + filename + " for write because " + buff + ", ret=" + ret;
        diskann::cerr << message << std::endl;
        throw diskann::ANNException(message, -1);
    }
}

inline size_t get_file_size(const std::string &fname)
{
    std::ifstream reader(fname, std::ios::binary | std::ios::ate);
    if (!reader.fail() && reader.is_open())
    {
        size_t end_pos = reader.tellg();
        reader.close();
        return end_pos;
    }
    else
    {
        diskann::cerr << "Could not open file: " << fname << std::endl;
        return 0;
    }
}

inline int delete_file(const std::string &fileName)
{
    if (file_exists(fileName))
    {
        auto rc = ::remove(fileName.c_str());
        if (rc != 0)
        {
            diskann::cerr << "Could not delete file: " << fileName
                          << " even though it exists. This might indicate a permissions "
                             "issue. "
                             "If you see this message, please contact the diskann team."
                          << std::endl;
        }
        return rc;
    }
    else
    {
        return 0;
    }
}

// generates formatted_label and _labels_map file.
inline void convert_labels_string_to_int(const std::string &inFileName, const std::string &outFileName,
                                         const std::string &mapFileName, const std::string &unv_label)
{
    std::unordered_map<std::string, uint32_t> string_int_map;
    std::ofstream label_writer(outFileName);
    std::ifstream label_reader(inFileName);
    if (unv_label != "")
        string_int_map[unv_label] = 0; // if universal label is provided map it to 0 always
    std::string line, token;
    while (std::getline(label_reader, line))
    {
        std::istringstream new_iss(line);
        std::vector<uint32_t> lbls;
        while (getline(new_iss, token, ','))
        {
            token.erase(std::remove(token.begin(), token.end(), '\n'), token.end());
            token.erase(std::remove(token.begin(), token.end(), '\r'), token.end());
            if (string_int_map.find(token) == string_int_map.end())
            {
                uint32_t nextId = (uint32_t)string_int_map.size() + 1;
                string_int_map[token] = nextId; // nextId can never be 0
            }
            lbls.push_back(string_int_map[token]);
        }
        if (lbls.size() <= 0)
        {
            std::cout << "No label found";
            exit(-1);
        }
        for (size_t j = 0; j < lbls.size(); j++)
        {
            if (j != lbls.size() - 1)
                label_writer << lbls[j] << ",";
            else
                label_writer << lbls[j] << std::endl;
        }
    }
    label_writer.close();

    std::ofstream map_writer(mapFileName);
    for (auto mp : string_int_map)
    {
        map_writer << mp.first << "\t" << mp.second << std::endl;
    }
    map_writer.close();
}

#ifdef EXEC_ENV_OLS
class AlignedFileReader;
#endif

namespace diskann
{
static const size_t MAX_SIZE_OF_STREAMBUF = 2LL * 1024 * 1024 * 1024;

inline void print_error_and_terminate(std::stringstream &error_stream)
{
    diskann::cerr << error_stream.str() << std::endl;
    throw diskann::ANNException(error_stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
}

inline void report_memory_allocation_failure()
{
    std::stringstream stream;
    stream << "Memory Allocation Failed.";
    print_error_and_terminate(stream);
}

inline void report_misalignment_of_requested_size(size_t align)
{
    std::stringstream stream;
    stream << "Requested memory size is not a multiple of " << align << ". Can not be allocated.";
    print_error_and_terminate(stream);
}

inline void alloc_aligned(void **ptr, size_t size, size_t align)
{
    *ptr = nullptr;
    if (IS_ALIGNED(size, align) == 0)
        report_misalignment_of_requested_size(align);
#ifndef _WINDOWS
    *ptr = ::aligned_alloc(align, size);
#else
    *ptr = ::_aligned_malloc(size, align); // note the swapped arguments!
#endif
    if (*ptr == nullptr)
        report_memory_allocation_failure();
}

inline void realloc_aligned(void **ptr, size_t size, size_t align)
{
    if (IS_ALIGNED(size, align) == 0)
        report_misalignment_of_requested_size(align);
#ifdef _WINDOWS
    *ptr = ::_aligned_realloc(*ptr, size, align);
#else
    diskann::cerr << "No aligned realloc on GCC. Must malloc and mem_align, "
                     "left it out for now."
                  << std::endl;
#endif
    if (*ptr == nullptr)
        report_memory_allocation_failure();
}

inline void check_stop(std::string arnd)
{
    int brnd;
    diskann::cout << arnd << std::endl;
    std::cin >> brnd;
}

inline void aligned_free(void *ptr)
{
    // Gopal. Must have a check here if the pointer was actually allocated by
    // _alloc_aligned
    if (ptr == nullptr)
    {
        return;
    }
#ifndef _WINDOWS
    free(ptr);
#else
    ::_aligned_free(ptr);
#endif
}

inline void GenRandom(std::mt19937 &rng, unsigned *addr, unsigned size, unsigned N)
{
    for (unsigned i = 0; i < size; ++i)
    {
        addr[i] = rng() % (N - size);
    }

    std::sort(addr, addr + size);
    for (unsigned i = 1; i < size; ++i)
    {
        if (addr[i] <= addr[i - 1])
        {
            addr[i] = addr[i - 1] + 1;
        }
    }
    unsigned off = rng() % N;
    for (unsigned i = 0; i < size; ++i)
    {
        addr[i] = (addr[i] + off) % N;
    }
}

// get_bin_metadata functions START
inline void get_bin_metadata_impl(std::basic_istream<char> &reader, size_t &nrows, size_t &ncols, size_t offset = 0)
{
    int nrows_32, ncols_32;
    reader.seekg(offset, reader.beg);
    reader.read((char *)&nrows_32, sizeof(int));
    reader.read((char *)&ncols_32, sizeof(int));
    nrows = nrows_32;
    ncols = ncols_32;
}

#ifdef EXEC_ENV_OLS
inline void get_bin_metadata(MemoryMappedFiles &files, const std::string &bin_file, size_t &nrows, size_t &ncols,
                             size_t offset = 0)
{
    diskann::cout << "Getting metadata for file: " << bin_file << std::endl;
    auto fc = files.getContent(bin_file);
    // auto                     cb = ContentBuf((char*) fc._content, fc._size);
    // std::basic_istream<char> reader(&cb);
    // get_bin_metadata_impl(reader, nrows, ncols, offset);

    int nrows_32, ncols_32;
    int32_t *metadata_ptr = (int32_t *)((char *)fc._content + offset);
    nrows_32 = *metadata_ptr;
    ncols_32 = *(metadata_ptr + 1);
    nrows = nrows_32;
    ncols = ncols_32;
}
#endif

inline void get_bin_metadata(const std::string &bin_file, size_t &nrows, size_t &ncols, size_t offset = 0)
{
    std::ifstream reader(bin_file.c_str(), std::ios::binary);
    get_bin_metadata_impl(reader, nrows, ncols, offset);
}



inline void get_bin_metadata(IReaderWrapperSPtr reader, size_t &nrows, size_t &ncols, size_t offset = 0)
{
    int nrows_32, ncols_32;
    reader->read((char *)&nrows_32, sizeof(int), offset);
    reader->read((char *)&ncols_32, sizeof(int), offset + 4);
    nrows = nrows_32;
    ncols = ncols_32;
}

inline void get_bin_metadata(std::stringstream &reader, size_t &nrows, size_t &ncols, size_t offset = 0)
{
    int nrows_32, ncols_32;
    reader.seekg(offset, reader.beg);
    reader.read((char *)&nrows_32, sizeof(int));
    reader.read((char *)&ncols_32, sizeof(int));
    nrows = nrows_32;
    ncols = ncols_32;
}


// get_bin_metadata functions END

#ifndef EXEC_ENV_OLS
inline size_t get_graph_num_frozen_points(const std::string &graph_file)
{
    size_t expected_file_size;
    uint32_t max_observed_degree, start;
    size_t file_frozen_pts;

    std::ifstream in;
    in.exceptions(std::ios::badbit | std::ios::failbit);

    in.open(graph_file, std::ios::binary);
    in.read((char *)&expected_file_size, sizeof(size_t));
    in.read((char *)&max_observed_degree, sizeof(uint32_t));
    in.read((char *)&start, sizeof(uint32_t));
    in.read((char *)&file_frozen_pts, sizeof(size_t));

    return file_frozen_pts;
}
#endif

template <typename T> inline std::string getValues(T *data, size_t num)
{
    std::stringstream stream;
    stream << "[";
    for (size_t i = 0; i < num; i++)
    {
        stream << std::to_string(data[i]) << ",";
    }
    stream << "]" << std::endl;

    return stream.str();
}

// load_bin functions START
template <typename T>
inline void load_bin_impl(std::basic_istream<char> &reader, T *&data, size_t &npts, size_t &dim, size_t file_offset = 0)
{
    int npts_i32, dim_i32;

    reader.seekg(file_offset, reader.beg);
    reader.read((char *)&npts_i32, sizeof(int));
    reader.read((char *)&dim_i32, sizeof(int));
    npts = (unsigned)npts_i32;
    dim = (unsigned)dim_i32;

    //std::cout << "Metadata: #pts = " << npts << ", #dims = " << dim << "..." << std::endl;

    data = new T[npts * dim];
    reader.read((char *)data, npts * dim * sizeof(T));
}

#ifdef EXEC_ENV_OLS
template <typename T>
inline void load_bin(MemoryMappedFiles &files, const std::string &bin_file, T *&data, size_t &npts, size_t &dim,
                     size_t offset = 0)
{
    diskann::cout << "Reading bin file " << bin_file.c_str() << " at offset: " << offset << "..." << std::endl;
    auto fc = files.getContent(bin_file);

    uint32_t t_npts, t_dim;
    uint32_t *contentAsIntPtr = (uint32_t *)((char *)fc._content + offset);
    t_npts = *(contentAsIntPtr);
    t_dim = *(contentAsIntPtr + 1);

    npts = t_npts;
    dim = t_dim;

    data = (T *)((char *)fc._content + offset + 2 * sizeof(uint32_t)); // No need to copy!
}

DISKANN_DLLEXPORT void get_bin_metadata(AlignedFileReader &reader, size_t &npts, size_t &ndim, size_t offset = 0);
template <typename T>
DISKANN_DLLEXPORT void load_bin(AlignedFileReader &reader, T *&data, size_t &npts, size_t &ndim, size_t offset = 0);
template <typename T>
DISKANN_DLLEXPORT void load_bin(AlignedFileReader &reader, std::unique_ptr<T[]> &data, size_t &npts, size_t &ndim,
                                size_t offset = 0);

template <typename T>
DISKANN_DLLEXPORT void copy_aligned_data_from_file(AlignedFileReader &reader, T *&data, size_t &npts, size_t &dim,
                                                   const size_t &rounded_dim, size_t offset = 0);

// Unlike load_bin, assumes that data is already allocated 'size' entries
template <typename T>
DISKANN_DLLEXPORT void read_array(AlignedFileReader &reader, T *data, size_t size, size_t offset = 0);

template <typename T> DISKANN_DLLEXPORT void read_value(AlignedFileReader &reader, T &value, size_t offset = 0);
#endif

template <typename T>
inline void load_bin(const std::string &bin_file, T *&data, size_t &npts, size_t &dim, size_t offset = 0)
{
    diskann::cout << "Reading bin file " << bin_file.c_str() << " ..." << std::endl;
    std::ifstream reader;
    reader.exceptions(std::ifstream::failbit | std::ifstream::badbit);

    try
    {
        diskann::cout << "Opening bin file " << bin_file.c_str() << "... " << std::endl;
        reader.open(bin_file, std::ios::binary | std::ios::ate);
        reader.seekg(0);
        load_bin_impl<T>(reader, data, npts, dim, offset);
    }
    catch (std::system_error &e)
    {
        throw FileException(bin_file, e, __FUNCSIG__, __FILE__, __LINE__);
    }
    diskann::cout << "done." << std::endl;
}



template <typename T>
inline void load_bin(IReaderWrapperSPtr data_stream, T *&data, size_t &npts, size_t &dim, size_t offset = 0)
{
    try
    {
        int npts_i32, dim_i32;
        data_stream->seek(offset);
        data_stream->read((char *)&npts_i32, sizeof(int), offset);
        data_stream->read((char *)&dim_i32, sizeof(int), offset+4);
        npts = (unsigned)npts_i32;
        dim = (unsigned)dim_i32;
        //std::cout << "Metadata: #pts = " << npts << ", #dims = " << dim << "..." << std::endl;
        data = new T[npts * dim];
        data_stream->read((char *)data, npts * dim * sizeof(T), offset + 8);
    }
    catch (std::system_error &e)
    {
        throw FileException("", e, __FUNCSIG__, __FILE__, __LINE__);
    }
    diskann::cout << "done." << std::endl;
}


template <typename T>
inline void load_bin(std::stringstream &data_stream, T *&data, size_t &npts, size_t &dim, size_t offset = 0)
{
    try
    {
        int npts_i32, dim_i32;
        data_stream.seekg(offset);
        data_stream.read((char *)&npts_i32, sizeof(int));
        data_stream.read((char *)&dim_i32, sizeof(int));
        npts = (unsigned)npts_i32;
        dim = (unsigned)dim_i32;
        //std::cout << "Metadata: #pts = " << npts << ", #dims = " << dim << "..." << std::endl;
        data = new T[npts * dim];
        data_stream.read((char *)data, npts * dim * sizeof(T));
    }
    catch (std::system_error &e)
    {
        throw FileException("", e, __FUNCSIG__, __FILE__, __LINE__);
    }
    diskann::cout << "done." << std::endl;
}



inline void wait_for_keystroke()
{
    int a;
    std::cout << "Press any number to continue.." << std::endl;
    std::cin >> a;
}
// load_bin functions END

inline void load_truthset(const std::string &bin_file, uint32_t *&ids, float *&dists, size_t &npts, size_t &dim)
{
    size_t read_blk_size = 64 * 1024 * 1024;
    cached_ifstream reader(bin_file, read_blk_size);
    diskann::cout << "Reading truthset file " << bin_file.c_str() << " ..." << std::endl;
    size_t actual_file_size = reader.get_file_size();

    int npts_i32, dim_i32;
    reader.read((char *)&npts_i32, sizeof(int));
    reader.read((char *)&dim_i32, sizeof(int));
    npts = (unsigned)npts_i32;
    dim = (unsigned)dim_i32;

    diskann::cout << "Metadata: #pts = " << npts << ", #dims = " << dim << "... " << std::endl;

    int truthset_type = -1; // 1 means truthset has ids and distances, 2 means
                            // only ids, -1 is error
    size_t expected_file_size_with_dists = 2 * npts * dim * sizeof(uint32_t) + 2 * sizeof(uint32_t);

    if (actual_file_size == expected_file_size_with_dists)
        truthset_type = 1;

    size_t expected_file_size_just_ids = npts * dim * sizeof(uint32_t) + 2 * sizeof(uint32_t);

    if (actual_file_size == expected_file_size_just_ids)
        truthset_type = 2;

    if (truthset_type == -1)
    {
        std::stringstream stream;
        stream << "Error. File size mismatch. File should have bin format, with "
                  "npts followed by ngt followed by npts*ngt ids and optionally "
                  "followed by npts*ngt distance values; actual size: "
               << actual_file_size << ", expected: " << expected_file_size_with_dists << " or "
               << expected_file_size_just_ids;
        diskann::cout << stream.str();
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    ids = new uint32_t[npts * dim];
    reader.read((char *)ids, npts * dim * sizeof(uint32_t));

    if (truthset_type == 1)
    {
        dists = new float[npts * dim];
        reader.read((char *)dists, npts * dim * sizeof(float));
    }
}

inline void prune_truthset_for_range(const std::string &bin_file, float range,
                                     std::vector<std::vector<uint32_t>> &groundtruth, size_t &npts)
{
    size_t read_blk_size = 64 * 1024 * 1024;
    cached_ifstream reader(bin_file, read_blk_size);
    diskann::cout << "Reading truthset file " << bin_file.c_str() << "... " << std::endl;
    size_t actual_file_size = reader.get_file_size();

    int npts_i32, dim_i32;
    reader.read((char *)&npts_i32, sizeof(int));
    reader.read((char *)&dim_i32, sizeof(int));
    npts = (unsigned)npts_i32;
    uint64_t dim = (unsigned)dim_i32;
    uint32_t *ids;
    float *dists;

    diskann::cout << "Metadata: #pts = " << npts << ", #dims = " << dim << "... " << std::endl;

    int truthset_type = -1; // 1 means truthset has ids and distances, 2 means
                            // only ids, -1 is error
    size_t expected_file_size_with_dists = 2 * npts * dim * sizeof(uint32_t) + 2 * sizeof(uint32_t);

    if (actual_file_size == expected_file_size_with_dists)
        truthset_type = 1;

    if (truthset_type == -1)
    {
        std::stringstream stream;
        stream << "Error. File size mismatch. File should have bin format, with "
                  "npts followed by ngt followed by npts*ngt ids and optionally "
                  "followed by npts*ngt distance values; actual size: "
               << actual_file_size << ", expected: " << expected_file_size_with_dists;
        diskann::cout << stream.str();
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    ids = new uint32_t[npts * dim];
    reader.read((char *)ids, npts * dim * sizeof(uint32_t));

    if (truthset_type == 1)
    {
        dists = new float[npts * dim];
        reader.read((char *)dists, npts * dim * sizeof(float));
    }
    float min_dist = std::numeric_limits<float>::max();
    float max_dist = 0;
    groundtruth.resize(npts);
    for (uint32_t i = 0; i < npts; i++)
    {
        groundtruth[i].clear();
        for (uint32_t j = 0; j < dim; j++)
        {
            if (dists[i * dim + j] <= range)
            {
                groundtruth[i].emplace_back(ids[i * dim + j]);
            }
            min_dist = min_dist > dists[i * dim + j] ? dists[i * dim + j] : min_dist;
            max_dist = max_dist < dists[i * dim + j] ? dists[i * dim + j] : max_dist;
        }
        // std::cout<<groundtruth[i].size() << " " ;
    }
    std::cout << "Min dist: " << min_dist << ", Max dist: " << max_dist << std::endl;
    delete[] ids;
    delete[] dists;
}

inline void load_range_truthset(const std::string &bin_file, std::vector<std::vector<uint32_t>> &groundtruth,
                                uint64_t &gt_num)
{
    size_t read_blk_size = 64 * 1024 * 1024;
    cached_ifstream reader(bin_file, read_blk_size);
    diskann::cout << "Reading truthset file " << bin_file.c_str() << "... " << std::flush;
    size_t actual_file_size = reader.get_file_size();

    int nptsuint32_t, totaluint32_t;
    reader.read((char *)&nptsuint32_t, sizeof(int));
    reader.read((char *)&totaluint32_t, sizeof(int));

    gt_num = (uint64_t)nptsuint32_t;
    uint64_t total_res = (uint64_t)totaluint32_t;

    diskann::cout << "Metadata: #pts = " << gt_num << ", #total_results = " << total_res << "..." << std::endl;

    size_t expected_file_size = 2 * sizeof(uint32_t) + gt_num * sizeof(uint32_t) + total_res * sizeof(uint32_t);

    if (actual_file_size != expected_file_size)
    {
        std::stringstream stream;
        stream << "Error. File size mismatch in range truthset. actual size: " << actual_file_size
               << ", expected: " << expected_file_size;
        diskann::cout << stream.str();
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    groundtruth.clear();
    groundtruth.resize(gt_num);
    std::vector<uint32_t> gt_count(gt_num);

    reader.read((char *)gt_count.data(), sizeof(uint32_t) * gt_num);

    std::vector<uint32_t> gt_stats(gt_count);
    std::sort(gt_stats.begin(), gt_stats.end());

    std::cout << "GT count percentiles:" << std::endl;
    for (uint32_t p = 0; p < 100; p += 5)
        std::cout << "percentile " << p << ": " << gt_stats[static_cast<size_t>(std::floor((p / 100.0) * gt_num))]
                  << std::endl;
    std::cout << "percentile 100"
              << ": " << gt_stats[gt_num - 1] << std::endl;

    for (uint32_t i = 0; i < gt_num; i++)
    {
        groundtruth[i].clear();
        groundtruth[i].resize(gt_count[i]);
        if (gt_count[i] != 0)
            reader.read((char *)groundtruth[i].data(), sizeof(uint32_t) * gt_count[i]);
    }
}

#ifdef EXEC_ENV_OLS
template <typename T>
inline void load_bin(MemoryMappedFiles &files, const std::string &bin_file, std::unique_ptr<T[]> &data, size_t &npts,
                     size_t &dim, size_t offset = 0)
{
    T *ptr;
    load_bin<T>(files, bin_file, ptr, npts, dim, offset);
    data.reset(ptr);
}
#endif

inline void copy_file(std::string in_file, std::string out_file)
{
    std::ifstream source(in_file, std::ios::binary);
    std::ofstream dest(out_file, std::ios::binary);

    std::istreambuf_iterator<char> begin_source(source);
    std::istreambuf_iterator<char> end_source;
    std::ostreambuf_iterator<char> begin_dest(dest);
    std::copy(begin_source, end_source, begin_dest);

    source.close();
    dest.close();
}

DISKANN_DLLEXPORT double calculate_recall(unsigned num_queries, unsigned *gold_std, float *gs_dist, unsigned dim_gs,
                                          unsigned *our_results, unsigned dim_or, unsigned recall_at);

DISKANN_DLLEXPORT double calculate_recall(unsigned num_queries, unsigned *gold_std, float *gs_dist, unsigned dim_gs,
                                          unsigned *our_results, unsigned dim_or, unsigned recall_at,
                                          const tsl::robin_set<unsigned> &active_tags);

DISKANN_DLLEXPORT double calculate_range_search_recall(unsigned num_queries,
                                                       std::vector<std::vector<uint32_t>> &groundtruth,
                                                       std::vector<std::vector<uint32_t>> &our_results);

template <typename T>
inline void load_bin(const std::string &bin_file, std::unique_ptr<T[]> &data, size_t &npts, size_t &dim,
                     size_t offset = 0)
{
    T *ptr;
    load_bin<T>(bin_file, ptr, npts, dim, offset);
    data.reset(ptr);
}

template <typename T>
inline void load_bin(IReaderWrapperSPtr data_source, std::unique_ptr<T[]> &data, size_t &npts, size_t &dim,
                     size_t offset = 0)
{
    T *ptr;
    load_bin<T>(data_source, ptr, npts, dim, offset);
    data.reset(ptr);
}

template <typename T>
inline void load_bin(std::stringstream &data_source, std::unique_ptr<T[]> &data, size_t &npts, size_t &dim,
                     size_t offset = 0)
{
    T *ptr;
    load_bin<T>(data_source, ptr, npts, dim, offset);
    data.reset(ptr);
}

inline void open_file_to_write(std::ofstream &writer, const std::string &filename)
{
    writer.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    if (!file_exists(filename))
        writer.open(filename, std::ios::binary | std::ios::out);
    else
        writer.open(filename, std::ios::binary | std::ios::in | std::ios::out);

    if (writer.fail())
    {
        char buff[1024];
#ifdef _WINDOWS
        auto ret = std::to_string(strerror_s(buff, 1024, errno));
#else
        auto ret = std::string(strerror_r(errno, buff, 1024));
#endif
        std::string error_message =
            std::string("Failed to open file") + filename + " for write because " + buff + ", ret=" + ret;
        diskann::cerr << error_message << std::endl;
        throw diskann::ANNException(error_message, -1);
    }
}

template <typename T>
inline size_t save_bin(const std::string &filename, T *data, size_t npts, size_t ndims, size_t offset = 0)
{
    std::ofstream writer;
    open_file_to_write(writer, filename);

    diskann::cout << "Writing bin: " << filename.c_str() << std::endl;
    writer.seekp(offset, writer.beg);
    int npts_i32 = (int)npts, ndims_i32 = (int)ndims;
    size_t bytes_written = npts * ndims * sizeof(T) + 2 * sizeof(uint32_t);
    writer.write((char *)&npts_i32, sizeof(int));
    writer.write((char *)&ndims_i32, sizeof(int));
    diskann::cout << "bin: #pts = " << npts << ", #dims = " << ndims << ", size = " << bytes_written << "B"
                  << std::endl;

    writer.write((char *)data, npts * ndims * sizeof(T));
    writer.close();
    diskann::cout << "Finished writing bin." << std::endl;
    return bytes_written;
}

template <typename T>
inline size_t save_bin(std::stringstream &writer, T *data, size_t npts, size_t ndims, size_t offset = 0)
{
    writer.seekp(offset, writer.beg);
    int npts_i32 = (int)npts, ndims_i32 = (int)ndims;
    size_t bytes_written = npts * ndims * sizeof(T) + 2 * sizeof(uint32_t);
    writer.write((char *)&npts_i32, sizeof(int));
    writer.write((char *)&ndims_i32, sizeof(int));
    diskann::cout << "bin: #pts = " << npts << ", #dims = " << ndims << ", size = " << bytes_written << "B"
                  << std::endl;

    writer.write((char *)data, npts * ndims * sizeof(T));
    if (writer.fail()) {
        std::cerr << "Error: writer is in a failed state!" << std::endl;
    }
    diskann::cout << "Finished writing bin." << std::endl;
    return bytes_written;
}

inline void print_progress(double percentage)
{
    int val = (int)(percentage * 100);
    int lpad = (int)(percentage * PBWIDTH);
    int rpad = PBWIDTH - lpad;
    printf("\r%3d%% [%.*s%*s]", val, lpad, PBSTR, rpad, "");
    fflush(stdout);
}

// load_aligned_bin functions START

template <typename T>
inline void load_aligned_bin_impl(std::basic_istream<char> &reader, size_t actual_file_size, T *&data, size_t &npts,
                                  size_t &dim, size_t &rounded_dim)
{
    int npts_i32, dim_i32;
    reader.read((char *)&npts_i32, sizeof(int));
    reader.read((char *)&dim_i32, sizeof(int));
    npts = (unsigned)npts_i32;
    dim = (unsigned)dim_i32;

    size_t expected_actual_file_size = npts * dim * sizeof(T) + 2 * sizeof(uint32_t);
    if (actual_file_size != expected_actual_file_size)
    {
        std::stringstream stream;
        stream << "Error. File size mismatch. Actual size is " << actual_file_size << " while expected size is  "
               << expected_actual_file_size << " npts = " << npts << " dim = " << dim << " size of <T>= " << sizeof(T)
               << std::endl;
        diskann::cout << stream.str() << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    rounded_dim = ROUND_UP(dim, 8);
    diskann::cout << "Metadata: #pts = " << npts << ", #dims = " << dim << ", aligned_dim = " << rounded_dim << "... "
                  << std::flush;
    size_t allocSize = npts * rounded_dim * sizeof(T);
    diskann::cout << "allocating aligned memory of " << allocSize << " bytes... " << std::flush;
    alloc_aligned(((void **)&data), allocSize, 8 * sizeof(T));
    diskann::cout << "done. Copying data to mem_aligned buffer..." << std::flush;

    for (size_t i = 0; i < npts; i++)
    {
        reader.read((char *)(data + i * rounded_dim), dim * sizeof(T));
        memset(data + i * rounded_dim + dim, 0, (rounded_dim - dim) * sizeof(T));
    }
    diskann::cout << " done." << std::endl;
}

#ifdef EXEC_ENV_OLS
template <typename T>
inline void load_aligned_bin(MemoryMappedFiles &files, const std::string &bin_file, T *&data, size_t &npts, size_t &dim,
                             size_t &rounded_dim)
{
    try
    {
        diskann::cout << "Opening bin file " << bin_file << " ..." << std::flush;
        FileContent fc = files.getContent(bin_file);
        ContentBuf buf((char *)fc._content, fc._size);
        std::basic_istream<char> reader(&buf);

        size_t actual_file_size = fc._size;
        load_aligned_bin_impl(reader, actual_file_size, data, npts, dim, rounded_dim);
    }
    catch (std::system_error &e)
    {
        throw FileException(bin_file, e, __FUNCSIG__, __FILE__, __LINE__);
    }
}
#endif

template <typename T>
inline void load_aligned_bin(const std::string &bin_file, T *&data, size_t &npts, size_t &dim, size_t &rounded_dim)
{
    std::ifstream reader;
    reader.exceptions(std::ifstream::failbit | std::ifstream::badbit);

    try
    {
        diskann::cout << "Reading (with alignment) bin file " << bin_file << " ..." << std::flush;
        reader.open(bin_file, std::ios::binary | std::ios::ate);

        uint64_t fsize = reader.tellg();
        reader.seekg(0);
        load_aligned_bin_impl(reader, fsize, data, npts, dim, rounded_dim);
    }
    catch (std::system_error &e)
    {
        throw FileException(bin_file, e, __FUNCSIG__, __FILE__, __LINE__);
    }
}

template <typename InType, typename OutType>
void convert_types(const InType *srcmat, OutType *destmat, size_t npts, size_t dim)
{
#pragma omp parallel for schedule(static, 65536)
    for (int64_t i = 0; i < (int64_t)npts; i++)
    {
        for (uint64_t j = 0; j < dim; j++)
        {
            destmat[i * dim + j] = (OutType)srcmat[i * dim + j];
        }
    }
}

// this function will take in_file of n*d dimensions and save the output as a
// floating point matrix
// with n*(d+1) dimensions. All vectors are scaled by a large value M so that
// the norms are <=1 and the final coordinate is set so that the resulting
// norm (in d+1 coordinates) is equal to 1 this is a classical transformation
// from MIPS to L2 search from "On Symmetric and Asymmetric LSHs for Inner
// Product Search" by Neyshabur and Srebro

template <typename T> float prepare_base_for_inner_products(const std::string in_file, const std::string out_file)
{
    std::cout << "Pre-processing base file by adding extra coordinate" << std::endl;
    std::ifstream in_reader(in_file.c_str(), std::ios::binary);
    std::ofstream out_writer(out_file.c_str(), std::ios::binary);
    uint64_t npts, in_dims, out_dims;
    float max_norm = 0;

    uint32_t npts32, dims32;
    in_reader.read((char *)&npts32, sizeof(uint32_t));
    in_reader.read((char *)&dims32, sizeof(uint32_t));

    npts = npts32;
    in_dims = dims32;
    out_dims = in_dims + 1;
    uint32_t outdims32 = (uint32_t)out_dims;

    out_writer.write((char *)&npts32, sizeof(uint32_t));
    out_writer.write((char *)&outdims32, sizeof(uint32_t));

    size_t BLOCK_SIZE = 100000;
    size_t block_size = npts <= BLOCK_SIZE ? npts : BLOCK_SIZE;
    std::unique_ptr<T[]> in_block_data = std::make_unique<T[]>(block_size * in_dims);
    std::unique_ptr<float[]> out_block_data = std::make_unique<float[]>(block_size * out_dims);

    std::memset(out_block_data.get(), 0, sizeof(float) * block_size * out_dims);
    uint64_t num_blocks = DIV_ROUND_UP(npts, block_size);

    std::vector<float> norms(npts, 0);

    for (uint64_t b = 0; b < num_blocks; b++)
    {
        uint64_t start_id = b * block_size;
        uint64_t end_id = (b + 1) * block_size < npts ? (b + 1) * block_size : npts;
        uint64_t block_pts = end_id - start_id;
        in_reader.read((char *)in_block_data.get(), block_pts * in_dims * sizeof(T));
        for (uint64_t p = 0; p < block_pts; p++)
        {
            for (uint64_t j = 0; j < in_dims; j++)
            {
                norms[start_id + p] += in_block_data[p * in_dims + j] * in_block_data[p * in_dims + j];
            }
            max_norm = max_norm > norms[start_id + p] ? max_norm : norms[start_id + p];
        }
    }

    max_norm = std::sqrt(max_norm);

    in_reader.seekg(2 * sizeof(uint32_t), std::ios::beg);
    for (uint64_t b = 0; b < num_blocks; b++)
    {
        uint64_t start_id = b * block_size;
        uint64_t end_id = (b + 1) * block_size < npts ? (b + 1) * block_size : npts;
        uint64_t block_pts = end_id - start_id;
        in_reader.read((char *)in_block_data.get(), block_pts * in_dims * sizeof(T));
        for (uint64_t p = 0; p < block_pts; p++)
        {
            for (uint64_t j = 0; j < in_dims; j++)
            {
                out_block_data[p * out_dims + j] = in_block_data[p * in_dims + j] / max_norm;
            }
            float res = 1 - (norms[start_id + p] / (max_norm * max_norm));
            res = res <= 0 ? 0 : std::sqrt(res);
            out_block_data[p * out_dims + out_dims - 1] = res;
        }
        out_writer.write((char *)out_block_data.get(), block_pts * out_dims * sizeof(float));
    }
    out_writer.close();
    return max_norm;
}

// plain saves data as npts X ndims array into filename
template <typename T> void save_Tvecs(const char *filename, T *data, size_t npts, size_t ndims)
{
    std::string fname(filename);

    // create cached ofstream with 64MB cache
    cached_ofstream writer(fname, 64 * 1048576);

    unsigned dims_u32 = (unsigned)ndims;

    // start writing
    for (size_t i = 0; i < npts; i++)
    {
        // write dims in u32
        writer.write((char *)&dims_u32, sizeof(unsigned));

        // get cur point in data
        T *cur_pt = data + i * ndims;
        writer.write((char *)cur_pt, ndims * sizeof(T));
    }
}
template <typename T>
inline size_t save_data_in_base_dimensions(const std::string &filename, T *data, size_t npts, size_t ndims,
                                           size_t aligned_dim, size_t offset = 0)
{
    std::ofstream writer; //(filename, std::ios::binary | std::ios::out);
    open_file_to_write(writer, filename);
    int npts_i32 = (int)npts, ndims_i32 = (int)ndims;
    size_t bytes_written = 2 * sizeof(uint32_t) + npts * ndims * sizeof(T);
    writer.seekp(offset, writer.beg);
    writer.write((char *)&npts_i32, sizeof(int));
    writer.write((char *)&ndims_i32, sizeof(int));
    for (size_t i = 0; i < npts; i++)
    {
        writer.write((char *)(data + i * aligned_dim), ndims * sizeof(T));
    }
    writer.close();
    return bytes_written;
}

template <typename T>
inline void copy_aligned_data_from_file(const char *bin_file, T *&data, size_t &npts, size_t &dim,
                                        const size_t &rounded_dim, size_t offset = 0)
{
    if (data == nullptr)
    {
        diskann::cerr << "Memory was not allocated for " << data << " before calling the load function. Exiting..."
                      << std::endl;
        throw diskann::ANNException("Null pointer passed to copy_aligned_data_from_file function", -1, __FUNCSIG__,
                                    __FILE__, __LINE__);
    }
    std::ifstream reader;
    reader.exceptions(std::ios::badbit | std::ios::failbit);
    reader.open(bin_file, std::ios::binary);
    reader.seekg(offset, reader.beg);

    int npts_i32, dim_i32;
    reader.read((char *)&npts_i32, sizeof(int));
    reader.read((char *)&dim_i32, sizeof(int));
    npts = (unsigned)npts_i32;
    dim = (unsigned)dim_i32;

    for (size_t i = 0; i < npts; i++)
    {
        reader.read((char *)(data + i * rounded_dim), dim * sizeof(T));
        memset(data + i * rounded_dim + dim, 0, (rounded_dim - dim) * sizeof(T));
    }
}

// NOTE :: good efficiency when total_vec_size is integral multiple of 64
inline void prefetch_vector(const char *vec, size_t vecsize)
{
    size_t max_prefetch_size = (vecsize / 64) * 64;
    for (size_t d = 0; d < max_prefetch_size; d += 64)
        _mm_prefetch((const char *)vec + d, _MM_HINT_T0);
}

// NOTE :: good efficiency when total_vec_size is integral multiple of 64
inline void prefetch_vector_l2(const char *vec, size_t vecsize)
{
    size_t max_prefetch_size = (vecsize / 64) * 64;
    for (size_t d = 0; d < max_prefetch_size; d += 64)
        _mm_prefetch((const char *)vec + d, _MM_HINT_T1);
}

// NOTE: Implementation in utils.cpp.
void block_convert(std::ofstream &writr, std::ifstream &readr, float *read_buf, uint64_t npts, uint64_t ndims);

DISKANN_DLLEXPORT void normalize_data_file(const std::string &inFileName, const std::string &outFileName);

inline std::string get_tag_string(std::uint64_t tag)
{
    return std::to_string(tag);
}

inline std::string get_tag_string(const tag_uint128 &tag)
{
    std::string str = std::to_string(tag._data2) + "_" + std::to_string(tag._data1);
    return str;
}

}; // namespace diskann

struct PivotContainer
{
    PivotContainer() = default;

    PivotContainer(size_t pivo_id, float pivo_dist) : piv_id{pivo_id}, piv_dist{pivo_dist}
    {
    }

    bool operator<(const PivotContainer &p) const
    {
        return p.piv_dist < piv_dist;
    }

    bool operator>(const PivotContainer &p) const
    {
        return p.piv_dist > piv_dist;
    }

    size_t piv_id;
    float piv_dist;
};

inline bool validate_index_file_size(std::ifstream &in)
{
    if (!in.is_open())
        throw diskann::ANNException("Index file size check called on unopened file stream", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    in.seekg(0, in.end);
    size_t actual_file_size = in.tellg();
    in.seekg(0, in.beg);
    size_t expected_file_size;
    in.read((char *)&expected_file_size, sizeof(uint64_t));
    in.seekg(0, in.beg);
    if (actual_file_size != expected_file_size)
    {
        diskann::cerr << "Index file size error. Expected size (metadata): " << expected_file_size
                      << ", actual file size : " << actual_file_size << "." << std::endl;
        return false;
    }
    return true;
}

template <typename T> inline float get_norm(T *arr, const size_t dim)
{
    float sum = 0.0f;
    for (uint32_t i = 0; i < dim; i++)
    {
        sum += arr[i] * arr[i];
    }
    return sqrt(sum);
}

// This function is valid only for float data type.
template <typename T = float> inline void normalize(T *arr, const size_t dim)
{
    float norm = get_norm(arr, dim);
    for (uint32_t i = 0; i < dim; i++)
    {
        arr[i] = (T)(arr[i] / norm);
    }
}

inline std::vector<std::string> read_file_to_vector_of_strings(const std::string &filename, bool unique = false)
{
    std::vector<std::string> result;
    std::set<std::string> elementSet;
    if (filename != "")
    {
        std::ifstream file(filename);
        if (file.fail())
        {
            throw diskann::ANNException(std::string("Failed to open file ") + filename, -1);
        }
        std::string line;
        while (std::getline(file, line))
        {
            if (line.empty())
            {
                break;
            }
            if (line.find(',') != std::string::npos)
            {
                std::cerr << "Every query must have exactly one filter" << std::endl;
                exit(-1);
            }
            if (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
            {
                line.erase(line.size() - 1);
            }
            if (!elementSet.count(line))
            {
                result.push_back(line);
            }
            if (unique)
            {
                elementSet.insert(line);
            }
        }
        file.close();
    }
    else
    {
        throw diskann::ANNException(std::string("Failed to open file. filename can not be blank"), -1);
    }
    return result;
}

inline void clean_up_artifacts(tsl::robin_set<std::string> paths_to_clean, tsl::robin_set<std::string> path_suffixes)
{
    try
    {
        for (const auto &path : paths_to_clean)
        {
            for (const auto &suffix : path_suffixes)
            {
                std::string curr_path_to_clean(path + "_" + suffix);
                if (std::remove(curr_path_to_clean.c_str()) != 0)
                    diskann::cout << "Warning: Unable to remove file :" << curr_path_to_clean << std::endl;
            }
        }
        diskann::cout << "Cleaned all artifacts" << std::endl;
    }
    catch (const std::exception &e)
    {
        diskann::cout << "Warning: Unable to clean all artifacts " << e.what() << std::endl;
    }
}

template <typename T> inline const char *diskann_type_to_name() = delete;
template <> inline const char *diskann_type_to_name<float>()
{
    return "float";
}
template <> inline const char *diskann_type_to_name<uint8_t>()
{
    return "uint8";
}
template <> inline const char *diskann_type_to_name<int8_t>()
{
    return "int8";
}
template <> inline const char *diskann_type_to_name<uint16_t>()
{
    return "uint16";
}
template <> inline const char *diskann_type_to_name<int16_t>()
{
    return "int16";
}
template <> inline const char *diskann_type_to_name<uint32_t>()
{
    return "uint32";
}
template <> inline const char *diskann_type_to_name<int32_t>()
{
    return "int32";
}
template <> inline const char *diskann_type_to_name<uint64_t>()
{
    return "uint64";
}
template <> inline const char *diskann_type_to_name<int64_t>()
{
    return "int64";
}

#ifdef _WINDOWS
#include <intrin.h>
#include <Psapi.h>

extern bool AvxSupportedCPU;
extern bool Avx2SupportedCPU;

inline size_t getMemoryUsage()
{
    PROCESS_MEMORY_COUNTERS_EX pmc;
    GetProcessMemoryInfo(GetCurrentProcess(), (PROCESS_MEMORY_COUNTERS *)&pmc, sizeof(pmc));
    return pmc.PrivateUsage;
}

inline std::string getWindowsErrorMessage(DWORD lastError)
{
    char *errorText;
    FormatMessageA(
        // use system message tables to retrieve error text
        FORMAT_MESSAGE_FROM_SYSTEM
            // allocate buffer on local heap for error text
            | FORMAT_MESSAGE_ALLOCATE_BUFFER
            // Important! will fail otherwise, since we're not
            // (and CANNOT) pass insertion parameters
            | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL, // unused with FORMAT_MESSAGE_FROM_SYSTEM
        lastError, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&errorText, // output
        0,                 // minimum size for output buffer
        NULL);             // arguments - see note

    return errorText != nullptr ? std::string(errorText) : std::string();
}

inline void printProcessMemory(const char *message)
{
    PROCESS_MEMORY_COUNTERS counters;
    HANDLE h = GetCurrentProcess();
    GetProcessMemoryInfo(h, &counters, sizeof(counters));
    diskann::cout << message
                  << " [Peaking Working Set size: " << counters.PeakWorkingSetSize * 1.0 / (1024.0 * 1024 * 1024)
                  << "GB Working set size: " << counters.WorkingSetSize * 1.0 / (1024.0 * 1024 * 1024)
                  << "GB Private bytes " << counters.PagefileUsage * 1.0 / (1024 * 1024 * 1024) << "GB]" << std::endl;
}
#else

// need to check and change this
inline bool avx2Supported()
{
    return true;
}
inline void printProcessMemory(const char *)
{
}

inline size_t getMemoryUsage()
{ // for non-windows, we have not implemented this function
    return 0;
}

#endif

extern bool AvxSupportedCPU;
extern bool Avx2SupportedCPU;
