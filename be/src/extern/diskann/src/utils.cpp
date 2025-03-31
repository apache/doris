// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "utils.h"

#include <stdio.h>

#ifdef EXEC_ENV_OLS
#include "aligned_file_reader.h"
#endif

const uint32_t MAX_REQUEST_SIZE = 1024 * 1024 * 1024; // 64MB
const uint32_t MAX_SIMULTANEOUS_READ_REQUESTS = 128;

#ifdef _WINDOWS
#include <intrin.h>

// Taken from:
// https://insufficientlycomplicated.wordpress.com/2011/11/07/detecting-intel-advanced-vector-extensions-avx-in-visual-studio/
bool cpuHasAvxSupport()
{
    bool avxSupported = false;

    // Checking for AVX requires 3 things:
    // 1) CPUID indicates that the OS uses XSAVE and XRSTORE
    //     instructions (allowing saving YMM registers on context
    //     switch)
    // 2) CPUID indicates support for AVX
    // 3) XGETBV indicates the AVX registers will be saved and
    //     restored on context switch
    //
    // Note that XGETBV is only available on 686 or later CPUs, so
    // the instruction needs to be conditionally run.
    int cpuInfo[4];
    __cpuid(cpuInfo, 1);

    bool osUsesXSAVE_XRSTORE = cpuInfo[2] & (1 << 27) || false;
    bool cpuAVXSuport = cpuInfo[2] & (1 << 28) || false;

    if (osUsesXSAVE_XRSTORE && cpuAVXSuport)
    {
        // Check if the OS will save the YMM registers
        unsigned long long xcrFeatureMask = _xgetbv(_XCR_XFEATURE_ENABLED_MASK);
        avxSupported = (xcrFeatureMask & 0x6) || false;
    }

    return avxSupported;
}

bool cpuHasAvx2Support()
{
    int cpuInfo[4];
    __cpuid(cpuInfo, 0);
    int n = cpuInfo[0];
    if (n >= 7)
    {
        __cpuidex(cpuInfo, 7, 0);
        static int avx2Mask = 0x20;
        return (cpuInfo[1] & avx2Mask) > 0;
    }
    return false;
}

bool AvxSupportedCPU = cpuHasAvxSupport();
bool Avx2SupportedCPU = cpuHasAvx2Support();

#else

bool Avx2SupportedCPU = true;
bool AvxSupportedCPU = false;
#endif

namespace diskann
{

void block_convert(std::ofstream &writr, std::ifstream &readr, float *read_buf, size_t npts, size_t ndims)
{
    readr.read((char *)read_buf, npts * ndims * sizeof(float));
    uint32_t ndims_u32 = (uint32_t)ndims;
#pragma omp parallel for
    for (int64_t i = 0; i < (int64_t)npts; i++)
    {
        float norm_pt = std::numeric_limits<float>::epsilon();
        for (uint32_t dim = 0; dim < ndims_u32; dim++)
        {
            norm_pt += *(read_buf + i * ndims + dim) * *(read_buf + i * ndims + dim);
        }
        norm_pt = std::sqrt(norm_pt);
        for (uint32_t dim = 0; dim < ndims_u32; dim++)
        {
            *(read_buf + i * ndims + dim) = *(read_buf + i * ndims + dim) / norm_pt;
        }
    }
    writr.write((char *)read_buf, npts * ndims * sizeof(float));
}

void normalize_data_file(const std::string &inFileName, const std::string &outFileName)
{
    std::ifstream readr(inFileName, std::ios::binary);
    std::ofstream writr(outFileName, std::ios::binary);

    int npts_s32, ndims_s32;
    readr.read((char *)&npts_s32, sizeof(int32_t));
    readr.read((char *)&ndims_s32, sizeof(int32_t));

    writr.write((char *)&npts_s32, sizeof(int32_t));
    writr.write((char *)&ndims_s32, sizeof(int32_t));

    size_t npts = (size_t)npts_s32;
    size_t ndims = (size_t)ndims_s32;
    diskann::cout << "Normalizing FLOAT vectors in file: " << inFileName << std::endl;
    diskann::cout << "Dataset: #pts = " << npts << ", # dims = " << ndims << std::endl;

    size_t blk_size = 131072;
    size_t nblks = ROUND_UP(npts, blk_size) / blk_size;
    diskann::cout << "# blks: " << nblks << std::endl;

    float *read_buf = new float[npts * ndims];
    for (size_t i = 0; i < nblks; i++)
    {
        size_t cblk_size = std::min(npts - i * blk_size, blk_size);
        block_convert(writr, readr, read_buf, cblk_size, ndims);
    }
    delete[] read_buf;

    diskann::cout << "Wrote normalized points to file: " << outFileName << std::endl;
}

double calculate_recall(uint32_t num_queries, uint32_t *gold_std, float *gs_dist, uint32_t dim_gs,
                        uint32_t *our_results, uint32_t dim_or, uint32_t recall_at)
{
    double total_recall = 0;
    std::set<uint32_t> gt, res;

    for (size_t i = 0; i < num_queries; i++)
    {
        gt.clear();
        res.clear();
        uint32_t *gt_vec = gold_std + dim_gs * i;
        uint32_t *res_vec = our_results + dim_or * i;
        size_t tie_breaker = recall_at;
        if (gs_dist != nullptr)
        {
            tie_breaker = recall_at - 1;
            float *gt_dist_vec = gs_dist + dim_gs * i;
            while (tie_breaker < dim_gs && gt_dist_vec[tie_breaker] == gt_dist_vec[recall_at - 1])
                tie_breaker++;
        }

        gt.insert(gt_vec, gt_vec + tie_breaker);
        res.insert(res_vec,
                   res_vec + recall_at); // change to recall_at for recall k@k
                                         // or dim_or for k@dim_or
        uint32_t cur_recall = 0;
        for (auto &v : gt)
        {
            if (res.find(v) != res.end())
            {
                cur_recall++;
            }
        }
        total_recall += cur_recall;
    }
    return total_recall / (num_queries) * (100.0 / recall_at);
}

double calculate_recall(uint32_t num_queries, uint32_t *gold_std, float *gs_dist, uint32_t dim_gs,
                        uint32_t *our_results, uint32_t dim_or, uint32_t recall_at,
                        const tsl::robin_set<uint32_t> &active_tags)
{
    double total_recall = 0;
    std::set<uint32_t> gt, res;
    bool printed = false;
    for (size_t i = 0; i < num_queries; i++)
    {
        gt.clear();
        res.clear();
        uint32_t *gt_vec = gold_std + dim_gs * i;
        uint32_t *res_vec = our_results + dim_or * i;
        size_t tie_breaker = recall_at;
        uint32_t active_points_count = 0;
        uint32_t cur_counter = 0;
        while (active_points_count < recall_at && cur_counter < dim_gs)
        {
            if (active_tags.find(*(gt_vec + cur_counter)) != active_tags.end())
            {
                active_points_count++;
            }
            cur_counter++;
        }
        if (active_tags.empty())
            cur_counter = recall_at;

        if ((active_points_count < recall_at && !active_tags.empty()) && !printed)
        {
            diskann::cout << "Warning: Couldn't find enough closest neighbors " << active_points_count << "/"
                          << recall_at
                          << " from "
                             "truthset for query # "
                          << i << ". Will result in under-reported value of recall." << std::endl;
            printed = true;
        }
        if (gs_dist != nullptr)
        {
            tie_breaker = cur_counter - 1;
            float *gt_dist_vec = gs_dist + dim_gs * i;
            while (tie_breaker < dim_gs && gt_dist_vec[tie_breaker] == gt_dist_vec[cur_counter - 1])
                tie_breaker++;
        }

        gt.insert(gt_vec, gt_vec + tie_breaker);
        res.insert(res_vec, res_vec + recall_at);
        uint32_t cur_recall = 0;
        for (auto &v : res)
        {
            if (gt.find(v) != gt.end())
            {
                cur_recall++;
            }
        }
        total_recall += cur_recall;
    }
    return ((double)(total_recall / (num_queries))) * ((double)(100.0 / recall_at));
}

double calculate_range_search_recall(uint32_t num_queries, std::vector<std::vector<uint32_t>> &groundtruth,
                                     std::vector<std::vector<uint32_t>> &our_results)
{
    double total_recall = 0;
    std::set<uint32_t> gt, res;

    for (size_t i = 0; i < num_queries; i++)
    {
        gt.clear();
        res.clear();

        gt.insert(groundtruth[i].begin(), groundtruth[i].end());
        res.insert(our_results[i].begin(), our_results[i].end());
        uint32_t cur_recall = 0;
        for (auto &v : gt)
        {
            if (res.find(v) != res.end())
            {
                cur_recall++;
            }
        }
        if (gt.size() != 0)
            total_recall += ((100.0 * cur_recall) / gt.size());
        else
            total_recall += 100;
    }
    return total_recall / (num_queries);
}

#ifdef EXEC_ENV_OLS
void get_bin_metadata(AlignedFileReader &reader, size_t &npts, size_t &ndim, size_t offset)
{
    std::vector<AlignedRead> readReqs;
    AlignedRead readReq;
    uint32_t buf[2]; // npts/ndim are uint32_ts.

    readReq.buf = buf;
    readReq.offset = offset;
    readReq.len = 2 * sizeof(uint32_t);
    readReqs.push_back(readReq);

    IOContext &ctx = reader.get_ctx();
    reader.read(readReqs, ctx); // synchronous
    if ((*(ctx.m_pRequestsStatus))[0] == IOContext::READ_SUCCESS)
    {
        npts = buf[0];
        ndim = buf[1];
        diskann::cout << "File has: " << npts << " points, " << ndim << " dimensions at offset: " << offset
                      << std::endl;
    }
    else
    {
        std::stringstream str;
        str << "Could not read binary metadata from index file at offset: " << offset << std::endl;
        throw diskann::ANNException(str.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
}

template <typename T> void load_bin(AlignedFileReader &reader, T *&data, size_t &npts, size_t &ndim, size_t offset)
{
    // Code assumes that the reader is already setup correctly.
    get_bin_metadata(reader, npts, ndim, offset);
    data = new T[npts * ndim];

    size_t data_size = npts * ndim * sizeof(T);
    size_t write_offset = 0;
    size_t read_start = offset + 2 * sizeof(uint32_t);

    // BingAlignedFileReader can only read uint32_t bytes of data. So,
    // we limit ourselves even more to reading 1GB at a time.
    std::vector<AlignedRead> readReqs;
    while (data_size > 0)
    {
        AlignedRead readReq;
        readReq.buf = data + write_offset;
        readReq.offset = read_start + write_offset;
        readReq.len = data_size > MAX_REQUEST_SIZE ? MAX_REQUEST_SIZE : data_size;
        readReqs.push_back(readReq);
        // in the corner case, the loop will not execute
        data_size -= readReq.len;
        write_offset += readReq.len;
    }
    IOContext &ctx = reader.get_ctx();
    reader.read(readReqs, ctx);
    for (int i = 0; i < readReqs.size(); i++)
    {
        // Since we are making sync calls, no request will be in the
        // READ_WAIT state.
        if ((*(ctx.m_pRequestsStatus))[i] != IOContext::READ_SUCCESS)
        {
            std::stringstream str;
            str << "Could not read binary data from index file at offset: " << readReqs[i].offset << std::endl;
            throw diskann::ANNException(str.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
        }
    }
}
template <typename T>
void load_bin(AlignedFileReader &reader, std::unique_ptr<T[]> &data, size_t &npts, size_t &ndim, size_t offset)
{
    T *ptr = nullptr;
    load_bin(reader, ptr, npts, ndim, offset);
    data.reset(ptr);
}

template <typename T>
void copy_aligned_data_from_file(AlignedFileReader &reader, T *&data, size_t &npts, size_t &ndim,
                                 const size_t &rounded_dim, size_t offset)
{
    if (data == nullptr)
    {
        diskann::cerr << "Memory was not allocated for " << data << " before calling the load function. Exiting..."
                      << std::endl;
        throw diskann::ANNException("Null pointer passed to copy_aligned_data_from_file()", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }

    size_t pts, dim;
    get_bin_metadata(reader, pts, dim, offset);

    if (ndim != dim || npts != pts)
    {
        std::stringstream ss;
        ss << "Either file dimension: " << dim << " is != passed dimension: " << ndim << " or file #pts: " << pts
           << " is != passed #pts: " << npts << std::endl;
        throw diskann::ANNException(ss.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    // Instead of reading one point of ndim size and setting (rounded_dim - dim)
    // values to zero We'll set everything to zero and read in chunks of data at
    // the appropriate locations.
    size_t read_offset = offset + 2 * sizeof(uint32_t);
    memset(data, 0, npts * rounded_dim * sizeof(T));
    int i = 0;
    std::vector<AlignedRead> read_requests;

    while (i < npts)
    {
        int j = 0;
        read_requests.clear();
        while (j < MAX_SIMULTANEOUS_READ_REQUESTS && i < npts)
        {
            AlignedRead read_req;
            read_req.buf = data + i * rounded_dim;
            read_req.len = dim * sizeof(T);
            read_req.offset = read_offset + i * dim * sizeof(T);
            read_requests.push_back(read_req);
            i++;
            j++;
        }
        IOContext &ctx = reader.get_ctx();
        reader.read(read_requests, ctx);
        for (int k = 0; k < read_requests.size(); k++)
        {
            if ((*ctx.m_pRequestsStatus)[k] != IOContext::READ_SUCCESS)
            {
                throw diskann::ANNException("Load data from file using AlignedReader failed.", -1, __FUNCSIG__,
                                            __FILE__, __LINE__);
            }
        }
    }
}

// Unlike load_bin, assumes that data is already allocated 'size' entries
template <typename T> void read_array(AlignedFileReader &reader, T *data, size_t size, size_t offset)
{
    if (data == nullptr)
    {
        throw diskann::ANNException("read_array requires an allocated buffer.", -1);
    }

    if (size * sizeof(T) > MAX_REQUEST_SIZE)
    {
        std::stringstream ss;
        ss << "Cannot read more than " << MAX_REQUEST_SIZE << " bytes. Current request size: " << std::to_string(size)
           << " sizeof(T): " << sizeof(T) << std::endl;
        throw diskann::ANNException(ss.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    std::vector<AlignedRead> read_requests;
    AlignedRead read_req;
    read_req.buf = data;
    read_req.len = size * sizeof(T);
    read_req.offset = offset;
    read_requests.push_back(read_req);
    IOContext &ctx = reader.get_ctx();
    reader.read(read_requests, ctx);

    if ((*(ctx.m_pRequestsStatus))[0] != IOContext::READ_SUCCESS)
    {
        std::stringstream ss;
        ss << "Failed to read_array() of size: " << size * sizeof(T) << " at offset: " << offset << " from reader. "
           << std::endl;
        throw diskann::ANNException(ss.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
}

template <typename T> void read_value(AlignedFileReader &reader, T &value, size_t offset)
{
    read_array(reader, &value, 1, offset);
}

template DISKANN_DLLEXPORT void load_bin<uint8_t>(AlignedFileReader &reader, std::unique_ptr<uint8_t[]> &data,
                                                  size_t &npts, size_t &ndim, size_t offset);
template DISKANN_DLLEXPORT void load_bin<int8_t>(AlignedFileReader &reader, std::unique_ptr<int8_t[]> &data,
                                                 size_t &npts, size_t &ndim, size_t offset);
template DISKANN_DLLEXPORT void load_bin<uint32_t>(AlignedFileReader &reader, std::unique_ptr<uint32_t[]> &data,
                                                   size_t &npts, size_t &ndim, size_t offset);
template DISKANN_DLLEXPORT void load_bin<uint64_t>(AlignedFileReader &reader, std::unique_ptr<uint64_t[]> &data,
                                                   size_t &npts, size_t &ndim, size_t offset);
template DISKANN_DLLEXPORT void load_bin<int64_t>(AlignedFileReader &reader, std::unique_ptr<int64_t[]> &data,
                                                  size_t &npts, size_t &ndim, size_t offset);
template DISKANN_DLLEXPORT void load_bin<float>(AlignedFileReader &reader, std::unique_ptr<float[]> &data, size_t &npts,
                                                size_t &ndim, size_t offset);

template DISKANN_DLLEXPORT void load_bin<uint8_t>(AlignedFileReader &reader, uint8_t *&data, size_t &npts, size_t &ndim,
                                                  size_t offset);
template DISKANN_DLLEXPORT void load_bin<int64_t>(AlignedFileReader &reader, int64_t *&data, size_t &npts, size_t &ndim,
                                                  size_t offset);
template DISKANN_DLLEXPORT void load_bin<uint64_t>(AlignedFileReader &reader, uint64_t *&data, size_t &npts,
                                                   size_t &ndim, size_t offset);
template DISKANN_DLLEXPORT void load_bin<uint32_t>(AlignedFileReader &reader, uint32_t *&data, size_t &npts,
                                                   size_t &ndim, size_t offset);
template DISKANN_DLLEXPORT void load_bin<int32_t>(AlignedFileReader &reader, int32_t *&data, size_t &npts, size_t &ndim,
                                                  size_t offset);

template DISKANN_DLLEXPORT void copy_aligned_data_from_file<uint8_t>(AlignedFileReader &reader, uint8_t *&data,
                                                                     size_t &npts, size_t &dim,
                                                                     const size_t &rounded_dim, size_t offset);
template DISKANN_DLLEXPORT void copy_aligned_data_from_file<int8_t>(AlignedFileReader &reader, int8_t *&data,
                                                                    size_t &npts, size_t &dim,
                                                                    const size_t &rounded_dim, size_t offset);
template DISKANN_DLLEXPORT void copy_aligned_data_from_file<float>(AlignedFileReader &reader, float *&data,
                                                                   size_t &npts, size_t &dim, const size_t &rounded_dim,
                                                                   size_t offset);

template DISKANN_DLLEXPORT void read_array<char>(AlignedFileReader &reader, char *data, size_t size, size_t offset);

template DISKANN_DLLEXPORT void read_array<uint8_t>(AlignedFileReader &reader, uint8_t *data, size_t size,
                                                    size_t offset);
template DISKANN_DLLEXPORT void read_array<int8_t>(AlignedFileReader &reader, int8_t *data, size_t size, size_t offset);
template DISKANN_DLLEXPORT void read_array<uint32_t>(AlignedFileReader &reader, uint32_t *data, size_t size,
                                                     size_t offset);
template DISKANN_DLLEXPORT void read_array<float>(AlignedFileReader &reader, float *data, size_t size, size_t offset);

template DISKANN_DLLEXPORT void read_value<uint8_t>(AlignedFileReader &reader, uint8_t &value, size_t offset);
template DISKANN_DLLEXPORT void read_value<int8_t>(AlignedFileReader &reader, int8_t &value, size_t offset);
template DISKANN_DLLEXPORT void read_value<float>(AlignedFileReader &reader, float &value, size_t offset);
template DISKANN_DLLEXPORT void read_value<uint32_t>(AlignedFileReader &reader, uint32_t &value, size_t offset);
template DISKANN_DLLEXPORT void read_value<uint64_t>(AlignedFileReader &reader, uint64_t &value, size_t offset);

#endif

} // namespace diskann
