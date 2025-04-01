
#include <iostream>
#include <omp.h>
#include <boost/program_options.hpp>
#include <random>

#include "extern/diskann/include/utils.h"
#include "extern/diskann/include/disk_utils.h"
#include "extern/diskann/include/math_utils.h"
#include "extern/diskann/include/index.h"
#include "extern/diskann/include/partition.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "extern/diskann/include/linux_aligned_file_reader.h"

#include "extern/diskann/include/pq_flash_index.h"
#include "extern/diskann/include/combined_file.h"
#include <string.h>
#include "extern/diskann/include/timer.h"

#include <fstream>
#include <filesystem>
#include <queue>
#include <map>
#include <condition_variable>
 
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include "diskann_vector_index.h"
#include "ann_parse.h"

#define FINALLY_CLOSE(x)              \
    try {                             \
        if (x != nullptr) {           \
            x->close();               \
            delete x;                  \
        }                             \
    } catch (...) {                   \
    }

doris::Status DiskannVectorIndex::add(int n, const float *vec){
    // 追加数据
    _data_stream.write(reinterpret_cast<const char*>(vec), n * ndim * sizeof(float));
    npt_num += n;
    // 保存当前位置
    std::streampos current_pos = _data_stream.tellp();
    // 回到 npt_num 位置更新它
    _data_stream.seekp(npt_num_pos);
    _data_stream.write(reinterpret_cast<const char*>(&npt_num), sizeof(npt_num));
    // 恢复写入指针
    _data_stream.seekp(current_pos);
    if (_data_stream.fail()) {
        return doris::Status::IOError("Failed to write vector data");
    }
    return doris::Status::OK();
}  


int  DiskannVectorIndex::calculate_num_pq_chunks(){
    double final_index_ram_limit = diskann::get_memory_budget(builderParameterPtr->get_search_ram_budget());
    int num_pq_chunks = diskann::calculate_num_pq_chunks(final_index_ram_limit, npt_num, builderParameterPtr->dim);
    return num_pq_chunks;
}

doris::Status DiskannVectorIndex::build(){
    try{
        diskann::generate_quantized_data<float>(_data_stream, 
                                            _pq_pivots_stream, 
                                            _pq_compressed_stream, 
                                            builderParameterPtr->get_metric_type(),
                                            builderParameterPtr->get_sample_rate(),
                                            calculate_num_pq_chunks(), 
                                            false, 
                                            "");
        _data_stream.seekg(0, _data_stream.beg);
        diskann::build_merged_vamana_index<float>(_data_stream, 
                                            builderParameterPtr->get_metric_type(), 
                                            builderParameterPtr->get_l(),
                                            builderParameterPtr->get_r(), 
                                            1,
                                            builderParameterPtr->get_indexing_ram_budget(), 
                                            _vamana_index_stream, "", "",
                                            0, false, 
                                            builderParameterPtr->get_num_threads(), false, "",
                                            "", "", 0);
        _data_stream.seekg(0, _data_stream.beg);
        diskann::create_disk_layout<float>(_data_stream, _vamana_index_stream, _disk_layout_stream);
        return doris::Status::OK();
    } catch (const std::exception& e) {
        return doris::Status::InternalError<true>(std::string(e.what()));
    }
}




doris::Status DiskannVectorIndex::save(){
    try{
        //构建索引到临时stringstream中
        RETURN_IF_ERROR(build());
        //把stream刷到存储层
        lucene::store::IndexOutput* pq_pivots_output =  _dir->createOutput(DiskannFileDesc::get_pq_pivots_file_name());
        lucene::store::IndexOutput* pq_compressed_output = _dir->createOutput(DiskannFileDesc::get_pq_compressed_file_name());
        lucene::store::IndexOutput* vamana_index_output = _dir->createOutput(DiskannFileDesc::get_vamana_index_file_name());
        lucene::store::IndexOutput* disk_layout_output = _dir->createOutput(DiskannFileDesc::get_disklayout_file_name());
        lucene::store::IndexOutput* tag_output = _dir->createOutput(DiskannFileDesc::get_tag_file_name());
        RETURN_IF_ERROR(stream_write_to_output(_pq_pivots_stream, pq_pivots_output));
        RETURN_IF_ERROR(stream_write_to_output(_pq_compressed_stream, pq_compressed_output));
        RETURN_IF_ERROR(stream_write_to_output(_vamana_index_stream, vamana_index_output));
        RETURN_IF_ERROR(stream_write_to_output(_disk_layout_stream, disk_layout_output));
        RETURN_IF_ERROR(stream_write_to_output(_tag_stream, tag_output));
        FINALLY_CLOSE(pq_pivots_output);
        FINALLY_CLOSE(pq_compressed_output);
        FINALLY_CLOSE(vamana_index_output);
        FINALLY_CLOSE(disk_layout_output);
        FINALLY_CLOSE(tag_output);
    } catch (const std::exception& e) {
        return doris::Status::InternalError(e.what());
    }
    return doris::Status::OK();
}

doris::Status DiskannVectorIndex::stream_write_to_output(std::stringstream &stream, lucene::store::IndexOutput *output) {
    try {
        stream.seekg(0, std::ios::beg); // 确保从头开始读取
        if (!stream.good()) {
            return doris::Status::Corruption("stream seekg failed");
        }
        const size_t buffer_size = 4096;  // 4KB 缓冲区
        std::vector<char> buffer(buffer_size);
        while (stream) {  // 只要 stream 仍然有效，就继续读取
            stream.read(buffer.data(), buffer_size);
            std::streamsize bytes_read = stream.gcount();  // 获取实际读取的字节数
            if (bytes_read > 0) {
                output->writeBytes(reinterpret_cast<const uint8_t*>(buffer.data()), static_cast<int32_t>(bytes_read));
            }
        }
        return doris::Status::OK();
    } catch (const std::exception &e) {
        return doris::Status::Corruption(std::string("failed stream write to output, message=") + e.what());
    }
}



doris::Status  DiskannVectorIndex::load(VectorIndex::Metric dist_fn){
    diskann::Metric metric;
    if (dist_fn == VectorIndex::Metric::L2) {
        metric = diskann::Metric::L2;
    } else if (dist_fn == VectorIndex::Metric::INNER_PRODUCT) {
        metric = diskann::Metric::INNER_PRODUCT;
    } else if (dist_fn ==  VectorIndex::Metric::COSINE){
        metric = diskann::Metric::COSINE;
    } else {
        std::cout << "Error. Only l2 and mips distance functions are supported" << std::endl;
        return doris::Status::InternalError("Error. Only l2 and mips distance functions are supported");
    }
    lucene::store::IndexInput* pq_pivots_input = _dir->openInput(DiskannFileDesc::get_pq_pivots_file_name());
    std::cout << "Actual type: " << typeid(*pq_pivots_input).name() << std::endl;

    lucene::store::IndexInput* pq_compressed_input = _dir->openInput(DiskannFileDesc::get_pq_compressed_file_name());
    lucene::store::IndexInput* vamana_index_input = _dir->openInput(DiskannFileDesc::get_vamana_index_file_name());
    lucene::store::IndexInput* disk_layout_input = _dir->openInput(DiskannFileDesc::get_disklayout_file_name());
    lucene::store::IndexInput* tag_input = _dir->openInput(DiskannFileDesc::get_tag_file_name());
    //Try to minimize the intrusion of Lucene code into the source code of Diskann, so we will split it into a layer here
    std::shared_ptr<IndexInputReaderWrapper> pq_pivots_reader(new IndexInputReaderWrapper(pq_pivots_input));
    std::shared_ptr<IndexInputReaderWrapper> pq_compressed_reader(new IndexInputReaderWrapper(pq_compressed_input));
    std::shared_ptr<IndexInputReaderWrapper> vamana_index_reader(new IndexInputReaderWrapper(vamana_index_input));
    std::shared_ptr<IndexInputReaderWrapper> disk_layout_reader(new IndexInputReaderWrapper(disk_layout_input));
    std::shared_ptr<IndexInputReaderWrapper> tag_reader(new IndexInputReaderWrapper(tag_input));
    
    _pFlashIndex = std::make_shared<diskann::PQFlashIndex<float,uint16_t>>(disk_layout_reader, metric);
    _pFlashIndex->load(8, pq_pivots_reader, pq_compressed_reader, vamana_index_reader, disk_layout_reader, tag_reader);
    return doris::Status::OK();
}

doris::Status DiskannVectorIndex::search(const float * query_vec,int topk, SearchResult *result,const SearchParameters* params){
    try {
        DiskannSearchParameter *searchParam = (DiskannSearchParameter*)params;
        IDFilter *filter = searchParam->get_filter();
        int optimized_beamwidth = searchParam->get_beam_width();
        int search_list = searchParam->get_search_list();
        std::vector<uint64_t> query_result_ids_64(topk);
        std::vector<float> query_result_dists(topk);
        diskann::QueryStats* stats =  static_cast<diskann::QueryStats*>(result->stat);
        uint32_t k =  _pFlashIndex->cached_beam_search(query_vec, topk, search_list,
                                            query_result_ids_64.data(),
                                            query_result_dists.data(),
                                            optimized_beamwidth, filter, stats);
        result->rows = k;
        for(int i=0;i<k;i++){
            result->distances.push_back(query_result_dists[i]);
            result->ids.push_back(query_result_ids_64[i]);
        }
        return doris::Status::OK();
    } catch (const std::exception& e) {
        return doris::Status::InternalError(e.what());
    }
}



