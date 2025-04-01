#pragma once

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>

#include "extern/diskann/include/distance.h"
#include "extern/diskann/include/pq_flash_index.h"
#include "vector_index.h"
#include <roaring/roaring.hh>

struct DiskannBuilderParameter : public BuilderParameter{
    diskann::Metric metric_type;
    int L;
    int R;
    int num_threads;
    double sample_rate;
    float indexing_ram_budget; //单位GB 
    float search_ram_budget; //单位GB
    int dim;

    DiskannBuilderParameter& with_mertic_type(VectorIndex::Metric metric){
        metric_type = convert_to_diskann_metric(metric);
        return *this;
    }    

    diskann::Metric convert_to_diskann_metric(VectorIndex::Metric metric) {
        switch (metric) {
            case VectorIndex::Metric::L2:
                return diskann::Metric::L2;
            case VectorIndex::Metric::COSINE:
                return diskann::Metric::COSINE;
            case VectorIndex::Metric::INNER_PRODUCT:
                return diskann::Metric::INNER_PRODUCT;
            default:
                throw std::invalid_argument("Unknown metric type");
        }
    }

    std::string metric_to_string(diskann::Metric metric) {
        switch (metric) {
            case diskann::Metric::L2:
                return "L2";
            case diskann::Metric::INNER_PRODUCT:
                return "INNER_PRODUCT";
            case diskann::Metric::COSINE:
                return "COSINE";
            case diskann::Metric::FAST_L2:
                return "FAST_L2";
            default:
                return "UNKNOWN";
        }
    }

    DiskannBuilderParameter& with_dim(int d){
        dim = d;
        return *this;
    }

    DiskannBuilderParameter& with_indexing_ram_budget_mb(float ram){
        indexing_ram_budget = ram / 1024;
        return *this;
    }

    DiskannBuilderParameter& with_search_ram_budget_mb(float ram){
        search_ram_budget = ram / 1024;
        return *this;
    }

    DiskannBuilderParameter& with_sample_rate(float rate){
        sample_rate = rate;
        return *this;
    }

    DiskannBuilderParameter& with_build_num_threads(int threads){
        num_threads = threads;
        return *this;
    }

    DiskannBuilderParameter& with_L(int l){
        L = l;
        return *this;
    }

    DiskannBuilderParameter& with_R(int r){
        R = r;
        return *this;
    }

    std::string  to_string(){
        std::ostringstream oss;
        oss << "metric_type:" << metric_to_string(metric_type)
                  << ", L:" << L
                  << ", R:" << R
                  << ", num_threads:" << num_threads
                  << ", sample_rate:" << sample_rate
                  << ", indexing_ram_budget:" << indexing_ram_budget <<"G"
                  << ", search_ram_budget:" << search_ram_budget <<"G"
                  << ", dim:" << dim;
        return oss.str();
    }
    diskann::Metric get_metric_type(){
        return metric_type;
    }
    int get_l(){
        return L;
    }
    int get_r(){
        return R;
    }
    int get_num_threads(){
        return num_threads;
    }
    double get_sample_rate(){
        return sample_rate;
    }
    float get_indexing_ram_budget(){
        return indexing_ram_budget;
    }
    int get_dim(){
        return dim;
    }
    float get_search_ram_budget(){
        return search_ram_budget;
    }
};


struct IDFilter : public diskann::Filter {
    private:
        std::shared_ptr<roaring::Roaring> _bitmap;
    public:
        IDFilter(std::shared_ptr<roaring::Roaring> bitmap){
            _bitmap = bitmap;
        }
        bool is_member(uint32_t idx){
            return _bitmap->contains(idx);
        }
};

struct DiskannSearchParameter : public SearchParameters{
    int search_list;
    int beam_width;
    std::shared_ptr<IDFilter> filter;
    DiskannSearchParameter(){
        filter = nullptr;
        search_list = 100;
        beam_width = 2;
    }
    DiskannSearchParameter& with_search_list(int l){
        search_list = l;
        return *this;
    }
    DiskannSearchParameter& with_beam_width(int width){
        beam_width = width;
        return *this;
    }

    DiskannSearchParameter& set_filter(std::shared_ptr<IDFilter> f){
        filter = f;
        return *this;
    }

    IDFilter *get_filter(){
        return filter.get();
    }
    int get_search_list(){
        return search_list;
    }
    int get_beam_width(){
        return beam_width;
    }
};



class WriterWrapper {
    private:
        std::shared_ptr<lucene::store::IndexOutput> _out;
    public:
        WriterWrapper(std::shared_ptr<lucene::store::IndexOutput> out);
        void write(uint8_t* b, uint64_t len);
};


//diskann的索引文件合到到1个文件，重新定义下每个部分代表什么含义
class DiskannFileDesc {
    public:
        static constexpr const char* PQ_PIVOTS_FILE_NAME = "pq_pivots_file";
        static constexpr const char* PQ_COMPRESSED_FILE_NAME = "pq_compressed_file";
        static constexpr const char* VAMANA_INDEX_FILE_NAME = "vamana_index_file";
        static constexpr const char* DISK_LAYOUT_FILE_NAME = "disk_layout_file";
        static constexpr const char* TAG_FILE_NAME = "tag_file";
        static const char* get_pq_pivots_file_name(){
            return PQ_PIVOTS_FILE_NAME;
        }
        static const char* get_pq_compressed_file_name(){
            return PQ_COMPRESSED_FILE_NAME;
        }
        static const char* get_vamana_index_file_name(){
            return VAMANA_INDEX_FILE_NAME;
        }
        static const char* get_disklayout_file_name(){
            return DISK_LAYOUT_FILE_NAME;
        }
        static const char* get_tag_file_name(){
            return TAG_FILE_NAME;
        }
};

class DiskannVectorIndex : public VectorIndex{
    private:
        std::shared_ptr<diskann::PQFlashIndex<float,uint16_t> > _pFlashIndex;
        std::shared_ptr<DiskannBuilderParameter> builderParameterPtr;
        //适配doris的存储介质
        std::shared_ptr<lucene::store::Directory> _dir;

        //原始向量
        std::stringstream _data_stream;
        //codebook临时缓存
        std::stringstream _pq_pivots_stream;
        //量化向量临时缓存
        std::stringstream _pq_compressed_stream;
        //图索引
        std::stringstream _vamana_index_stream;
        //最终的磁盘布局
        std::stringstream _disk_layout_stream;
        std::stringstream _tag_stream;

        int npt_num = 0 ;  // 向量总数
        int ndim = 0;     // 向量维度
        std::streampos npt_num_pos; // 记录 npt_num 在流中的位置

        std::mutex _data_stream_mutex;


        
    private:
        int calculate_num_pq_chunks();

    public:
        DiskannVectorIndex(std::shared_ptr<lucene::store::Directory> dir){
            builderParameterPtr = nullptr;
            _dir = dir;
            // 先写入占位的 npt_num 和 ndim
            npt_num_pos = _data_stream.tellp(); // 记录 npt_num 的偏移
            _data_stream.seekp(static_cast<std::streampos>(npt_num_pos));
            _data_stream.write(reinterpret_cast<const char*>(&npt_num), sizeof(npt_num));
            _data_stream.write(reinterpret_cast<const char*>(&ndim), sizeof(ndim));
        }
        doris::Status add(int n, const float *vec);
        doris::Status build();
        void set_build_params(std::shared_ptr<BuilderParameter> params){
            builderParameterPtr = std::static_pointer_cast<DiskannBuilderParameter>(params);

            //设置dim
            ndim = builderParameterPtr->get_dim();
            std::streamoff pos = static_cast<std::streamoff>(npt_num_pos + std::streampos(sizeof(npt_num)));
            _data_stream.seekp(pos, std::ios::beg);
            _data_stream.write(reinterpret_cast<const char*>(&ndim), sizeof(ndim));
        }
        doris::Status search(
                const float * query_vec,
                int k,
                SearchResult *result,
                const SearchParameters* params = nullptr);
        //把std::string的内容刷到_dir中
        doris::Status save();
        //负责从dir中解析内容
        doris::Status load(VectorIndex::Metric dist_fn);
    private:
        doris::Status stream_write_to_output(std::stringstream &stream, lucene::store::IndexOutput *output);
};

