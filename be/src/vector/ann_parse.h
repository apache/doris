
#include <iostream>
#include "extern/diskann/include/utils.h"

// used for debug
class AnnParse{
    public:
        static void parse_data_stream(std::stringstream &data_stream, const std::string &point) {
            int32_t nrows_32 = 0, ncols_32 = 0;
            data_stream.seekg(0, std::ios::beg);
            if (!data_stream.read(reinterpret_cast<char*>(&nrows_32), sizeof(int32_t)) ||
                !data_stream.read(reinterpret_cast<char*>(&ncols_32), sizeof(int32_t))) {
                std::cerr << "Error: Failed to read nrows_32 and ncols_32 from data_stream." << std::endl;
                return;
            }
            std::cout << "====================data stream parse result(" << point << ")====================" << std::endl;
            std::cout << "rows:" << nrows_32
                    << ", clos:" << ncols_32 << std::endl;
            for(int i=0;i<nrows_32;i++){
                std::cout << "vec["<<i<<"]"<<"->"<<"[";
                for(int j=0;j<ncols_32;j++){
                    float vec;
                    data_stream.read(reinterpret_cast<char*>(&vec), 4);
                    std::cout << vec << ",";
                }
                std::cout << "]\n";
            }
            data_stream.clear();
            data_stream.seekg(0, std::ios::beg);
        }

        static void parse_compressed_stream(std::stringstream &data_stream, const std::string &point) {
            int32_t nrows_32 = 0, ncols_32 = 0;
            data_stream.seekg(0, std::ios::beg);
            if (!data_stream.read(reinterpret_cast<char*>(&nrows_32), sizeof(int32_t)) ||
                !data_stream.read(reinterpret_cast<char*>(&ncols_32), sizeof(int32_t))) {
                std::cerr << "Error: Failed to read nrows_32 and ncols_32 from data_stream." << std::endl;
                return;
            }
            std::cout << "====================compressed stream parse result(" << point << ")====================" << std::endl;
            std::cout << "rows:" << nrows_32
                    << ", clos:" << ncols_32 << std::endl;
            for(int i=0;i<nrows_32;i++){
                std::cout << "vec["<<i<<"]"<<"->"<<"[";
                for(int j=0;j<ncols_32;j++){
                    uint8_t vec;
                    if(!data_stream.read(reinterpret_cast<char*>(&vec), 1)){
                        std::cerr << "Error: Failed to read from compressed_stream." << std::endl;
                    }
                    std::cout << int(vec) << ",";
                }
                std::cout << "]\n";
            }
            data_stream.clear();
            data_stream.seekg(0, std::ios::beg);
        }

        static void parse_pivots_stream(std::stringstream &ss, const std::string &point){
            std::cout << "==================== pivots stream parse result(" << point << ")====================" << std::endl;
            ss.seekg(0);
            size_t *cumu_offsets;
            size_t rows;
            size_t clos;
            diskann::load_bin<size_t>(ss, cumu_offsets, rows, clos, 0);
            std::cout << "cumu_offsets:[";
            for(int i=0;i<rows;i++){
                std::cout << *(cumu_offsets+i) << ",";
            }
            std::cout << "]";

            float *codebook;
            diskann::load_bin<float>(ss, codebook, rows, clos, cumu_offsets[0]);
            std::cout << "codebook:["<<rows<<","<<clos<<"]"<< std::endl;
            for(int i=0;i<rows;i++){
                std::cout << "vec["<< i<<"]->[";
                for(int j=0;j<clos;j++){
                    std::cout << codebook[i*clos+j] <<",";
                }
                std::cout << "]\n";
            }

            float *centroid;
            diskann::load_bin<float>(ss, centroid, rows, clos, cumu_offsets[1]);
            std::cout << "centroid:["<<rows<<","<<clos<<"]->[";
            for(int i=0;i<rows;i++){
                std::cout << centroid[i]<<",";
            }
            std::cout << "]\n";

            uint32_t *chunk_offsets;
            diskann::load_bin<uint32_t>(ss, chunk_offsets, rows, clos, cumu_offsets[2]);
            std::cout << "chunk_offsets:["<<rows<<","<<clos<<"]->[";
            for(int i=0;i<rows;i++){
                std::cout << chunk_offsets[i]<<",";
            }
            std::cout << "]\n";
            delete []cumu_offsets;
            delete []codebook;
            delete []centroid;
            delete []chunk_offsets;
        }

        static void parse_index_stream(std::stringstream &ss, const std::string &point){
            std::cout << "====================index stream parse result(" <<  point << ")====================" << std::endl;
            ss.seekg(0);
            // 读取头部信息
            uint64_t index_size;
            uint32_t max_observed_max_degree;
            uint32_t start_point;
            uint64_t num_forzen_points;
            ss.read(reinterpret_cast<char*>(&index_size), sizeof(uint64_t));
            ss.read(reinterpret_cast<char*>(&max_observed_max_degree), sizeof(uint32_t));
            ss.read(reinterpret_cast<char*>(&start_point), sizeof(uint32_t));
            ss.read(reinterpret_cast<char*>(&num_forzen_points), sizeof(uint64_t));

            std::cout << "index_size:" << index_size << "\n"
                      << "max_observed_max_degree:" << max_observed_max_degree << "\n"
                      << "start_point:" << start_point << "\n"
                      << "num_forzen_points" << num_forzen_points << "\n";
            
            // 持续读取向量，直到达到index_size
            size_t current_offset = 24;
            int idx = 0;
            while (current_offset < index_size) {
                // 读取邻居个数
                uint32_t neighbor_count;
                ss.read(reinterpret_cast<char*>(&neighbor_count), sizeof(uint32_t));
                current_offset += 4;
                // 读取邻居ID列表
                std::vector<uint32_t> neighbor_ids(neighbor_count);
                ss.read(reinterpret_cast<char*>(neighbor_ids.data()), neighbor_count * sizeof(uint32_t));
                current_offset += neighbor_count * 4;
                std::cout <<"vec["<<idx<<"] has "<< neighbor_count <<" neighbors, neighbor ids=[";
                for(int m=0;m<neighbor_count;m++){
                    std::cout << neighbor_ids[m] << ",";
                }
                std::cout << "]" << std::endl;
                idx++;
            }
            ss.seekg(0);
        }

};