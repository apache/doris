#pragma once

#include "common/status.h"
#include "common/exception.h"
struct SearchResult {
    int rows;
    std::vector<float> distances;
    std::vector<int64_t> ids;
    void *stat;  //统计分析

    SearchResult(){
        rows = 0;
        stat = nullptr;
    }
    float get_distance(int idx){
        if (idx < 0 || idx >= static_cast<int>(distances.size())) {
            throw std::out_of_range("Invalid distance index");
        }
        return distances[idx];
    }
    int64_t get_id(int idx){
        if (idx < 0 || idx >= static_cast<int>(ids.size())) {
            throw std::out_of_range("Invalid ID index");
        }
        return ids[idx];
    }
    void reset(){
        rows = 0;
        distances.clear();
        ids.clear();
    }
    bool has_rows(){
        return rows > 0;
    }
    int row_count(){
        return rows;
    }
};

struct SearchParameters {
    virtual ~SearchParameters() {}
};

struct BuilderParameter {

};

class VectorIndex {
public:
    enum class Metric {
        L2,
        COSINE,
        INNER_PRODUCT,
        UNKNOWN
    };
    
    virtual doris::Status add(int n, const float *vec) =0;
    virtual void  set_build_params(std::shared_ptr<BuilderParameter> params)=0;
    virtual doris::Status search(
                const float * query_vec,
                int k,
                SearchResult *result,
                const SearchParameters* params = nullptr) =0;
   //virtual Status save(FileWriter* writer); 
    virtual doris::Status save()=0;
    
    //virtual Status load(FileReader* reader);
    virtual doris::Status load(Metric type)=0;
    //void reset();
    static std::string metric_to_string(Metric metric) {
        switch (metric) {
            case Metric::L2:
                return "L2";
            case Metric::COSINE:
                return "COSINE";
            case Metric::INNER_PRODUCT:
                return "INNER_PRODUCT";
            default:
                return "UNKNOWN";
        }
    }
    static Metric string_to_metric(const std::string& metric) {
        if (metric == "l2") {
            return Metric::L2;
        } else if (metric == "cosine") {
            return Metric::COSINE;
        } else if (metric == "inner_product") {
            return Metric::INNER_PRODUCT;
        } else {
            return Metric::UNKNOWN;
        }
    }
    virtual ~VectorIndex() = default;
};