#pragma once
#include "common/status.h"

namespace doris {

class BuildHelper {
public:
    static BuildHelper* init_instance();
    ~BuildHelper();
    // Return global instance.
    static BuildHelper* instance() { return _s_instance; }

    void initial_build_env();
    void open(const std::string & meta_file, const std::string & build_dir, 
                const std::string & data_path, const std::string & file_type);
    Status build();
    
private:
    static BuildHelper* _s_instance;
    std::string read_local_file(const std::string & file);
    std::string _meta_file;
    std::string _build_dir;
    std::string _data_path;
    std::string _file_type;
};

}
