// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include<chrono>

#include "io/file_factory.h"
#include "io/fs/benchmark/base_benchmark.h"
#include "io/fs/hdfs_file_reader.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/hdfs_file_writer.h"
#include "io/hdfs_builder.h"
#include "util/slice.h"
#include "util/jni-util.h"

#include "io/file_factory.h"


namespace doris::io {

/// @brief  用在生成文件的时候，防止文件名的冲突
/// @return 当前系统的时间精确的纳秒级别
static std::string _GetStringFromTime(){
    auto tpMicro = std::chrono::time_point_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now());

    return std::to_string((time_t)tpMicro.time_since_epoch().count());
}

/// @brief 读取conf中的file参数,读取默认大小为1G,可以通过size参数来设定需要读取的比特数。
class HdfsReadBenchmark final : public BaseBenchmark {
public:
    HdfsReadBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsReadBenchmark", iterations, conf_map) , _byte_read(1ll*1024*1024*1024)
            {}
    virtual ~HdfsReadBenchmark() = default;

    Status init() override {
    
        bm_log("begin to init {}", _name);
        std::string file_path = _conf_map["file"];
         
        if ( _conf_map.count("size") != 0  ) {
            _byte_read = stoll( _conf_map["size"]);
        }
        if ( _byte_read <=  0){
            return Status::InvalidArgument(
                ": size . The size read in from the file should be a positive number!\n");
        }

        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
            
        RETURN_IF_ERROR(
                    FileFactory::create_hdfs_reader(hdfs_params, file_path, &_fs, &_reader, reader_opts));
        
        static long long int len = 1ll*1024*1024;
        static std::string str(len,'.');         
        _result.data = str.data();
        _result.size = str.size();

        bm_log("finish to init {}", _name);
        return Status::OK();
    }
    Status run() override { 

        
        long long int  count =0;
        long long int loc = 0;
        while ( count < _byte_read ) {
            size_t bytes = 0;
            RETURN_IF_ERROR(_reader->read_at(loc, _result, &bytes)); 
            
            count += bytes;
            if (bytes < _result.size ) loc = 0;
            else loc += bytes;
        }

        return Status::OK();
    }

private:
    std::shared_ptr<io::FileSystem> _fs;
    io::FileReaderSPtr _reader;
    // char buffer[128];
    doris::Slice _result;
    // size_t _bytes_read = 0;
    long long int _byte_read;
};

/// @brief 创建文件在conf的path目录下，文件数量默认是10（可以通过time参数来设定）
class HdfsCreateBenchmark final : public BaseBenchmark {
public:
    HdfsCreateBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
        :BaseBenchmark("HdfsCreateBenchmark", iterations, conf_map) ,_time(10){}
    virtual ~HdfsCreateBenchmark() = default;


    Status init() override {
    
        bm_log("begin to init {}", _name);

    
        std::string time  = _GetStringFromTime();
        bm_log("The created file directory is benchmark_{}", time);
        
        _path = _conf_map["path"];//创建文件的目录
        if (_path.back() != '/' ){
            _path += "/";
        } 
        _path += "benchmark_"+time+"/";  
        if (_conf_map.count("time") != 0)        
            _time = std::stoll(_conf_map["time"]);
        if (_time <= 0 ) {
            return Status::InvalidArgument(
                ": time . Need to create a positive number of files!\n");
        }

        THdfsParams hdfs_params = parse_properties(_conf_map);
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &_fs));
        
        bm_log("finish to init {}", _name);
        return Status::OK();

    }
    Status run() override {         

        doris::Slice data(" ",1);
        for(int i = 0 ;i< _time ; i++  ) {
            std::string file_name = _path +  std::to_string(i);
            std::unique_ptr<io::FileWriter> writer;
            RETURN_IF_ERROR(_fs->create_file(file_name, &writer));
            RETURN_IF_ERROR( writer->appendv(&data,1) );
            //创建的时候需要写入内容，才能创建成功。
        }       
        return Status::OK();    
    }
private: 
    std::shared_ptr<io::HdfsFileSystem> _fs;
    long long int  _time;//创建文件的个数   
    std::string _path;//创建文件的目录
};

/// @brief 以conf中的file参数为前缀，写入文件大小为1G，
class HdfsWriteBenchmark final : public BaseBenchmark {
public:
    HdfsWriteBenchmark( int iterations,const std::map<std::string,std::string>& conf_map )
        : BaseBenchmark("HdfsWriteBenchmark", iterations, conf_map) ,_size(1ll*1024*1024*1024) //1G 
        {
	}

    virtual ~HdfsWriteBenchmark() = default;

    Status init() override {
        bm_log("begin to init {}", _name);

        std::string file_path = _conf_map["file"] + "_" + _GetStringFromTime();

        if (_conf_map.count("size") != 0)
            _size =  stoll(_conf_map["size"]);

        if(_size <= 0) {
            return Status::InvalidArgument(
                ": size . The size written to the file should be a positive number!\n");
        }

        THdfsParams hdfs_params = parse_properties(_conf_map);
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &_fs));
    	
        RETURN_IF_ERROR(_fs->create_file(file_path, &_writer));
        //找不到支持向文件添加的函数！
        

        static long long int len = 1024*1024;
        static std::string str(len,'.');


        _buffer.resize( (_size + len - 1)/len  , doris::Slice( str.c_str() , len ));
        if (_size%len != 0 ) _buffer.back().size = _size%len; 
        
    
        bm_log("finish to init {}", _name);        
        return Status::OK();
    }

	Status run() override { 
        RETURN_IF_ERROR( _writer -> appendv(_buffer.data(), _buffer.size()) );    
        return Status::OK();
    }

private:

   long long int  _size;  

    std::shared_ptr<io::HdfsFileSystem> _fs;
    std::unique_ptr<io::FileWriter> _writer;
    
    std::vector<doris::Slice> _buffer;
};


/// @brief 删除init中创建的临时文件 
class HdfsDeleteBenchmark final : public BaseBenchmark {
public:
    HdfsDeleteBenchmark( int iterations,const std::map<std::string,std::string>& conf_map )
        : BaseBenchmark("HdfsDeleteBenchmark", iterations , conf_map) 
        {
	}
    virtual ~HdfsDeleteBenchmark() = default;

    Status init() override {
        bm_log("begin to init {}", _name);

        _file_name = _conf_map["file"] + "_" + _GetStringFromTime();

        THdfsParams hdfs_params = parse_properties(_conf_map);
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &_fs));
    	        
        //创建临时文件用于删除
        std::unique_ptr<io::FileWriter> writer;
        RETURN_IF_ERROR(_fs->create_file(_file_name, &writer));
        doris::Slice data(" ",1);
        RETURN_IF_ERROR( writer->appendv(&data,1) );

        bm_log("finish to init {}", _name);        
        return Status::OK();
    }

	Status run() override { 
        RETURN_IF_ERROR( _fs->delete_file(_file_name));
        return Status::OK();
    }
private:
    std::shared_ptr<io::HdfsFileSystem> _fs;    
    std::string _file_name;

};


} // namespace doris::io


