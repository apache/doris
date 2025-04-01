#pragma once

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>

namespace diskann {

// 抽象的文件读取器基类
class Reader {
    public:
        virtual ~Reader() {}
        virtual size_t read(char* buffer, uint64_t offset, size_t size) = 0;
        virtual void seek(uint64_t offset) = 0;
        virtual bool open(const std::string& file_path) = 0;
        virtual void close() = 0;
        virtual uint64_t get_file_size() = 0;
        virtual uint64_t get_base_offset() = 0;
};

class IOWriter {
    public:
        virtual ~IOWriter() {}
        virtual void write(const char* data, size_t size) = 0;
        virtual void close() = 0;
};



class LocalFileReader : public Reader {
    private:
        std::ifstream in;
        std::string _file_path;
        //文件起始
        uint64_t _base_offset = 0;
        uint64_t _size = 0 ;
        std::mutex _mutex;
    public:
        LocalFileReader(const std::string& file_path, uint64_t start = 0, uint64_t size = 0);

        LocalFileReader();

        bool open(const std::string& file_path);

        uint64_t get_file_size();

        size_t read(char* buffer, uint64_t offset, size_t size);

        void seek(uint64_t offset);

        void close();

        uint64_t get_base_offset() {
            return _base_offset;
        }
};

class LocalFileIOWriter : public IOWriter {
    public:
        LocalFileIOWriter(const std::string& file_path);
        void write(const char* data, size_t size);
        void close();
    private:
        std::ofstream out;
};

class FileMerger {
    private:
        struct FileInfo {
            std::string alias;
            std::string path;
            uint64_t offset;
            uint64_t size;
        };
        std::vector<FileInfo> files;
        uint64_t current_offset;

        // 使用map来缓存已经实例化过的reader，键为文件别名，值为对应的reader智能指针
        std::map<std::string, std::shared_ptr<Reader>> reader_cache;

    public:
        FileMerger() : current_offset(0) {}

        // 添加文件方法
        bool add(const std::string& alias, const std::string& path);
        void clear();

        // 把最终合并的文件写到磁盘，文件的meta信息要放到最后，且方便反序列化，同时记录total_meta_size并写入磁盘末尾
        void save(IOWriter* writer);

        // 根据文件名原始文件别名， 获取base_offset和文件大小，并根据base_offset和文件创建一个reader，返回这个reader
        // 这里从磁盘读取文件相关offset和size信息（假设磁盘文件格式符合之前定义的规则），先获取total_meta_size来定位meta起始位置
        // 同时添加了缓存功能，记录已经找到的reader，如果发现之前实例化过则直接返回
        template <typename ReaderType>
        std::shared_ptr<ReaderType> get_reader(const std::string& alias, const std::string& merged_file_path);
};


}
