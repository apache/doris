#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdio>
#include <map>
#include <memory>
#include "combined_file.h"
#include <cstdio>

namespace diskann
{

    LocalFileReader::LocalFileReader(const std::string& file_path, uint64_t start, uint64_t size) {
        this->open(file_path);
        _base_offset = start;
        _size = size;
    }


    bool LocalFileReader::open(const std::string& file_path)  {
        if (!in.is_open()) {
            _file_path  = file_path;
            in.open(file_path, std::ios::binary);
            return in.is_open();
        }
        return true;
    }

    uint64_t LocalFileReader::get_file_size() {
        if (_size == 0){
            in.seekg(0, std::ios::end);
            uint64_t file_size = static_cast<uint64_t>(in.tellg());
            return file_size;
        } else {
            return _size;
        }
    }

    size_t LocalFileReader::read(char* buffer, uint64_t offset, size_t size)  {
        if (in.is_open()) {
            //如果_size大于说明的文件中的一段，则读取的大小不能超过_size
            if (_size > 0 && size > _size) {
                size = _size;
            }
            {
                std::lock_guard<std::mutex> lock(_mutex);
                // 这里将传入的offset加上base_offset来正确定位读取位置
                in.seekg(_base_offset + offset, std::ios::beg);
                in.read(buffer, size);
            }
            return in.gcount();
        }
        return 0;
    }

    void LocalFileReader::seek(uint64_t offset)  {
        if (in.is_open()) {
            in.seekg(offset, std::ios::beg);
        }
    }

    void LocalFileReader::close()  {
        if (in.is_open()) {
            in.close();
        }
    }

    LocalFileIOWriter::LocalFileIOWriter(const std::string& file_path): out(file_path, std::ios::binary){
       
    }

    void LocalFileIOWriter::write(const char* data, size_t size) {
        if (out.is_open()) {
            out.write(data, size);
        }
    }
    void LocalFileIOWriter::close()  {
        if (out.is_open()) {
            out.close();
        }
    }
    

    // 添加文件方法
    bool FileMerger::add(const std::string& alias, const std::string& path) {
        FileInfo info;
        info.alias = alias;
        info.path = path;
        info.offset = current_offset;

        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (file.is_open()) {
            info.size = static_cast<uint64_t>(file.tellg());
            if (info.size <= 0){
                std::cerr << "file can not empty: " << path << std::endl;
                return false;
            }
            file.close();
            current_offset += info.size;
            files.push_back(info);
        } else {
            std::cerr << "无法打开文件: " << path << std::endl;
            return false;
        }
        return true;
    }

    // 把最终合并的文件写到磁盘，文件的meta信息要放到最后，且方便反序列化，同时记录total_meta_size并写入磁盘末尾
    void FileMerger::save(IOWriter* writer) {
        // 先写入文件内容
        for (const auto& file : files) {
            std::ifstream in(file.path, std::ios::binary);
            if (in.is_open()) {
                char buffer[4096];
                while (!in.eof()) {
                    in.read(buffer, sizeof(buffer));
                    writer->write(buffer, in.gcount());
                }
                in.close();
            } else {
                std::cerr << "无法打开文件进行读取: " << file.path << std::endl;
            }
        }

        // 记录文件个数，方便后续反序列化时知道有多少个meta信息
        uint32_t file_count = files.size();
        writer->write(reinterpret_cast<const char*>(&file_count), sizeof(uint32_t));

        // 计算meta信息总大小
        uint64_t total_meta_size = sizeof(file_count);
        for (const auto& file : files) {
            total_meta_size += sizeof(uint32_t) + file.alias.size() + sizeof(file.offset) + sizeof(file.size);
        }

        // 再写入每个文件的meta信息（别名、offset、size）到磁盘最后
        for (const auto& file : files) {
            uint32_t alias_len = file.alias.size();
            writer->write(reinterpret_cast<const char*>(&alias_len), sizeof(uint32_t));
            writer->write(file.alias.c_str(), alias_len);
            writer->write(reinterpret_cast<const char*>(&file.offset), sizeof(file.offset));
            writer->write(reinterpret_cast<const char*>(&file.size), sizeof(file.size));
        }
        // 将total_meta_size写入磁盘末尾（占8个字节）
        //std::cout <<"write total_meta_size:" <<  total_meta_size << std::endl;
        writer->write(reinterpret_cast<const char*>(&total_meta_size), sizeof(total_meta_size));
        writer->close();
    }

    // 根据文件名原始文件别名， 获取base_offset和文件大小，并根据base_offset和文件创建一个reader，返回这个reader
    // 这里从磁盘读取文件相关offset和size信息（假设磁盘文件格式符合之前定义的规则），先获取total_meta_size来定位meta起始位置
    // 同时添加了缓存功能，记录已经找到的reader，如果发现之前实例化过则直接返回
    template <typename ReaderType>
    std::shared_ptr<ReaderType> FileMerger::get_reader(const std::string& alias, const std::string& merged_file_path) {
        // 先查看缓存中是否已经存在该别名对应的reader
        auto it = reader_cache.find(alias);
        if (it!= reader_cache.end()) {
            return std::unique_ptr<ReaderType>(static_cast<ReaderType*>(it->second.get()));
        }

        ReaderType merged_file(merged_file_path);

        //文件文件（文件大小）
        uint64_t file_size = merged_file.get_file_size();

        uint64_t total_meta_size;
        uint64_t total_meta_size_start = file_size - sizeof(total_meta_size);
        merged_file.read(reinterpret_cast<char*>(&total_meta_size), total_meta_size_start, sizeof(total_meta_size));

        // 计算meta信息起始位置
        uint64_t meta_start = file_size - total_meta_size - sizeof(total_meta_size);

        // 读取文件个数
        int file_count;
        merged_file.read(reinterpret_cast<char*>(&file_count), meta_start, sizeof(file_count));

        // 查找对应别名的文件meta信息
        uint64_t meta_offset = meta_start + sizeof(file_count);
        for (int i = 0; i < file_count; ++i) {
            int alias_len;
            merged_file.read(reinterpret_cast<char*>(&alias_len), meta_offset, sizeof(alias_len));
            meta_offset += sizeof(alias_len);
            char* buffer = new char[alias_len];
            merged_file.read(buffer, meta_offset, alias_len);
            meta_offset += alias_len;
            std::string read_alias(buffer, alias_len);
            delete[] buffer;

            uint64_t offset;
            merged_file.read(reinterpret_cast<char*>(&offset), meta_offset, sizeof(offset));
            meta_offset += sizeof(offset);
            uint64_t size;
            merged_file.read(reinterpret_cast<char*>(&size), meta_offset, sizeof(size));
            meta_offset += sizeof(size);
            //std::cout << "alias:" << read_alias << ", offset:" << offset << ", size:" << size << std::endl;
            if (read_alias == alias) {
                std::shared_ptr<ReaderType> reader = std::make_shared<ReaderType>(merged_file_path, offset, size);
                // 将新实例化的reader添加到缓存中
                reader_cache[alias] = reader;
                merged_file.close();
                return reader;
            }
        }

        std::cerr << "未找到对应别名的文件: " << alias << std::endl;
        merged_file.close();
        return nullptr;
    }

    void FileMerger::clear(){
        for (const auto& file : files) {
            remove(file.path.c_str());
        }
    }

    template std::shared_ptr<LocalFileReader> FileMerger::get_reader<LocalFileReader>(const std::string& alias, const std::string& merged_file_path);
}