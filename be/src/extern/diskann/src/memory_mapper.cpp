// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "logger.h"
#include "memory_mapper.h"
#include <iostream>
#include <sstream>

using namespace diskann;

MemoryMapper::MemoryMapper(const std::string &filename) : MemoryMapper(filename.c_str())
{
}

MemoryMapper::MemoryMapper(const char *filename)
{
#ifndef _WINDOWS
    _fd = open(filename, O_RDONLY);
    if (_fd <= 0)
    {
        std::cerr << "Inner vertices file not found" << std::endl;
        return;
    }
    struct stat sb;
    if (fstat(_fd, &sb) != 0)
    {
        std::cerr << "Inner vertices file not dound. " << std::endl;
        return;
    }
    _fileSize = sb.st_size;
    diskann::cout << "File Size: " << _fileSize << std::endl;
    _buf = (char *)mmap(NULL, _fileSize, PROT_READ, MAP_PRIVATE, _fd, 0);
#else
    _bareFile =
        CreateFileA(filename, GENERIC_READ | GENERIC_EXECUTE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (_bareFile == nullptr)
    {
        std::ostringstream message;
        message << "CreateFileA(" << filename << ") failed with error " << GetLastError() << std::endl;
        std::cerr << message.str();
        throw std::exception(message.str().c_str());
    }

    _fd = CreateFileMapping(_bareFile, NULL, PAGE_EXECUTE_READ, 0, 0, NULL);
    if (_fd == nullptr)
    {
        std::ostringstream message;
        message << "CreateFileMapping(" << filename << ") failed with error " << GetLastError() << std::endl;
        std::cerr << message.str() << std::endl;
        throw std::exception(message.str().c_str());
    }

    _buf = (char *)MapViewOfFile(_fd, FILE_MAP_READ, 0, 0, 0);
    if (_buf == nullptr)
    {
        std::ostringstream message;
        message << "MapViewOfFile(" << filename << ") failed with error: " << GetLastError() << std::endl;
        std::cerr << message.str() << std::endl;
        throw std::exception(message.str().c_str());
    }

    LARGE_INTEGER fSize;
    if (TRUE == GetFileSizeEx(_bareFile, &fSize))
    {
        _fileSize = fSize.QuadPart; // take the 64-bit value
        diskann::cout << "File Size: " << _fileSize << std::endl;
    }
    else
    {
        std::cerr << "Failed to get size of file " << filename << std::endl;
    }
#endif
}
char *MemoryMapper::getBuf()
{
    return _buf;
}

size_t MemoryMapper::getFileSize()
{
    return _fileSize;
}

MemoryMapper::~MemoryMapper()
{
#ifndef _WINDOWS
    if (munmap(_buf, _fileSize) != 0)
        std::cerr << "ERROR unmapping. CHECK!" << std::endl;
    close(_fd);
#else
    if (FALSE == UnmapViewOfFile(_buf))
    {
        std::cerr << "Unmap view of file failed. Error: " << GetLastError() << std::endl;
    }

    if (FALSE == CloseHandle(_fd))
    {
        std::cerr << "Failed to close memory mapped file. Error: " << GetLastError() << std::endl;
    }

    if (FALSE == CloseHandle(_bareFile))
    {
        std::cerr << "Failed to close file: " << _fileName << " Error: " << GetLastError() << std::endl;
    }

#endif
}
