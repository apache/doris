// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>
#include <vector>
#include <stdexcept>

#include <index.h>
#include <pq_flash_index.h>

namespace diskann
{
class SearchResult
{
  public:
    SearchResult(unsigned int K, unsigned int elapsed_time_in_ms, const unsigned *const indices,
                 const float *const distances, const std::string *const tags = nullptr,
                 const unsigned *const partitions = nullptr);

    const std::vector<unsigned int> &get_indices() const
    {
        return _indices;
    }
    const std::vector<float> &get_distances() const
    {
        return _distances;
    }
    bool tags_enabled() const
    {
        return _tags_enabled;
    }
    const std::vector<std::string> &get_tags() const
    {
        return _tags;
    }
    bool partitions_enabled() const
    {
        return _partitions_enabled;
    }
    const std::vector<unsigned> &get_partitions() const
    {
        return _partitions;
    }
    unsigned get_time() const
    {
        return _search_time_in_ms;
    }

  private:
    unsigned int _K;
    unsigned int _search_time_in_ms;
    std::vector<unsigned int> _indices;
    std::vector<float> _distances;

    bool _tags_enabled;
    std::vector<std::string> _tags;

    bool _partitions_enabled;
    std::vector<unsigned> _partitions;
};

class SearchNotImplementedException : public std::logic_error
{
  private:
    std::string _errormsg;

  public:
    SearchNotImplementedException(const char *type) : std::logic_error("Not Implemented")
    {
        _errormsg = "Search with data type ";
        _errormsg += std::string(type);
        _errormsg += " not implemented : ";
        _errormsg += __FUNCTION__;
    }

    virtual const char *what() const throw()
    {
        return _errormsg.c_str();
    }
};

class BaseSearch
{
  public:
    BaseSearch(const std::string &tagsFile = nullptr);
    virtual SearchResult search(const float *query, const unsigned int dimensions, const unsigned int K,
                                const unsigned int Ls)
    {
        throw SearchNotImplementedException("float");
    }
    virtual SearchResult search(const int8_t *query, const unsigned int dimensions, const unsigned int K,
                                const unsigned int Ls)
    {
        throw SearchNotImplementedException("int8_t");
    }

    virtual SearchResult search(const uint8_t *query, const unsigned int dimensions, const unsigned int K,
                                const unsigned int Ls)
    {
        throw SearchNotImplementedException("uint8_t");
    }

    void lookup_tags(const unsigned K, const unsigned *indices, std::string *ret_tags);

  protected:
    bool _tags_enabled;
    std::vector<std::string> _tags_str;
};

template <typename T> class InMemorySearch : public BaseSearch
{
  public:
    InMemorySearch(const std::string &baseFile, const std::string &indexFile, const std::string &tagsFile, Metric m,
                   uint32_t num_threads, uint32_t search_l);
    virtual ~InMemorySearch();

    SearchResult search(const T *query, const unsigned int dimensions, const unsigned int K, const unsigned int Ls);

  private:
    unsigned int _dimensions, _numPoints;
    std::unique_ptr<diskann::Index<T>> _index;
};

template <typename T> class PQFlashSearch : public BaseSearch
{
  public:
    PQFlashSearch(const std::string &indexPrefix, const unsigned num_nodes_to_cache, const unsigned num_threads,
                  const std::string &tagsFile, Metric m);
    virtual ~PQFlashSearch();

    SearchResult search(const T *query, const unsigned int dimensions, const unsigned int K, const unsigned int Ls);

  private:
    unsigned int _dimensions, _numPoints;
    std::unique_ptr<diskann::PQFlashIndex<T>> _index;
    std::shared_ptr<AlignedFileReader> reader;
};
} // namespace diskann
