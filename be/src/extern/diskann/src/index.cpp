// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <omp.h>

#include <type_traits>

#include "boost/dynamic_bitset.hpp"
#include "index_factory.h"
#include "memory_mapper.h"
#include "timer.h"
#include "tsl/robin_map.h"
#include "tsl/robin_set.h"
#include "windows_customizations.h"
#include "tag_uint128.h"
#if defined(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS) && defined(DISKANN_BUILD)
#include "gperftools/malloc_extension.h"
#endif

#ifdef _WINDOWS
#include <xmmintrin.h>
#endif

#include "index.h"

#define MAX_POINTS_FOR_USING_BITSET 10000000

namespace diskann
{
// Initialize an index with metric m, load the data of type T with filename
// (bin), and initialize max_points
template <typename T, typename TagT, typename LabelT>
Index<T, TagT, LabelT>::Index(const IndexConfig &index_config, std::shared_ptr<AbstractDataStore<T>> data_store,
                              std::unique_ptr<AbstractGraphStore> graph_store,
                              std::shared_ptr<AbstractDataStore<T>> pq_data_store)
    : _dist_metric(index_config.metric), _dim(index_config.dimension), _max_points(index_config.max_points),
      _num_frozen_pts(index_config.num_frozen_pts), _dynamic_index(index_config.dynamic_index),
      _enable_tags(index_config.enable_tags), _indexingMaxC(DEFAULT_MAXC), _query_scratch(nullptr),
      _pq_dist(index_config.pq_dist_build), _use_opq(index_config.use_opq),
      _filtered_index(index_config.filtered_index), _num_pq_chunks(index_config.num_pq_chunks),
      _delete_set(new tsl::robin_set<uint32_t>), _conc_consolidate(index_config.concurrent_consolidate)
{
    if (_dynamic_index && !_enable_tags)
    {
        throw ANNException("ERROR: Dynamic Indexing must have tags enabled.", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (_pq_dist)
    {
        if (_dynamic_index)
            throw ANNException("ERROR: Dynamic Indexing not supported with PQ distance based "
                               "index construction",
                               -1, __FUNCSIG__, __FILE__, __LINE__);
        if (_dist_metric == diskann::Metric::INNER_PRODUCT)
            throw ANNException("ERROR: Inner product metrics not yet supported "
                               "with PQ distance "
                               "base index",
                               -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (_dynamic_index && _num_frozen_pts == 0)
    {
        _num_frozen_pts = 1;
    }
    // Sanity check. While logically it is correct, max_points = 0 causes
    // downstream problems.
    if (_max_points == 0)
    {
        _max_points = 1;
    }
    const size_t total_internal_points = _max_points + _num_frozen_pts;

    _start = (uint32_t)_max_points;

    _data_store = data_store;
    _pq_data_store = pq_data_store;
    _graph_store = std::move(graph_store);

    _locks = std::vector<non_recursive_mutex>(total_internal_points);
    if (_enable_tags)
    {
        _location_to_tag.reserve(total_internal_points);
        _tag_to_location.reserve(total_internal_points);
    }

    if (_dynamic_index)
    {
        this->enable_delete(); // enable delete by default for dynamic index
        if (_filtered_index)
        {
            _location_to_labels.resize(total_internal_points);
        }
    }

    if (index_config.index_write_params != nullptr)
    {
        _indexingQueueSize = index_config.index_write_params->search_list_size;
        _indexingRange = index_config.index_write_params->max_degree;
        _indexingMaxC = index_config.index_write_params->max_occlusion_size;
        _indexingAlpha = index_config.index_write_params->alpha;
        _filterIndexingQueueSize = index_config.index_write_params->filter_list_size;
        _indexingThreads = index_config.index_write_params->num_threads;
        _saturate_graph = index_config.index_write_params->saturate_graph;

        if (index_config.index_search_params != nullptr)
        {
            uint32_t num_scratch_spaces = index_config.index_search_params->num_search_threads + _indexingThreads;
            initialize_query_scratch(num_scratch_spaces, index_config.index_search_params->initial_search_list_size,
                                     _indexingQueueSize, _indexingRange, _indexingMaxC, _data_store->get_dims());
        }
    }
}

template <typename T, typename TagT, typename LabelT>
Index<T, TagT, LabelT>::Index(Metric m, const size_t dim, const size_t max_points,
                              const std::shared_ptr<IndexWriteParameters> index_parameters,
                              const std::shared_ptr<IndexSearchParams> index_search_params, const size_t num_frozen_pts,
                              const bool dynamic_index, const bool enable_tags, const bool concurrent_consolidate,
                              const bool pq_dist_build, const size_t num_pq_chunks, const bool use_opq,
                              const bool filtered_index)
    : Index(
          IndexConfigBuilder()
              .with_metric(m)
              .with_dimension(dim)
              .with_max_points(max_points)
              .with_index_write_params(index_parameters)
              .with_index_search_params(index_search_params)
              .with_num_frozen_pts(num_frozen_pts)
              .is_dynamic_index(dynamic_index)
              .is_enable_tags(enable_tags)
              .is_concurrent_consolidate(concurrent_consolidate)
              .is_pq_dist_build(pq_dist_build)
              .with_num_pq_chunks(num_pq_chunks)
              .is_use_opq(use_opq)
              .is_filtered(filtered_index)
              .with_data_type(diskann_type_to_name<T>())
              .build(),
          IndexFactory::construct_datastore<T>(DataStoreStrategy::MEMORY,
                                               (max_points == 0 ? (size_t)1 : max_points) +
                                                   (dynamic_index && num_frozen_pts == 0 ? (size_t)1 : num_frozen_pts),
                                               dim, m),
          IndexFactory::construct_graphstore(GraphStoreStrategy::MEMORY,
                                             (max_points == 0 ? (size_t)1 : max_points) +
                                                 (dynamic_index && num_frozen_pts == 0 ? (size_t)1 : num_frozen_pts),
                                             (size_t)((index_parameters == nullptr ? 0 : index_parameters->max_degree) *
                                                      defaults::GRAPH_SLACK_FACTOR * 1.05)))
{
    if (_pq_dist)
    {
        _pq_data_store = IndexFactory::construct_pq_datastore<T>(DataStoreStrategy::MEMORY, max_points + num_frozen_pts,
                                                                 dim, m, num_pq_chunks, use_opq);
    }
    else
    {
        _pq_data_store = _data_store;
    }
}

template <typename T, typename TagT, typename LabelT> Index<T, TagT, LabelT>::~Index()
{
    // Ensure that no other activity is happening before dtor()
    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> cl(_consolidate_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);

    for (auto &lock : _locks)
    {
        LockGuard lg(lock);
    }

    if (_opt_graph != nullptr)
    {
        delete[] _opt_graph;
    }

    if (!_query_scratch.empty())
    {
        ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
        manager.destroy();
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::initialize_query_scratch(uint32_t num_threads, uint32_t search_l, uint32_t indexing_l,
                                                      uint32_t r, uint32_t maxc, size_t dim)
{
    for (uint32_t i = 0; i < num_threads; i++)
    {
        auto scratch = new InMemQueryScratch<T>(search_l, indexing_l, r, maxc, dim, _data_store->get_aligned_dim(),
                                                _data_store->get_alignment_factor(), _pq_dist);
        _query_scratch.push(scratch);
    }
}

template <typename T, typename TagT, typename LabelT> size_t Index<T, TagT, LabelT>::save_tags(std::string tags_file)
{
    if (!_enable_tags)
    {
        diskann::cout << "Not saving tags as they are not enabled." << std::endl;
        return 0;
    }

    size_t tag_bytes_written;
    TagT *tag_data = new TagT[_nd + _num_frozen_pts];
    for (uint32_t i = 0; i < _nd; i++)
    {
        TagT tag;
        if (_location_to_tag.try_get(i, tag))
        {
            tag_data[i] = tag;
        }
        else
        {
            // catering to future when tagT can be any type.
            std::memset((char *)&tag_data[i], 0, sizeof(TagT));
        }
    }
    if (_num_frozen_pts > 0)
    {
        std::memset((char *)&tag_data[_start], 0, sizeof(TagT) * _num_frozen_pts);
    }
    try
    {
        tag_bytes_written = save_bin<TagT>(tags_file, tag_data, _nd + _num_frozen_pts, 1);
    }
    catch (std::system_error &e)
    {
        throw FileException(tags_file, e, __FUNCSIG__, __FILE__, __LINE__);
    }
    delete[] tag_data;
    return tag_bytes_written;
}

template <typename T, typename TagT, typename LabelT> size_t Index<T, TagT, LabelT>::save_data(std::string data_file)
{
    // Note: at this point, either _nd == _max_points or any frozen points have
    // been temporarily moved to _nd, so _nd + _num_frozen_pts is the valid
    // location limit.
    return _data_store->save(data_file, (location_t)(_nd + _num_frozen_pts));
}

// save the graph index on a file as an adjacency list. For each point,
// first store the number of neighbors, and then the neighbor list (each as
// 4 byte uint32_t)
template <typename T, typename TagT, typename LabelT> size_t Index<T, TagT, LabelT>::save_graph(std::string graph_file)
{
    return _graph_store->store(graph_file, _nd + _num_frozen_pts, _num_frozen_pts, _start);
}

template <typename T, typename TagT, typename LabelT> size_t Index<T, TagT, LabelT>::save_graph(std::stringstream &graph_stream)
{
    return _graph_store->store(graph_stream, _nd + _num_frozen_pts, _num_frozen_pts, _start);
}


template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::save_delete_list(const std::string &filename)
{
    if (_delete_set->size() == 0)
    {
        return 0;
    }
    std::unique_ptr<uint32_t[]> delete_list = std::make_unique<uint32_t[]>(_delete_set->size());
    uint32_t i = 0;
    for (auto &del : *_delete_set)
    {
        delete_list[i++] = del;
    }
    return save_bin<uint32_t>(filename, delete_list.get(), _delete_set->size(), 1);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::save(const char *filename, bool compact_before_save)
{
    diskann::Timer timer;

    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> cl(_consolidate_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);

    if (compact_before_save)
    {
        compact_data();
        compact_frozen_point();
    }
    else
    {
        if (!_data_compacted)
        {
            throw ANNException("Index save for non-compacted index is not yet implemented", -1, __FUNCSIG__, __FILE__,
                               __LINE__);
        }
    }

    if (!_save_as_one_file)
    {
        if (_filtered_index)
        {
            if (_label_to_start_id.size() > 0)
            {
                std::ofstream medoid_writer(std::string(filename) + "_labels_to_medoids.txt");
                if (medoid_writer.fail())
                {
                    throw diskann::ANNException(std::string("Failed to open file ") + filename, -1);
                }
                for (auto iter : _label_to_start_id)
                {
                    medoid_writer << iter.first << ", " << iter.second << std::endl;
                }
                medoid_writer.close();
            }

            if (_use_universal_label)
            {
                std::ofstream universal_label_writer(std::string(filename) + "_universal_label.txt");
                assert(universal_label_writer.is_open());
                universal_label_writer << _universal_label << std::endl;
                universal_label_writer.close();
            }

            if (_location_to_labels.size() > 0)
            {
                std::ofstream label_writer(std::string(filename) + "_labels.txt");
                assert(label_writer.is_open());
                for (uint32_t i = 0; i < _nd + _num_frozen_pts; i++)
                {
                    for (uint32_t j = 0; j + 1 < _location_to_labels[i].size(); j++)
                    {
                        label_writer << _location_to_labels[i][j] << ",";
                    }
                    if (_location_to_labels[i].size() != 0)
                        label_writer << _location_to_labels[i][_location_to_labels[i].size() - 1];

                    label_writer << std::endl;
                }
                label_writer.close();

                // write compacted raw_labels if data hence _location_to_labels was also compacted
                if (compact_before_save && _dynamic_index)
                {
                    _label_map = load_label_map(std::string(filename) + "_labels_map.txt");
                    std::unordered_map<LabelT, std::string> mapped_to_raw_labels;
                    // invert label map
                    for (const auto &[key, value] : _label_map)
                    {
                        mapped_to_raw_labels.insert({value, key});
                    }

                    // write updated labels
                    std::ofstream raw_label_writer(std::string(filename) + "_raw_labels.txt");
                    assert(raw_label_writer.is_open());
                    for (uint32_t i = 0; i < _nd + _num_frozen_pts; i++)
                    {
                        for (uint32_t j = 0; j + 1 < _location_to_labels[i].size(); j++)
                        {
                            raw_label_writer << mapped_to_raw_labels[_location_to_labels[i][j]] << ",";
                        }
                        if (_location_to_labels[i].size() != 0)
                            raw_label_writer
                                << mapped_to_raw_labels[_location_to_labels[i][_location_to_labels[i].size() - 1]];

                        raw_label_writer << std::endl;
                    }
                    raw_label_writer.close();
                }
            }
        }

        std::string graph_file = std::string(filename);
        std::string tags_file = std::string(filename) + ".tags";
        std::string data_file = std::string(filename) + ".data";
        std::string delete_list_file = std::string(filename) + ".del";

        // Because the save_* functions use append mode, ensure that
        // the files are deleted before save. Ideally, we should check
        // the error code for delete_file, but will ignore now because
        // delete should succeed if save will succeed.
        delete_file(graph_file);
        save_graph(graph_file);
        delete_file(data_file);
        save_data(data_file);
        delete_file(tags_file);
        save_tags(tags_file);
        delete_file(delete_list_file);
        save_delete_list(delete_list_file);
    }
    else
    {
        diskann::cout << "Save index in a single file currently not supported. "
                         "Not saving the index."
                      << std::endl;
    }

    // If frozen points were temporarily compacted to _nd, move back to
    // _max_points.
    reposition_frozen_point_to_end();

    diskann::cout << "Time taken for save: " << timer.elapsed() / 1000000.0 << "s." << std::endl;
}


template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::save(std::stringstream &mem_index_stream, bool compact_before_save)
{
    diskann::Timer timer;

    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> cl(_consolidate_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);

    if (compact_before_save)
    {
        compact_data();
        compact_frozen_point();
    }
    else
    {
        if (!_data_compacted)
        {
            throw ANNException("Index save for non-compacted index is not yet implemented", -1, __FUNCSIG__, __FILE__,
                               __LINE__);
        }
    }

    if (!_save_as_one_file)
    {
        // if (_filtered_index)
        // {
        //     if (_label_to_start_id.size() > 0)
        //     {
        //         std::ofstream medoid_writer(std::string(filename) + "_labels_to_medoids.txt");
        //         if (medoid_writer.fail())
        //         {
        //             throw diskann::ANNException(std::string("Failed to open file ") + filename, -1);
        //         }
        //         for (auto iter : _label_to_start_id)
        //         {
        //             medoid_writer << iter.first << ", " << iter.second << std::endl;
        //         }
        //         medoid_writer.close();
        //     }

        //     if (_use_universal_label)
        //     {
        //         std::ofstream universal_label_writer(std::string(filename) + "_universal_label.txt");
        //         assert(universal_label_writer.is_open());
        //         universal_label_writer << _universal_label << std::endl;
        //         universal_label_writer.close();
        //     }

        //     if (_location_to_labels.size() > 0)
        //     {
        //         std::ofstream label_writer(std::string(filename) + "_labels.txt");
        //         assert(label_writer.is_open());
        //         for (uint32_t i = 0; i < _nd + _num_frozen_pts; i++)
        //         {
        //             for (uint32_t j = 0; j + 1 < _location_to_labels[i].size(); j++)
        //             {
        //                 label_writer << _location_to_labels[i][j] << ",";
        //             }
        //             if (_location_to_labels[i].size() != 0)
        //                 label_writer << _location_to_labels[i][_location_to_labels[i].size() - 1];

        //             label_writer << std::endl;
        //         }
        //         label_writer.close();

        //         // write compacted raw_labels if data hence _location_to_labels was also compacted
        //         if (compact_before_save && _dynamic_index)
        //         {
        //             _label_map = load_label_map(std::string(filename) + "_labels_map.txt");
        //             std::unordered_map<LabelT, std::string> mapped_to_raw_labels;
        //             // invert label map
        //             for (const auto &[key, value] : _label_map)
        //             {
        //                 mapped_to_raw_labels.insert({value, key});
        //             }

        //             // write updated labels
        //             std::ofstream raw_label_writer(std::string(filename) + "_raw_labels.txt");
        //             assert(raw_label_writer.is_open());
        //             for (uint32_t i = 0; i < _nd + _num_frozen_pts; i++)
        //             {
        //                 for (uint32_t j = 0; j + 1 < _location_to_labels[i].size(); j++)
        //                 {
        //                     raw_label_writer << mapped_to_raw_labels[_location_to_labels[i][j]] << ",";
        //                 }
        //                 if (_location_to_labels[i].size() != 0)
        //                     raw_label_writer
        //                         << mapped_to_raw_labels[_location_to_labels[i][_location_to_labels[i].size() - 1]];

        //                 raw_label_writer << std::endl;
        //             }
        //             raw_label_writer.close();
        //         }
        //     }
        // }

        // std::string graph_file = std::string(filename);
        // std::string tags_file = std::string(filename) + ".tags";
        // std::string data_file = std::string(filename) + ".data";
        // std::string delete_list_file = std::string(filename) + ".del";

        // Because the save_* functions use append mode, ensure that
        // the files are deleted before save. Ideally, we should check
        // the error code for delete_file, but will ignore now because
        // delete should succeed if save will succeed.
        //delete_file(graph_file);
        save_graph(mem_index_stream);
        // delete_file(data_file);
        // save_data(data_file);
        // delete_file(tags_file);
        // save_tags(tags_file);
        // delete_file(delete_list_file);
        // save_delete_list(delete_list_file);
    }
    else
    {
        diskann::cout << "Save index in a single file currently not supported. "
                         "Not saving the index."
                      << std::endl;
    }

    // If frozen points were temporarily compacted to _nd, move back to
    // _max_points.
    reposition_frozen_point_to_end();

    diskann::cout << "Time taken for save: " << timer.elapsed() / 1000000.0 << "s." << std::endl;
}

#ifdef EXEC_ENV_OLS
template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::load_tags(AlignedFileReader &reader)
{
#else
template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::load_tags(const std::string tag_filename)
{
    if (_enable_tags && !file_exists(tag_filename))
    {
        diskann::cerr << "Tag file " << tag_filename << " does not exist!" << std::endl;
        throw diskann::ANNException("Tag file " + tag_filename + " does not exist!", -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
    }
#endif
    if (!_enable_tags)
    {
        diskann::cout << "Tags not loaded as tags not enabled." << std::endl;
        return 0;
    }

    size_t file_dim, file_num_points;
    TagT *tag_data;
#ifdef EXEC_ENV_OLS
    load_bin<TagT>(reader, tag_data, file_num_points, file_dim);
#else
    load_bin<TagT>(std::string(tag_filename), tag_data, file_num_points, file_dim);
#endif

    if (file_dim != 1)
    {
        std::stringstream stream;
        stream << "ERROR: Found " << file_dim << " dimensions for tags,"
               << "but tag file must have 1 dimension." << std::endl;
        diskann::cerr << stream.str() << std::endl;
        delete[] tag_data;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    const size_t num_data_points = file_num_points - _num_frozen_pts;
    _location_to_tag.reserve(num_data_points);
    _tag_to_location.reserve(num_data_points);
    for (uint32_t i = 0; i < (uint32_t)num_data_points; i++)
    {
        TagT tag = *(tag_data + i);
        if (_delete_set->find(i) == _delete_set->end())
        {
            _location_to_tag.set(i, tag);
            _tag_to_location[tag] = i;
        }
    }
    diskann::cout << "Tags loaded." << std::endl;
    delete[] tag_data;
    return file_num_points;
}

template <typename T, typename TagT, typename LabelT>
#ifdef EXEC_ENV_OLS
size_t Index<T, TagT, LabelT>::load_data(AlignedFileReader &reader)
{
#else
size_t Index<T, TagT, LabelT>::load_data(std::string filename)
{
#endif
    size_t file_dim, file_num_points;
#ifdef EXEC_ENV_OLS
    diskann::get_bin_metadata(reader, file_num_points, file_dim);
#else
    if (!file_exists(filename))
    {
        std::stringstream stream;
        stream << "ERROR: data file " << filename << " does not exist." << std::endl;
        diskann::cerr << stream.str() << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    diskann::get_bin_metadata(filename, file_num_points, file_dim);
#endif

    // since we are loading a new dataset, _empty_slots must be cleared
    _empty_slots.clear();

    if (file_dim != _dim)
    {
        std::stringstream stream;
        stream << "ERROR: Driver requests loading " << _dim << " dimension,"
               << "but file has " << file_dim << " dimension." << std::endl;
        diskann::cerr << stream.str() << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (file_num_points > _max_points + _num_frozen_pts)
    {
        // update and tag lock acquired in load() before calling load_data
        resize(file_num_points - _num_frozen_pts);
    }

#ifdef EXEC_ENV_OLS
    // REFACTOR TODO: Must figure out how to support aligned reader in a clean
    // manner.
    copy_aligned_data_from_file<T>(reader, _data, file_num_points, file_dim, _data_store->get_aligned_dim());
#else
    _data_store->load(filename); // offset == 0.
#endif
    return file_num_points;
}

#ifdef EXEC_ENV_OLS
template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::load_delete_set(AlignedFileReader &reader)
{
#else
template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::load_delete_set(const std::string &filename)
{
#endif
    std::unique_ptr<uint32_t[]> delete_list;
    size_t npts, ndim;

#ifdef EXEC_ENV_OLS
    diskann::load_bin<uint32_t>(reader, delete_list, npts, ndim);
#else
    diskann::load_bin<uint32_t>(filename, delete_list, npts, ndim);
#endif
    assert(ndim == 1);
    for (uint32_t i = 0; i < npts; i++)
    {
        _delete_set->insert(delete_list[i]);
    }
    return npts;
}

// load the index from file and update the max_degree, cur (navigating
// node loc), and _final_graph (adjacency list)
template <typename T, typename TagT, typename LabelT>
#ifdef EXEC_ENV_OLS
void Index<T, TagT, LabelT>::load(AlignedFileReader &reader, uint32_t num_threads, uint32_t search_l)
{
#else
void Index<T, TagT, LabelT>::load(const char *filename, uint32_t num_threads, uint32_t search_l)
{
#endif
    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> cl(_consolidate_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);

    _has_built = true;

    size_t tags_file_num_pts = 0, graph_num_pts = 0, data_file_num_pts = 0, label_num_pts = 0;

    std::string mem_index_file(filename);
    std::string labels_file = mem_index_file + "_labels.txt";
    std::string labels_to_medoids = mem_index_file + "_labels_to_medoids.txt";
    std::string labels_map_file = mem_index_file + "_labels_map.txt";

    if (!_save_as_one_file)
    {
        // For DLVS Store, we will not support saving the index in multiple
        // files.
#ifndef EXEC_ENV_OLS
        std::string data_file = std::string(filename) + ".data";
        std::string tags_file = std::string(filename) + ".tags";
        std::string delete_set_file = std::string(filename) + ".del";
        std::string graph_file = std::string(filename);
        data_file_num_pts = load_data(data_file);
        if (file_exists(delete_set_file))
        {
            load_delete_set(delete_set_file);
        }
        if (_enable_tags)
        {
            tags_file_num_pts = load_tags(tags_file);
        }
        graph_num_pts = load_graph(graph_file, data_file_num_pts);
#endif
    }
    else
    {
        diskann::cout << "Single index file saving/loading support not yet "
                         "enabled. Not loading the index."
                      << std::endl;
        return;
    }

    if (data_file_num_pts != graph_num_pts || (data_file_num_pts != tags_file_num_pts && _enable_tags))
    {
        std::stringstream stream;
        stream << "ERROR: When loading index, loaded " << data_file_num_pts << " points from datafile, "
               << graph_num_pts << " from graph, and " << tags_file_num_pts
               << " tags, with num_frozen_pts being set to " << _num_frozen_pts << " in constructor." << std::endl;
        diskann::cerr << stream.str() << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (file_exists(labels_file))
    {
        _label_map = load_label_map(labels_map_file);
        parse_label_file(labels_file, label_num_pts);
        assert(label_num_pts == data_file_num_pts - _num_frozen_pts);
        if (file_exists(labels_to_medoids))
        {
            std::ifstream medoid_stream(labels_to_medoids);
            std::string line, token;
            uint32_t line_cnt = 0;

            _label_to_start_id.clear();

            while (std::getline(medoid_stream, line))
            {
                std::istringstream iss(line);
                uint32_t cnt = 0;
                uint32_t medoid = 0;
                LabelT label;
                while (std::getline(iss, token, ','))
                {
                    token.erase(std::remove(token.begin(), token.end(), '\n'), token.end());
                    token.erase(std::remove(token.begin(), token.end(), '\r'), token.end());
                    LabelT token_as_num = (LabelT)std::stoul(token);
                    if (cnt == 0)
                        label = token_as_num;
                    else
                        medoid = token_as_num;
                    cnt++;
                }
                _label_to_start_id[label] = medoid;
                line_cnt++;
            }
        }

        std::string universal_label_file(filename);
        universal_label_file += "_universal_label.txt";
        if (file_exists(universal_label_file))
        {
            std::ifstream universal_label_reader(universal_label_file);
            universal_label_reader >> _universal_label;
            _use_universal_label = true;
            universal_label_reader.close();
        }
    }

    _nd = data_file_num_pts - _num_frozen_pts;
    _empty_slots.clear();
    _empty_slots.reserve(_max_points);
    for (auto i = _nd; i < _max_points; i++)
    {
        _empty_slots.insert((uint32_t)i);
    }

    reposition_frozen_point_to_end();
    diskann::cout << "Num frozen points:" << _num_frozen_pts << " _nd: " << _nd << " _start: " << _start
                  << " size(_location_to_tag): " << _location_to_tag.size()
                  << " size(_tag_to_location):" << _tag_to_location.size() << " Max points: " << _max_points
                  << std::endl;

    // For incremental index, _query_scratch is initialized in the constructor.
    // For the bulk index, the params required to initialize _query_scratch
    // are known only at load time, hence this check and the call to
    // initialize_q_s().
    if (_query_scratch.size() == 0)
    {
        initialize_query_scratch(num_threads, search_l, search_l, (uint32_t)_graph_store->get_max_range_of_graph(),
                                 _indexingMaxC, _dim);
    }
}

#ifndef EXEC_ENV_OLS
template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::get_graph_num_frozen_points(const std::string &graph_file)
{
    size_t expected_file_size;
    uint32_t max_observed_degree, start;
    size_t file_frozen_pts;

    std::ifstream in;
    in.exceptions(std::ios::badbit | std::ios::failbit);

    in.open(graph_file, std::ios::binary);
    in.read((char *)&expected_file_size, sizeof(size_t));
    in.read((char *)&max_observed_degree, sizeof(uint32_t));
    in.read((char *)&start, sizeof(uint32_t));
    in.read((char *)&file_frozen_pts, sizeof(size_t));

    return file_frozen_pts;
}
#endif

#ifdef EXEC_ENV_OLS
template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::load_graph(AlignedFileReader &reader, size_t expected_num_points)
{
#else

template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::load_graph(std::string filename, size_t expected_num_points)
{
#endif
    auto res = _graph_store->load(filename, expected_num_points);
    _start = std::get<1>(res);
    _num_frozen_pts = std::get<2>(res);
    return std::get<0>(res);
}

template <typename T, typename TagT, typename LabelT>
int Index<T, TagT, LabelT>::_get_vector_by_tag(TagType &tag, DataType &vec)
{
    try
    {
        TagT tag_val = std::any_cast<TagT>(tag);
        T *vec_val = std::any_cast<T *>(vec);
        return this->get_vector_by_tag(tag_val, vec_val);
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException("Error: bad any cast while performing _get_vector_by_tags() " + std::string(e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error: " + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT> int Index<T, TagT, LabelT>::get_vector_by_tag(TagT &tag, T *vec)
{
    std::shared_lock<std::shared_timed_mutex> lock(_tag_lock);
    if (_tag_to_location.find(tag) == _tag_to_location.end())
    {
        diskann::cout << "Tag " << get_tag_string(tag) << " does not exist" << std::endl;
        return -1;
    }

    location_t location = _tag_to_location[tag];
    _data_store->get_vector(location, vec);

    return 0;
}

template <typename T, typename TagT, typename LabelT> uint32_t Index<T, TagT, LabelT>::calculate_entry_point()
{
    // REFACTOR TODO: This function does not support multi-threaded calculation of medoid.
    // Must revisit if perf is a concern.
    return _data_store->calculate_medoid();
}

template <typename T, typename TagT, typename LabelT> std::vector<uint32_t> Index<T, TagT, LabelT>::get_init_ids()
{
    std::vector<uint32_t> init_ids;
    init_ids.reserve(1 + _num_frozen_pts);

    init_ids.emplace_back(_start);

    for (uint32_t frozen = (uint32_t)_max_points; frozen < _max_points + _num_frozen_pts; frozen++)
    {
        if (frozen != _start)
        {
            init_ids.emplace_back(frozen);
        }
    }

    return init_ids;
}

// Find common filter between a node's labels and a given set of labels, while
// taking into account universal label
template <typename T, typename TagT, typename LabelT>
bool Index<T, TagT, LabelT>::detect_common_filters(uint32_t point_id, bool search_invocation,
                                                   const std::vector<LabelT> &incoming_labels)
{
    auto &curr_node_labels = _location_to_labels[point_id];
    std::vector<LabelT> common_filters;
    std::set_intersection(incoming_labels.begin(), incoming_labels.end(), curr_node_labels.begin(),
                          curr_node_labels.end(), std::back_inserter(common_filters));
    if (common_filters.size() > 0)
    {
        // This is to reduce the repetitive calls. If common_filters size is > 0 ,
        // we dont need to check further for universal label
        return true;
    }
    if (_use_universal_label)
    {
        if (!search_invocation)
        {
            if (std::find(incoming_labels.begin(), incoming_labels.end(), _universal_label) != incoming_labels.end() ||
                std::find(curr_node_labels.begin(), curr_node_labels.end(), _universal_label) != curr_node_labels.end())
                common_filters.push_back(_universal_label);
        }
        else
        {
            if (std::find(curr_node_labels.begin(), curr_node_labels.end(), _universal_label) != curr_node_labels.end())
                common_filters.push_back(_universal_label);
        }
    }
    return (common_filters.size() > 0);
}

template <typename T, typename TagT, typename LabelT>
std::pair<uint32_t, uint32_t> Index<T, TagT, LabelT>::iterate_to_fixed_point(
    InMemQueryScratch<T> *scratch, const uint32_t Lsize, const std::vector<uint32_t> &init_ids, bool use_filter,
    const std::vector<LabelT> &filter_labels, bool search_invocation)
{
    std::vector<Neighbor> &expanded_nodes = scratch->pool();
    NeighborPriorityQueue &best_L_nodes = scratch->best_l_nodes();
    best_L_nodes.reserve(Lsize);
    tsl::robin_set<uint32_t> &inserted_into_pool_rs = scratch->inserted_into_pool_rs();
    boost::dynamic_bitset<> &inserted_into_pool_bs = scratch->inserted_into_pool_bs();
    std::vector<uint32_t> &id_scratch = scratch->id_scratch();
    std::vector<float> &dist_scratch = scratch->dist_scratch();
    assert(id_scratch.size() == 0);

    T *aligned_query = scratch->aligned_query();

    float *pq_dists = nullptr;

    _pq_data_store->preprocess_query(aligned_query, scratch);

    if (expanded_nodes.size() > 0 || id_scratch.size() > 0)
    {
        throw ANNException("ERROR: Clear scratch space before passing.", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    // Decide whether to use bitset or robin set to mark visited nodes
    auto total_num_points = _max_points + _num_frozen_pts;
    bool fast_iterate = total_num_points <= MAX_POINTS_FOR_USING_BITSET;

    if (fast_iterate)
    {
        if (inserted_into_pool_bs.size() < total_num_points)
        {
            // hopefully using 2X will reduce the number of allocations.
            auto resize_size =
                2 * total_num_points > MAX_POINTS_FOR_USING_BITSET ? MAX_POINTS_FOR_USING_BITSET : 2 * total_num_points;
            inserted_into_pool_bs.resize(resize_size);
        }
    }

    // Lambda to determine if a node has been visited
    auto is_not_visited = [this, fast_iterate, &inserted_into_pool_bs, &inserted_into_pool_rs](const uint32_t id) {
        return fast_iterate ? inserted_into_pool_bs[id] == 0
                            : inserted_into_pool_rs.find(id) == inserted_into_pool_rs.end();
    };

    // Lambda to batch compute query<-> node distances in PQ space
    auto compute_dists = [this, scratch, pq_dists](const std::vector<uint32_t> &ids, std::vector<float> &dists_out) {
        _pq_data_store->get_distance(scratch->aligned_query(), ids, dists_out, scratch);
    };

    // Initialize the candidate pool with starting points
    for (auto id : init_ids)
    {
        if (id >= _max_points + _num_frozen_pts)
        {
            diskann::cerr << "Out of range loc found as an edge : " << id << std::endl;
            throw diskann::ANNException(std::string("Wrong loc") + std::to_string(id), -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
        }

        if (use_filter)
        {
            if (!detect_common_filters(id, search_invocation, filter_labels))
                continue;
        }

        if (is_not_visited(id))
        {
            if (fast_iterate)
            {
                inserted_into_pool_bs[id] = 1;
            }
            else
            {
                inserted_into_pool_rs.insert(id);
            }

            float distance;
            uint32_t ids[] = {id};
            float distances[] = {std::numeric_limits<float>::max()};
            _pq_data_store->get_distance(aligned_query, ids, 1, distances, scratch);
            distance = distances[0];

            Neighbor nn = Neighbor(id, distance);
            best_L_nodes.insert(nn);
        }
    }

    uint32_t hops = 0;
    uint32_t cmps = 0;
    std::cout << "best_L_nodes size: " << best_L_nodes.size() << ", expanded_nodes:" << expanded_nodes.size() << std::endl;
    while (best_L_nodes.has_unexpanded_node())
    {
        auto nbr = best_L_nodes.closest_unexpanded();
        auto n = nbr.id;

        // Add node to expanded nodes to create pool for prune later
        if (!search_invocation)
        {
            if (!use_filter)
            {
                expanded_nodes.emplace_back(nbr);
            }
            else
            { // in filter based indexing, the same point might invoke
                // multiple iterate_to_fixed_points, so need to be careful
                // not to add the same item to pool multiple times.
                if (std::find(expanded_nodes.begin(), expanded_nodes.end(), nbr) == expanded_nodes.end())
                {
                    expanded_nodes.emplace_back(nbr);
                }
            }
        }

        // Find which of the nodes in des have not been visited before
        id_scratch.clear();
        dist_scratch.clear();
        if (_dynamic_index)
        {
            LockGuard guard(_locks[n]);
            for (auto id : _graph_store->get_neighbours(n))
            {
                assert(id < _max_points + _num_frozen_pts);

                if (use_filter)
                {
                    // NOTE: NEED TO CHECK IF THIS CORRECT WITH NEW LOCKS.
                    if (!detect_common_filters(id, search_invocation, filter_labels))
                        continue;
                }

                if (is_not_visited(id))
                {
                    id_scratch.push_back(id);
                }
            }
        }
        else
        {
            _locks[n].lock();
            auto nbrs = _graph_store->get_neighbours(n);
            _locks[n].unlock();
            for (auto id : nbrs)
            {
                assert(id < _max_points + _num_frozen_pts);

                if (use_filter)
                {
                    // NOTE: NEED TO CHECK IF THIS CORRECT WITH NEW LOCKS.
                    if (!detect_common_filters(id, search_invocation, filter_labels))
                        continue;
                }

                if (is_not_visited(id))
                {
                    id_scratch.push_back(id);
                }
            }
        }

        // Mark nodes visited
        for (auto id : id_scratch)
        {
            if (fast_iterate)
            {
                inserted_into_pool_bs[id] = 1;
            }
            else
            {
                inserted_into_pool_rs.insert(id);
            }
        }

        assert(dist_scratch.capacity() >= id_scratch.size());
        compute_dists(id_scratch, dist_scratch);
        cmps += (uint32_t)id_scratch.size();

        // Insert <id, dist> pairs into the pool of candidates
        for (size_t m = 0; m < id_scratch.size(); ++m)
        {
            best_L_nodes.insert(Neighbor(id_scratch[m], dist_scratch[m]));
        }
    }
    std::cout << "best_L_nodes2 size: " << best_L_nodes.size() << ", expanded_nodes:" << expanded_nodes.size() << std::endl;
    return std::make_pair(hops, cmps);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::search_for_point_and_prune(int location, uint32_t Lindex,
                                                        std::vector<uint32_t> &pruned_list,
                                                        InMemQueryScratch<T> *scratch, bool use_filter,
                                                        uint32_t filteredLindex)
{
    const std::vector<uint32_t> init_ids = get_init_ids();
    const std::vector<LabelT> unused_filter_label;

    if (!use_filter)
    {
        _data_store->get_vector(location, scratch->aligned_query());
        std::cout << "init_ids size: " << init_ids.size() << std::endl;
        iterate_to_fixed_point(scratch, Lindex, init_ids, false, unused_filter_label, false);
        std::cout << "993 size: " <<  scratch->pool().size() << std::endl;
    }
    else
    {
        std::shared_lock<std::shared_timed_mutex> tl(_tag_lock, std::defer_lock);
        if (_dynamic_index)
            tl.lock();
        std::vector<uint32_t> filter_specific_start_nodes;
        for (auto &x : _location_to_labels[location])
            filter_specific_start_nodes.emplace_back(_label_to_start_id[x]);

        if (_dynamic_index)
            tl.unlock();

        _data_store->get_vector(location, scratch->aligned_query());
        iterate_to_fixed_point(scratch, filteredLindex, filter_specific_start_nodes, true,
                               _location_to_labels[location], false);

        // combine candidate pools obtained with filter and unfiltered criteria.
        std::set<Neighbor> best_candidate_pool;
        for (auto filtered_neighbor : scratch->pool())
        {
            best_candidate_pool.insert(filtered_neighbor);
        }

        // clear scratch for finding unfiltered candidates
        scratch->clear();

        _data_store->get_vector(location, scratch->aligned_query());
        iterate_to_fixed_point(scratch, Lindex, init_ids, false, unused_filter_label, false);

        for (auto unfiltered_neighbour : scratch->pool())
        {
            // insert if this neighbour is not already in best_candidate_pool
            if (best_candidate_pool.find(unfiltered_neighbour) == best_candidate_pool.end())
            {
                best_candidate_pool.insert(unfiltered_neighbour);
            }
        }

        scratch->pool().clear();
        std::copy(best_candidate_pool.begin(), best_candidate_pool.end(), std::back_inserter(scratch->pool()));
    }

    auto &pool = scratch->pool();
    std::cout << "1037 size: " << pool.size() << std::endl;
    for (uint32_t i = 0; i < pool.size(); i++)
    {
        if (pool[i].id == (uint32_t)location)
        {
            pool.erase(pool.begin() + i);
            i--;
        }
    }
    std::cout << "1047 size: " << pool.size() << std::endl;
    if (pruned_list.size() > 0)
    {
        throw diskann::ANNException("ERROR: non-empty pruned_list passed", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    prune_neighbors(location, pool, pruned_list, scratch);

    std::cout << "pool1 size: " << pool.size() << std::endl;
    std::cout << "pruned_list2 size: " << pruned_list.size() << std::endl;

    assert(!pruned_list.empty());
    assert(_graph_store->get_total_points() == _max_points + _num_frozen_pts);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::occlude_list(const uint32_t location, std::vector<Neighbor> &pool, const float alpha,
                                          const uint32_t degree, const uint32_t maxc, std::vector<uint32_t> &result,
                                          InMemQueryScratch<T> *scratch,
                                          const tsl::robin_set<uint32_t> *const delete_set_ptr)
{
    if (pool.size() == 0)
        return;

    // Truncate pool at maxc and initialize scratch spaces
    assert(std::is_sorted(pool.begin(), pool.end()));
    assert(result.size() == 0);
    if (pool.size() > maxc)
        pool.resize(maxc);
    std::vector<float> &occlude_factor = scratch->occlude_factor();
    // occlude_list can be called with the same scratch more than once by
    // search_for_point_and_add_link through inter_insert.
    occlude_factor.clear();
    // Initialize occlude_factor to pool.size() many 0.0f values for correctness
    occlude_factor.insert(occlude_factor.end(), pool.size(), 0.0f);

    float cur_alpha = 1;
    while (cur_alpha <= alpha && result.size() < degree)
    {
        // used for MIPS, where we store a value of eps in cur_alpha to
        // denote pruned out entries which we can skip in later rounds.
        float eps = cur_alpha + 0.01f;

        for (auto iter = pool.begin(); result.size() < degree && iter != pool.end(); ++iter)
        {
            if (occlude_factor[iter - pool.begin()] > cur_alpha)
            {
                continue;
            }
            // Set the entry to float::max so that is not considered again
            occlude_factor[iter - pool.begin()] = std::numeric_limits<float>::max();
            // Add the entry to the result if its not been deleted, and doesn't
            // add a self loop
            if (delete_set_ptr == nullptr || delete_set_ptr->find(iter->id) == delete_set_ptr->end())
            {
                if (iter->id != location)
                {
                    result.push_back(iter->id);
                }
            }

            // Update occlude factor for points from iter+1 to pool.end()
            for (auto iter2 = iter + 1; iter2 != pool.end(); iter2++)
            {
                auto t = iter2 - pool.begin();
                if (occlude_factor[t] > alpha)
                    continue;

                bool prune_allowed = true;
                if (_filtered_index)
                {
                    uint32_t a = iter->id;
                    uint32_t b = iter2->id;
                    if (_location_to_labels.size() < b || _location_to_labels.size() < a)
                        continue;
                    for (auto &x : _location_to_labels[b])
                    {
                        if (std::find(_location_to_labels[a].begin(), _location_to_labels[a].end(), x) ==
                            _location_to_labels[a].end())
                        {
                            prune_allowed = false;
                        }
                        if (!prune_allowed)
                            break;
                    }
                }
                if (!prune_allowed)
                    continue;

                float djk = _data_store->get_distance(iter2->id, iter->id);
                if (_dist_metric == diskann::Metric::L2 || _dist_metric == diskann::Metric::COSINE)
                {
                    occlude_factor[t] = (djk == 0) ? std::numeric_limits<float>::max()
                                                   : std::max(occlude_factor[t], iter2->distance / djk);
                }
                else if (_dist_metric == diskann::Metric::INNER_PRODUCT)
                {
                    // Improvization for flipping max and min dist for MIPS
                    float x = -iter2->distance;
                    float y = -djk;
                    if (y > cur_alpha * x)
                    {
                        occlude_factor[t] = std::max(occlude_factor[t], eps);
                    }
                }
            }
        }
        cur_alpha *= 1.2f;
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::prune_neighbors(const uint32_t location, std::vector<Neighbor> &pool,
                                             std::vector<uint32_t> &pruned_list, InMemQueryScratch<T> *scratch)
{
    prune_neighbors(location, pool, _indexingRange, _indexingMaxC, _indexingAlpha, pruned_list, scratch);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::prune_neighbors(const uint32_t location, std::vector<Neighbor> &pool, const uint32_t range,
                                             const uint32_t max_candidate_size, const float alpha,
                                             std::vector<uint32_t> &pruned_list, InMemQueryScratch<T> *scratch)
{
    if (pool.size() == 0)
    {
        // if the pool is empty, behave like a noop
        pruned_list.clear();
        return;
    }

    // If using _pq_build, over-write the PQ distances with actual distances
    // REFACTOR PQ: TODO: How to get rid of this!?
    if (_pq_dist)
    {
        for (auto &ngh : pool)
            ngh.distance = _data_store->get_distance(ngh.id, location);
    }

    // sort the pool based on distance to query and prune it with occlude_list
    std::sort(pool.begin(), pool.end());
    pruned_list.clear();
    pruned_list.reserve(range);

    occlude_list(location, pool, alpha, range, max_candidate_size, pruned_list, scratch);
    assert(pruned_list.size() <= range);
    if (_saturate_graph && alpha > 1)
    {
        for (const auto &node : pool)
        {
            if (pruned_list.size() >= range)
                break;
            if ((std::find(pruned_list.begin(), pruned_list.end(), node.id) == pruned_list.end()) &&
                node.id != location)
                pruned_list.push_back(node.id);
        }
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::inter_insert(uint32_t n, std::vector<uint32_t> &pruned_list, const uint32_t range,
                                          InMemQueryScratch<T> *scratch)
{
    const auto &src_pool = pruned_list;

    assert(!src_pool.empty());

    for (auto des : src_pool)
    {
        // des.loc is the loc of the neighbors of n
        assert(des < _max_points + _num_frozen_pts);
        // des_pool contains the neighbors of the neighbors of n
        std::vector<uint32_t> copy_of_neighbors;
        bool prune_needed = false;
        {
            LockGuard guard(_locks[des]);
            auto &des_pool = _graph_store->get_neighbours(des);
            if (std::find(des_pool.begin(), des_pool.end(), n) == des_pool.end())
            {
                if (des_pool.size() < (uint64_t)(defaults::GRAPH_SLACK_FACTOR * range))
                {
                    // des_pool.emplace_back(n);
                    _graph_store->add_neighbour(des, n);
                    prune_needed = false;
                }
                else
                {
                    copy_of_neighbors.reserve(des_pool.size() + 1);
                    copy_of_neighbors = des_pool;
                    copy_of_neighbors.push_back(n);
                    prune_needed = true;
                }
            }
        } // des lock is released by this point

        if (prune_needed)
        {
            tsl::robin_set<uint32_t> dummy_visited(0);
            std::vector<Neighbor> dummy_pool(0);

            size_t reserveSize = (size_t)(std::ceil(1.05 * defaults::GRAPH_SLACK_FACTOR * range));
            dummy_visited.reserve(reserveSize);
            dummy_pool.reserve(reserveSize);

            for (auto cur_nbr : copy_of_neighbors)
            {
                if (dummy_visited.find(cur_nbr) == dummy_visited.end() && cur_nbr != des)
                {
                    float dist = _data_store->get_distance(des, cur_nbr);
                    dummy_pool.emplace_back(Neighbor(cur_nbr, dist));
                    dummy_visited.insert(cur_nbr);
                }
            }
            std::vector<uint32_t> new_out_neighbors;
            prune_neighbors(des, dummy_pool, new_out_neighbors, scratch);
            {
                LockGuard guard(_locks[des]);

                _graph_store->set_neighbours(des, new_out_neighbors);
            }
        }
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::inter_insert(uint32_t n, std::vector<uint32_t> &pruned_list, InMemQueryScratch<T> *scratch)
{
    inter_insert(n, pruned_list, _indexingRange, scratch);
}

template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::link()
{
    uint32_t num_threads = _indexingThreads;
    if (num_threads != 0)
        omp_set_num_threads(num_threads);

    /* visit_order is a vector that is initialized to the entire graph */
    std::vector<uint32_t> visit_order;
    std::vector<diskann::Neighbor> pool, tmp;
    tsl::robin_set<uint32_t> visited;
    visit_order.reserve(_nd + _num_frozen_pts);
    for (uint32_t i = 0; i < (uint32_t)_nd; i++)
    {
        visit_order.emplace_back(i);
    }

    // If there are any frozen points, add them all.
    for (uint32_t frozen = (uint32_t)_max_points; frozen < _max_points + _num_frozen_pts; frozen++)
    {
        visit_order.emplace_back(frozen);
    }

    // if there are frozen points, the first such one is set to be the _start
    if (_num_frozen_pts > 0)
        _start = (uint32_t)_max_points;
    else
        _start = calculate_entry_point();

    diskann::Timer link_timer;

#pragma omp parallel for schedule(dynamic, 2048)
    for (int64_t node_ctr = 0; node_ctr < (int64_t)(visit_order.size()); node_ctr++)
    {
        auto node = visit_order[node_ctr];

        // Find and add appropriate graph edges
        ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
        auto scratch = manager.scratch_space();
        std::vector<uint32_t> pruned_list;
        if (_filtered_index)
        {
            search_for_point_and_prune(node, _indexingQueueSize, pruned_list, scratch, true, _filterIndexingQueueSize);
        }
        else
        {
            search_for_point_and_prune(node, _indexingQueueSize, pruned_list, scratch);
        }
        assert(pruned_list.size() > 0);

        {
            LockGuard guard(_locks[node]);

            _graph_store->set_neighbours(node, pruned_list);
            assert(_graph_store->get_neighbours((location_t)node).size() <= _indexingRange);
        }

        inter_insert(node, pruned_list, scratch);

        if (node_ctr % 100000 == 0)
        {
            diskann::cout << "\r" << (100.0 * node_ctr) / (visit_order.size()) << "% of index build completed."
                          << std::flush;
        }
    }

    if (_nd > 0)
    {
        diskann::cout << "Starting final cleanup.." << std::flush;
    }
#pragma omp parallel for schedule(dynamic, 2048)
    for (int64_t node_ctr = 0; node_ctr < (int64_t)(visit_order.size()); node_ctr++)
    {
        auto node = visit_order[node_ctr];
        if (_graph_store->get_neighbours((location_t)node).size() > _indexingRange)
        {
            ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
            auto scratch = manager.scratch_space();

            tsl::robin_set<uint32_t> dummy_visited(0);
            std::vector<Neighbor> dummy_pool(0);
            std::vector<uint32_t> new_out_neighbors;

            for (auto cur_nbr : _graph_store->get_neighbours((location_t)node))
            {
                if (dummy_visited.find(cur_nbr) == dummy_visited.end() && cur_nbr != node)
                {
                    float dist = _data_store->get_distance(node, cur_nbr);
                    dummy_pool.emplace_back(Neighbor(cur_nbr, dist));
                    dummy_visited.insert(cur_nbr);
                }
            }
            prune_neighbors(node, dummy_pool, new_out_neighbors, scratch);

            _graph_store->clear_neighbours((location_t)node);
            _graph_store->set_neighbours((location_t)node, new_out_neighbors);
        }
    }
    if (_nd > 0)
    {
        diskann::cout << "done. Link time: " << ((double)link_timer.elapsed() / (double)1000000) << "s" << std::endl;
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::prune_all_neighbors(const uint32_t max_degree, const uint32_t max_occlusion_size,
                                                 const float alpha)
{
    const uint32_t range = max_degree;
    const uint32_t maxc = max_occlusion_size;

    _filtered_index = true;

    diskann::Timer timer;
#pragma omp parallel for
    for (int64_t node = 0; node < (int64_t)(_max_points + _num_frozen_pts); node++)
    {
        if ((size_t)node < _nd || (size_t)node >= _max_points)
        {
            if (_graph_store->get_neighbours((location_t)node).size() > range)
            {
                tsl::robin_set<uint32_t> dummy_visited(0);
                std::vector<Neighbor> dummy_pool(0);
                std::vector<uint32_t> new_out_neighbors;

                ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
                auto scratch = manager.scratch_space();

                for (auto cur_nbr : _graph_store->get_neighbours((location_t)node))
                {
                    if (dummy_visited.find(cur_nbr) == dummy_visited.end() && cur_nbr != node)
                    {
                        float dist = _data_store->get_distance((location_t)node, (location_t)cur_nbr);
                        dummy_pool.emplace_back(Neighbor(cur_nbr, dist));
                        dummy_visited.insert(cur_nbr);
                    }
                }

                prune_neighbors((uint32_t)node, dummy_pool, range, maxc, alpha, new_out_neighbors, scratch);
                _graph_store->clear_neighbours((location_t)node);
                _graph_store->set_neighbours((location_t)node, new_out_neighbors);
            }
        }
    }

    diskann::cout << "Prune time : " << timer.elapsed() / 1000 << "ms" << std::endl;
    size_t max = 0, min = 1 << 30, total = 0, cnt = 0;
    for (size_t i = 0; i < _max_points + _num_frozen_pts; i++)
    {
        if (i < _nd || i >= _max_points)
        {
            const std::vector<uint32_t> &pool = _graph_store->get_neighbours((location_t)i);
            max = (std::max)(max, pool.size());
            min = (std::min)(min, pool.size());
            total += pool.size();
            if (pool.size() < 2)
                cnt++;
        }
    }
    if (min > max)
        min = max;
    if (_nd > 0)
    {
        diskann::cout << "Index built with degree: max:" << max
                      << "  avg:" << (float)total / (float)(_nd + _num_frozen_pts) << "  min:" << min
                      << "  count(deg<2):" << cnt << std::endl;
    }
}

// REFACTOR
template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::set_start_points(const T *data, size_t data_count)
{
    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    if (_nd > 0)
        throw ANNException("Can not set starting point for a non-empty index", -1, __FUNCSIG__, __FILE__, __LINE__);

    if (data_count != _num_frozen_pts * _dim)
        throw ANNException("Invalid number of points", -1, __FUNCSIG__, __FILE__, __LINE__);

    //     memcpy(_data + _aligned_dim * _max_points, data, _aligned_dim *
    //     sizeof(T) * _num_frozen_pts);
    for (location_t i = 0; i < _num_frozen_pts; i++)
    {
        _data_store->set_vector((location_t)(i + _max_points), data + i * _dim);
    }
    _has_built = true;
    diskann::cout << "Index start points set: #" << _num_frozen_pts << std::endl;
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::_set_start_points_at_random(DataType radius, uint32_t random_seed)
{
    try
    {
        T radius_to_use = std::any_cast<T>(radius);
        this->set_start_points_at_random(radius_to_use, random_seed);
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException(
            "Error: bad any cast while performing _set_start_points_at_random() " + std::string(e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error: " + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::set_start_points_at_random(T radius, uint32_t random_seed)
{
    std::mt19937 gen{random_seed};
    std::normal_distribution<> d{0.0, 1.0};

    std::vector<T> points_data;
    points_data.reserve(_dim * _num_frozen_pts);
    std::vector<double> real_vec(_dim);

    for (size_t frozen_point = 0; frozen_point < _num_frozen_pts; frozen_point++)
    {
        double norm_sq = 0.0;
        for (size_t i = 0; i < _dim; ++i)
        {
            auto r = d(gen);
            real_vec[i] = r;
            norm_sq += r * r;
        }

        const double norm = std::sqrt(norm_sq);
        for (auto iter : real_vec)
            points_data.push_back(static_cast<T>(iter * radius / norm));
    }

    set_start_points(points_data.data(), points_data.size());
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::build_with_data_populated(const std::vector<TagT> &tags)
{
    diskann::cout << "Starting index build with " << _nd << " points... " << std::endl;

    if (_nd < 1)
        throw ANNException("Error: Trying to build an index with 0 points", -1, __FUNCSIG__, __FILE__, __LINE__);

    if (_enable_tags && tags.size() != _nd)
    {
        std::stringstream stream;
        stream << "ERROR: Driver requests loading " << _nd << " points from file,"
               << "but tags vector is of size " << tags.size() << "." << std::endl;
        diskann::cerr << stream.str() << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    if (_enable_tags)
    {
        for (size_t i = 0; i < tags.size(); ++i)
        {
            _tag_to_location[tags[i]] = (uint32_t)i;
            _location_to_tag.set(static_cast<uint32_t>(i), tags[i]);
        }
    }

    uint32_t index_R = _indexingRange;
    uint32_t num_threads_index = _indexingThreads;
    uint32_t index_L = _indexingQueueSize;
    uint32_t maxc = _indexingMaxC;

    if (_query_scratch.size() == 0)
    {
        initialize_query_scratch(5 + num_threads_index, index_L, index_L, index_R, maxc,
                                 _data_store->get_aligned_dim());
    }

    generate_frozen_point();
    link();

    size_t max = 0, min = SIZE_MAX, total = 0, cnt = 0;
    for (size_t i = 0; i < _nd; i++)
    {
        auto &pool = _graph_store->get_neighbours((location_t)i);
        max = std::max(max, pool.size());
        min = std::min(min, pool.size());
        total += pool.size();
        if (pool.size() < 2)
            cnt++;
    }
    diskann::cout << "Index built with degree: max:" << max << "  avg:" << (float)total / (float)(_nd + _num_frozen_pts)
                  << "  min:" << min << "  count(deg<2):" << cnt << std::endl;

    _has_built = true;
}
template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::_build(const DataType &data, const size_t num_points_to_load, TagVector &tags)
{
    try
    {
        this->build(std::any_cast<const T *>(data), num_points_to_load, tags.get<const std::vector<TagT>>());
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException("Error: bad any cast in while building index. " + std::string(e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error" + std::string(e.what()), -1);
    }
}
template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::build(const T *data, const size_t num_points_to_load, const std::vector<TagT> &tags)
{
    if (num_points_to_load == 0)
    {
        throw ANNException("Do not call build with 0 points", -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    if (_pq_dist)
    {
        throw ANNException("ERROR: DO not use this build interface with PQ distance", -1, __FUNCSIG__, __FILE__,
                           __LINE__);
    }

    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);

    {
        std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
        _nd = num_points_to_load;

        _data_store->populate_data(data, (location_t)num_points_to_load);
    }

    build_with_data_populated(tags);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::build(const char *filename, const size_t num_points_to_load, const std::vector<TagT> &tags)
{
    // idealy this should call build_filtered_index based on params passed

    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);

    // error checks
    if (num_points_to_load == 0)
        throw ANNException("Do not call build with 0 points", -1, __FUNCSIG__, __FILE__, __LINE__);

    if (!file_exists(filename))
    {
        std::stringstream stream;
        stream << "ERROR: Data file " << filename << " does not exist." << std::endl;
        diskann::cerr << stream.str() << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    size_t file_num_points, file_dim;
    if (filename == nullptr)
    {
        throw diskann::ANNException("Can not build with an empty file", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    diskann::get_bin_metadata(filename, file_num_points, file_dim);
    if (file_num_points > _max_points)
    {
        std::stringstream stream;
        stream << "ERROR: Driver requests loading " << num_points_to_load << " points and file has " << file_num_points
               << " points, but "
               << "index can support only " << _max_points << " points as specified in constructor." << std::endl;

        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (num_points_to_load > file_num_points)
    {
        std::stringstream stream;
        stream << "ERROR: Driver requests loading " << num_points_to_load << " points and file has only "
               << file_num_points << " points." << std::endl;

        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (file_dim != _dim)
    {
        std::stringstream stream;
        stream << "ERROR: Driver requests loading " << _dim << " dimension,"
               << "but file has " << file_dim << " dimension." << std::endl;
        diskann::cerr << stream.str() << std::endl;

        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    // REFACTOR PQ TODO: We can remove this if and add a check in the InMemDataStore
    // to not populate_data if it has been called once.
    if (_pq_dist)
    {
#ifdef EXEC_ENV_OLS
        std::stringstream ss;
        ss << "PQ Build is not supported in DLVS environment (i.e. if EXEC_ENV_OLS is defined)" << std::endl;
        diskann::cerr << ss.str() << std::endl;
        throw ANNException(ss.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
#else
        // REFACTOR TODO: Both in the previous code and in the current PQDataStore,
        // we are writing the PQ files in the same path as the input file. Now we
        // may not have write permissions to that folder, but we will always have
        // write permissions to the output folder. So we should write the PQ files
        // there. The problem is that the Index class gets the output folder prefix
        // only at the time of save(), by which time we are too late. So leaving it
        // as-is for now.
        _pq_data_store->populate_data(filename, 0U);
#endif
    }

    _data_store->populate_data(filename, 0U);
    diskann::cout << "Using only first " << num_points_to_load << " from file.. " << std::endl;

    {
        std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
        _nd = num_points_to_load;
    }
    build_with_data_populated(tags);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::build(const char *filename, const size_t num_points_to_load, const char *tag_filename)
{
    std::vector<TagT> tags;

    if (_enable_tags)
    {
        std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
        if (tag_filename == nullptr)
        {
            throw ANNException("Tag filename is null, while _enable_tags is set", -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        else
        {
            if (file_exists(tag_filename))
            {
                diskann::cout << "Loading tags from " << tag_filename << " for vamana index build" << std::endl;
                TagT *tag_data = nullptr;
                size_t npts, ndim;
                diskann::load_bin(tag_filename, tag_data, npts, ndim);
                if (npts < num_points_to_load)
                {
                    std::stringstream sstream;
                    sstream << "Loaded " << npts << " tags, insufficient to populate tags for " << num_points_to_load
                            << "  points to load";
                    throw diskann::ANNException(sstream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
                }
                for (size_t i = 0; i < num_points_to_load; i++)
                {
                    tags.push_back(tag_data[i]);
                }
                delete[] tag_data;
            }
            else
            {
                throw diskann::ANNException(std::string("Tag file") + tag_filename + " does not exist", -1, __FUNCSIG__,
                                            __FILE__, __LINE__);
            }
        }
    }
    build(filename, num_points_to_load, tags);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::build(const std::string &data_file, const size_t num_points_to_load,
                                   IndexFilterParams &filter_params)
{
    size_t points_to_load = num_points_to_load == 0 ? _max_points : num_points_to_load;

    auto s = std::chrono::high_resolution_clock::now();
    if (filter_params.label_file == "")
    {
        this->build(data_file.c_str(), points_to_load);
    }
    else
    {
        // TODO: this should ideally happen in save()
        std::string labels_file_to_use = filter_params.save_path_prefix + "_label_formatted.txt";
        std::string mem_labels_int_map_file = filter_params.save_path_prefix + "_labels_map.txt";
        convert_labels_string_to_int(filter_params.label_file, labels_file_to_use, mem_labels_int_map_file,
                                     filter_params.universal_label);
        if (filter_params.universal_label != "")
        {
            LabelT unv_label_as_num = 0;
            this->set_universal_label(unv_label_as_num);
        }
        this->build_filtered_index(data_file.c_str(), labels_file_to_use, points_to_load);
    }
    std::chrono::duration<double> diff = std::chrono::high_resolution_clock::now() - s;
    std::cout << "Indexing time: " << diff.count() << "\n";
}

template <typename T, typename TagT, typename LabelT>
std::unordered_map<std::string, LabelT> Index<T, TagT, LabelT>::load_label_map(const std::string &labels_map_file)
{
    std::unordered_map<std::string, LabelT> string_to_int_mp;
    std::ifstream map_reader(labels_map_file);
    std::string line, token;
    LabelT token_as_num;
    std::string label_str;
    while (std::getline(map_reader, line))
    {
        std::istringstream iss(line);
        getline(iss, token, '\t');
        label_str = token;
        getline(iss, token, '\t');
        token_as_num = (LabelT)std::stoul(token);
        string_to_int_mp[label_str] = token_as_num;
    }
    return string_to_int_mp;
}

template <typename T, typename TagT, typename LabelT>
LabelT Index<T, TagT, LabelT>::get_converted_label(const std::string &raw_label)
{
    if (_label_map.find(raw_label) != _label_map.end())
    {
        return _label_map[raw_label];
    }
    if (_use_universal_label)
    {
        return _universal_label;
    }
    std::stringstream stream;
    stream << "Unable to find label in the Label Map";
    diskann::cerr << stream.str() << std::endl;
    throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::parse_label_file(const std::string &label_file, size_t &num_points)
{
    // Format of Label txt file: filters with comma separators

    std::ifstream infile(label_file);
    if (infile.fail())
    {
        throw diskann::ANNException(std::string("Failed to open file ") + label_file, -1);
    }

    std::string line, token;
    uint32_t line_cnt = 0;

    while (std::getline(infile, line))
    {
        line_cnt++;
    }
    _location_to_labels.resize(line_cnt, std::vector<LabelT>());

    infile.clear();
    infile.seekg(0, std::ios::beg);
    line_cnt = 0;

    while (std::getline(infile, line))
    {
        std::istringstream iss(line);
        std::vector<LabelT> lbls(0);
        getline(iss, token, '\t');
        std::istringstream new_iss(token);
        while (getline(new_iss, token, ','))
        {
            token.erase(std::remove(token.begin(), token.end(), '\n'), token.end());
            token.erase(std::remove(token.begin(), token.end(), '\r'), token.end());
            LabelT token_as_num = (LabelT)std::stoul(token);
            lbls.push_back(token_as_num);
            _labels.insert(token_as_num);
        }

        std::sort(lbls.begin(), lbls.end());
        _location_to_labels[line_cnt] = lbls;
        line_cnt++;
    }
    num_points = (size_t)line_cnt;
    diskann::cout << "Identified " << _labels.size() << " distinct label(s)" << std::endl;
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::_set_universal_label(const LabelType universal_label)
{
    this->set_universal_label(std::any_cast<const LabelT>(universal_label));
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::set_universal_label(const LabelT &label)
{
    _use_universal_label = true;
    _universal_label = label;
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::build_filtered_index(const char *filename, const std::string &label_file,
                                                  const size_t num_points_to_load, const std::vector<TagT> &tags)
{
    _filtered_index = true;
    _label_to_start_id.clear();
    size_t num_points_labels = 0;

    parse_label_file(label_file,
                     num_points_labels); // determines medoid for each label and identifies
                                         // the points to label mapping

    std::unordered_map<LabelT, std::vector<uint32_t>> label_to_points;

    for (uint32_t point_id = 0; point_id < num_points_to_load; point_id++)
    {
        for (auto label : _location_to_labels[point_id])
        {
            if (label != _universal_label)
            {
                label_to_points[label].emplace_back(point_id);
            }
            else
            {
                for (typename tsl::robin_set<LabelT>::size_type lbl = 0; lbl < _labels.size(); lbl++)
                {
                    auto itr = _labels.begin();
                    std::advance(itr, lbl);
                    auto &x = *itr;
                    label_to_points[x].emplace_back(point_id);
                }
            }
        }
    }

    uint32_t num_cands = 25;
    for (auto itr = _labels.begin(); itr != _labels.end(); itr++)
    {
        uint32_t best_medoid_count = std::numeric_limits<uint32_t>::max();
        auto &curr_label = *itr;
        uint32_t best_medoid;
        auto labeled_points = label_to_points[curr_label];
        for (uint32_t cnd = 0; cnd < num_cands; cnd++)
        {
            uint32_t cur_cnd = labeled_points[rand() % labeled_points.size()];
            uint32_t cur_cnt = std::numeric_limits<uint32_t>::max();
            if (_medoid_counts.find(cur_cnd) == _medoid_counts.end())
            {
                _medoid_counts[cur_cnd] = 0;
                cur_cnt = 0;
            }
            else
            {
                cur_cnt = _medoid_counts[cur_cnd];
            }
            if (cur_cnt < best_medoid_count)
            {
                best_medoid_count = cur_cnt;
                best_medoid = cur_cnd;
            }
        }
        _label_to_start_id[curr_label] = best_medoid;
        _medoid_counts[best_medoid]++;
    }

    this->build(filename, num_points_to_load, tags);
}

template <typename T, typename TagT, typename LabelT>
std::pair<uint32_t, uint32_t> Index<T, TagT, LabelT>::_search(const DataType &query, const size_t K, const uint32_t L,
                                                              std::any &indices, float *distances)
{
    try
    {
        auto typed_query = std::any_cast<const T *>(query);
        if (typeid(uint32_t *) == indices.type())
        {
            auto u32_ptr = std::any_cast<uint32_t *>(indices);
            return this->search(typed_query, K, L, u32_ptr, distances);
        }
        else if (typeid(uint64_t *) == indices.type())
        {
            auto u64_ptr = std::any_cast<uint64_t *>(indices);
            return this->search(typed_query, K, L, u64_ptr, distances);
        }
        else
        {
            throw ANNException("Error: indices type can only be uint64_t or uint32_t.", -1);
        }
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException("Error: bad any cast while searching. " + std::string(e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error: " + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
template <typename IdType>
std::pair<uint32_t, uint32_t> Index<T, TagT, LabelT>::search(const T *query, const size_t K, const uint32_t L,
                                                             IdType *indices, float *distances)
{
    if (K > (uint64_t)L)
    {
        throw ANNException("Set L to a value of at least K", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
    auto scratch = manager.scratch_space();

    if (L > scratch->get_L())
    {
        diskann::cout << "Attempting to expand query scratch_space. Was created "
                      << "with Lsize: " << scratch->get_L() << " but search L is: " << L << std::endl;
        scratch->resize_for_new_L(L);
        diskann::cout << "Resize completed. New scratch->L is " << scratch->get_L() << std::endl;
    }

    const std::vector<LabelT> unused_filter_label;
    const std::vector<uint32_t> init_ids = get_init_ids();

    std::shared_lock<std::shared_timed_mutex> lock(_update_lock);

    _data_store->preprocess_query(query, scratch);

    auto retval = iterate_to_fixed_point(scratch, L, init_ids, false, unused_filter_label, true);

    NeighborPriorityQueue &best_L_nodes = scratch->best_l_nodes();

    size_t pos = 0;
    for (size_t i = 0; i < best_L_nodes.size(); ++i)
    {
        if (best_L_nodes[i].id < _max_points)
        {
            // safe because Index uses uint32_t ids internally
            // and IDType will be uint32_t or uint64_t
            indices[pos] = (IdType)best_L_nodes[i].id;
            if (distances != nullptr)
            {
#ifdef EXEC_ENV_OLS
                // DLVS expects negative distances
                distances[pos] = best_L_nodes[i].distance;
#else
                distances[pos] = _dist_metric == diskann::Metric::INNER_PRODUCT ? -1 * best_L_nodes[i].distance
                                                                                : best_L_nodes[i].distance;
#endif
            }
            pos++;
        }
        if (pos == K)
            break;
    }
    if (pos < K)
    {
        diskann::cerr << "Found pos: " << pos << "fewer than K elements " << K << " for query" << std::endl;
    }

    return retval;
}

template <typename T, typename TagT, typename LabelT>
std::pair<uint32_t, uint32_t> Index<T, TagT, LabelT>::_search_with_filters(const DataType &query,
                                                                           const std::string &raw_label, const size_t K,
                                                                           const uint32_t L, std::any &indices,
                                                                           float *distances)
{
    auto converted_label = this->get_converted_label(raw_label);
    if (typeid(uint64_t *) == indices.type())
    {
        auto ptr = std::any_cast<uint64_t *>(indices);
        return this->search_with_filters(std::any_cast<T *>(query), converted_label, K, L, ptr, distances);
    }
    else if (typeid(uint32_t *) == indices.type())
    {
        auto ptr = std::any_cast<uint32_t *>(indices);
        return this->search_with_filters(std::any_cast<T *>(query), converted_label, K, L, ptr, distances);
    }
    else
    {
        throw ANNException("Error: Id type can only be uint64_t or uint32_t.", -1);
    }
}

template <typename T, typename TagT, typename LabelT>
template <typename IdType>
std::pair<uint32_t, uint32_t> Index<T, TagT, LabelT>::search_with_filters(const T *query, const LabelT &filter_label,
                                                                          const size_t K, const uint32_t L,
                                                                          IdType *indices, float *distances)
{
    if (K > (uint64_t)L)
    {
        throw ANNException("Set L to a value of at least K", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
    auto scratch = manager.scratch_space();

    if (L > scratch->get_L())
    {
        diskann::cout << "Attempting to expand query scratch_space. Was created "
                      << "with Lsize: " << scratch->get_L() << " but search L is: " << L << std::endl;
        scratch->resize_for_new_L(L);
        diskann::cout << "Resize completed. New scratch->L is " << scratch->get_L() << std::endl;
    }

    std::vector<LabelT> filter_vec;
    std::vector<uint32_t> init_ids = get_init_ids();

    std::shared_lock<std::shared_timed_mutex> lock(_update_lock);
    std::shared_lock<std::shared_timed_mutex> tl(_tag_lock, std::defer_lock);
    if (_dynamic_index)
        tl.lock();

    if (_label_to_start_id.find(filter_label) != _label_to_start_id.end())
    {
        init_ids.emplace_back(_label_to_start_id[filter_label]);
    }
    else
    {
        diskann::cout << "No filtered medoid found. exitting "
                      << std::endl; // RKNOTE: If universal label found start there
        throw diskann::ANNException("No filtered medoid found. exitting ", -1);
    }
    if (_dynamic_index)
        tl.unlock();

    filter_vec.emplace_back(filter_label);

    _data_store->preprocess_query(query, scratch);
    auto retval = iterate_to_fixed_point(scratch, L, init_ids, true, filter_vec, true);

    auto best_L_nodes = scratch->best_l_nodes();

    size_t pos = 0;
    for (size_t i = 0; i < best_L_nodes.size(); ++i)
    {
        if (best_L_nodes[i].id < _max_points)
        {
            indices[pos] = (IdType)best_L_nodes[i].id;

            if (distances != nullptr)
            {
#ifdef EXEC_ENV_OLS
                // DLVS expects negative distances
                distances[pos] = best_L_nodes[i].distance;
#else
                distances[pos] = _dist_metric == diskann::Metric::INNER_PRODUCT ? -1 * best_L_nodes[i].distance
                                                                                : best_L_nodes[i].distance;
#endif
            }
            pos++;
        }
        if (pos == K)
            break;
    }
    if (pos < K)
    {
        diskann::cerr << "Found fewer than K elements for query" << std::endl;
    }

    return retval;
}

template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::_search_with_tags(const DataType &query, const uint64_t K, const uint32_t L,
                                                 const TagType &tags, float *distances, DataVector &res_vectors,
                                                 bool use_filters, const std::string filter_label)
{
    try
    {
        return this->search_with_tags(std::any_cast<const T *>(query), K, L, std::any_cast<TagT *>(tags), distances,
                                      res_vectors.get<std::vector<T *>>(), use_filters, filter_label);
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException("Error: bad any cast while performing _search_with_tags() " + std::string(e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error: " + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::search_with_tags(const T *query, const uint64_t K, const uint32_t L, TagT *tags,
                                                float *distances, std::vector<T *> &res_vectors, bool use_filters,
                                                const std::string filter_label)
{
    if (K > (uint64_t)L)
    {
        throw ANNException("Set L to a value of at least K", -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
    auto scratch = manager.scratch_space();

    if (L > scratch->get_L())
    {
        diskann::cout << "Attempting to expand query scratch_space. Was created "
                      << "with Lsize: " << scratch->get_L() << " but search L is: " << L << std::endl;
        scratch->resize_for_new_L(L);
        diskann::cout << "Resize completed. New scratch->L is " << scratch->get_L() << std::endl;
    }

    std::shared_lock<std::shared_timed_mutex> ul(_update_lock);

    const std::vector<uint32_t> init_ids = get_init_ids();

    //_distance->preprocess_query(query, _data_store->get_dims(),
    // scratch->aligned_query());
    _data_store->preprocess_query(query, scratch);
    if (!use_filters)
    {
        const std::vector<LabelT> unused_filter_label;
        iterate_to_fixed_point(scratch, L, init_ids, false, unused_filter_label, true);
    }
    else
    {
        std::vector<LabelT> filter_vec;
        auto converted_label = this->get_converted_label(filter_label);
        filter_vec.push_back(converted_label);
        iterate_to_fixed_point(scratch, L, init_ids, true, filter_vec, true);
    }

    NeighborPriorityQueue &best_L_nodes = scratch->best_l_nodes();
    assert(best_L_nodes.size() <= L);

    std::shared_lock<std::shared_timed_mutex> tl(_tag_lock);

    size_t pos = 0;
    for (size_t i = 0; i < best_L_nodes.size(); ++i)
    {
        auto node = best_L_nodes[i];

        TagT tag;
        if (_location_to_tag.try_get(node.id, tag))
        {
            tags[pos] = tag;

            if (res_vectors.size() > 0)
            {
                _data_store->get_vector(node.id, res_vectors[pos]);
            }

            if (distances != nullptr)
            {
#ifdef EXEC_ENV_OLS
                distances[pos] = node.distance; // DLVS expects negative distances
#else
                distances[pos] = _dist_metric == INNER_PRODUCT ? -1 * node.distance : node.distance;
#endif
            }
            pos++;
            // If res_vectors.size() < k, clip at the value.
            if (pos == K || pos == res_vectors.size())
                break;
        }
    }

    return pos;
}

template <typename T, typename TagT, typename LabelT> size_t Index<T, TagT, LabelT>::get_num_points()
{
    std::shared_lock<std::shared_timed_mutex> tl(_tag_lock);
    return _nd;
}

template <typename T, typename TagT, typename LabelT> size_t Index<T, TagT, LabelT>::get_max_points()
{
    std::shared_lock<std::shared_timed_mutex> tl(_tag_lock);
    return _max_points;
}

template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::generate_frozen_point()
{
    if (_num_frozen_pts == 0)
        return;

    if (_num_frozen_pts > 1)
    {
        throw ANNException("More than one frozen point not supported in generate_frozen_point", -1, __FUNCSIG__,
                           __FILE__, __LINE__);
    }

    if (_nd == 0)
    {
        throw ANNException("ERROR: Can not pick a frozen point since nd=0", -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    size_t res = calculate_entry_point();

    // REFACTOR PQ: Not sure if we should do this for both stores.
    if (_pq_dist)
    {
        // copy the PQ data corresponding to the point returned by
        // calculate_entry_point
        // memcpy(_pq_data + _max_points * _num_pq_chunks,
        //       _pq_data + res * _num_pq_chunks,
        //       _num_pq_chunks * DIV_ROUND_UP(NUM_PQ_BITS, 8));
        _pq_data_store->copy_vectors((location_t)res, (location_t)_max_points, 1);
    }
    else
    {
        _data_store->copy_vectors((location_t)res, (location_t)_max_points, 1);
    }
    _frozen_pts_used++;
}

template <typename T, typename TagT, typename LabelT> int Index<T, TagT, LabelT>::enable_delete()
{
    assert(_enable_tags);

    if (!_enable_tags)
    {
        diskann::cerr << "Tags must be instantiated for deletions" << std::endl;
        return -2;
    }

    if (this->_deletes_enabled)
    {
        return 0;
    }

    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);

    if (_data_compacted)
    {
        for (uint32_t slot = (uint32_t)_nd; slot < _max_points; ++slot)
        {
            _empty_slots.insert(slot);
        }
    }
    this->_deletes_enabled = true;
    return 0;
}

template <typename T, typename TagT, typename LabelT>
inline void Index<T, TagT, LabelT>::process_delete(const tsl::robin_set<uint32_t> &old_delete_set, size_t loc,
                                                   const uint32_t range, const uint32_t maxc, const float alpha,
                                                   InMemQueryScratch<T> *scratch)
{
    tsl::robin_set<uint32_t> &expanded_nodes_set = scratch->expanded_nodes_set();
    std::vector<Neighbor> &expanded_nghrs_vec = scratch->expanded_nodes_vec();

    // If this condition were not true, deadlock could result
    assert(old_delete_set.find((uint32_t)loc) == old_delete_set.end());

    std::vector<uint32_t> adj_list;
    {
        // Acquire and release lock[loc] before acquiring locks for neighbors
        std::unique_lock<non_recursive_mutex> adj_list_lock;
        if (_conc_consolidate)
            adj_list_lock = std::unique_lock<non_recursive_mutex>(_locks[loc]);
        adj_list = _graph_store->get_neighbours((location_t)loc);
    }

    bool modify = false;
    for (auto ngh : adj_list)
    {
        if (old_delete_set.find(ngh) == old_delete_set.end())
        {
            expanded_nodes_set.insert(ngh);
        }
        else
        {
            modify = true;

            std::unique_lock<non_recursive_mutex> ngh_lock;
            if (_conc_consolidate)
                ngh_lock = std::unique_lock<non_recursive_mutex>(_locks[ngh]);
            for (auto j : _graph_store->get_neighbours((location_t)ngh))
                if (j != loc && old_delete_set.find(j) == old_delete_set.end())
                    expanded_nodes_set.insert(j);
        }
    }

    if (modify)
    {
        if (expanded_nodes_set.size() <= range)
        {
            std::unique_lock<non_recursive_mutex> adj_list_lock(_locks[loc]);
            _graph_store->clear_neighbours((location_t)loc);
            for (auto &ngh : expanded_nodes_set)
                _graph_store->add_neighbour((location_t)loc, ngh);
        }
        else
        {
            // Create a pool of Neighbor candidates from the expanded_nodes_set
            expanded_nghrs_vec.reserve(expanded_nodes_set.size());
            for (auto &ngh : expanded_nodes_set)
            {
                expanded_nghrs_vec.emplace_back(ngh, _data_store->get_distance((location_t)loc, (location_t)ngh));
            }
            std::sort(expanded_nghrs_vec.begin(), expanded_nghrs_vec.end());
            std::vector<uint32_t> &occlude_list_output = scratch->occlude_list_output();
            occlude_list((uint32_t)loc, expanded_nghrs_vec, alpha, range, maxc, occlude_list_output, scratch,
                         &old_delete_set);
            std::unique_lock<non_recursive_mutex> adj_list_lock(_locks[loc]);
            _graph_store->set_neighbours((location_t)loc, occlude_list_output);
        }
    }
}

// Returns number of live points left after consolidation
template <typename T, typename TagT, typename LabelT>
consolidation_report Index<T, TagT, LabelT>::consolidate_deletes(const IndexWriteParameters &params)
{
    if (!_enable_tags)
        throw diskann::ANNException("Point tag array not instantiated", -1, __FUNCSIG__, __FILE__, __LINE__);

    {
        std::shared_lock<std::shared_timed_mutex> ul(_update_lock);
        std::shared_lock<std::shared_timed_mutex> tl(_tag_lock);
        std::shared_lock<std::shared_timed_mutex> dl(_delete_lock);
        if (_empty_slots.size() + _nd != _max_points)
        {
            std::string err = "#empty slots + nd != max points";
            diskann::cerr << err << std::endl;
            throw ANNException(err, -1, __FUNCSIG__, __FILE__, __LINE__);
        }

        if (_location_to_tag.size() + _delete_set->size() != _nd)
        {
            diskann::cerr << "Error: _location_to_tag.size (" << _location_to_tag.size() << ")  + _delete_set->size ("
                          << _delete_set->size() << ") != _nd(" << _nd << ") ";
            return consolidation_report(diskann::consolidation_report::status_code::INCONSISTENT_COUNT_ERROR, 0, 0, 0,
                                        0, 0, 0, 0);
        }

        if (_location_to_tag.size() != _tag_to_location.size())
        {
            throw diskann::ANNException("_location_to_tag and _tag_to_location not of same size", -1, __FUNCSIG__,
                                        __FILE__, __LINE__);
        }
    }

    std::unique_lock<std::shared_timed_mutex> update_lock(_update_lock, std::defer_lock);
    if (!_conc_consolidate)
        update_lock.lock();

    std::unique_lock<std::shared_timed_mutex> cl(_consolidate_lock, std::defer_lock);
    if (!cl.try_lock())
    {
        diskann::cerr << "Consildate delete function failed to acquire consolidate lock" << std::endl;
        return consolidation_report(diskann::consolidation_report::status_code::LOCK_FAIL, 0, 0, 0, 0, 0, 0, 0);
    }

    diskann::cout << "Starting consolidate_deletes... ";

    std::unique_ptr<tsl::robin_set<uint32_t>> old_delete_set(new tsl::robin_set<uint32_t>);
    {
        std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);
        std::swap(_delete_set, old_delete_set);
    }

    if (old_delete_set->find(_start) != old_delete_set->end())
    {
        throw diskann::ANNException("ERROR: start node has been deleted", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    const uint32_t range = params.max_degree;
    const uint32_t maxc = params.max_occlusion_size;
    const float alpha = params.alpha;
    const uint32_t num_threads = params.num_threads == 0 ? omp_get_num_procs() : params.num_threads;

    uint32_t num_calls_to_process_delete = 0;
    diskann::Timer timer;
#pragma omp parallel for num_threads(num_threads) schedule(dynamic, 8192) reduction(+ : num_calls_to_process_delete)
    for (int64_t loc = 0; loc < (int64_t)_max_points; loc++)
    {
        if (old_delete_set->find((uint32_t)loc) == old_delete_set->end() && !_empty_slots.is_in_set((uint32_t)loc))
        {
            ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
            auto scratch = manager.scratch_space();
            process_delete(*old_delete_set, loc, range, maxc, alpha, scratch);
            num_calls_to_process_delete += 1;
        }
    }
    for (int64_t loc = _max_points; loc < (int64_t)(_max_points + _num_frozen_pts); loc++)
    {
        ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
        auto scratch = manager.scratch_space();
        process_delete(*old_delete_set, loc, range, maxc, alpha, scratch);
        num_calls_to_process_delete += 1;
    }

    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    size_t ret_nd = release_locations(*old_delete_set);
    size_t max_points = _max_points;
    size_t empty_slots_size = _empty_slots.size();

    std::shared_lock<std::shared_timed_mutex> dl(_delete_lock);
    size_t delete_set_size = _delete_set->size();
    size_t old_delete_set_size = old_delete_set->size();

    if (!_conc_consolidate)
    {
        update_lock.unlock();
    }

    double duration = timer.elapsed() / 1000000.0;
    diskann::cout << " done in " << duration << " seconds." << std::endl;
    return consolidation_report(diskann::consolidation_report::status_code::SUCCESS, ret_nd, max_points,
                                empty_slots_size, old_delete_set_size, delete_set_size, num_calls_to_process_delete,
                                duration);
}

template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::compact_frozen_point()
{
    if (_nd < _max_points && _num_frozen_pts > 0)
    {
        reposition_points((uint32_t)_max_points, (uint32_t)_nd, (uint32_t)_num_frozen_pts);
        _start = (uint32_t)_nd;

        if (_filtered_index && _dynamic_index)
        {
            //  update medoid id's as frozen points are treated as medoid
            for (auto &[label, medoid_id] : _label_to_start_id)
            {
                /*  if (label == _universal_label)
                      continue;*/
                _label_to_start_id[label] = (uint32_t)_nd + (medoid_id - (uint32_t)_max_points);
            }
        }
    }
}

// Should be called after acquiring _update_lock
template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::compact_data()
{
    if (!_dynamic_index)
        throw ANNException("Can not compact a non-dynamic index", -1, __FUNCSIG__, __FILE__, __LINE__);

    if (_data_compacted)
    {
        diskann::cerr << "Warning! Calling compact_data() when _data_compacted is true!" << std::endl;
        return;
    }

    if (_delete_set->size() > 0)
    {
        throw ANNException("Can not compact data when index has non-empty _delete_set of "
                           "size: " +
                               std::to_string(_delete_set->size()),
                           -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    diskann::Timer timer;

    std::vector<uint32_t> new_location = std::vector<uint32_t>(_max_points + _num_frozen_pts, UINT32_MAX);

    uint32_t new_counter = 0;
    std::set<uint32_t> empty_locations;
    for (uint32_t old_location = 0; old_location < _max_points; old_location++)
    {
        if (_location_to_tag.contains(old_location))
        {
            new_location[old_location] = new_counter;
            new_counter++;
        }
        else
        {
            empty_locations.insert(old_location);
        }
    }
    for (uint32_t old_location = (uint32_t)_max_points; old_location < _max_points + _num_frozen_pts; old_location++)
    {
        new_location[old_location] = old_location;
    }

    // If start node is removed, throw an exception
    if (_start < _max_points && !_location_to_tag.contains(_start))
    {
        throw diskann::ANNException("ERROR: Start node deleted.", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    size_t num_dangling = 0;
    for (uint32_t old = 0; old < _max_points + _num_frozen_pts; ++old)
    {
        // compact _final_graph
        std::vector<uint32_t> new_adj_list;

        if ((new_location[old] < _max_points) // If point continues to exist
            || (old >= _max_points && old < _max_points + _num_frozen_pts))
        {
            new_adj_list.reserve(_graph_store->get_neighbours((location_t)old).size());
            for (auto ngh_iter : _graph_store->get_neighbours((location_t)old))
            {
                if (empty_locations.find(ngh_iter) != empty_locations.end())
                {
                    ++num_dangling;
                    diskann::cerr << "Error in compact_data(). _final_graph[" << old << "] has neighbor " << ngh_iter
                                  << " which is a location not associated with any tag." << std::endl;
                }
                else
                {
                    new_adj_list.push_back(new_location[ngh_iter]);
                }
            }
            //_graph_store->get_neighbours((location_t)old).swap(new_adj_list);
            _graph_store->set_neighbours((location_t)old, new_adj_list);

            // Move the data and adj list to the correct position
            if (new_location[old] != old)
            {
                assert(new_location[old] < old);
                _graph_store->swap_neighbours(new_location[old], (location_t)old);

                if (_filtered_index)
                {
                    _location_to_labels[new_location[old]].swap(_location_to_labels[old]);
                }

                _data_store->copy_vectors(old, new_location[old], 1);
            }
        }
        else
        {
            _graph_store->clear_neighbours((location_t)old);
        }
    }
    diskann::cerr << "#dangling references after data compaction: " << num_dangling << std::endl;

    _tag_to_location.clear();
    for (auto pos = _location_to_tag.find_first(); pos.is_valid(); pos = _location_to_tag.find_next(pos))
    {
        const auto tag = _location_to_tag.get(pos);
        _tag_to_location[tag] = new_location[pos._key];
    }
    _location_to_tag.clear();
    for (const auto &iter : _tag_to_location)
    {
        _location_to_tag.set(iter.second, iter.first);
    }
    // remove all cleared up old
    for (size_t old = _nd; old < _max_points; ++old)
    {
        _graph_store->clear_neighbours((location_t)old);
    }
    if (_filtered_index)
    {
        for (size_t old = _nd; old < _max_points; old++)
        {
            _location_to_labels[old].clear();
        }
    }

    _empty_slots.clear();
    // mark all slots after _nd as empty
    for (auto i = _nd; i < _max_points; i++)
    {
        _empty_slots.insert((uint32_t)i);
    }
    _data_compacted = true;
    diskann::cout << "Time taken for compact_data: " << timer.elapsed() / 1000000. << "s." << std::endl;
}

//
// Caller must hold unique _tag_lock and _delete_lock before calling this
//
template <typename T, typename TagT, typename LabelT> int Index<T, TagT, LabelT>::reserve_location()
{
    if (_nd >= _max_points)
    {
        return -1;
    }
    uint32_t location;
    if (_data_compacted && _empty_slots.is_empty())
    {
        // This code path is encountered when enable_delete hasn't been
        // called yet, so no points have been deleted and _empty_slots
        // hasn't been filled in. In that case, just keep assigning
        // consecutive locations.
        location = (uint32_t)_nd;
    }
    else
    {
        assert(_empty_slots.size() != 0);
        assert(_empty_slots.size() + _nd == _max_points);

        location = _empty_slots.pop_any();
        _delete_set->erase(location);
    }
    ++_nd;
    return location;
}

template <typename T, typename TagT, typename LabelT> size_t Index<T, TagT, LabelT>::release_location(int location)
{
    if (_empty_slots.is_in_set(location))
        throw ANNException("Trying to release location, but location already in empty slots", -1, __FUNCSIG__, __FILE__,
                           __LINE__);
    _empty_slots.insert(location);

    _nd--;
    return _nd;
}

template <typename T, typename TagT, typename LabelT>
size_t Index<T, TagT, LabelT>::release_locations(const tsl::robin_set<uint32_t> &locations)
{
    for (auto location : locations)
    {
        if (_empty_slots.is_in_set(location))
            throw ANNException("Trying to release location, but location "
                               "already in empty slots",
                               -1, __FUNCSIG__, __FILE__, __LINE__);
        _empty_slots.insert(location);

        _nd--;
    }

    if (_empty_slots.size() + _nd != _max_points)
        throw ANNException("#empty slots + nd != max points", -1, __FUNCSIG__, __FILE__, __LINE__);

    return _nd;
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::reposition_points(uint32_t old_location_start, uint32_t new_location_start,
                                               uint32_t num_locations)
{
    if (num_locations == 0 || old_location_start == new_location_start)
    {
        return;
    }

    // Update pointers to the moved nodes. Note: the computation is correct even
    // when new_location_start < old_location_start given the C++ uint32_t
    // integer arithmetic rules.
    const uint32_t location_delta = new_location_start - old_location_start;

    std::vector<location_t> updated_neighbours_location;
    for (uint32_t i = 0; i < _max_points + _num_frozen_pts; i++)
    {
        auto &i_neighbours = _graph_store->get_neighbours((location_t)i);
        std::vector<location_t> i_neighbours_copy(i_neighbours.begin(), i_neighbours.end());
        for (auto &loc : i_neighbours_copy)
        {
            if (loc >= old_location_start && loc < old_location_start + num_locations)
                loc += location_delta;
        }
        _graph_store->set_neighbours(i, i_neighbours_copy);
    }

    // The [start, end) interval which will contain obsolete points to be
    // cleared.
    uint32_t mem_clear_loc_start = old_location_start;
    uint32_t mem_clear_loc_end_limit = old_location_start + num_locations;

    // Move the adjacency lists. Make sure that overlapping ranges are handled
    // correctly.
    if (new_location_start < old_location_start)
    {
        // New location before the old location: copy the entries in order
        // to avoid modifying locations that are yet to be copied.
        for (uint32_t loc_offset = 0; loc_offset < num_locations; loc_offset++)
        {
            assert(_graph_store->get_neighbours(new_location_start + loc_offset).empty());
            _graph_store->swap_neighbours(new_location_start + loc_offset, old_location_start + loc_offset);
            if (_dynamic_index && _filtered_index)
            {
                _location_to_labels[new_location_start + loc_offset].swap(
                    _location_to_labels[old_location_start + loc_offset]);
            }
        }
        // If ranges are overlapping, make sure not to clear the newly copied
        // data.
        if (mem_clear_loc_start < new_location_start + num_locations)
        {
            // Clear only after the end of the new range.
            mem_clear_loc_start = new_location_start + num_locations;
        }
    }
    else
    {
        // Old location after the new location: copy from the end of the range
        // to avoid modifying locations that are yet to be copied.
        for (uint32_t loc_offset = num_locations; loc_offset > 0; loc_offset--)
        {
            assert(_graph_store->get_neighbours(new_location_start + loc_offset - 1u).empty());
            _graph_store->swap_neighbours(new_location_start + loc_offset - 1u, old_location_start + loc_offset - 1u);
            if (_dynamic_index && _filtered_index)
            {
                _location_to_labels[new_location_start + loc_offset - 1u].swap(
                    _location_to_labels[old_location_start + loc_offset - 1u]);
            }
        }

        // If ranges are overlapping, make sure not to clear the newly copied
        // data.
        if (mem_clear_loc_end_limit > new_location_start)
        {
            // Clear only up to the beginning of the new range.
            mem_clear_loc_end_limit = new_location_start;
        }
    }
    _data_store->move_vectors(old_location_start, new_location_start, num_locations);
}

template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::reposition_frozen_point_to_end()
{
    if (_num_frozen_pts == 0)
        return;

    if (_nd == _max_points)
    {
        diskann::cout << "Not repositioning frozen point as it is already at the end." << std::endl;
        return;
    }

    reposition_points((uint32_t)_nd, (uint32_t)_max_points, (uint32_t)_num_frozen_pts);
    _start = (uint32_t)_max_points;

    // update medoid id's as frozen points are treated as medoid
    if (_filtered_index && _dynamic_index)
    {
        for (auto &[label, medoid_id] : _label_to_start_id)
        {
            /*if (label == _universal_label)
                continue;*/
            _label_to_start_id[label] = (uint32_t)_max_points + (medoid_id - (uint32_t)_nd);
        }
    }
}

template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::resize(size_t new_max_points)
{
    const size_t new_internal_points = new_max_points + _num_frozen_pts;
    auto start = std::chrono::high_resolution_clock::now();
    assert(_empty_slots.size() == 0); // should not resize if there are empty slots.

    _data_store->resize((location_t)new_internal_points);
    _graph_store->resize_graph(new_internal_points);
    _locks = std::vector<non_recursive_mutex>(new_internal_points);

    if (_num_frozen_pts != 0)
    {
        reposition_points((uint32_t)_max_points, (uint32_t)new_max_points, (uint32_t)_num_frozen_pts);
        _start = (uint32_t)new_max_points;
    }

    _max_points = new_max_points;
    _empty_slots.reserve(_max_points);
    for (auto i = _nd; i < _max_points; i++)
    {
        _empty_slots.insert((uint32_t)i);
    }

    auto stop = std::chrono::high_resolution_clock::now();
    diskann::cout << "Resizing took: " << std::chrono::duration<double>(stop - start).count() << "s" << std::endl;
}

template <typename T, typename TagT, typename LabelT>
int Index<T, TagT, LabelT>::_insert_point(const DataType &point, const TagType tag)
{
    try
    {
        return this->insert_point(std::any_cast<const T *>(point), std::any_cast<const TagT>(tag));
    }
    catch (const std::bad_any_cast &anycast_e)
    {
        throw new ANNException("Error:Trying to insert invalid data type" + std::string(anycast_e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw new ANNException("Error:" + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
int Index<T, TagT, LabelT>::_insert_point(const DataType &point, const TagType tag, Labelvector &labels)
{
    try
    {
        return this->insert_point(std::any_cast<const T *>(point), std::any_cast<const TagT>(tag),
                                  labels.get<const std::vector<LabelT>>());
    }
    catch (const std::bad_any_cast &anycast_e)
    {
        throw new ANNException("Error:Trying to insert invalid data type" + std::string(anycast_e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw new ANNException("Error:" + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
int Index<T, TagT, LabelT>::insert_point(const T *point, const TagT tag)
{
    std::vector<LabelT> no_labels{0};
    return insert_point(point, tag, no_labels);
}

template <typename T, typename TagT, typename LabelT>
int Index<T, TagT, LabelT>::insert_point(const T *point, const TagT tag, const std::vector<LabelT> &labels)
{

    assert(_has_built);
    if (tag == 0)
    {
        throw diskann::ANNException("Do not insert point with tag 0. That is "
                                    "reserved for points hidden "
                                    "from the user.",
                                    -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    std::shared_lock<std::shared_timed_mutex> shared_ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);

    auto location = reserve_location();
    if (_filtered_index)
    {
        if (labels.empty())
        {
            release_location(location);
            std::cerr << "Error: Can't insert point with tag " + get_tag_string(tag) +
                             " . there are no labels for the point."
                      << std::endl;
            return -1;
        }

        _location_to_labels[location] = labels;

        for (LabelT label : labels)
        {
            if (_labels.find(label) == _labels.end())
            {
                if (_frozen_pts_used >= _num_frozen_pts)
                {
                    throw ANNException(
                        "Error: For dynamic filtered index, the number of frozen points should be atleast equal "
                        "to number of unique labels.",
                        -1);
                }

                auto fz_location = (int)(_max_points) + _frozen_pts_used; // as first _fz_point
                _labels.insert(label);
                _label_to_start_id[label] = (uint32_t)fz_location;
                _location_to_labels[fz_location] = {label};
                _data_store->set_vector((location_t)fz_location, point);
                _frozen_pts_used++;
            }
        }
    }

    if (location == -1)
    {
#if EXPAND_IF_FULL
        dl.unlock();
        tl.unlock();
        shared_ul.unlock();

        {
            std::unique_lock<std::shared_timed_mutex> ul(_update_lock);
            tl.lock();
            dl.lock();

            if (_nd >= _max_points)
            {
                auto new_max_points = (size_t)(_max_points * INDEX_GROWTH_FACTOR);
                resize(new_max_points);
            }

            dl.unlock();
            tl.unlock();
            ul.unlock();
        }

        shared_ul.lock();
        tl.lock();
        dl.lock();

        location = reserve_location();
        if (location == -1)
        {
            throw diskann::ANNException("Cannot reserve location even after "
                                        "expanding graph. Terminating.",
                                        -1, __FUNCSIG__, __FILE__, __LINE__);
        }
#else
        return -1;
#endif
    } // cant insert as active pts >= max_pts
    dl.unlock();

    // Insert tag and mapping to location
    if (_enable_tags)
    {
        // if tags are enabled and tag is already inserted. so we can't reuse that tag.
        if (_tag_to_location.find(tag) != _tag_to_location.end())
        {
            release_location(location);
            return -1;
        }

        _tag_to_location[tag] = location;
        _location_to_tag.set(location, tag);
    }
    tl.unlock();

    _data_store->set_vector(location, point); // update datastore

    // Find and add appropriate graph edges
    ScratchStoreManager<InMemQueryScratch<T>> manager(_query_scratch);
    auto scratch = manager.scratch_space();
    std::vector<uint32_t> pruned_list; // it is the set best candidates to connect to this point
    if (_filtered_index)
    {
        // when filtered the best_candidates will share the same label ( label_present > distance)
        search_for_point_and_prune(location, _indexingQueueSize, pruned_list, scratch, true, _filterIndexingQueueSize);
    }
    else
    {
        search_for_point_and_prune(location, _indexingQueueSize, pruned_list, scratch);
    }
    assert(pruned_list.size() > 0); // should find atleast one neighbour (i.e frozen point acting as medoid)

    {
        std::shared_lock<std::shared_timed_mutex> tlock(_tag_lock, std::defer_lock);
        if (_conc_consolidate)
            tlock.lock();

        LockGuard guard(_locks[location]);
        _graph_store->clear_neighbours(location);

        std::vector<uint32_t> neighbor_links;
        for (auto link : pruned_list)
        {
            if (_conc_consolidate)
                if (!_location_to_tag.contains(link))
                    continue;
            neighbor_links.emplace_back(link);
        }
        _graph_store->set_neighbours(location, neighbor_links);
        assert(_graph_store->get_neighbours(location).size() <= _indexingRange);

        if (_conc_consolidate)
            tlock.unlock();
    }

    inter_insert(location, pruned_list, scratch);

    return 0;
}

template <typename T, typename TagT, typename LabelT> int Index<T, TagT, LabelT>::_lazy_delete(const TagType &tag)
{
    try
    {
        return lazy_delete(std::any_cast<const TagT>(tag));
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException(std::string("Error: ") + e.what(), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::_lazy_delete(TagVector &tags, TagVector &failed_tags)
{
    try
    {
        this->lazy_delete(tags.get<const std::vector<TagT>>(), failed_tags.get<std::vector<TagT>>());
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException("Error: bad any cast while performing _lazy_delete() " + std::string(e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error: " + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT> int Index<T, TagT, LabelT>::lazy_delete(const TagT &tag)
{
    std::shared_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);
    _data_compacted = false;

    if (_tag_to_location.find(tag) == _tag_to_location.end())
    {
        diskann::cerr << "Delete tag not found " << get_tag_string(tag) << std::endl;
        return -1;
    }
    assert(_tag_to_location[tag] < _max_points);

    const auto location = _tag_to_location[tag];
    _delete_set->insert(location);
    _location_to_tag.erase(location);
    _tag_to_location.erase(tag);
    return 0;
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::lazy_delete(const std::vector<TagT> &tags, std::vector<TagT> &failed_tags)
{
    if (failed_tags.size() > 0)
    {
        throw ANNException("failed_tags should be passed as an empty list", -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    std::shared_lock<std::shared_timed_mutex> ul(_update_lock);
    std::unique_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::unique_lock<std::shared_timed_mutex> dl(_delete_lock);
    _data_compacted = false;

    for (auto tag : tags)
    {
        if (_tag_to_location.find(tag) == _tag_to_location.end())
        {
            failed_tags.push_back(tag);
        }
        else
        {
            const auto location = _tag_to_location[tag];
            _delete_set->insert(location);
            _location_to_tag.erase(location);
            _tag_to_location.erase(tag);
        }
    }
}

template <typename T, typename TagT, typename LabelT> bool Index<T, TagT, LabelT>::is_index_saved()
{
    return _is_saved;
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::_get_active_tags(TagRobinSet &active_tags)
{
    try
    {
        this->get_active_tags(active_tags.get<tsl::robin_set<TagT>>());
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException("Error: bad_any cast while performing _get_active_tags() " + std::string(e.what()), -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error :" + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::get_active_tags(tsl::robin_set<TagT> &active_tags)
{
    active_tags.clear();
    std::shared_lock<std::shared_timed_mutex> tl(_tag_lock);
    for (auto iter : _tag_to_location)
    {
        active_tags.insert(iter.first);
    }
}

template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::print_status()
{
    std::shared_lock<std::shared_timed_mutex> ul(_update_lock);
    std::shared_lock<std::shared_timed_mutex> cl(_consolidate_lock);
    std::shared_lock<std::shared_timed_mutex> tl(_tag_lock);
    std::shared_lock<std::shared_timed_mutex> dl(_delete_lock);

    diskann::cout << "------------------- Index object: " << (uint64_t)this << " -------------------" << std::endl;
    diskann::cout << "Number of points: " << _nd << std::endl;
    diskann::cout << "Graph size: " << _graph_store->get_total_points() << std::endl;
    diskann::cout << "Location to tag size: " << _location_to_tag.size() << std::endl;
    diskann::cout << "Tag to location size: " << _tag_to_location.size() << std::endl;
    diskann::cout << "Number of empty slots: " << _empty_slots.size() << std::endl;
    diskann::cout << std::boolalpha << "Data compacted: " << this->_data_compacted << std::endl;
    diskann::cout << "---------------------------------------------------------"
                     "------------"
                  << std::endl;
}

template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::count_nodes_at_bfs_levels()
{
    std::unique_lock<std::shared_timed_mutex> ul(_update_lock);

    boost::dynamic_bitset<> visited(_max_points + _num_frozen_pts);

    size_t MAX_BFS_LEVELS = 32;
    auto bfs_sets = new tsl::robin_set<uint32_t>[MAX_BFS_LEVELS];

    bfs_sets[0].insert(_start);
    visited.set(_start);

    for (uint32_t i = (uint32_t)_max_points; i < _max_points + _num_frozen_pts; ++i)
    {
        if (i != _start)
        {
            bfs_sets[0].insert(i);
            visited.set(i);
        }
    }

    for (size_t l = 0; l < MAX_BFS_LEVELS - 1; ++l)
    {
        diskann::cout << "Number of nodes at BFS level " << l << " is " << bfs_sets[l].size() << std::endl;
        if (bfs_sets[l].size() == 0)
            break;
        for (auto node : bfs_sets[l])
        {
            for (auto nghbr : _graph_store->get_neighbours((location_t)node))
            {
                if (!visited.test(nghbr))
                {
                    visited.set(nghbr);
                    bfs_sets[l + 1].insert(nghbr);
                }
            }
        }
    }

    delete[] bfs_sets;
}

// REFACTOR: This should be an OptimizedDataStore class
template <typename T, typename TagT, typename LabelT> void Index<T, TagT, LabelT>::optimize_index_layout()
{ // use after build or load
    if (_dynamic_index)
    {
        throw diskann::ANNException("Optimize_index_layout not implemented for dyanmic indices", -1, __FUNCSIG__,
                                    __FILE__, __LINE__);
    }

    float *cur_vec = new float[_data_store->get_aligned_dim()];
    std::memset(cur_vec, 0, _data_store->get_aligned_dim() * sizeof(float));
    _data_len = (_data_store->get_aligned_dim() + 1) * sizeof(float);
    _neighbor_len = (_graph_store->get_max_observed_degree() + 1) * sizeof(uint32_t);
    _node_size = _data_len + _neighbor_len;
    _opt_graph = new char[_node_size * _nd];
    auto dist_fast = (DistanceFastL2<T> *)(_data_store->get_dist_fn());
    for (uint32_t i = 0; i < _nd; i++)
    {
        char *cur_node_offset = _opt_graph + i * _node_size;
        _data_store->get_vector(i, (T *)cur_vec);
        float cur_norm = dist_fast->norm((T *)cur_vec, (uint32_t)_data_store->get_aligned_dim());
        std::memcpy(cur_node_offset, &cur_norm, sizeof(float));
        std::memcpy(cur_node_offset + sizeof(float), cur_vec, _data_len - sizeof(float));

        cur_node_offset += _data_len;
        uint32_t k = (uint32_t)_graph_store->get_neighbours(i).size();
        std::memcpy(cur_node_offset, &k, sizeof(uint32_t));
        std::memcpy(cur_node_offset + sizeof(uint32_t), _graph_store->get_neighbours(i).data(), k * sizeof(uint32_t));
        // std::vector<uint32_t>().swap(_graph_store->get_neighbours(i));
        _graph_store->clear_neighbours(i);
    }
    _graph_store->clear_graph();
    _graph_store->resize_graph(0);
    delete[] cur_vec;
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::_search_with_optimized_layout(const DataType &query, size_t K, size_t L, uint32_t *indices)
{
    try
    {
        return this->search_with_optimized_layout(std::any_cast<const T *>(query), K, L, indices);
    }
    catch (const std::bad_any_cast &e)
    {
        throw ANNException("Error: bad any cast while performing "
                           "_search_with_optimized_layout() " +
                               std::string(e.what()),
                           -1);
    }
    catch (const std::exception &e)
    {
        throw ANNException("Error: " + std::string(e.what()), -1);
    }
}

template <typename T, typename TagT, typename LabelT>
void Index<T, TagT, LabelT>::search_with_optimized_layout(const T *query, size_t K, size_t L, uint32_t *indices)
{
    DistanceFastL2<T> *dist_fast = (DistanceFastL2<T> *)(_data_store->get_dist_fn());

    NeighborPriorityQueue retset(L);
    std::vector<uint32_t> init_ids(L);

    boost::dynamic_bitset<> flags{_nd, 0};
    uint32_t tmp_l = 0;
    uint32_t *neighbors = (uint32_t *)(_opt_graph + _node_size * _start + _data_len);
    uint32_t MaxM_ep = *neighbors;
    neighbors++;

    for (; tmp_l < L && tmp_l < MaxM_ep; tmp_l++)
    {
        init_ids[tmp_l] = neighbors[tmp_l];
        flags[init_ids[tmp_l]] = true;
    }

    while (tmp_l < L)
    {
        uint32_t id = rand() % _nd;
        if (flags[id])
            continue;
        flags[id] = true;
        init_ids[tmp_l] = id;
        tmp_l++;
    }

    for (uint32_t i = 0; i < init_ids.size(); i++)
    {
        uint32_t id = init_ids[i];
        if (id >= _nd)
            continue;
        _mm_prefetch(_opt_graph + _node_size * id, _MM_HINT_T0);
    }
    L = 0;
    for (uint32_t i = 0; i < init_ids.size(); i++)
    {
        uint32_t id = init_ids[i];
        if (id >= _nd)
            continue;
        T *x = (T *)(_opt_graph + _node_size * id);
        float norm_x = *x;
        x++;
        float dist = dist_fast->compare(x, query, norm_x, (uint32_t)_data_store->get_aligned_dim());
        retset.insert(Neighbor(id, dist));
        flags[id] = true;
        L++;
    }

    while (retset.has_unexpanded_node())
    {
        auto nbr = retset.closest_unexpanded();
        auto n = nbr.id;
        _mm_prefetch(_opt_graph + _node_size * n + _data_len, _MM_HINT_T0);
        neighbors = (uint32_t *)(_opt_graph + _node_size * n + _data_len);
        uint32_t MaxM = *neighbors;
        neighbors++;
        for (uint32_t m = 0; m < MaxM; ++m)
            _mm_prefetch(_opt_graph + _node_size * neighbors[m], _MM_HINT_T0);
        for (uint32_t m = 0; m < MaxM; ++m)
        {
            uint32_t id = neighbors[m];
            if (flags[id])
                continue;
            flags[id] = 1;
            T *data = (T *)(_opt_graph + _node_size * id);
            float norm = *data;
            data++;
            float dist = dist_fast->compare(query, data, norm, (uint32_t)_data_store->get_aligned_dim());
            Neighbor nn(id, dist);
            retset.insert(nn);
        }
    }

    for (size_t i = 0; i < K; i++)
    {
        indices[i] = retset[i].id;
    }
}

/*  Internals of the library */
template <typename T, typename TagT, typename LabelT> const float Index<T, TagT, LabelT>::INDEX_GROWTH_FACTOR = 1.5f;

// EXPORTS
template DISKANN_DLLEXPORT class Index<float, int32_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<int8_t, int32_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, int32_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<float, uint32_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<int8_t, uint32_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, uint32_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<float, int64_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<int8_t, int64_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, int64_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<float, uint64_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<int8_t, uint64_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, uint64_t, uint32_t>;
template DISKANN_DLLEXPORT class Index<float, tag_uint128, uint32_t>;
template DISKANN_DLLEXPORT class Index<int8_t, tag_uint128, uint32_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, tag_uint128, uint32_t>;
// Label with short int 2 byte
template DISKANN_DLLEXPORT class Index<float, int32_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<int8_t, int32_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, int32_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<float, uint32_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<int8_t, uint32_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, uint32_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<float, int64_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<int8_t, int64_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, int64_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<float, uint64_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<int8_t, uint64_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, uint64_t, uint16_t>;
template DISKANN_DLLEXPORT class Index<float, tag_uint128, uint16_t>;
template DISKANN_DLLEXPORT class Index<int8_t, tag_uint128, uint16_t>;
template DISKANN_DLLEXPORT class Index<uint8_t, tag_uint128, uint16_t>;

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint32_t>::search<uint64_t>(
    const float *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint32_t>::search<uint32_t>(
    const float *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint32_t>::search<uint64_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint32_t>::search<uint32_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint32_t>::search<uint64_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint32_t>::search<uint32_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
// TagT==uint32_t
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint32_t>::search<uint64_t>(
    const float *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint32_t>::search<uint32_t>(
    const float *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint32_t>::search<uint64_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint32_t>::search<uint32_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint32_t>::search<uint64_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint32_t>::search<uint32_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint32_t>::search_with_filters<
    uint64_t>(const float *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint32_t>::search_with_filters<
    uint32_t>(const float *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint32_t>::search_with_filters<
    uint64_t>(const uint8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint32_t>::search_with_filters<
    uint32_t>(const uint8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint32_t>::search_with_filters<
    uint64_t>(const int8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint32_t>::search_with_filters<
    uint32_t>(const int8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
// TagT==uint32_t
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint32_t>::search_with_filters<
    uint64_t>(const float *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint32_t>::search_with_filters<
    uint32_t>(const float *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint32_t>::search_with_filters<
    uint64_t>(const uint8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint32_t>::search_with_filters<
    uint32_t>(const uint8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint32_t>::search_with_filters<
    uint64_t>(const int8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint32_t>::search_with_filters<
    uint32_t>(const int8_t *query, const uint32_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint16_t>::search<uint64_t>(
    const float *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint16_t>::search<uint32_t>(
    const float *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint16_t>::search<uint64_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint16_t>::search<uint32_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint16_t>::search<uint64_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint16_t>::search<uint32_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
// TagT==uint32_t
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint16_t>::search<uint64_t>(
    const float *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint16_t>::search<uint32_t>(
    const float *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint16_t>::search<uint64_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint16_t>::search<uint32_t>(
    const uint8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint16_t>::search<uint64_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint64_t *indices, float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint16_t>::search<uint32_t>(
    const int8_t *query, const size_t K, const uint32_t L, uint32_t *indices, float *distances);

template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint16_t>::search_with_filters<
    uint64_t>(const float *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint64_t, uint16_t>::search_with_filters<
    uint32_t>(const float *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint16_t>::search_with_filters<
    uint64_t>(const uint8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint64_t, uint16_t>::search_with_filters<
    uint32_t>(const uint8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint16_t>::search_with_filters<
    uint64_t>(const int8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint64_t, uint16_t>::search_with_filters<
    uint32_t>(const int8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
// TagT==uint32_t
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint16_t>::search_with_filters<
    uint64_t>(const float *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<float, uint32_t, uint16_t>::search_with_filters<
    uint32_t>(const float *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint16_t>::search_with_filters<
    uint64_t>(const uint8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<uint8_t, uint32_t, uint16_t>::search_with_filters<
    uint32_t>(const uint8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint16_t>::search_with_filters<
    uint64_t>(const int8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint64_t *indices,
              float *distances);
template DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> Index<int8_t, uint32_t, uint16_t>::search_with_filters<
    uint32_t>(const int8_t *query, const uint16_t &filter_label, const size_t K, const uint32_t L, uint32_t *indices,
              float *distances);

} // namespace diskann
