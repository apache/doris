// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "common_includes.h"

#ifdef EXEC_ENV_OLS
#include "aligned_file_reader.h"
#endif

#include "distance.h"
#include "locking.h"
#include "natural_number_map.h"
#include "natural_number_set.h"
#include "neighbor.h"
#include "parameters.h"
#include "utils.h"
#include "windows_customizations.h"
#include "scratch.h"
#include "in_mem_data_store.h"
#include "in_mem_graph_store.h"
#include "abstract_index.h"

#include "quantized_distance.h"
#include "pq_data_store.h"

#define OVERHEAD_FACTOR 1.1
#define EXPAND_IF_FULL 0
#define DEFAULT_MAXC 750

namespace diskann
{

inline double estimate_ram_usage(size_t size, uint32_t dim, uint32_t datasize, uint32_t degree)
{
    double size_of_data = ((double)size) * ROUND_UP(dim, 8) * datasize;
    double size_of_graph = ((double)size) * degree * sizeof(uint32_t) * defaults::GRAPH_SLACK_FACTOR;
    double size_of_locks = ((double)size) * sizeof(non_recursive_mutex);
    double size_of_outer_vector = ((double)size) * sizeof(ptrdiff_t);

    return OVERHEAD_FACTOR * (size_of_data + size_of_graph + size_of_locks + size_of_outer_vector);
}

template <typename T, typename TagT = uint32_t, typename LabelT = uint32_t> class Index : public AbstractIndex
{
    /**************************************************************************
     *
     * Public functions acquire one or more of _update_lock, _consolidate_lock,
     * _tag_lock, _delete_lock before calling protected functions which DO NOT
     * acquire these locks. They might acquire locks on _locks[i]
     *
     **************************************************************************/

  public:
    // Constructor for Bulk operations and for creating the index object solely
    // for loading a prexisting index.
    DISKANN_DLLEXPORT Index(const IndexConfig &index_config, std::shared_ptr<AbstractDataStore<T>> data_store,
                            std::unique_ptr<AbstractGraphStore> graph_store,
                            std::shared_ptr<AbstractDataStore<T>> pq_data_store = nullptr);

    // Constructor for incremental index
    DISKANN_DLLEXPORT Index(Metric m, const size_t dim, const size_t max_points,
                            const std::shared_ptr<IndexWriteParameters> index_parameters,
                            const std::shared_ptr<IndexSearchParams> index_search_params,
                            const size_t num_frozen_pts = 0, const bool dynamic_index = false,
                            const bool enable_tags = false, const bool concurrent_consolidate = false,
                            const bool pq_dist_build = false, const size_t num_pq_chunks = 0,
                            const bool use_opq = false, const bool filtered_index = false);

    DISKANN_DLLEXPORT ~Index();

    // Saves graph, data, metadata and associated tags.
    DISKANN_DLLEXPORT void save(const char *filename, bool compact_before_save = false);
    DISKANN_DLLEXPORT void save(std::stringstream &mem_index_stream, bool compact_before_save = false);

    // Load functions
#ifdef EXEC_ENV_OLS
    DISKANN_DLLEXPORT void load(AlignedFileReader &reader, uint32_t num_threads, uint32_t search_l);
#else
    // Reads the number of frozen points from graph's metadata file section.
    DISKANN_DLLEXPORT static size_t get_graph_num_frozen_points(const std::string &graph_file);

    DISKANN_DLLEXPORT void load(const char *index_file, uint32_t num_threads, uint32_t search_l);
#endif

    // get some private variables
    DISKANN_DLLEXPORT size_t get_num_points();
    DISKANN_DLLEXPORT size_t get_max_points();

    DISKANN_DLLEXPORT bool detect_common_filters(uint32_t point_id, bool search_invocation,
                                                 const std::vector<LabelT> &incoming_labels);

    // Batch build from a file. Optionally pass tags vector.
    DISKANN_DLLEXPORT void build(const char *filename, const size_t num_points_to_load,
                                 const std::vector<TagT> &tags = std::vector<TagT>());

    // Batch build from a file. Optionally pass tags file.
    DISKANN_DLLEXPORT void build(const char *filename, const size_t num_points_to_load, const char *tag_filename);

    // Batch build from a data array, which must pad vectors to aligned_dim
    DISKANN_DLLEXPORT void build(const T *data, const size_t num_points_to_load, const std::vector<TagT> &tags);

    // Based on filter params builds a filtered or unfiltered index
    DISKANN_DLLEXPORT void build(const std::string &data_file, const size_t num_points_to_load,
                                 IndexFilterParams &filter_params);

    // Filtered Support
    DISKANN_DLLEXPORT void build_filtered_index(const char *filename, const std::string &label_file,
                                                const size_t num_points_to_load,
                                                const std::vector<TagT> &tags = std::vector<TagT>());

    DISKANN_DLLEXPORT void set_universal_label(const LabelT &label);

    // Get converted integer label from string to int map (_label_map)
    DISKANN_DLLEXPORT LabelT get_converted_label(const std::string &raw_label);

    // Set starting point of an index before inserting any points incrementally.
    // The data count should be equal to _num_frozen_pts * _aligned_dim.
    DISKANN_DLLEXPORT void set_start_points(const T *data, size_t data_count);
    // Set starting points to random points on a sphere of certain radius.
    // A fixed random seed can be specified for scenarios where it's important
    // to have higher consistency between index builds.
    DISKANN_DLLEXPORT void set_start_points_at_random(T radius, uint32_t random_seed = 0);

    // For FastL2 search on a static index, we interleave the data with graph
    DISKANN_DLLEXPORT void optimize_index_layout();

    // For FastL2 search on optimized layout
    DISKANN_DLLEXPORT void search_with_optimized_layout(const T *query, size_t K, size_t L, uint32_t *indices);

    // Added search overload that takes L as parameter, so that we
    // can customize L on a per-query basis without tampering with "Parameters"
    template <typename IDType>
    DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> search(const T *query, const size_t K, const uint32_t L,
                                                           IDType *indices, float *distances = nullptr);

    // Initialize space for res_vectors before calling.
    DISKANN_DLLEXPORT size_t search_with_tags(const T *query, const uint64_t K, const uint32_t L, TagT *tags,
                                              float *distances, std::vector<T *> &res_vectors, bool use_filters = false,
                                              const std::string filter_label = "");

    // Filter support search
    template <typename IndexType>
    DISKANN_DLLEXPORT std::pair<uint32_t, uint32_t> search_with_filters(const T *query, const LabelT &filter_label,
                                                                        const size_t K, const uint32_t L,
                                                                        IndexType *indices, float *distances);

    // Will fail if tag already in the index or if tag=0.
    DISKANN_DLLEXPORT int insert_point(const T *point, const TagT tag);

    // Will fail if tag already in the index or if tag=0.
    DISKANN_DLLEXPORT int insert_point(const T *point, const TagT tag, const std::vector<LabelT> &label);

    // call this before issuing deletions to sets relevant flags
    DISKANN_DLLEXPORT int enable_delete();

    // Record deleted point now and restructure graph later. Return -1 if tag
    // not found, 0 if OK.
    DISKANN_DLLEXPORT int lazy_delete(const TagT &tag);

    // Record deleted points now and restructure graph later. Add to failed_tags
    // if tag not found.
    DISKANN_DLLEXPORT void lazy_delete(const std::vector<TagT> &tags, std::vector<TagT> &failed_tags);

    // Call after a series of lazy deletions
    // Returns number of live points left after consolidation
    // If _conc_consolidates is set in the ctor, then this call can be invoked
    // alongside inserts and lazy deletes, else it acquires _update_lock
    DISKANN_DLLEXPORT consolidation_report consolidate_deletes(const IndexWriteParameters &parameters);

    DISKANN_DLLEXPORT void prune_all_neighbors(const uint32_t max_degree, const uint32_t max_occlusion,
                                               const float alpha);

    DISKANN_DLLEXPORT bool is_index_saved();

    // repositions frozen points to the end of _data - if they have been moved
    // during deletion
    DISKANN_DLLEXPORT void reposition_frozen_point_to_end();
    DISKANN_DLLEXPORT void reposition_points(uint32_t old_location_start, uint32_t new_location_start,
                                             uint32_t num_locations);

    // DISKANN_DLLEXPORT void save_index_as_one_file(bool flag);

    DISKANN_DLLEXPORT void get_active_tags(tsl::robin_set<TagT> &active_tags);

    // memory should be allocated for vec before calling this function
    DISKANN_DLLEXPORT int get_vector_by_tag(TagT &tag, T *vec);

    DISKANN_DLLEXPORT void print_status();

    DISKANN_DLLEXPORT void count_nodes_at_bfs_levels();

    // This variable MUST be updated if the number of entries in the metadata
    // change.
    DISKANN_DLLEXPORT static const int METADATA_ROWS = 5;

    // ********************************
    //
    // Internals of the library
    //
    // ********************************

  protected:
    // overload of abstract index virtual methods
    virtual void _build(const DataType &data, const size_t num_points_to_load, TagVector &tags) override;

    virtual std::pair<uint32_t, uint32_t> _search(const DataType &query, const size_t K, const uint32_t L,
                                                  std::any &indices, float *distances = nullptr) override;
    virtual std::pair<uint32_t, uint32_t> _search_with_filters(const DataType &query,
                                                               const std::string &filter_label_raw, const size_t K,
                                                               const uint32_t L, std::any &indices,
                                                               float *distances) override;

    virtual int _insert_point(const DataType &data_point, const TagType tag) override;
    virtual int _insert_point(const DataType &data_point, const TagType tag, Labelvector &labels) override;

    virtual int _lazy_delete(const TagType &tag) override;

    virtual void _lazy_delete(TagVector &tags, TagVector &failed_tags) override;

    virtual void _get_active_tags(TagRobinSet &active_tags) override;

    virtual void _set_start_points_at_random(DataType radius, uint32_t random_seed = 0) override;

    virtual int _get_vector_by_tag(TagType &tag, DataType &vec) override;

    virtual void _search_with_optimized_layout(const DataType &query, size_t K, size_t L, uint32_t *indices) override;

    virtual size_t _search_with_tags(const DataType &query, const uint64_t K, const uint32_t L, const TagType &tags,
                                     float *distances, DataVector &res_vectors, bool use_filters = false,
                                     const std::string filter_label = "") override;

    virtual void _set_universal_label(const LabelType universal_label) override;

    // No copy/assign.
    Index(const Index<T, TagT, LabelT> &) = delete;
    Index<T, TagT, LabelT> &operator=(const Index<T, TagT, LabelT> &) = delete;

    // Use after _data and _nd have been populated
    // Acquire exclusive _update_lock before calling
    void build_with_data_populated(const std::vector<TagT> &tags);

    // generates 1 frozen point that will never be deleted from the graph
    // This is not visible to the user
    void generate_frozen_point();

    // determines navigating node of the graph by calculating medoid of datafopt
    uint32_t calculate_entry_point();

    void parse_label_file(const std::string &label_file, size_t &num_pts_labels);

    std::unordered_map<std::string, LabelT> load_label_map(const std::string &map_file);

    // Returns the locations of start point and frozen points suitable for use
    // with iterate_to_fixed_point.
    std::vector<uint32_t> get_init_ids();

    // The query to use is placed in scratch->aligned_query
    std::pair<uint32_t, uint32_t> iterate_to_fixed_point(InMemQueryScratch<T> *scratch, const uint32_t Lindex,
                                                         const std::vector<uint32_t> &init_ids, bool use_filter,
                                                         const std::vector<LabelT> &filters, bool search_invocation);

    void search_for_point_and_prune(int location, uint32_t Lindex, std::vector<uint32_t> &pruned_list,
                                    InMemQueryScratch<T> *scratch, bool use_filter = false,
                                    uint32_t filteredLindex = 0);

    void prune_neighbors(const uint32_t location, std::vector<Neighbor> &pool, std::vector<uint32_t> &pruned_list,
                         InMemQueryScratch<T> *scratch);

    void prune_neighbors(const uint32_t location, std::vector<Neighbor> &pool, const uint32_t range,
                         const uint32_t max_candidate_size, const float alpha, std::vector<uint32_t> &pruned_list,
                         InMemQueryScratch<T> *scratch);

    // Prunes candidates in @pool to a shorter list @result
    // @pool must be sorted before calling
    void occlude_list(const uint32_t location, std::vector<Neighbor> &pool, const float alpha, const uint32_t degree,
                      const uint32_t maxc, std::vector<uint32_t> &result, InMemQueryScratch<T> *scratch,
                      const tsl::robin_set<uint32_t> *const delete_set_ptr = nullptr);

    // add reverse links from all the visited nodes to node n.
    void inter_insert(uint32_t n, std::vector<uint32_t> &pruned_list, const uint32_t range,
                      InMemQueryScratch<T> *scratch);

    void inter_insert(uint32_t n, std::vector<uint32_t> &pruned_list, InMemQueryScratch<T> *scratch);

    // Acquire exclusive _update_lock before calling
    void link();

    // Acquire exclusive _tag_lock and _delete_lock before calling
    int reserve_location();

    // Acquire exclusive _tag_lock before calling
    size_t release_location(int location);
    size_t release_locations(const tsl::robin_set<uint32_t> &locations);

    // Resize the index when no slots are left for insertion.
    // Acquire exclusive _update_lock and _tag_lock before calling.
    void resize(size_t new_max_points);

    // Acquire unique lock on _update_lock, _consolidate_lock, _tag_lock
    // and _delete_lock before calling these functions.
    // Renumber nodes, update tag and location maps and compact the
    // graph, mode = _consolidated_order in case of lazy deletion and
    // _compacted_order in case of eager deletion
    DISKANN_DLLEXPORT void compact_data();
    DISKANN_DLLEXPORT void compact_frozen_point();

    // Remove deleted nodes from adjacency list of node loc
    // Replace removed neighbors with second order neighbors.
    // Also acquires _locks[i] for i = loc and out-neighbors of loc.
    void process_delete(const tsl::robin_set<uint32_t> &old_delete_set, size_t loc, const uint32_t range,
                        const uint32_t maxc, const float alpha, InMemQueryScratch<T> *scratch);

    void initialize_query_scratch(uint32_t num_threads, uint32_t search_l, uint32_t indexing_l, uint32_t r,
                                  uint32_t maxc, size_t dim);

    // Do not call without acquiring appropriate locks
    // call public member functions save and load to invoke these.
    DISKANN_DLLEXPORT size_t save_graph(std::string filename);
    DISKANN_DLLEXPORT size_t save_graph(std::stringstream &index_stream);
    DISKANN_DLLEXPORT size_t save_data(std::string filename);
    DISKANN_DLLEXPORT size_t save_tags(std::string filename);
    DISKANN_DLLEXPORT size_t save_delete_list(const std::string &filename);
    DISKANN_DLLEXPORT size_t load_graph(const std::string filename, size_t expected_num_points);
    DISKANN_DLLEXPORT size_t load_data(std::string filename0);
    DISKANN_DLLEXPORT size_t load_tags(const std::string tag_file_name);
    DISKANN_DLLEXPORT size_t load_delete_set(const std::string &filename);
  private:
    // Distance functions
    Metric _dist_metric = diskann::L2;

    // Data
    std::shared_ptr<AbstractDataStore<T>> _data_store;

    // Graph related data structures
    std::unique_ptr<AbstractGraphStore> _graph_store;

    char *_opt_graph = nullptr;

    // Dimensions
    size_t _dim = 0;
    size_t _nd = 0;         // number of active points i.e. existing in the graph
    size_t _max_points = 0; // total number of points in given data set

    // _num_frozen_pts is the number of points which are used as initial
    // candidates when iterating to closest point(s). These are not visible
    // externally and won't be returned by search. At least 1 frozen point is
    // needed for a dynamic index. The frozen points have consecutive locations.
    // See also _start below.
    size_t _num_frozen_pts = 0;
    size_t _frozen_pts_used = 0;
    size_t _node_size;
    size_t _data_len;
    size_t _neighbor_len;

    //  Start point of the search. When _num_frozen_pts is greater than zero,
    //  this is the location of the first frozen point. Otherwise, this is a
    //  location of one of the points in index.
    uint32_t _start = 0;

    bool _has_built = false;
    bool _saturate_graph = false;
    bool _save_as_one_file = false; // plan to support in next version
    bool _dynamic_index = false;
    bool _enable_tags = false;
    bool _normalize_vecs = false; // Using normalied L2 for cosine.
    bool _deletes_enabled = false;

    // Filter Support

    bool _filtered_index = false;
    // Location to label is only updated during insert_point(), all other reads are protected by
    // default as a location can only be released at end of consolidate deletes
    std::vector<std::vector<LabelT>> _location_to_labels;
    tsl::robin_set<LabelT> _labels;
    std::string _labels_file;
    std::unordered_map<LabelT, uint32_t> _label_to_start_id;
    std::unordered_map<uint32_t, uint32_t> _medoid_counts;

    bool _use_universal_label = false;
    LabelT _universal_label = 0;
    uint32_t _filterIndexingQueueSize;
    std::unordered_map<std::string, LabelT> _label_map;

    // Indexing parameters
    uint32_t _indexingQueueSize;
    uint32_t _indexingRange;
    uint32_t _indexingMaxC;
    float _indexingAlpha;
    uint32_t _indexingThreads;

    // Query scratch data structures
    ConcurrentQueue<InMemQueryScratch<T> *> _query_scratch;

    // Flags for PQ based distance calculation
    bool _pq_dist = false;
    bool _use_opq = false;
    size_t _num_pq_chunks = 0;
    // REFACTOR
    // uint8_t *_pq_data = nullptr;
    std::shared_ptr<QuantizedDistance<T>> _pq_distance_fn = nullptr;
    std::shared_ptr<AbstractDataStore<T>> _pq_data_store = nullptr;
    bool _pq_generated = false;
    FixedChunkPQTable _pq_table;

    //
    // Data structures, locks and flags for dynamic indexing and tags
    //

    // lazy_delete removes entry from _location_to_tag and _tag_to_location. If
    // _location_to_tag does not resolve a location, infer that it was deleted.
    tsl::sparse_map<TagT, uint32_t> _tag_to_location;
    natural_number_map<uint32_t, TagT> _location_to_tag;

    // _empty_slots has unallocated slots and those freed by consolidate_delete.
    // _delete_set has locations marked deleted by lazy_delete. Will not be
    // immediately available for insert. consolidate_delete will release these
    // slots to _empty_slots.
    natural_number_set<uint32_t> _empty_slots;
    std::unique_ptr<tsl::robin_set<uint32_t>> _delete_set;

    bool _data_compacted = true;    // true if data has been compacted
    bool _is_saved = false;         // Checking if the index is already saved.
    bool _conc_consolidate = false; // use _lock while searching

    // Acquire locks in the order below when acquiring multiple locks
    std::shared_timed_mutex // RW mutex between save/load (exclusive lock) and
        _update_lock;       // search/inserts/deletes/consolidate (shared lock)
    std::shared_timed_mutex // Ensure only one consolidate or compact_data is
        _consolidate_lock;  // ever active
    std::shared_timed_mutex // RW lock for _tag_to_location,
        _tag_lock;          // _location_to_tag, _empty_slots, _nd, _max_points, _label_to_start_id
    std::shared_timed_mutex // RW Lock on _delete_set and _data_compacted
        _delete_lock;       // variable

    // Per node lock, cardinality=_max_points + _num_frozen_points
    std::vector<non_recursive_mutex> _locks;

    static const float INDEX_GROWTH_FACTOR;
};
} // namespace diskann
