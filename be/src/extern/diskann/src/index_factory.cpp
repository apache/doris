#include "index_factory.h"
#include "pq_l2_distance.h"

namespace diskann
{

IndexFactory::IndexFactory(const IndexConfig &config) : _config(std::make_unique<IndexConfig>(config))
{
    check_config();
}

std::unique_ptr<AbstractIndex> IndexFactory::create_instance()
{
    return create_instance(_config->data_type, _config->tag_type, _config->label_type);
}

void IndexFactory::check_config()
{
    if (_config->dynamic_index && !_config->enable_tags)
    {
        throw ANNException("ERROR: Dynamic Indexing must have tags enabled.", -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (_config->pq_dist_build)
    {
        if (_config->dynamic_index)
            throw ANNException("ERROR: Dynamic Indexing not supported with PQ distance based "
                               "index construction",
                               -1, __FUNCSIG__, __FILE__, __LINE__);
        if (_config->metric == diskann::Metric::INNER_PRODUCT)
            throw ANNException("ERROR: Inner product metrics not yet supported "
                               "with PQ distance "
                               "base index",
                               -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    if (_config->data_type != "float" && _config->data_type != "uint8" && _config->data_type != "int8")
    {
        throw ANNException("ERROR: invalid data type : + " + _config->data_type +
                               " is not supported. please select from [float, int8, uint8]",
                           -1);
    }

    if (_config->tag_type != "int32" && _config->tag_type != "uint32" && _config->tag_type != "int64" &&
        _config->tag_type != "uint64")
    {
        throw ANNException("ERROR: invalid data type : + " + _config->tag_type +
                               " is not supported. please select from [int32, uint32, int64, uint64]",
                           -1);
    }
}

template <typename T> Distance<T> *IndexFactory::construct_inmem_distance_fn(Metric metric)
{
    if (metric == diskann::Metric::COSINE && std::is_same<T, float>::value)
    {
        return (Distance<T> *)new AVXNormalizedCosineDistanceFloat();
    }
    else
    {
        return (Distance<T> *)get_distance_function<T>(metric);
    }
}

template <typename T>
std::shared_ptr<AbstractDataStore<T>> IndexFactory::construct_datastore(DataStoreStrategy strategy,
                                                                        size_t total_internal_points, size_t dimension,
                                                                        Metric metric)
{
    std::unique_ptr<Distance<T>> distance;
    switch (strategy)
    {
    case DataStoreStrategy::MEMORY:
        distance.reset(construct_inmem_distance_fn<T>(metric));
        return std::make_shared<diskann::InMemDataStore<T>>((location_t)total_internal_points, dimension,
                                                            std::move(distance));
    default:
        break;
    }
    return nullptr;
}

std::unique_ptr<AbstractGraphStore> IndexFactory::construct_graphstore(const GraphStoreStrategy strategy,
                                                                       const size_t size,
                                                                       const size_t reserve_graph_degree)
{
    switch (strategy)
    {
    case GraphStoreStrategy::MEMORY:
        return std::make_unique<InMemGraphStore>(size, reserve_graph_degree);
    default:
        throw ANNException("Error : Current GraphStoreStratagy is not supported.", -1);
    }
}

template <typename T>
std::shared_ptr<PQDataStore<T>> IndexFactory::construct_pq_datastore(DataStoreStrategy strategy, size_t num_points,
                                                                     size_t dimension, Metric m, size_t num_pq_chunks,
                                                                     bool use_opq)
{
    std::unique_ptr<Distance<T>> distance_fn;
    std::unique_ptr<QuantizedDistance<T>> quantized_distance_fn;

    quantized_distance_fn = std::move(std::make_unique<PQL2Distance<T>>((uint32_t)num_pq_chunks, use_opq));
    switch (strategy)
    {
    case DataStoreStrategy::MEMORY:
        distance_fn.reset(construct_inmem_distance_fn<T>(m));
        return std::make_shared<diskann::PQDataStore<T>>(dimension, (location_t)(num_points), num_pq_chunks,
                                                         std::move(distance_fn), std::move(quantized_distance_fn));
    default:
        // REFACTOR TODO: We do support diskPQ - so we may need to add a new class for SSDPQDataStore!
        break;
    }
    return nullptr;
}

template <typename data_type, typename tag_type, typename label_type>
std::unique_ptr<AbstractIndex> IndexFactory::create_instance()
{
    size_t num_points = _config->max_points + _config->num_frozen_pts;
    size_t dim = _config->dimension;
    // auto graph_store = construct_graphstore(_config->graph_strategy, num_points);
    auto data_store = construct_datastore<data_type>(_config->data_strategy, num_points, dim, _config->metric);
    std::shared_ptr<AbstractDataStore<data_type>> pq_data_store = nullptr;

    if (_config->data_strategy == DataStoreStrategy::MEMORY && _config->pq_dist_build)
    {
        pq_data_store =
            construct_pq_datastore<data_type>(_config->data_strategy, num_points + _config->num_frozen_pts, dim,
                                              _config->metric, _config->num_pq_chunks, _config->use_opq);
    }
    else
    {
        pq_data_store = data_store;
    }
    size_t max_reserve_degree =
        (size_t)(defaults::GRAPH_SLACK_FACTOR * 1.05 *
                 (_config->index_write_params == nullptr ? 0 : _config->index_write_params->max_degree));
    std::unique_ptr<AbstractGraphStore> graph_store =
        construct_graphstore(_config->graph_strategy, num_points + _config->num_frozen_pts, max_reserve_degree);

    // REFACTOR TODO: Must construct in-memory PQDatastore if strategy == ONDISK and must construct
    // in-mem and on-disk PQDataStore if strategy == ONDISK and diskPQ is required.
    return std::make_unique<diskann::Index<data_type, tag_type, label_type>>(*_config, data_store,
                                                                             std::move(graph_store), pq_data_store);
}

std::unique_ptr<AbstractIndex> IndexFactory::create_instance(const std::string &data_type, const std::string &tag_type,
                                                             const std::string &label_type)
{
    if (data_type == std::string("float"))
    {
        return create_instance<float>(tag_type, label_type);
    }
    else if (data_type == std::string("uint8"))
    {
        return create_instance<uint8_t>(tag_type, label_type);
    }
    else if (data_type == std::string("int8"))
    {
        return create_instance<int8_t>(tag_type, label_type);
    }
    else
        throw ANNException("Error: unsupported data_type please choose from [float/int8/uint8]", -1);
}

template <typename data_type>
std::unique_ptr<AbstractIndex> IndexFactory::create_instance(const std::string &tag_type, const std::string &label_type)
{
    if (tag_type == std::string("int32"))
    {
        return create_instance<data_type, int32_t>(label_type);
    }
    else if (tag_type == std::string("uint32"))
    {
        return create_instance<data_type, uint32_t>(label_type);
    }
    else if (tag_type == std::string("int64"))
    {
        return create_instance<data_type, int64_t>(label_type);
    }
    else if (tag_type == std::string("uint64"))
    {
        return create_instance<data_type, uint64_t>(label_type);
    }
    else
        throw ANNException("Error: unsupported tag_type please choose from [int32/uint32/int64/uint64]", -1);
}

template <typename data_type, typename tag_type>
std::unique_ptr<AbstractIndex> IndexFactory::create_instance(const std::string &label_type)
{
    if (label_type == std::string("uint16") || label_type == std::string("ushort"))
    {
        return create_instance<data_type, tag_type, uint16_t>();
    }
    else if (label_type == std::string("uint32") || label_type == std::string("uint"))
    {
        return create_instance<data_type, tag_type, uint32_t>();
    }
    else
        throw ANNException("Error: unsupported label_type please choose from [uint/ushort]", -1);
}

// template DISKANN_DLLEXPORT std::shared_ptr<AbstractDataStore<uint8_t>> IndexFactory::construct_datastore(
//     DataStoreStrategy stratagy, size_t num_points, size_t dimension, Metric m);
// template DISKANN_DLLEXPORT std::shared_ptr<AbstractDataStore<int8_t>> IndexFactory::construct_datastore(
//     DataStoreStrategy stratagy, size_t num_points, size_t dimension, Metric m);
// template DISKANN_DLLEXPORT std::shared_ptr<AbstractDataStore<float>> IndexFactory::construct_datastore(
//     DataStoreStrategy stratagy, size_t num_points, size_t dimension, Metric m);

} // namespace diskann
