#include <exception>

#include "pq_data_store.h"
#include "pq.h"
#include "pq_scratch.h"
#include "utils.h"
#include "distance.h"

namespace diskann
{

// REFACTOR TODO: Assuming that num_pq_chunks is known already. Must verify if
// this is true.
template <typename data_t>
PQDataStore<data_t>::PQDataStore(size_t dim, location_t num_points, size_t num_pq_chunks,
                                 std::unique_ptr<Distance<data_t>> distance_fn,
                                 std::unique_ptr<QuantizedDistance<data_t>> pq_distance_fn)
    : AbstractDataStore<data_t>(num_points, dim), _quantized_data(nullptr), _num_chunks(num_pq_chunks),
      _distance_metric(distance_fn->get_metric())
{
    if (num_pq_chunks > dim)
    {
        throw diskann::ANNException("ERROR: num_pq_chunks > dim", -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    _distance_fn = std::move(distance_fn);
    _pq_distance_fn = std::move(pq_distance_fn);
}

template <typename data_t> PQDataStore<data_t>::~PQDataStore()
{
    if (_quantized_data != nullptr)
    {
        aligned_free(_quantized_data);
        _quantized_data = nullptr;
    }
}

template <typename data_t> location_t PQDataStore<data_t>::load(const std::string &filename)
{
    return load_impl(filename);
}
template <typename data_t> size_t PQDataStore<data_t>::save(const std::string &filename, const location_t num_points)
{
    return diskann::save_bin(filename, _quantized_data, this->capacity(), _num_chunks, 0);
}

template <typename data_t> size_t PQDataStore<data_t>::get_aligned_dim() const
{
    return this->get_dims();
}

// Populate quantized data from regular data.
template <typename data_t> void PQDataStore<data_t>::populate_data(const data_t *vectors, const location_t num_pts)
{
    throw std::logic_error("Not implemented yet");
}

template <typename data_t> void PQDataStore<data_t>::populate_data(const std::string &filename, const size_t offset)
{
    if (_quantized_data != nullptr)
    {
        aligned_free(_quantized_data);
    }

    uint64_t file_num_points = 0, file_dim = 0;
    get_bin_metadata(filename, file_num_points, file_dim, offset);
    this->_capacity = (location_t)file_num_points;
    this->_dim = file_dim;

    double p_val = std::min(1.0, ((double)MAX_PQ_TRAINING_SET_SIZE / (double)file_num_points));

    auto pivots_file = _pq_distance_fn->get_pivot_data_filename(filename);
    auto compressed_file = _pq_distance_fn->get_quantized_vectors_filename(filename);

    generate_quantized_data<data_t>(filename, pivots_file, compressed_file, _distance_metric, p_val, _num_chunks,
                                    _pq_distance_fn->is_opq());

    // REFACTOR TODO: Not sure of the alignment. Just copying from index.cpp
    alloc_aligned(((void **)&_quantized_data), file_num_points * _num_chunks * sizeof(uint8_t), 1);
    copy_aligned_data_from_file<uint8_t>(compressed_file.c_str(), _quantized_data, file_num_points, _num_chunks,
                                         _num_chunks);
#ifdef EXEC_ENV_OLS
    throw ANNException("load_pq_centroid_bin should not be called when "
                       "EXEC_ENV_OLS is defined.",
                       -1, __FUNCSIG__, __FILE__, __LINE__);
#else
    _pq_distance_fn->load_pivot_data(pivots_file.c_str(), _num_chunks);
#endif
}

template <typename data_t>
void PQDataStore<data_t>::extract_data_to_bin(const std::string &filename, const location_t num_pts)
{
    throw std::logic_error("Not implemented yet");
}

template <typename data_t> void PQDataStore<data_t>::get_vector(const location_t i, data_t *target) const
{
    // REFACTOR TODO: Should we inflate the compressed vector here?
    if (i < this->capacity())
    {
        throw std::logic_error("Not implemented yet.");
    }
    else
    {
        std::stringstream ss;
        ss << "Requested vector " << i << " but only  " << this->capacity() << " vectors are present";
        throw diskann::ANNException(ss.str(), -1);
    }
}
template <typename data_t> void PQDataStore<data_t>::set_vector(const location_t i, const data_t *const vector)
{
    // REFACTOR TODO: Should we accept a normal vector and compress here?
    // memcpy (_data + i * _num_chunks, vector, _num_chunks * sizeof(data_t));
    throw std::logic_error("Not implemented yet");
}

template <typename data_t> void PQDataStore<data_t>::prefetch_vector(const location_t loc)
{
    const uint8_t *ptr = _quantized_data + ((size_t)loc) * _num_chunks * sizeof(data_t);
    diskann::prefetch_vector((const char *)ptr, _num_chunks * sizeof(data_t));
}

template <typename data_t>
void PQDataStore<data_t>::move_vectors(const location_t old_location_start, const location_t new_location_start,
                                       const location_t num_points)
{
    // REFACTOR TODO: Moving vectors is only for in-mem fresh.
    throw std::logic_error("Not implemented yet");
}

template <typename data_t>
void PQDataStore<data_t>::copy_vectors(const location_t from_loc, const location_t to_loc, const location_t num_points)
{
    // REFACTOR TODO: Is the number of bytes correct?
    memcpy(_quantized_data + to_loc * _num_chunks, _quantized_data + from_loc * _num_chunks, _num_chunks * num_points);
}

// REFACTOR TODO: Currently, we take aligned_query as parameter, but this
// function should also do the alignment.
template <typename data_t>
void PQDataStore<data_t>::preprocess_query(const data_t *aligned_query, AbstractScratch<data_t> *scratch) const
{
    if (scratch == nullptr)
    {
        throw diskann::ANNException("Scratch space is null", -1);
    }

    PQScratch<data_t> *pq_scratch = scratch->pq_scratch();

    if (pq_scratch == nullptr)
    {
        throw diskann::ANNException("PQScratch space has not been set in the scratch object.", -1);
    }

    _pq_distance_fn->preprocess_query(aligned_query, (location_t)this->get_dims(), *pq_scratch);
}

template <typename data_t> float PQDataStore<data_t>::get_distance(const data_t *query, const location_t loc) const
{
    throw std::logic_error("Not implemented yet");
}

template <typename data_t> float PQDataStore<data_t>::get_distance(const location_t loc1, const location_t loc2) const
{
    throw std::logic_error("Not implemented yet");
}

template <typename data_t>
void PQDataStore<data_t>::get_distance(const data_t *preprocessed_query, const location_t *locations,
                                       const uint32_t location_count, float *distances,
                                       AbstractScratch<data_t> *scratch_space) const
{
    if (scratch_space == nullptr)
    {
        throw diskann::ANNException("Scratch space is null", -1);
    }
    PQScratch<data_t> *pq_scratch = scratch_space->pq_scratch();
    if (pq_scratch == nullptr)
    {
        throw diskann::ANNException("PQScratch not set in scratch space.", -1);
    }
    diskann::aggregate_coords(locations, location_count, _quantized_data, this->_num_chunks,
                              pq_scratch->aligned_pq_coord_scratch);
    _pq_distance_fn->preprocessed_distance(*pq_scratch, location_count, distances);
}

template <typename data_t>
void PQDataStore<data_t>::get_distance(const data_t *preprocessed_query, const std::vector<location_t> &ids,
                                       std::vector<float> &distances, AbstractScratch<data_t> *scratch_space) const
{
    if (scratch_space == nullptr)
    {
        throw diskann::ANNException("Scratch space is null", -1);
    }
    PQScratch<data_t> *pq_scratch = scratch_space->pq_scratch();
    if (pq_scratch == nullptr)
    {
        throw diskann::ANNException("PQScratch not set in scratch space.", -1);
    }
    diskann::aggregate_coords(ids, _quantized_data, this->_num_chunks, pq_scratch->aligned_pq_coord_scratch);
    _pq_distance_fn->preprocessed_distance(*pq_scratch, (location_t)ids.size(), distances);
}

template <typename data_t> location_t PQDataStore<data_t>::calculate_medoid() const
{
    // REFACTOR TODO: Must calculate this just like we do with data store.
    size_t r = (size_t)rand() * (size_t)RAND_MAX + (size_t)rand();
    return (uint32_t)(r % (size_t)this->capacity());
}

template <typename data_t> size_t PQDataStore<data_t>::get_alignment_factor() const
{
    return 1;
}

template <typename data_t> Distance<data_t> *PQDataStore<data_t>::get_dist_fn() const
{
    return _distance_fn.get();
}

template <typename data_t> location_t PQDataStore<data_t>::load_impl(const std::string &file_prefix)
{
    if (_quantized_data != nullptr)
    {
        aligned_free(_quantized_data);
    }
    auto quantized_vectors_file = _pq_distance_fn->get_quantized_vectors_filename(file_prefix);

    size_t num_points;
    load_aligned_bin(quantized_vectors_file, _quantized_data, num_points, _num_chunks, _num_chunks);
    this->_capacity = (location_t)num_points;

    auto pivots_file = _pq_distance_fn->get_pivot_data_filename(file_prefix);
    _pq_distance_fn->load_pivot_data(pivots_file, _num_chunks);

    return this->_capacity;
}

template <typename data_t> location_t PQDataStore<data_t>::expand(const location_t new_size)
{
    throw std::logic_error("Not implemented yet");
}

template <typename data_t> location_t PQDataStore<data_t>::shrink(const location_t new_size)
{
    throw std::logic_error("Not implemented yet");
}

#ifdef EXEC_ENV_OLS
template <typename data_t> location_t PQDataStore<data_t>::load_impl(AlignedFileReader &reader)
{
}
#endif

template DISKANN_DLLEXPORT class PQDataStore<int8_t>;
template DISKANN_DLLEXPORT class PQDataStore<float>;
template DISKANN_DLLEXPORT class PQDataStore<uint8_t>;

} // namespace diskann