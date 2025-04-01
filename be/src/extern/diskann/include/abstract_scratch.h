#pragma once
namespace diskann
{

template <typename data_t> class PQScratch;

// By somewhat more than a coincidence, it seems that both InMemQueryScratch
// and SSDQueryScratch have the aligned query and PQScratch objects. So we
// can put them in a neat hierarchy and keep PQScratch as a standalone class.
template <typename data_t> class AbstractScratch
{
  public:
    AbstractScratch() = default;
    // This class does not take any responsibilty for memory management of
    // its members. It is the responsibility of the derived classes to do so.
    virtual ~AbstractScratch() = default;

    // Scratch objects should not be copied
    AbstractScratch(const AbstractScratch &) = delete;
    AbstractScratch &operator=(const AbstractScratch &) = delete;

    data_t *aligned_query_T()
    {
        return _aligned_query_T;
    }
    PQScratch<data_t> *pq_scratch()
    {
        return _pq_scratch;
    }

  protected:
    data_t *_aligned_query_T = nullptr;
    PQScratch<data_t> *_pq_scratch = nullptr;
};
} // namespace diskann
