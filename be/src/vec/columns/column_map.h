#pragma once

#include "vec/columns/column_array.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/common/arena.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {


/** A column of map values.
  */
class ColumnMap final : public COWHelper<IColumn, ColumnMap> {

public:  
  /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnMap>;

    static Ptr create(const ColumnPtr& keys, const ColumnPtr& values) {
        return ColumnMap::create(keys->assume_mutable(), values->assume_mutable());
    }

    template <typename... Args,
              typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    std::string get_name() const override;
    const char * get_family_name() const override { return "Map"; }
    TypeIndex get_data_type() const { return TypeIndex::Map; }

    void for_each_subcolumn(ColumnCallback callback) override {
        callback(keys);
        callback(values);
    }

    MutableColumnPtr clone_resized(size_t size) const override;

    bool can_be_inside_nullable() const override { return true; }
    size_t size() const override { return keys->size(); }
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    StringRef get_data_at(size_t n) const override;

    void insert_data(const char* pos, size_t length) override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_from(const IColumn& src_, size_t n) override;
    void insert(const Field & x) override;
    void insert_default() override;

    void pop_back(size_t n) override;

    StringRef serialize_value_into_arena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserialize_and_insert_from_arena(const char * pos) override;

    void update_hash_with_value(size_t n, SipHash & hash) const override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override {
        return scatter_impl<ColumnMap>(num_columns, selector);
    }
    void get_extremes(Field & min, Field & max) const override;
    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs_,
                                int nan_direction_hint) const override {
        LOG(FATAL) << "compare_at not implemented";
    }
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override {
       LOG(FATAL) << "get_permutation not implemented";
    }
    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        return append_data_by_selector_impl<ColumnMap>(res, selector);
    }


    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data not implemented";
    }
    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data_default not implemented";
    }
    void check_size() const;
    ColumnArray::Offsets64& get_offsets() const;
    void reserve(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    void protect() override;

   /******************** keys and values ***************/
    const ColumnPtr& get_keys_ptr() const { return keys; }
    ColumnPtr& get_keys_ptr() { return keys; }

    const IColumn& get_keys() const { return *keys; }
    IColumn& get_keys() { return *keys; }

    const ColumnPtr& get_values_ptr() const { return values; }
    ColumnPtr& get_values_ptr() { return values; }

    const IColumn& get_values() const { return *values; }
    IColumn& get_values() { return *values; }

private:
    friend class COWHelper<IColumn, ColumnMap>;

    WrappedPtr keys; // nullable
    WrappedPtr values; // nullable

    size_t ALWAYS_INLINE offset_at(ssize_t i) const { return get_offsets()[i - 1]; }
    size_t ALWAYS_INLINE size_at(ssize_t i) const {
        return get_offsets()[i] - get_offsets()[i - 1];
    }

    explicit ColumnMap(MutableColumnPtr && keys, MutableColumnPtr && values);

    ColumnMap(const ColumnMap &) = default;
};

}