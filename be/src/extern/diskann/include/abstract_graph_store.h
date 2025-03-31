// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>
#include <vector>
#include "types.h"

namespace diskann
{

class AbstractGraphStore
{
  public:
    AbstractGraphStore(const size_t total_pts, const size_t reserve_graph_degree)
        : _capacity(total_pts), _reserve_graph_degree(reserve_graph_degree)
    {
    }

    virtual ~AbstractGraphStore() = default;

    // returns tuple of <nodes_read, start, num_frozen_points>
    virtual std::tuple<uint32_t, uint32_t, size_t> load(const std::string &index_path_prefix,
                                                        const size_t num_points) = 0;
    virtual int store(const std::string &index_path_prefix, const size_t num_points, const size_t num_fz_points,
                      const uint32_t start) = 0;

    virtual int store(std::stringstream &index_stream, const size_t num_points,
                           const size_t num_frozen_points, const uint32_t start){
                              return 0;
                           }
    virtual int save_graph(std::stringstream &out, const size_t num_points,
                                const size_t num_frozen_points, const uint32_t start){ return 0;}

    // not synchronised, user should use lock when necvessary.
    virtual const std::vector<location_t> &get_neighbours(const location_t i) const = 0;
    virtual void add_neighbour(const location_t i, location_t neighbour_id) = 0;
    virtual void clear_neighbours(const location_t i) = 0;
    virtual void swap_neighbours(const location_t a, location_t b) = 0;

    virtual void set_neighbours(const location_t i, std::vector<location_t> &neighbours) = 0;

    virtual size_t resize_graph(const size_t new_size) = 0;
    virtual void clear_graph() = 0;

    virtual uint32_t get_max_observed_degree() = 0;

    // set during load
    virtual size_t get_max_range_of_graph() = 0;

    // Total internal points _max_points + _num_frozen_points
    size_t get_total_points()
    {
        return _capacity;
    }

  protected:
    // Internal function, changes total points when resize_graph is called.
    void set_total_points(size_t new_capacity)
    {
        _capacity = new_capacity;
    }

    size_t get_reserve_graph_degree()
    {
        return _reserve_graph_degree;
    }

  private:
    size_t _capacity;
    size_t _reserve_graph_degree;
};

} // namespace diskann