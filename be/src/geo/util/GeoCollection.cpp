// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "GeoCollection.h"
#include "GeoMultiPoint.h"
#include "GeoMultiLineString.h"
#include "GeoMultiPolygon.h"
#include <sstream>

namespace doris {

    GeoCollection::GeoCollection() = default;
    GeoCollection::~GeoCollection() = default;

    bool GeoCollection::is_valid() const {
        for(const auto& g : geometries) {
            if(!g->is_valid()) {
                return false;
            }
        }
        return true;
    }

    bool GeoCollection::is_closed() const {
        for(const auto& g : geometries) {
            if(!g->is_closed()) {
                return false;
            }
        }
        return true;
    }

    GeoParseStatus GeoCollection::add_one_geometry(GeoShape* shape){
        geometries.emplace_back(std::unique_ptr<GeoShape>(shape));
        return GEO_PARSE_OK;
    }

    std::size_t GeoCollection::get_num_geometries() const {
        return geometries.size();
    }

    GeoShape* GeoCollection::get_geometries_n(std::size_t n) const {
        return geometries[n].get();
    }

    std::string GeoCollection::as_wkt() const {
        std::stringstream ss;
        ss << "GEOMETRYCOLLECTION ";

        if (is_empty()){
            ss << "EMPTY";
            return ss.str();
        }
        ss << "(";

        for (int i = 0; i < geometries.size(); ++i) {
            ss << geometries[i]->as_wkt();
            if(i != geometries.size()-1){
                ss << ",";
            }
        }
        ss << ")";
        return ss.str();
    }

    bool GeoCollection::contains(const GeoShape* rhs) const {
        switch (rhs->type()) {
            case GEO_SHAPE_POINT: {
                const GeoPoint* point = (const GeoPoint*)rhs;
                for(const auto& g : geometries) {
                    if(!g->contains(point)){
                        return false;
                    }
                }
                return true;
            }
            case GEO_SHAPE_LINE_STRING:{
                const GeoLineString* line = (const GeoLineString*)rhs;
                for(const auto& g : geometries) {
                    if(!g->contains(line)){
                        return false;
                    }
                }
                return true;
            }
            case GEO_SHAPE_POLYGON:{
                const GeoPolygon* polygon = (const GeoPolygon*)rhs;
                for(const auto& g : geometries) {
                    if(!g->contains(polygon)){
                        return false;
                    }
                }
                return true;
            }
            case GEO_SHAPE_MULTI_POINT:
            case GEO_SHAPE_MULTI_LINE_STRING:
            case GEO_SHAPE_MULTI_POLYGON:
            case GEO_SHAPE_GEOMETRY_COLLECTION:{
                const GeoCollection* collection = (const GeoCollection*)rhs;
                for(const auto& g1 : collection->geometries) {
                    for(const auto& g2 : geometries) {
                        if(!g2->contains(g1.get())){
                            return false;
                        }
                    }
                }
                return true;
            }
            default:
                return false;
        }
    }

    std::size_t GeoCollection::get_num_point() const {
        if(is_empty()) return 0;
        std::size_t num_point = 0;
        for(const auto& g : geometries) {
            num_point += g->get_num_point();
        }
        return num_point;
    }

    std::unique_ptr<GeoShape> GeoCollection::boundary() const {
        std::unique_ptr<GeoCollection> collection = GeoCollection::create_unique();
        if(is_empty()){
            collection->set_empty();
            return collection;
        }

        for(const auto& g : geometries) {
            std::unique_ptr<GeoShape> shape =  g->boundary();
            collection->add_one_geometry(shape.release());
        }
        return collection;

        //to_homogenize 还有问题
        //return collection->to_homogenize();
    }

    std::unique_ptr<GeoCollection> GeoCollection::to_homogenize(){
        std::unique_ptr<GeoMultiPoint> multi_point = GeoMultiPoint::create_unique();
        std::unique_ptr<GeoMultiLineString> multi_linestring = GeoMultiLineString::create_unique();
        std::unique_ptr<GeoMultiPolygon> multi_polygon = GeoMultiPolygon::create_unique();

        for(const auto& g : geometries) {
            switch (g->type()) {
                case GEO_SHAPE_POINT: {
                    multi_point->add_one_geometry(g.get());
                    break;
                }
                case GEO_SHAPE_LINE_STRING: {
                    GeoLineString* linestring = dynamic_cast<GeoLineString*>(g.get());
                    multi_linestring->add_one_geometry(linestring);
                    break;
                }
                case GEO_SHAPE_POLYGON: {
                    GeoPolygon* polygon = dynamic_cast<GeoPolygon*>(g.get());
                    multi_polygon->add_one_geometry(polygon);
                    break;
                }
                case GEO_SHAPE_MULTI_POINT: {
                    const GeoMultiPoint* multi_point_tmp = dynamic_cast<const GeoMultiPoint*>(g.get());
                    for (int i = 0; i < multi_point_tmp->get_num_point(); ++i) {
                        GeoPoint* point = dynamic_cast<GeoPoint*>(multi_point_tmp->get_geometries_n(i));
                        multi_point->add_one_geometry(point);
                    }
                    break;
                }
                case GEO_SHAPE_MULTI_LINE_STRING: {
                    const GeoMultiLineString* multi_linestring_tmp =  dynamic_cast<const GeoMultiLineString*>(g.get());
                    for (int i = 0; i < multi_linestring_tmp->get_num_line(); ++i) {
                        GeoLineString* linestring = dynamic_cast<GeoLineString*>(multi_linestring_tmp->get_geometries_n(i));
                        multi_linestring->add_one_geometry(linestring);
                    }
                    break;
                }
                case GEO_SHAPE_MULTI_POLYGON: {
                    const GeoMultiPolygon* multi_polygon_tmp =  dynamic_cast<const GeoMultiPolygon*>(g.get());
                    for (int i = 0; i < multi_polygon_tmp->get_num_polygon(); ++i) {
                        GeoPolygon* polygon = dynamic_cast<GeoPolygon*>(multi_polygon_tmp->get_geometries_n(i));
                        multi_polygon->add_one_geometry(polygon);
                    }
                    break;
                }
                default: {
                    //这里应该输出告警日志
                    return nullptr;
                }
            }
        }

        std::unique_ptr<GeoCollection> res_collection = GeoCollection::create_unique();

        if(multi_point->get_num_point() == 1){
            GeoPoint* point =  (GeoPoint*)multi_point->get_geometries_n(0);
            res_collection->add_one_geometry(point);
        }else if(multi_point->get_num_point() > 1){
            res_collection->add_one_geometry(multi_point.release());
        }
        /*
         * else {
         * 这里应该输出告警日志
         * }
         */

        if(multi_linestring->get_num_line() == 1){
            GeoLineString* linestring =  (GeoLineString*)multi_linestring->get_geometries_n(0);
            res_collection->add_one_geometry(linestring);
        }else if(multi_linestring->get_num_line() > 1){
            res_collection->add_one_geometry(multi_linestring.release());
        }
        /*
         * else {
         * 这里应该输出告警日志
         * }
         */

        if(multi_polygon->get_num_polygon() == 1){
            GeoPolygon* polygon =  (GeoPolygon*)multi_polygon->get_geometries_n(0);
            res_collection->add_one_geometry(polygon);
        }else if(multi_polygon->get_num_polygon() > 1){
            res_collection->add_one_geometry(multi_polygon.release());
        }
        /*
         * else {
         * 这里应该输出告警日志
         * }
         */

        return res_collection;
    }

    bool GeoCollection::add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const {
        if(is_empty() || !is_valid()) return false;
        for(const auto& g : geometries) {
            switch (g->type()) {
                case GEO_SHAPE_POINT: {
                    GeoPoint* point = dynamic_cast<GeoPoint*>(g.get());
                    if(!point->add_to_s2shape_index(S2shape_index)){
                        return false;
                    }
                    break;
                }
                case GEO_SHAPE_LINE_STRING: {
                    GeoLineString* linestring = dynamic_cast<GeoLineString*>(g.get());
                    if(!linestring->add_to_s2shape_index(S2shape_index)){
                        return false;
                    }
                    break;
                }
                case GEO_SHAPE_POLYGON: {
                    GeoPolygon* polygon = dynamic_cast<GeoPolygon*>(g.get());
                    if(!polygon->add_to_s2shape_index(S2shape_index)){
                        return false;
                    }
                    break;
                }
                case GEO_SHAPE_MULTI_POINT: {
                    const GeoMultiPoint* multi_point_tmp = dynamic_cast<const GeoMultiPoint*>(g.get());
                    for (int i = 0; i < multi_point_tmp->get_num_point(); ++i) {
                        GeoPoint* point = dynamic_cast<GeoPoint*>(multi_point_tmp->get_geometries_n(i));
                        if(!point->add_to_s2shape_index(S2shape_index)){
                            return false;
                        }
                    }
                    break;
                }
                case GEO_SHAPE_MULTI_LINE_STRING: {
                    const GeoMultiLineString* multi_linestring_tmp =  dynamic_cast<const GeoMultiLineString*>(g.get());
                    for (int i = 0; i < multi_linestring_tmp->get_num_line(); ++i) {
                        GeoLineString* line_string = dynamic_cast<GeoLineString*>(multi_linestring_tmp->get_geometries_n(i));
                        if(!line_string->add_to_s2shape_index(S2shape_index)){
                            return false;
                        }
                    }
                    break;
                }
                case GEO_SHAPE_MULTI_POLYGON: {
                    const GeoMultiPolygon* multi_polygon_tmp =  dynamic_cast<const GeoMultiPolygon*>(g.get());
                    for (int i = 0; i < multi_polygon_tmp->get_num_polygon(); ++i) {
                        GeoPolygon* polygon = dynamic_cast<GeoPolygon*>(multi_polygon_tmp->get_geometries_n(i));
                        if(!polygon->add_to_s2shape_index(S2shape_index)){
                            return false;
                        }
                    }
                    break;
                }
                default: {
                    //这里应该输出告警日志
                    return false;
                }
            }

        }
        return true;
    }


    void GeoCollection::encode(std::string* buf, size_t& data_size) {

        for (int i = 0; i < geometries.size(); ++i) {
            std::string data;
            geometries[i]->encode_to(&data);
            buf->append(data);
            data_size += data.size();
        }
    }

    bool GeoCollection::decode(const void* data, size_t size) {

        size_t data_size = 0;

        while(size - data_size > 0){
            size_t one_data_size = 0;
            std::unique_ptr<GeoShape> shape;
            shape.reset(GeoShape::from_encoded((const char *) data + data_size,one_data_size));
            geometries.emplace_back(std::move(shape));
            data_size += one_data_size + 10;
        }

        return true;
    }

}// namespace doris
