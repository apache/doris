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

#include "GeoShape.h"

#include <s2/s2error.h>
#include <s2/s2polyline.h>
#include <s2/s2polygon.h>
#include <s2/s2earth.h>
#include <s2/s2boolean_operation.h>
#include <s2/s2buffer_operation.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2closest_edge_query.h>
#include <s2/s2centroids.h>
#include <s2/s2lax_polygon_shape.h>
#include <s2/s2point_vector_shape.h>
#include <s2/s2builderutil_lax_polygon_layer.h>
#include <s2/s2builderutil_s2polygon_layer.h>
#include <s2/mutable_s2shape_index.h>
#include "GeoPoint.h"
#include "GeoLineString.h"
#include "GeoPolygon.h"
#include "GeoCircle.h"
#include "GeoCollection.h"
#include "GeoMultiPoint.h"
#include "GeoMultiLineString.h"
#include "GeoMultiPolygon.h"

#include "geo/geo_tojson.h"
#include "geo/geo_tobinary.h"
#include "geo/geojson_parse.h"
#include "geo/wkb_parse.h"
#include "geo/wkt_parse.h"

namespace doris {

    size_t hash_coord(const GeoCoordinate& coord) {
        return std::hash<double>()(coord.x) ^ std::hash<double>()(coord.y);
    }

    bool GeoShape::is_ring() const {
        if(type()==GEO_SHAPE_LINE_STRING && is_closed()){
            const GeoLineString* line_string = (const GeoLineString *) this;
            std::unique_ptr<GeoCoordinates> coords(line_string->to_coords_ptr().release());
            std::set<GeoCoordinate,CompareA> coords_set(coords->coords.begin(),coords->coords.end());
            /*for (int i = 0; i < line_string->get_num_point() - 1; ++i) {
                auto [it, inserted] = coords_set.insert(coords->coords[i]);
                if (!inserted) {
                    return false;
                }
            }*/
            return coords->coords.size()-1 == coords_set.size();
        }else {
            //这里应该报错
            return false;
        }
    }

    bool GeoShape::decode_from(const void *data, size_t& data_size) {
        char reserved_byte = ((const char *) data)[0];
        if (reserved_byte == 0X01){
            set_empty();
            return true;
        }

        char type_byte = ((const char *) data)[1];
        if (reserved_byte != 0X00 || type_byte != type()) {
            return false;
        }

        std::memcpy(&data_size, (const char *) data + 2, sizeof(data_size));

        return decode((const char *) data + 10, data_size);
    }

    void GeoShape::encode_to(std::string *buf) {
        size_t data_size = 0;
        // Whether the first byte storage is empty, 0X01 is empty.6
        if(is_empty()){
            buf->push_back(0X01);
            buf->push_back((char) type());
            buf->append(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
            return;
        }
        buf->push_back(0X00);
        buf->push_back((char) type());
        buf->append(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
        encode(buf,data_size);
        std::memcpy(&(*buf)[2], &data_size, sizeof(data_size));
    }

    GeoShape *GeoShape::from_wkt(const char *data, size_t size, GeoParseStatus *status) {
        GeoShape *shape = nullptr;
        *status = WktParse::parse_wkt(data, size, &shape);
        return shape;
    }

    GeoShape *GeoShape::from_wkb(const char *data, size_t size, GeoParseStatus *status) {
        std::stringstream wkb;

        for (int i = 0; i < size; ++i) {
            if ((i == 1 && wkb.str() == "x") || (i == 2 && wkb.str() == "\\x")) {
                wkb.str(std::string());
            }
            wkb << *data;
            data++;
        }
        GeoShape *shape = nullptr;
        *status = WkbParse::parse_wkb(wkb, &shape);
        return shape;
    }

    std::string GeoShape::as_binary(GeoShape *rhs, int is_hex) {
        std::string res;
        if (toBinary::geo_tobinary(rhs, &res,is_hex)) {
            return res;
        }
        return res;
    }

    std::string GeoShape::geo_tohex(std::string binary) {
        return toBinary::to_hex(binary);
    }

    GeoShape* GeoShape::from_geojson(const char* data, size_t size, GeoParseStatus* status){
        std::string geojson(data,size);

        GeoShape *shape = nullptr;
        *status = GeoJsonParse::parse_geojson(geojson, &shape);
        return shape;
    }

    bool GeoShape::as_geojson(std::string& res){
        return toGeoJson::geo_tojson(this, res);
    }

    GeoShape *GeoShape::from_encoded(const void *ptr, size_t& size) {
        std::unique_ptr<GeoShape> shape;
        switch (((const char *) ptr)[1]) {
            case GEO_SHAPE_POINT: {
                shape.reset(GeoPoint::create_unique().release());
                break;
            }
            case GEO_SHAPE_LINE_STRING: {
                shape.reset(GeoLineString::create_unique().release());
                break;
            }
            case GEO_SHAPE_POLYGON: {
                shape.reset(GeoPolygon::create_unique().release());
                break;
            }
            case GEO_SHAPE_MULTI_POINT: {
                shape.reset(GeoMultiPoint::create_unique().release());
                break;
            }
            case GEO_SHAPE_MULTI_LINE_STRING: {
                shape.reset(GeoMultiLineString::create_unique().release());
                break;
            }
            case GEO_SHAPE_MULTI_POLYGON: {
                shape.reset(GeoMultiPolygon::create_unique().release());
                break;
            }
            case GEO_SHAPE_GEOMETRY_COLLECTION: {
                shape.reset(GeoCollection::create_unique().release());
                break;
            }
            case GEO_SHAPE_CIRCLE: {
                shape.reset(GeoCircle::create_unique().release());
                break;
            }
            default:
                return nullptr;
        }
        auto res = shape->decode_from(ptr,size); //shape->decode((const char *) ptr + 2, size - 2);
        if (!res) {
            return nullptr;
        }

        //shape->decode_from(ptr,size);
        return shape.release();
    }

    bool GeoShape::intersects(GeoShape* shape){
        MutableS2ShapeIndex shape_index1;
        MutableS2ShapeIndex shape_index2;
        if(!this->add_to_s2shape_index(shape_index1) || !shape->add_to_s2shape_index(shape_index2)){
            //这里应该返回报错
            return false;
        }

        return  S2BooleanOperation::Intersects(shape_index1,shape_index2);

    }


    bool GeoShape::ComputeArea(double *area, std::string square_unit) {

        if(is_empty()) {
            *area = 0;
            return true;
        }

        std::vector<double> steradians;
        switch (type()) {
            case GEO_SHAPE_CIRCLE: {
                const GeoCircle *circle = (const GeoCircle *) this;
                steradians.emplace_back(circle->get_area());
                break;
            }
            case GEO_SHAPE_POLYGON: {
                const GeoPolygon *polygon = (const GeoPolygon *) this;
                steradians.emplace_back(polygon->get_area());
                break;
            }
            case GEO_SHAPE_POINT:
            case GEO_SHAPE_LINE_STRING:
            case GEO_SHAPE_MULTI_POINT:
            case GEO_SHAPE_MULTI_LINE_STRING:{
                *area = 0;
                return true;
            }
            case GEO_SHAPE_MULTI_POLYGON:
            case GEO_SHAPE_GEOMETRY_COLLECTION:{
                const GeoCollection *collection = (const GeoCollection *) this;
                for (int i = 0; i < collection->get_num_geometries(); ++i) {
                    switch(collection->get_geometries_n(i)->type()){
                        case GEO_SHAPE_POLYGON: {
                            const GeoPolygon *polygon = (const GeoPolygon *) collection->get_geometries_n(i);
                            steradians.emplace_back(polygon->get_area());
                            break;
                        }
                        default:
                            steradians.emplace_back(0);
                    }
                }
                break;
            }
            default:
                return false;
        }

        if (square_unit.compare("square_meters") == 0) {
            for (double& s : steradians) {
                *area = S2Earth::SteradiansToSquareMeters(s) + *area;
            }
            return true;
        } else if (square_unit.compare("square_km") == 0) {
            for (double& s : steradians) {
                *area = S2Earth::SteradiansToSquareKm(s) + *area;
            }
            return true;
        } else {
            return false;
        }
    }

    double GeoShape::get_length() const{
        double length = 0;

        switch (type()) {
            case GEO_SHAPE_LINE_STRING: {
                return S2Earth::RadiansToMeters(((const GeoLineString *) this)->length());
            }
            case GEO_SHAPE_MULTI_LINE_STRING:
            case GEO_SHAPE_GEOMETRY_COLLECTION: {
                if(get_dimension() != 1) return length;
                for (int i = 0; i < get_num_geometries(); ++i) {
                    if((get_geometries_n(i)->type()==GEO_SHAPE_LINE_STRING || get_geometries_n(i)->type() ==GEO_SHAPE_MULTI_LINE_STRING || get_geometries_n(i)->type() ==GEO_SHAPE_GEOMETRY_COLLECTION) && !get_geometries_n(i)->is_empty()){
                        length += get_geometries_n(i)->get_length();
                    }
                }
                return length;
            }
            default:
                return length;
        }
    }

    double GeoShape::get_perimeter() const {
        double perimeter = 0;

        switch (type()) {
            case GEO_SHAPE_POLYGON: {
                return S2Earth::RadiansToMeters(((const GeoPolygon *) this)->get_perimeter());
            }
            case GEO_SHAPE_MULTI_POLYGON:
            case GEO_SHAPE_GEOMETRY_COLLECTION: {
                if(get_dimension() != 2) return perimeter;
                for (int i = 0; i < get_num_geometries(); ++i) {
                    perimeter += S2Earth::RadiansToMeters(((const GeoLineString *)get_geometries_n(i))->get_perimeter());
                }
                return perimeter;
            }
            default:
                return perimeter;
        }
    }

    std::unique_ptr<GeoShape> GeoShape::get_centroid() const{
        std::unique_ptr<GeoPoint> centroid = GeoPoint::create_unique();
        if(is_empty()){
            centroid->set_empty();
            return centroid;
        }

        S2Point centroid_vector(0, 0, 0);

        switch (type()) {
            case GEO_SHAPE_POINT: {
                centroid->from_s2point(const_cast<S2Point *>(((GeoPoint *) this)->point()));
                return centroid;
            }
            case GEO_SHAPE_LINE_STRING: {
                S2Point s2_point = ((const GeoLineString *) this)->polyline()->GetCentroid();
                centroid->from_s2point(&s2_point);
                return centroid;
            }
            case GEO_SHAPE_POLYGON: {
                S2Point s2_point = ((const GeoPolygon *) this)->polygon()->GetCentroid();
                centroid->from_s2point(&s2_point);
                return centroid;
            }
            case GEO_SHAPE_MULTI_POINT: {
                std::vector<S2Point> points ;
                const GeoMultiPoint *multi_point = (const GeoMultiPoint *)this;
                for (int i = 0; i < multi_point->get_num_point(); ++i) {
                    if(multi_point->get_point_n(i)->is_empty()) continue;
                    points.emplace_back(*multi_point->get_point_n(i)->point());
                }

                auto shape = std::make_unique<S2PointVectorShape>(points);
                for (int j = 0; j < shape->num_edges(); j++) {
                    S2Shape::Edge e = shape->edge(j);
                    centroid_vector += e.v0;
                }

                centroid->from_s2point(&centroid_vector);

                return centroid;
            }
            case GEO_SHAPE_MULTI_LINE_STRING: {
                const GeoMultiLineString *multi_line_string = (const GeoMultiLineString *)this;
                for (int i = 0; i < multi_line_string->get_num_line(); ++i) {
                    if(multi_line_string->get_line_string_n(i)->is_empty()) continue;
                    auto shape =std::make_unique<S2Polyline::Shape>(multi_line_string->get_line_string_n(i)->polyline());
                    for (int j = 0; j < shape->num_edges(); j++) {
                        S2Shape::Edge e = shape->edge(j);
                        centroid_vector += S2::TrueCentroid(e.v0, e.v1);
                    }
                }
                centroid->from_s2point(&centroid_vector);
                return centroid;
            }
            case GEO_SHAPE_MULTI_POLYGON: {
                const GeoMultiPolygon *multi_polygon = (const GeoMultiPolygon *)this;
                for (int i = 0; i < multi_polygon->get_num_polygon() ; ++i) {
                    if(multi_polygon->get_polygon_n(i)->is_empty()) continue;
                    S2Point centroid_tmp =  multi_polygon->get_polygon_n(i)->polygon()->GetCentroid();
                    centroid_vector += centroid_tmp;
                }
                centroid->from_s2point(&centroid_vector);
                return centroid;
            }
            case GEO_SHAPE_GEOMETRY_COLLECTION: {
                const GeoCollection *collection = (const GeoCollection *)this;
                for (int i = 0; i < collection->get_num_geometries(); ++i) {
                    GeoPoint* point = (GeoPoint *)collection->get_geometries_n(i)->get_centroid().release();
                    centroid_vector += *point->point();
                }
                centroid->from_s2point(&centroid_vector);
                return centroid;
            }
            default:
                //报错
                return nullptr;
        }
    }

    bool GeoShape::get_type_string(std::string& type_string) const {
        switch (type()) {
            case GEO_SHAPE_POINT: {
                type_string = "ST_Point";
                return true;
            }
            case GEO_SHAPE_LINE_STRING: {
                type_string = "ST_LineString";
                return true;
            }
            case GEO_SHAPE_POLYGON: {
                type_string = "ST_Polygon";
                return true;
            }
            case GEO_SHAPE_MULTI_POINT: {
                type_string = "ST_MultiPoint";
                return true;
            }
            case GEO_SHAPE_MULTI_LINE_STRING: {
                type_string = "ST_MultiLineString";
                return true;
            }
            case GEO_SHAPE_MULTI_POLYGON: {
                type_string = "ST_MultiPolygon";
                return true;
            }
            case GEO_SHAPE_GEOMETRY_COLLECTION: {
                type_string = "ST_GeometryCollection";
                return true;
            }
            default:
                return false;
        }
    }

    std::unique_ptr<GeoShape> GeoShape::find_closest_point(const GeoShape* shape){

        MutableS2ShapeIndex shape_index1;
        MutableS2ShapeIndex shape_index2;

        if(!this->add_to_s2shape_index(shape_index1) || !shape->add_to_s2shape_index(shape_index2)){
            //这里应该返回报错
            return nullptr;
        }

        S2ClosestEdgeQuery query1(&shape_index1);

        query1.mutable_options()->set_include_interiors(false);
        S2ClosestEdgeQuery::ShapeIndexTarget target(&shape_index2);

        const auto& result1 = query1.FindClosestEdge(&target);

        if (result1.edge_id() == -1) {
            //这里表示没找到最近的edge，应该返回失败，理论上不会走到这里，因为set_include_interiors(false);
            return nullptr;
        }

        // Get the edge from index1 (edge1) that is closest to index2.
        S2Shape::Edge edge1 = query1.GetEdge(result1);

        // Now find the edge from index2 (edge2) that is closest to edge1.
        S2ClosestEdgeQuery query2(&shape_index2);
        query2.mutable_options()->set_include_interiors(false);
        S2ClosestEdgeQuery::EdgeTarget target2(edge1.v0, edge1.v1);
        auto result2 = query2.FindClosestEdge(&target2);

        // what if result2 has no edges?
        if (result2.is_interior()) {
            //这里应该返回报错
            return nullptr;
            //throw Exception("S2ClosestEdgeQuery result is interior!");
        }

        S2Shape::Edge edge2 = query2.GetEdge(result2);

        std::unique_ptr<GeoPoint> res_point = GeoPoint::create_unique();

        S2Point s2Point = S2::GetEdgePairClosestPoints(edge1.v0, edge1.v1, edge2.v0, edge2.v1).first;
        res_point->from_s2point(&s2Point);

        return res_point;

    }

    bool GeoShape::dwithin(const GeoShape* shape,double distance){
        MutableS2ShapeIndex shape_index1;
        MutableS2ShapeIndex shape_index2;

        if(!this->add_to_s2shape_index(shape_index1) || !shape->add_to_s2shape_index(shape_index2)){
            //这里应该返回报错
            return false;
        }

        S2ClosestEdgeQuery query(&shape_index1);
        S2ClosestEdgeQuery::ShapeIndexTarget target(&shape_index2);

        const auto& result = query.FindClosestEdge(&target);

        return S2Earth::RadiansToMeters(result.distance().ToAngle().radians())  <= distance;

    }

    std::unique_ptr<GeoShape> one_geo_buffer(S2BufferOperation::Options& options, MutableS2ShapeIndex& shape_index){

        auto output = std::make_unique<S2Polygon>();
        S2BufferOperation op(std::make_unique<s2builderutil::S2PolygonLayer>(output.get()), options);

        op.AddShapeIndex(shape_index);

        S2Error error;
        if(!op.Build(&error)){
            //这里应该返回报错
            return nullptr;
        }
        std::unique_ptr<GeoPolygon> res_polygon = GeoPolygon::create_unique();
        res_polygon->from_s2polygon(std::move(output));
        return res_polygon;

    }

    std::unique_ptr<GeoShape> GeoShape::buffer(double buffer_radius, double num_seg_quarter_circle, std::string end_cap, std::string side){
        S2BufferOperation::Options options;
        options.set_buffer_radius(S2Earth::MetersToAngle(buffer_radius));
        options.set_circle_segments(num_seg_quarter_circle*4);

        if(end_cap == "ROUND") {
            options.set_end_cap_style(S2BufferOperation::EndCapStyle::ROUND);
        } else if (end_cap == "FLAT") {
            options.set_end_cap_style(S2BufferOperation::EndCapStyle::FLAT);
        } else {
            //这里应该返回不正确的 end_cap 或者在function.cpp校验。
            return nullptr;
        }

        if(side == "BOTH") {
            options.set_polyline_side(S2BufferOperation::PolylineSide::BOTH);
        } else if (end_cap == "LEFT") {
            options.set_polyline_side(S2BufferOperation::PolylineSide::LEFT);
        } else if (end_cap == "RIGHT") {
            options.set_polyline_side(S2BufferOperation::PolylineSide::RIGHT);
        } else {
            //这里应该返回不正确的 end_cap 或者在function.cpp校验。
            return nullptr;
        }

        switch (type()) {
            case GEO_SHAPE_POINT:
            case GEO_SHAPE_LINE_STRING:
            case GEO_SHAPE_POLYGON:{
                MutableS2ShapeIndex shape_index;
                if(!this->add_to_s2shape_index(shape_index) ){
                    //这里应该返回报错
                    return nullptr;
                }
                return one_geo_buffer(options,shape_index);
            }
            case GEO_SHAPE_MULTI_POINT:
            case GEO_SHAPE_MULTI_LINE_STRING:
            case GEO_SHAPE_MULTI_POLYGON:
            case GEO_SHAPE_GEOMETRY_COLLECTION: {
                std::unique_ptr<GeoMultiPolygon> res_multi_polygon = GeoMultiPolygon::create_unique();
                for (int i = 0; i < this->get_num_geometries(); ++i) {
                    MutableS2ShapeIndex shape_index;
                    if(this->get_geometries_n(i)->is_empty()) continue;
                    if(!this->get_geometries_n(i)->add_to_s2shape_index(shape_index) ){
                        //这里应该返回报错
                        return nullptr;
                    }
                    res_multi_polygon->add_one_geometry(one_geo_buffer(options,shape_index).release());
                }
                return res_multi_polygon;
            }
            default: {
                //这里应该返回报错
                return nullptr;
            }

        }

    }

} // namespace doris
