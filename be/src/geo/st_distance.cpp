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

#include "geo/geo_types.h"

#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <cmath>
#include <limits>

namespace doris {

// Helper function to compute distance from a point to a line segment
double distance_point_to_segment(const S2Point& point, const S2Point& line_start, 
                                 const S2Point& line_end) {
    S2LatLng point_ll = S2LatLng(point);
    S2LatLng start_ll = S2LatLng(line_start);
    S2LatLng end_ll = S2LatLng(line_end);
    
    double px = point_ll.lng().degrees();
    double py = point_ll.lat().degrees();
    double x1 = start_ll.lng().degrees();
    double y1 = start_ll.lat().degrees();
    double x2 = end_ll.lng().degrees();
    double y2 = end_ll.lat().degrees();
    
    double dx = x2 - x1;
    double dy = y2 - y1;
    
    if (dx == 0 && dy == 0) {
        return S2Earth::GetDistanceMeters(point_ll, start_ll);
    }
    
    double t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy);
    t = std::max(0.0, std::min(1.0, t));
    
    double closest_x = x1 + t * dx;
    double closest_y = y1 + t * dy;
    
    S2LatLng closest_point = S2LatLng::FromDegrees(closest_y, closest_x);
    return S2Earth::GetDistanceMeters(point_ll, closest_point);
}

// Helper function to compute distance from a point to a polyline
double distance_point_to_polyline(const S2Point& point, const S2Polyline* polyline) {
    double min_distance = std::numeric_limits<double>::max();
    
    for (int i = 0; i < polyline->num_vertices() - 1; ++i) {
        const S2Point& p1 = polyline->vertex(i);
        const S2Point& p2 = polyline->vertex(i + 1);
        
        double dist = distance_point_to_segment(point, p1, p2);
        min_distance = std::min(min_distance, dist);
    }
    
    return min_distance;
}

// Helper function to compute distance from a point to a polygon
double distance_point_to_polygon(const S2Point& point, const S2Polygon* polygon) {
    // Check if point is inside polygon
    if (polygon->Contains(point)) {
        return 0.0;
    }
    
    // Find minimum distance to polygon boundary
    double min_distance = std::numeric_limits<double>::max();
    
    for (int i = 0; i < polygon->num_loops(); ++i) {
        const S2Loop* loop = polygon->loop(i);
        
        for (int j = 0; j < loop->num_vertices(); ++j) {
            const S2Point& p1 = loop->vertex(j);
            const S2Point& p2 = loop->vertex((j + 1) % loop->num_vertices());
            
            double dist = distance_point_to_segment(point, p1, p2);
            min_distance = std::min(min_distance, dist);
        }
    }
    
    return min_distance;
}

double GeoPoint::Distance(const GeoShape* rhs) const {
    if (rhs == nullptr) {
        return -1.0;
    }
    
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = static_cast<const GeoPoint*>(rhs);
        S2LatLng this_ll = S2LatLng(*_point);
        S2LatLng other_ll = S2LatLng(*point->point());
        return S2Earth::GetDistanceMeters(this_ll, other_ll);
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = static_cast<const GeoLine*>(rhs);
        return distance_point_to_polyline(*_point, line->polyline());
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = static_cast<const GeoPolygon*>(rhs);
        return distance_point_to_polygon(*_point, polygon->polygon());
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = static_cast<const GeoCircle*>(rhs);
        S2LatLng this_ll = S2LatLng(*_point);
        S2LatLng center_ll = S2LatLng(circle->circle()->center());
        double dist_to_center = S2Earth::GetDistanceMeters(this_ll, center_ll);
        double circle_radius = S2Earth::ToMeters(circle->circle()->radius());
        
        // Distance from point to circle is distance to center minus radius
        return std::max(0.0, dist_to_center - circle_radius);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi = static_cast<const GeoMultiPolygon*>(rhs);
        return rhs->Distance(this);  // Delegate to MultiPolygon's implementation
    }
    default:
        return -1.0;
    }
}

double GeoLine::Distance(const GeoShape* rhs) const {
    if (rhs == nullptr) {
        return -1.0;
    }
    
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = static_cast<const GeoPoint*>(rhs);
        return distance_point_to_polyline(*point->point(), _polyline.get());
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* other_line = static_cast<const GeoLine*>(rhs);
        double min_distance = std::numeric_limits<double>::max();
        
        // Check distance from each vertex of this line to other line
        for (int i = 0; i < _polyline->num_vertices(); ++i) {
            double dist = distance_point_to_polyline(_polyline->vertex(i), 
                                                     other_line->polyline());
            min_distance = std::min(min_distance, dist);
        }
        
        // Check distance from each vertex of other line to this line
        for (int i = 0; i < other_line->polyline()->num_vertices(); ++i) {
            double dist = distance_point_to_polyline(other_line->polyline()->vertex(i), 
                                                     _polyline.get());
            min_distance = std::min(min_distance, dist);
        }
        
        return min_distance;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = static_cast<const GeoPolygon*>(rhs);
        return rhs->Distance(this);  // Delegate to Polygon's implementation
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = static_cast<const GeoCircle*>(rhs);
        return rhs->Distance(this);  // Delegate to Circle's implementation
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi = static_cast<const GeoMultiPolygon*>(rhs);
        return rhs->Distance(this);  // Delegate to MultiPolygon's implementation
    }
    default:
        return -1.0;
    }
}

double GeoPolygon::Distance(const GeoShape* rhs) const {
    if (rhs == nullptr) {
        return -1.0;
    }
    
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = static_cast<const GeoPoint*>(rhs);
        return distance_point_to_polygon(*point->point(), _polygon.get());
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = static_cast<const GeoLine*>(rhs);
        double min_distance = std::numeric_limits<double>::max();
        
        // Check distance from each vertex of line to polygon
        for (int i = 0; i < line->polyline()->num_vertices(); ++i) {
            double dist = distance_point_to_polygon(line->polyline()->vertex(i), _polygon.get());
            min_distance = std::min(min_distance, dist);
        }
        
        // Check distance from each polygon vertex to line
        for (int i = 0; i < _polygon->num_loops(); ++i) {
            const S2Loop* loop = _polygon->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                double dist = distance_point_to_polyline(loop->vertex(j), line->polyline());
                min_distance = std::min(min_distance, dist);
            }
        }
        
        return min_distance;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = static_cast<const GeoPolygon*>(rhs);
        double min_distance = std::numeric_limits<double>::max();
        
        // Check distance from each vertex of this polygon to other polygon
        for (int i = 0; i < _polygon->num_loops(); ++i) {
            const S2Loop* loop = _polygon->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                double dist = distance_point_to_polygon(loop->vertex(j), other->polygon());
                min_distance = std::min(min_distance, dist);
            }
        }
        
        // Check distance from each vertex of other polygon to this polygon
        for (int i = 0; i < other->polygon()->num_loops(); ++i) {
            const S2Loop* loop = other->polygon()->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                double dist = distance_point_to_polygon(loop->vertex(j), _polygon.get());
                min_distance = std::min(min_distance, dist);
            }
        }
        
        return min_distance;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = static_cast<const GeoCircle*>(rhs);
        return rhs->Distance(this);  // Delegate to Circle's implementation
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi = static_cast<const GeoMultiPolygon*>(rhs);
        return rhs->Distance(this);  // Delegate to MultiPolygon's implementation
    }
    default:
        return -1.0;
    }
}

double GeoMultiPolygon::Distance(const GeoShape* rhs) const {
    if (rhs == nullptr) {
        return -1.0;
    }
    
    double min_distance = std::numeric_limits<double>::max();
    
    // Calculate minimum distance from any polygon to the other shape
    for (const auto& polygon : _polygons) {
        double dist = polygon->Distance(rhs);
        if (dist >= 0) {
            min_distance = std::min(min_distance, dist);
        }
    }
    
    return (min_distance == std::numeric_limits<double>::max()) ? -1.0 : min_distance;
}

double GeoCircle::Distance(const GeoShape* rhs) const {
    if (rhs == nullptr || _cap == nullptr) {
        return -1.0;
    }
    
    double circle_radius = S2Earth::ToMeters(_cap->radius());
    S2LatLng center_ll = S2LatLng(_cap->center());
    
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = static_cast<const GeoPoint*>(rhs);
        S2LatLng point_ll = S2LatLng(*point->point());
        double dist_to_center = S2Earth::GetDistanceMeters(center_ll, point_ll);
        return std::max(0.0, dist_to_center - circle_radius);
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = static_cast<const GeoLine*>(rhs);
        double min_distance = std::numeric_limits<double>::max();
        
        // Find minimum distance from circle center to line
        for (int i = 0; i < line->polyline()->num_vertices() - 1; ++i) {
            double dist = distance_point_to_segment(_cap->center(), 
                                                    line->polyline()->vertex(i),
                                                    line->polyline()->vertex(i + 1));
            min_distance = std::min(min_distance, dist);
        }
        
        return std::max(0.0, min_distance - circle_radius);
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = static_cast<const GeoPolygon*>(rhs);
        
        // If center is inside polygon, distance is 0
        if (polygon->polygon()->Contains(_cap->center())) {
            return 0.0;
        }
        
        // Find minimum distance from circle center to polygon boundary
        double min_distance = std::numeric_limits<double>::max();
        
        for (int i = 0; i < polygon->polygon()->num_loops(); ++i) {
            const S2Loop* loop = polygon->polygon()->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                double dist = distance_point_to_segment(_cap->center(),
                                                        loop->vertex(j),
                                                        loop->vertex((j + 1) % loop->num_vertices()));
                min_distance = std::min(min_distance, dist);
            }
        }
        
        return std::max(0.0, min_distance - circle_radius);
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* other = static_cast<const GeoCircle*>(rhs);
        double other_radius = S2Earth::ToMeters(other->circle()->radius());
        S2LatLng other_center_ll = S2LatLng(other->circle()->center());
        double dist_centers = S2Earth::GetDistanceMeters(center_ll, other_center_ll);
        
        // Distance between circles is distance between centers minus sum of radii
        return std::max(0.0, dist_centers - circle_radius - other_radius);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi = static_cast<const GeoMultiPolygon*>(rhs);
        return rhs->Distance(this);  // Delegate to MultiPolygon's implementation
    }
    default:
        return -1.0;
    }
}

} // namespace doris
