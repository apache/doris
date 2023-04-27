SELECT round(sum(st_distance_sphere(longitude_1, latitude_1, longitude_2, latitude_2) / 1000)) FROM opensky;
