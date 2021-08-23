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

package org.apache.doris.stack.service.config;

import org.apache.doris.stack.model.response.config.GeoInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Description：Default value definition of configuration item
 */
public class ConfigValueDef {

    private ConfigValueDef() {
        throw new UnsupportedOperationException();
    }

    /**
     * The object type contains formatting defaults
     */
    public static final String TYPE_TEMPORAL = "{ \"type/Temporal\":{ \"date_style\":\"M/D/YYYY\", "
            + "\"date_abbreviate\":false, \"time_style\":\"h:mm A\", \"date_separator\":\"/\" }, "
            + "\"type/Number\":{ \"number_separators\":\".,\" } }";

    /**
     * All values of time zone types
     */
    public static final Set<String> AVAILABLE_TIMEZONES = Sets.newHashSet("Africa/Algiers", "Africa/Cairo",
            "Africa/Casablanca", "Africa/Harare", "Africa/Nairobi", "Africa/Windhoek", "America/Bogota",
            "America/Buenos_Aires", "America/Caracas", "America/Costa_Rica", "America/Chihuahua", "America/Godthab",
            "America/Guatemala", "America/Manaus", "America/Mexico_City", "America/Montevideo", "America/Santiago",
            "America/Sao_Paulo", "America/Tijuana", "Asia/Amman", "Asia/Baghdad", "Asia/Baku", "Asia/Bangkok",
            "Asia/Beirut", "Asia/Calcutta", "Asia/Colombo", "Asia/Dhaka", "Asia/Hong_Kong", "Asia/Irkutsk",
            "Asia/Jerusalem", "Asia/Kabul", "Asia/Karachi", "Asia/Katmandu", "Asia/Krasnoyarsk", "Asia/Kuala_Lumpur",
            "Asia/Kuwait", "Asia/Magadan", "Asia/Muscat", "Asia/Novosibirsk", "Asia/Rangoon", "Asia/Seoul",
            "Asia/Taipei", "Asia/Tbilisi", "Asia/Tehran", "Asia/Tokyo", "Asia/Vladivostok", "Asia/Yakutsk",
            "Asia/Yekaterinburg", "Asia/Yerevan", "Atlantic/Azores", "Atlantic/Cape_Verde", "Atlantic/South_Georgia",
            "Australia/Adelaide", "Australia/Brisbane", "Australia/Darwin", "Australia/Hobart", "Australia/Perth",
            "Australia/Sydney", "Brazil/East", "Canada/Eastern", "Canada/Newfoundland", "Canada/Saskatchewan",
            "Europe/Athens", "Europe/Belgrade", "Europe/Berlin", "Europe/Brussels", "Europe/Helsinki",
            "Europe/Istanbul", "Europe/London", "Europe/Minsk", "Europe/Moscow", "Europe/Warsaw", "Pacific/Auckland",
            "Pacific/Fiji", "Pacific/Guam", "Pacific/Midway", "Pacific/Tongatapu", "US/Alaska", "US/Arizona",
            "US/Central", "US/East-Indiana", "US/Eastern", "US/Hawaii", "US/Mountain", "US/Pacific", "GMT", "UTC");

    /**
     * All values of language type
     */
    public static List<List<String>> availableLocales = Lists.newArrayList();

    /**
     * All values of geographic location information
     */
    public static Map<String, GeoInfo> customGeojson = new HashMap<>();

    static {
        List<String> en = Lists.newArrayList("en", "English");
        List<String> zh = Lists.newArrayList("zh", "Chinese");
        availableLocales.add(en);
        availableLocales.add(zh);

        GeoInfo statesGeo = new GeoInfo();
        statesGeo.setName("United States");
        statesGeo.setBuiltin(true);
        statesGeo.setRegionKey("STATE");
        statesGeo.setRegionName("NAME");
        statesGeo.setUrl("app/assets/geojson/us-states.json");
        customGeojson.put("us_states", statesGeo);

        GeoInfo countryGeo = new GeoInfo();
        countryGeo.setName("World");
        countryGeo.setBuiltin(true);
        countryGeo.setRegionKey("ISO_A2");
        countryGeo.setRegionName("NAME");
        countryGeo.setUrl("app/assets/geojson/world.json");
        customGeojson.put("world_countries", countryGeo);
    }
}
