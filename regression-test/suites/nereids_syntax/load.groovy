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

suite("load") {
    def tables = ["customer", "lineorder", "part", "dates", "supplier"]

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text
        sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    }

    sql """
        INSERT INTO customer VALUES
        (1303, "Customer#000001303", "fQ Lp,FoozZe1", "ETHIOPIA 3", "ETHIOPIA", "AFRICA", "15-658-234-7985", "MACHINERY"),
        (1309, "Customer#000001309", "vQcJGUXPHMH2 5OWs1XUP0kx", "IRAN     2", "IRAN", "MIDDLE EAST", "20-821-905-5952", "AUTOMOBILE"),
        (1312, "Customer#000001312", "MVsKeqWejff8jQ30", "CANADA   9", "CANADA", "AMERICA", "13-153-492-9898", "BUILDING");
    """

    sql """
        INSERT INTO dates VALUES
        (19920410, "April 10, 1992", "Saturday", "April", 1992, 199204, "Apr1992", 7, 10, 101, 4, 15, "Spring", 1, 1, 0, 0),
        (19920411, "April 11, 1992", "Sunday", "April", 1992, 199204, "Apr1992", 1, 11, 102, 4, 15, "Spring", 0, 1, 0, 0),
        (19920412, "April 12, 1992", "Monday", "April", 1992, 199204, "Apr1992", 2, 12, 103, 4, 15, "Spring", 0, 1, 0, 1);
    """

    sql """
        INSERT INTO part VALUES
        (1165, "linen midnight", "MFGR#3", "MFGR#31", "MFGR#3117", "seashell", "MEDIUM BURNISHED NICKEL", 1, "LG PACK"),
        (1432, "metallic bisque", "MFGR#3", "MFGR#35", "MFGR#352", "aquamarine", "LARGE BURNISHED TIN", 21, "MED PKG"),
        (1455, "magenta blush", "MFGR#1", "MFGR#13", "MFGR#1324", "blue", "LARGE BRUSHED STEEL", 42, "SM PACK");
    """

    sql """
        INSERT INTO supplier VALUES
        (9, "Supplier#000000009", ",gJ6K2MKveYxQT", "IRAN     6", "IRAN", "MIDDLE EAST", "20-338-906-3675"),
        (15, "Supplier#000000015", "DF35PepL5saAK", "INDIA    0", "INDIA", "ASIA", "18-687-542-7601"),
        (29, "Supplier#000000029", "VVSymB3fbwaN", "ARGENTINA4", "ARGENTINA", "AMERICA", "11-773-203-7342");
    """

    sql """
        INSERT INTO lineorder VALUES
        (1309892, 2, 1303, 1165, 9, 19920517, "3-MEDIUM", 0, 21, 2404899, 5119906, 8, 2212507, 68711, 7, 19920616, "RAIL"),
        (1309892, 1, 1303, 1432, 15, 19920517, "3-MEDIUM", 0, 24, 2959704, 5119906, 7, 2752524, 73992, 0, 19920619, "TRUCK"),
        (1310179, 6, 1312, 1455, 29, 19921110, "3-MEDIUM", 0, 15, 1705830, 20506457, 10, 1535247, 68233, 8, 19930114, "FOB");
    """
}
