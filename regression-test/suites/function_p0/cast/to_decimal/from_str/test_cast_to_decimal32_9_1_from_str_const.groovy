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


suite("test_cast_to_decimal32_9_1_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_1186 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 1));"""
    qt_sql_1186_strict "${const_sql_1186}"
    testFoldConst("${const_sql_1186}")
    def const_sql_1187 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 1));"""
    qt_sql_1187_strict "${const_sql_1187}"
    testFoldConst("${const_sql_1187}")
    def const_sql_1188 = """select cast("0" as decimalv3(9, 1));"""
    qt_sql_1188_strict "${const_sql_1188}"
    testFoldConst("${const_sql_1188}")
    def const_sql_1189 = """select cast("1" as decimalv3(9, 1));"""
    qt_sql_1189_strict "${const_sql_1189}"
    testFoldConst("${const_sql_1189}")
    def const_sql_1190 = """select cast("9" as decimalv3(9, 1));"""
    qt_sql_1190_strict "${const_sql_1190}"
    testFoldConst("${const_sql_1190}")
    def const_sql_1191 = """select cast("9999999" as decimalv3(9, 1));"""
    qt_sql_1191_strict "${const_sql_1191}"
    testFoldConst("${const_sql_1191}")
    def const_sql_1192 = """select cast("90000000" as decimalv3(9, 1));"""
    qt_sql_1192_strict "${const_sql_1192}"
    testFoldConst("${const_sql_1192}")
    def const_sql_1193 = """select cast("90000001" as decimalv3(9, 1));"""
    qt_sql_1193_strict "${const_sql_1193}"
    testFoldConst("${const_sql_1193}")
    def const_sql_1194 = """select cast("99999998" as decimalv3(9, 1));"""
    qt_sql_1194_strict "${const_sql_1194}"
    testFoldConst("${const_sql_1194}")
    def const_sql_1195 = """select cast("99999999" as decimalv3(9, 1));"""
    qt_sql_1195_strict "${const_sql_1195}"
    testFoldConst("${const_sql_1195}")
    def const_sql_1196 = """select cast("0." as decimalv3(9, 1));"""
    qt_sql_1196_strict "${const_sql_1196}"
    testFoldConst("${const_sql_1196}")
    def const_sql_1197 = """select cast("1." as decimalv3(9, 1));"""
    qt_sql_1197_strict "${const_sql_1197}"
    testFoldConst("${const_sql_1197}")
    def const_sql_1198 = """select cast("9." as decimalv3(9, 1));"""
    qt_sql_1198_strict "${const_sql_1198}"
    testFoldConst("${const_sql_1198}")
    def const_sql_1199 = """select cast("9999999." as decimalv3(9, 1));"""
    qt_sql_1199_strict "${const_sql_1199}"
    testFoldConst("${const_sql_1199}")
    def const_sql_1200 = """select cast("90000000." as decimalv3(9, 1));"""
    qt_sql_1200_strict "${const_sql_1200}"
    testFoldConst("${const_sql_1200}")
    def const_sql_1201 = """select cast("90000001." as decimalv3(9, 1));"""
    qt_sql_1201_strict "${const_sql_1201}"
    testFoldConst("${const_sql_1201}")
    def const_sql_1202 = """select cast("99999998." as decimalv3(9, 1));"""
    qt_sql_1202_strict "${const_sql_1202}"
    testFoldConst("${const_sql_1202}")
    def const_sql_1203 = """select cast("99999999." as decimalv3(9, 1));"""
    qt_sql_1203_strict "${const_sql_1203}"
    testFoldConst("${const_sql_1203}")
    def const_sql_1204 = """select cast("-0" as decimalv3(9, 1));"""
    qt_sql_1204_strict "${const_sql_1204}"
    testFoldConst("${const_sql_1204}")
    def const_sql_1205 = """select cast("-1" as decimalv3(9, 1));"""
    qt_sql_1205_strict "${const_sql_1205}"
    testFoldConst("${const_sql_1205}")
    def const_sql_1206 = """select cast("-9" as decimalv3(9, 1));"""
    qt_sql_1206_strict "${const_sql_1206}"
    testFoldConst("${const_sql_1206}")
    def const_sql_1207 = """select cast("-9999999" as decimalv3(9, 1));"""
    qt_sql_1207_strict "${const_sql_1207}"
    testFoldConst("${const_sql_1207}")
    def const_sql_1208 = """select cast("-90000000" as decimalv3(9, 1));"""
    qt_sql_1208_strict "${const_sql_1208}"
    testFoldConst("${const_sql_1208}")
    def const_sql_1209 = """select cast("-90000001" as decimalv3(9, 1));"""
    qt_sql_1209_strict "${const_sql_1209}"
    testFoldConst("${const_sql_1209}")
    def const_sql_1210 = """select cast("-99999998" as decimalv3(9, 1));"""
    qt_sql_1210_strict "${const_sql_1210}"
    testFoldConst("${const_sql_1210}")
    def const_sql_1211 = """select cast("-99999999" as decimalv3(9, 1));"""
    qt_sql_1211_strict "${const_sql_1211}"
    testFoldConst("${const_sql_1211}")
    def const_sql_1212 = """select cast("-0." as decimalv3(9, 1));"""
    qt_sql_1212_strict "${const_sql_1212}"
    testFoldConst("${const_sql_1212}")
    def const_sql_1213 = """select cast("-1." as decimalv3(9, 1));"""
    qt_sql_1213_strict "${const_sql_1213}"
    testFoldConst("${const_sql_1213}")
    def const_sql_1214 = """select cast("-9." as decimalv3(9, 1));"""
    qt_sql_1214_strict "${const_sql_1214}"
    testFoldConst("${const_sql_1214}")
    def const_sql_1215 = """select cast("-9999999." as decimalv3(9, 1));"""
    qt_sql_1215_strict "${const_sql_1215}"
    testFoldConst("${const_sql_1215}")
    def const_sql_1216 = """select cast("-90000000." as decimalv3(9, 1));"""
    qt_sql_1216_strict "${const_sql_1216}"
    testFoldConst("${const_sql_1216}")
    def const_sql_1217 = """select cast("-90000001." as decimalv3(9, 1));"""
    qt_sql_1217_strict "${const_sql_1217}"
    testFoldConst("${const_sql_1217}")
    def const_sql_1218 = """select cast("-99999998." as decimalv3(9, 1));"""
    qt_sql_1218_strict "${const_sql_1218}"
    testFoldConst("${const_sql_1218}")
    def const_sql_1219 = """select cast("-99999999." as decimalv3(9, 1));"""
    qt_sql_1219_strict "${const_sql_1219}"
    testFoldConst("${const_sql_1219}")
    def const_sql_1220 = """select cast(".04" as decimalv3(9, 1));"""
    qt_sql_1220_strict "${const_sql_1220}"
    testFoldConst("${const_sql_1220}")
    def const_sql_1221 = """select cast(".14" as decimalv3(9, 1));"""
    qt_sql_1221_strict "${const_sql_1221}"
    testFoldConst("${const_sql_1221}")
    def const_sql_1222 = """select cast(".84" as decimalv3(9, 1));"""
    qt_sql_1222_strict "${const_sql_1222}"
    testFoldConst("${const_sql_1222}")
    def const_sql_1223 = """select cast(".94" as decimalv3(9, 1));"""
    qt_sql_1223_strict "${const_sql_1223}"
    testFoldConst("${const_sql_1223}")
    def const_sql_1224 = """select cast(".05" as decimalv3(9, 1));"""
    qt_sql_1224_strict "${const_sql_1224}"
    testFoldConst("${const_sql_1224}")
    def const_sql_1225 = """select cast(".15" as decimalv3(9, 1));"""
    qt_sql_1225_strict "${const_sql_1225}"
    testFoldConst("${const_sql_1225}")
    def const_sql_1226 = """select cast(".85" as decimalv3(9, 1));"""
    qt_sql_1226_strict "${const_sql_1226}"
    testFoldConst("${const_sql_1226}")
    def const_sql_1227 = """select cast(".94" as decimalv3(9, 1));"""
    qt_sql_1227_strict "${const_sql_1227}"
    testFoldConst("${const_sql_1227}")
    def const_sql_1228 = """select cast("-.04" as decimalv3(9, 1));"""
    qt_sql_1228_strict "${const_sql_1228}"
    testFoldConst("${const_sql_1228}")
    def const_sql_1229 = """select cast("-.14" as decimalv3(9, 1));"""
    qt_sql_1229_strict "${const_sql_1229}"
    testFoldConst("${const_sql_1229}")
    def const_sql_1230 = """select cast("-.84" as decimalv3(9, 1));"""
    qt_sql_1230_strict "${const_sql_1230}"
    testFoldConst("${const_sql_1230}")
    def const_sql_1231 = """select cast("-.94" as decimalv3(9, 1));"""
    qt_sql_1231_strict "${const_sql_1231}"
    testFoldConst("${const_sql_1231}")
    def const_sql_1232 = """select cast("-.05" as decimalv3(9, 1));"""
    qt_sql_1232_strict "${const_sql_1232}"
    testFoldConst("${const_sql_1232}")
    def const_sql_1233 = """select cast("-.15" as decimalv3(9, 1));"""
    qt_sql_1233_strict "${const_sql_1233}"
    testFoldConst("${const_sql_1233}")
    def const_sql_1234 = """select cast("-.85" as decimalv3(9, 1));"""
    qt_sql_1234_strict "${const_sql_1234}"
    testFoldConst("${const_sql_1234}")
    def const_sql_1235 = """select cast("-.94" as decimalv3(9, 1));"""
    qt_sql_1235_strict "${const_sql_1235}"
    testFoldConst("${const_sql_1235}")
    def const_sql_1236 = """select cast("00400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1236_strict "${const_sql_1236}"
    testFoldConst("${const_sql_1236}")
    def const_sql_1237 = """select cast("01400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1237_strict "${const_sql_1237}"
    testFoldConst("${const_sql_1237}")
    def const_sql_1238 = """select cast("08400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1238_strict "${const_sql_1238}"
    testFoldConst("${const_sql_1238}")
    def const_sql_1239 = """select cast("09400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1239_strict "${const_sql_1239}"
    testFoldConst("${const_sql_1239}")
    def const_sql_1240 = """select cast("10400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1240_strict "${const_sql_1240}"
    testFoldConst("${const_sql_1240}")
    def const_sql_1241 = """select cast("11400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1241_strict "${const_sql_1241}"
    testFoldConst("${const_sql_1241}")
    def const_sql_1242 = """select cast("18400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1242_strict "${const_sql_1242}"
    testFoldConst("${const_sql_1242}")
    def const_sql_1243 = """select cast("19400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1243_strict "${const_sql_1243}"
    testFoldConst("${const_sql_1243}")
    def const_sql_1244 = """select cast("90400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1244_strict "${const_sql_1244}"
    testFoldConst("${const_sql_1244}")
    def const_sql_1245 = """select cast("91400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1245_strict "${const_sql_1245}"
    testFoldConst("${const_sql_1245}")
    def const_sql_1246 = """select cast("98400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1246_strict "${const_sql_1246}"
    testFoldConst("${const_sql_1246}")
    def const_sql_1247 = """select cast("99400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1247_strict "${const_sql_1247}"
    testFoldConst("${const_sql_1247}")
    def const_sql_1248 = """select cast("99999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1248_strict "${const_sql_1248}"
    testFoldConst("${const_sql_1248}")
    def const_sql_1249 = """select cast("99999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1249_strict "${const_sql_1249}"
    testFoldConst("${const_sql_1249}")
    def const_sql_1250 = """select cast("99999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1250_strict "${const_sql_1250}"
    testFoldConst("${const_sql_1250}")
    def const_sql_1251 = """select cast("99999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1251_strict "${const_sql_1251}"
    testFoldConst("${const_sql_1251}")
    def const_sql_1252 = """select cast("900000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1252_strict "${const_sql_1252}"
    testFoldConst("${const_sql_1252}")
    def const_sql_1253 = """select cast("900000001400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1253_strict "${const_sql_1253}"
    testFoldConst("${const_sql_1253}")
    def const_sql_1254 = """select cast("900000008400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1254_strict "${const_sql_1254}"
    testFoldConst("${const_sql_1254}")
    def const_sql_1255 = """select cast("900000009400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1255_strict "${const_sql_1255}"
    testFoldConst("${const_sql_1255}")
    def const_sql_1256 = """select cast("900000010400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1256_strict "${const_sql_1256}"
    testFoldConst("${const_sql_1256}")
    def const_sql_1257 = """select cast("900000011400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1257_strict "${const_sql_1257}"
    testFoldConst("${const_sql_1257}")
    def const_sql_1258 = """select cast("900000018400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1258_strict "${const_sql_1258}"
    testFoldConst("${const_sql_1258}")
    def const_sql_1259 = """select cast("900000019400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1259_strict "${const_sql_1259}"
    testFoldConst("${const_sql_1259}")
    def const_sql_1260 = """select cast("999999980400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1260_strict "${const_sql_1260}"
    testFoldConst("${const_sql_1260}")
    def const_sql_1261 = """select cast("999999981400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1261_strict "${const_sql_1261}"
    testFoldConst("${const_sql_1261}")
    def const_sql_1262 = """select cast("999999988400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1262_strict "${const_sql_1262}"
    testFoldConst("${const_sql_1262}")
    def const_sql_1263 = """select cast("999999989400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1263_strict "${const_sql_1263}"
    testFoldConst("${const_sql_1263}")
    def const_sql_1264 = """select cast("999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1264_strict "${const_sql_1264}"
    testFoldConst("${const_sql_1264}")
    def const_sql_1265 = """select cast("999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1265_strict "${const_sql_1265}"
    testFoldConst("${const_sql_1265}")
    def const_sql_1266 = """select cast("999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1266_strict "${const_sql_1266}"
    testFoldConst("${const_sql_1266}")
    def const_sql_1267 = """select cast("999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1267_strict "${const_sql_1267}"
    testFoldConst("${const_sql_1267}")
    def const_sql_1268 = """select cast("00500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1268_strict "${const_sql_1268}"
    testFoldConst("${const_sql_1268}")
    def const_sql_1269 = """select cast("01500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1269_strict "${const_sql_1269}"
    testFoldConst("${const_sql_1269}")
    def const_sql_1270 = """select cast("08500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1270_strict "${const_sql_1270}"
    testFoldConst("${const_sql_1270}")
    def const_sql_1271 = """select cast("09500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1271_strict "${const_sql_1271}"
    testFoldConst("${const_sql_1271}")
    def const_sql_1272 = """select cast("10500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1272_strict "${const_sql_1272}"
    testFoldConst("${const_sql_1272}")
    def const_sql_1273 = """select cast("11500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1273_strict "${const_sql_1273}"
    testFoldConst("${const_sql_1273}")
    def const_sql_1274 = """select cast("18500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1274_strict "${const_sql_1274}"
    testFoldConst("${const_sql_1274}")
    def const_sql_1275 = """select cast("19500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1275_strict "${const_sql_1275}"
    testFoldConst("${const_sql_1275}")
    def const_sql_1276 = """select cast("90500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1276_strict "${const_sql_1276}"
    testFoldConst("${const_sql_1276}")
    def const_sql_1277 = """select cast("91500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1277_strict "${const_sql_1277}"
    testFoldConst("${const_sql_1277}")
    def const_sql_1278 = """select cast("98500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1278_strict "${const_sql_1278}"
    testFoldConst("${const_sql_1278}")
    def const_sql_1279 = """select cast("99500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1279_strict "${const_sql_1279}"
    testFoldConst("${const_sql_1279}")
    def const_sql_1280 = """select cast("99999990500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1280_strict "${const_sql_1280}"
    testFoldConst("${const_sql_1280}")
    def const_sql_1281 = """select cast("99999991500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1281_strict "${const_sql_1281}"
    testFoldConst("${const_sql_1281}")
    def const_sql_1282 = """select cast("99999998500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1282_strict "${const_sql_1282}"
    testFoldConst("${const_sql_1282}")
    def const_sql_1283 = """select cast("99999999500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1283_strict "${const_sql_1283}"
    testFoldConst("${const_sql_1283}")
    def const_sql_1284 = """select cast("900000000500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1284_strict "${const_sql_1284}"
    testFoldConst("${const_sql_1284}")
    def const_sql_1285 = """select cast("900000001500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1285_strict "${const_sql_1285}"
    testFoldConst("${const_sql_1285}")
    def const_sql_1286 = """select cast("900000008500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1286_strict "${const_sql_1286}"
    testFoldConst("${const_sql_1286}")
    def const_sql_1287 = """select cast("900000009500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1287_strict "${const_sql_1287}"
    testFoldConst("${const_sql_1287}")
    def const_sql_1288 = """select cast("900000010500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1288_strict "${const_sql_1288}"
    testFoldConst("${const_sql_1288}")
    def const_sql_1289 = """select cast("900000011500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1289_strict "${const_sql_1289}"
    testFoldConst("${const_sql_1289}")
    def const_sql_1290 = """select cast("900000018500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1290_strict "${const_sql_1290}"
    testFoldConst("${const_sql_1290}")
    def const_sql_1291 = """select cast("900000019500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1291_strict "${const_sql_1291}"
    testFoldConst("${const_sql_1291}")
    def const_sql_1292 = """select cast("999999980500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1292_strict "${const_sql_1292}"
    testFoldConst("${const_sql_1292}")
    def const_sql_1293 = """select cast("999999981500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1293_strict "${const_sql_1293}"
    testFoldConst("${const_sql_1293}")
    def const_sql_1294 = """select cast("999999988500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1294_strict "${const_sql_1294}"
    testFoldConst("${const_sql_1294}")
    def const_sql_1295 = """select cast("999999989500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1295_strict "${const_sql_1295}"
    testFoldConst("${const_sql_1295}")
    def const_sql_1296 = """select cast("999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1296_strict "${const_sql_1296}"
    testFoldConst("${const_sql_1296}")
    def const_sql_1297 = """select cast("999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1297_strict "${const_sql_1297}"
    testFoldConst("${const_sql_1297}")
    def const_sql_1298 = """select cast("999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1298_strict "${const_sql_1298}"
    testFoldConst("${const_sql_1298}")
    def const_sql_1299 = """select cast("999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1299_strict "${const_sql_1299}"
    testFoldConst("${const_sql_1299}")
    def const_sql_1300 = """select cast("-00400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1300_strict "${const_sql_1300}"
    testFoldConst("${const_sql_1300}")
    def const_sql_1301 = """select cast("-01400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1301_strict "${const_sql_1301}"
    testFoldConst("${const_sql_1301}")
    def const_sql_1302 = """select cast("-08400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1302_strict "${const_sql_1302}"
    testFoldConst("${const_sql_1302}")
    def const_sql_1303 = """select cast("-09400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1303_strict "${const_sql_1303}"
    testFoldConst("${const_sql_1303}")
    def const_sql_1304 = """select cast("-10400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1304_strict "${const_sql_1304}"
    testFoldConst("${const_sql_1304}")
    def const_sql_1305 = """select cast("-11400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1305_strict "${const_sql_1305}"
    testFoldConst("${const_sql_1305}")
    def const_sql_1306 = """select cast("-18400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1306_strict "${const_sql_1306}"
    testFoldConst("${const_sql_1306}")
    def const_sql_1307 = """select cast("-19400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1307_strict "${const_sql_1307}"
    testFoldConst("${const_sql_1307}")
    def const_sql_1308 = """select cast("-90400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1308_strict "${const_sql_1308}"
    testFoldConst("${const_sql_1308}")
    def const_sql_1309 = """select cast("-91400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1309_strict "${const_sql_1309}"
    testFoldConst("${const_sql_1309}")
    def const_sql_1310 = """select cast("-98400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1310_strict "${const_sql_1310}"
    testFoldConst("${const_sql_1310}")
    def const_sql_1311 = """select cast("-99400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1311_strict "${const_sql_1311}"
    testFoldConst("${const_sql_1311}")
    def const_sql_1312 = """select cast("-99999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1312_strict "${const_sql_1312}"
    testFoldConst("${const_sql_1312}")
    def const_sql_1313 = """select cast("-99999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1313_strict "${const_sql_1313}"
    testFoldConst("${const_sql_1313}")
    def const_sql_1314 = """select cast("-99999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1314_strict "${const_sql_1314}"
    testFoldConst("${const_sql_1314}")
    def const_sql_1315 = """select cast("-99999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1315_strict "${const_sql_1315}"
    testFoldConst("${const_sql_1315}")
    def const_sql_1316 = """select cast("-900000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1316_strict "${const_sql_1316}"
    testFoldConst("${const_sql_1316}")
    def const_sql_1317 = """select cast("-900000001400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1317_strict "${const_sql_1317}"
    testFoldConst("${const_sql_1317}")
    def const_sql_1318 = """select cast("-900000008400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1318_strict "${const_sql_1318}"
    testFoldConst("${const_sql_1318}")
    def const_sql_1319 = """select cast("-900000009400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1319_strict "${const_sql_1319}"
    testFoldConst("${const_sql_1319}")
    def const_sql_1320 = """select cast("-900000010400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1320_strict "${const_sql_1320}"
    testFoldConst("${const_sql_1320}")
    def const_sql_1321 = """select cast("-900000011400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1321_strict "${const_sql_1321}"
    testFoldConst("${const_sql_1321}")
    def const_sql_1322 = """select cast("-900000018400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1322_strict "${const_sql_1322}"
    testFoldConst("${const_sql_1322}")
    def const_sql_1323 = """select cast("-900000019400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1323_strict "${const_sql_1323}"
    testFoldConst("${const_sql_1323}")
    def const_sql_1324 = """select cast("-999999980400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1324_strict "${const_sql_1324}"
    testFoldConst("${const_sql_1324}")
    def const_sql_1325 = """select cast("-999999981400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1325_strict "${const_sql_1325}"
    testFoldConst("${const_sql_1325}")
    def const_sql_1326 = """select cast("-999999988400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1326_strict "${const_sql_1326}"
    testFoldConst("${const_sql_1326}")
    def const_sql_1327 = """select cast("-999999989400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1327_strict "${const_sql_1327}"
    testFoldConst("${const_sql_1327}")
    def const_sql_1328 = """select cast("-999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1328_strict "${const_sql_1328}"
    testFoldConst("${const_sql_1328}")
    def const_sql_1329 = """select cast("-999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1329_strict "${const_sql_1329}"
    testFoldConst("${const_sql_1329}")
    def const_sql_1330 = """select cast("-999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1330_strict "${const_sql_1330}"
    testFoldConst("${const_sql_1330}")
    def const_sql_1331 = """select cast("-999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1331_strict "${const_sql_1331}"
    testFoldConst("${const_sql_1331}")
    def const_sql_1332 = """select cast("-00500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1332_strict "${const_sql_1332}"
    testFoldConst("${const_sql_1332}")
    def const_sql_1333 = """select cast("-01500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1333_strict "${const_sql_1333}"
    testFoldConst("${const_sql_1333}")
    def const_sql_1334 = """select cast("-08500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1334_strict "${const_sql_1334}"
    testFoldConst("${const_sql_1334}")
    def const_sql_1335 = """select cast("-09500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1335_strict "${const_sql_1335}"
    testFoldConst("${const_sql_1335}")
    def const_sql_1336 = """select cast("-10500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1336_strict "${const_sql_1336}"
    testFoldConst("${const_sql_1336}")
    def const_sql_1337 = """select cast("-11500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1337_strict "${const_sql_1337}"
    testFoldConst("${const_sql_1337}")
    def const_sql_1338 = """select cast("-18500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1338_strict "${const_sql_1338}"
    testFoldConst("${const_sql_1338}")
    def const_sql_1339 = """select cast("-19500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1339_strict "${const_sql_1339}"
    testFoldConst("${const_sql_1339}")
    def const_sql_1340 = """select cast("-90500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1340_strict "${const_sql_1340}"
    testFoldConst("${const_sql_1340}")
    def const_sql_1341 = """select cast("-91500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1341_strict "${const_sql_1341}"
    testFoldConst("${const_sql_1341}")
    def const_sql_1342 = """select cast("-98500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1342_strict "${const_sql_1342}"
    testFoldConst("${const_sql_1342}")
    def const_sql_1343 = """select cast("-99500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1343_strict "${const_sql_1343}"
    testFoldConst("${const_sql_1343}")
    def const_sql_1344 = """select cast("-99999990500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1344_strict "${const_sql_1344}"
    testFoldConst("${const_sql_1344}")
    def const_sql_1345 = """select cast("-99999991500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1345_strict "${const_sql_1345}"
    testFoldConst("${const_sql_1345}")
    def const_sql_1346 = """select cast("-99999998500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1346_strict "${const_sql_1346}"
    testFoldConst("${const_sql_1346}")
    def const_sql_1347 = """select cast("-99999999500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1347_strict "${const_sql_1347}"
    testFoldConst("${const_sql_1347}")
    def const_sql_1348 = """select cast("-900000000500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1348_strict "${const_sql_1348}"
    testFoldConst("${const_sql_1348}")
    def const_sql_1349 = """select cast("-900000001500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1349_strict "${const_sql_1349}"
    testFoldConst("${const_sql_1349}")
    def const_sql_1350 = """select cast("-900000008500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1350_strict "${const_sql_1350}"
    testFoldConst("${const_sql_1350}")
    def const_sql_1351 = """select cast("-900000009500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1351_strict "${const_sql_1351}"
    testFoldConst("${const_sql_1351}")
    def const_sql_1352 = """select cast("-900000010500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1352_strict "${const_sql_1352}"
    testFoldConst("${const_sql_1352}")
    def const_sql_1353 = """select cast("-900000011500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1353_strict "${const_sql_1353}"
    testFoldConst("${const_sql_1353}")
    def const_sql_1354 = """select cast("-900000018500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1354_strict "${const_sql_1354}"
    testFoldConst("${const_sql_1354}")
    def const_sql_1355 = """select cast("-900000019500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1355_strict "${const_sql_1355}"
    testFoldConst("${const_sql_1355}")
    def const_sql_1356 = """select cast("-999999980500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1356_strict "${const_sql_1356}"
    testFoldConst("${const_sql_1356}")
    def const_sql_1357 = """select cast("-999999981500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1357_strict "${const_sql_1357}"
    testFoldConst("${const_sql_1357}")
    def const_sql_1358 = """select cast("-999999988500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1358_strict "${const_sql_1358}"
    testFoldConst("${const_sql_1358}")
    def const_sql_1359 = """select cast("-999999989500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1359_strict "${const_sql_1359}"
    testFoldConst("${const_sql_1359}")
    def const_sql_1360 = """select cast("-999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1360_strict "${const_sql_1360}"
    testFoldConst("${const_sql_1360}")
    def const_sql_1361 = """select cast("-999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1361_strict "${const_sql_1361}"
    testFoldConst("${const_sql_1361}")
    def const_sql_1362 = """select cast("-999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1362_strict "${const_sql_1362}"
    testFoldConst("${const_sql_1362}")
    def const_sql_1363 = """select cast("-999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 1));"""
    qt_sql_1363_strict "${const_sql_1363}"
    testFoldConst("${const_sql_1363}")
    sql "set enable_strict_cast=false;"
    qt_sql_1186_non_strict "${const_sql_1186}"
    testFoldConst("${const_sql_1186}")
    qt_sql_1187_non_strict "${const_sql_1187}"
    testFoldConst("${const_sql_1187}")
    qt_sql_1188_non_strict "${const_sql_1188}"
    testFoldConst("${const_sql_1188}")
    qt_sql_1189_non_strict "${const_sql_1189}"
    testFoldConst("${const_sql_1189}")
    qt_sql_1190_non_strict "${const_sql_1190}"
    testFoldConst("${const_sql_1190}")
    qt_sql_1191_non_strict "${const_sql_1191}"
    testFoldConst("${const_sql_1191}")
    qt_sql_1192_non_strict "${const_sql_1192}"
    testFoldConst("${const_sql_1192}")
    qt_sql_1193_non_strict "${const_sql_1193}"
    testFoldConst("${const_sql_1193}")
    qt_sql_1194_non_strict "${const_sql_1194}"
    testFoldConst("${const_sql_1194}")
    qt_sql_1195_non_strict "${const_sql_1195}"
    testFoldConst("${const_sql_1195}")
    qt_sql_1196_non_strict "${const_sql_1196}"
    testFoldConst("${const_sql_1196}")
    qt_sql_1197_non_strict "${const_sql_1197}"
    testFoldConst("${const_sql_1197}")
    qt_sql_1198_non_strict "${const_sql_1198}"
    testFoldConst("${const_sql_1198}")
    qt_sql_1199_non_strict "${const_sql_1199}"
    testFoldConst("${const_sql_1199}")
    qt_sql_1200_non_strict "${const_sql_1200}"
    testFoldConst("${const_sql_1200}")
    qt_sql_1201_non_strict "${const_sql_1201}"
    testFoldConst("${const_sql_1201}")
    qt_sql_1202_non_strict "${const_sql_1202}"
    testFoldConst("${const_sql_1202}")
    qt_sql_1203_non_strict "${const_sql_1203}"
    testFoldConst("${const_sql_1203}")
    qt_sql_1204_non_strict "${const_sql_1204}"
    testFoldConst("${const_sql_1204}")
    qt_sql_1205_non_strict "${const_sql_1205}"
    testFoldConst("${const_sql_1205}")
    qt_sql_1206_non_strict "${const_sql_1206}"
    testFoldConst("${const_sql_1206}")
    qt_sql_1207_non_strict "${const_sql_1207}"
    testFoldConst("${const_sql_1207}")
    qt_sql_1208_non_strict "${const_sql_1208}"
    testFoldConst("${const_sql_1208}")
    qt_sql_1209_non_strict "${const_sql_1209}"
    testFoldConst("${const_sql_1209}")
    qt_sql_1210_non_strict "${const_sql_1210}"
    testFoldConst("${const_sql_1210}")
    qt_sql_1211_non_strict "${const_sql_1211}"
    testFoldConst("${const_sql_1211}")
    qt_sql_1212_non_strict "${const_sql_1212}"
    testFoldConst("${const_sql_1212}")
    qt_sql_1213_non_strict "${const_sql_1213}"
    testFoldConst("${const_sql_1213}")
    qt_sql_1214_non_strict "${const_sql_1214}"
    testFoldConst("${const_sql_1214}")
    qt_sql_1215_non_strict "${const_sql_1215}"
    testFoldConst("${const_sql_1215}")
    qt_sql_1216_non_strict "${const_sql_1216}"
    testFoldConst("${const_sql_1216}")
    qt_sql_1217_non_strict "${const_sql_1217}"
    testFoldConst("${const_sql_1217}")
    qt_sql_1218_non_strict "${const_sql_1218}"
    testFoldConst("${const_sql_1218}")
    qt_sql_1219_non_strict "${const_sql_1219}"
    testFoldConst("${const_sql_1219}")
    qt_sql_1220_non_strict "${const_sql_1220}"
    testFoldConst("${const_sql_1220}")
    qt_sql_1221_non_strict "${const_sql_1221}"
    testFoldConst("${const_sql_1221}")
    qt_sql_1222_non_strict "${const_sql_1222}"
    testFoldConst("${const_sql_1222}")
    qt_sql_1223_non_strict "${const_sql_1223}"
    testFoldConst("${const_sql_1223}")
    qt_sql_1224_non_strict "${const_sql_1224}"
    testFoldConst("${const_sql_1224}")
    qt_sql_1225_non_strict "${const_sql_1225}"
    testFoldConst("${const_sql_1225}")
    qt_sql_1226_non_strict "${const_sql_1226}"
    testFoldConst("${const_sql_1226}")
    qt_sql_1227_non_strict "${const_sql_1227}"
    testFoldConst("${const_sql_1227}")
    qt_sql_1228_non_strict "${const_sql_1228}"
    testFoldConst("${const_sql_1228}")
    qt_sql_1229_non_strict "${const_sql_1229}"
    testFoldConst("${const_sql_1229}")
    qt_sql_1230_non_strict "${const_sql_1230}"
    testFoldConst("${const_sql_1230}")
    qt_sql_1231_non_strict "${const_sql_1231}"
    testFoldConst("${const_sql_1231}")
    qt_sql_1232_non_strict "${const_sql_1232}"
    testFoldConst("${const_sql_1232}")
    qt_sql_1233_non_strict "${const_sql_1233}"
    testFoldConst("${const_sql_1233}")
    qt_sql_1234_non_strict "${const_sql_1234}"
    testFoldConst("${const_sql_1234}")
    qt_sql_1235_non_strict "${const_sql_1235}"
    testFoldConst("${const_sql_1235}")
    qt_sql_1236_non_strict "${const_sql_1236}"
    testFoldConst("${const_sql_1236}")
    qt_sql_1237_non_strict "${const_sql_1237}"
    testFoldConst("${const_sql_1237}")
    qt_sql_1238_non_strict "${const_sql_1238}"
    testFoldConst("${const_sql_1238}")
    qt_sql_1239_non_strict "${const_sql_1239}"
    testFoldConst("${const_sql_1239}")
    qt_sql_1240_non_strict "${const_sql_1240}"
    testFoldConst("${const_sql_1240}")
    qt_sql_1241_non_strict "${const_sql_1241}"
    testFoldConst("${const_sql_1241}")
    qt_sql_1242_non_strict "${const_sql_1242}"
    testFoldConst("${const_sql_1242}")
    qt_sql_1243_non_strict "${const_sql_1243}"
    testFoldConst("${const_sql_1243}")
    qt_sql_1244_non_strict "${const_sql_1244}"
    testFoldConst("${const_sql_1244}")
    qt_sql_1245_non_strict "${const_sql_1245}"
    testFoldConst("${const_sql_1245}")
    qt_sql_1246_non_strict "${const_sql_1246}"
    testFoldConst("${const_sql_1246}")
    qt_sql_1247_non_strict "${const_sql_1247}"
    testFoldConst("${const_sql_1247}")
    qt_sql_1248_non_strict "${const_sql_1248}"
    testFoldConst("${const_sql_1248}")
    qt_sql_1249_non_strict "${const_sql_1249}"
    testFoldConst("${const_sql_1249}")
    qt_sql_1250_non_strict "${const_sql_1250}"
    testFoldConst("${const_sql_1250}")
    qt_sql_1251_non_strict "${const_sql_1251}"
    testFoldConst("${const_sql_1251}")
    qt_sql_1252_non_strict "${const_sql_1252}"
    testFoldConst("${const_sql_1252}")
    qt_sql_1253_non_strict "${const_sql_1253}"
    testFoldConst("${const_sql_1253}")
    qt_sql_1254_non_strict "${const_sql_1254}"
    testFoldConst("${const_sql_1254}")
    qt_sql_1255_non_strict "${const_sql_1255}"
    testFoldConst("${const_sql_1255}")
    qt_sql_1256_non_strict "${const_sql_1256}"
    testFoldConst("${const_sql_1256}")
    qt_sql_1257_non_strict "${const_sql_1257}"
    testFoldConst("${const_sql_1257}")
    qt_sql_1258_non_strict "${const_sql_1258}"
    testFoldConst("${const_sql_1258}")
    qt_sql_1259_non_strict "${const_sql_1259}"
    testFoldConst("${const_sql_1259}")
    qt_sql_1260_non_strict "${const_sql_1260}"
    testFoldConst("${const_sql_1260}")
    qt_sql_1261_non_strict "${const_sql_1261}"
    testFoldConst("${const_sql_1261}")
    qt_sql_1262_non_strict "${const_sql_1262}"
    testFoldConst("${const_sql_1262}")
    qt_sql_1263_non_strict "${const_sql_1263}"
    testFoldConst("${const_sql_1263}")
    qt_sql_1264_non_strict "${const_sql_1264}"
    testFoldConst("${const_sql_1264}")
    qt_sql_1265_non_strict "${const_sql_1265}"
    testFoldConst("${const_sql_1265}")
    qt_sql_1266_non_strict "${const_sql_1266}"
    testFoldConst("${const_sql_1266}")
    qt_sql_1267_non_strict "${const_sql_1267}"
    testFoldConst("${const_sql_1267}")
    qt_sql_1268_non_strict "${const_sql_1268}"
    testFoldConst("${const_sql_1268}")
    qt_sql_1269_non_strict "${const_sql_1269}"
    testFoldConst("${const_sql_1269}")
    qt_sql_1270_non_strict "${const_sql_1270}"
    testFoldConst("${const_sql_1270}")
    qt_sql_1271_non_strict "${const_sql_1271}"
    testFoldConst("${const_sql_1271}")
    qt_sql_1272_non_strict "${const_sql_1272}"
    testFoldConst("${const_sql_1272}")
    qt_sql_1273_non_strict "${const_sql_1273}"
    testFoldConst("${const_sql_1273}")
    qt_sql_1274_non_strict "${const_sql_1274}"
    testFoldConst("${const_sql_1274}")
    qt_sql_1275_non_strict "${const_sql_1275}"
    testFoldConst("${const_sql_1275}")
    qt_sql_1276_non_strict "${const_sql_1276}"
    testFoldConst("${const_sql_1276}")
    qt_sql_1277_non_strict "${const_sql_1277}"
    testFoldConst("${const_sql_1277}")
    qt_sql_1278_non_strict "${const_sql_1278}"
    testFoldConst("${const_sql_1278}")
    qt_sql_1279_non_strict "${const_sql_1279}"
    testFoldConst("${const_sql_1279}")
    qt_sql_1280_non_strict "${const_sql_1280}"
    testFoldConst("${const_sql_1280}")
    qt_sql_1281_non_strict "${const_sql_1281}"
    testFoldConst("${const_sql_1281}")
    qt_sql_1282_non_strict "${const_sql_1282}"
    testFoldConst("${const_sql_1282}")
    qt_sql_1283_non_strict "${const_sql_1283}"
    testFoldConst("${const_sql_1283}")
    qt_sql_1284_non_strict "${const_sql_1284}"
    testFoldConst("${const_sql_1284}")
    qt_sql_1285_non_strict "${const_sql_1285}"
    testFoldConst("${const_sql_1285}")
    qt_sql_1286_non_strict "${const_sql_1286}"
    testFoldConst("${const_sql_1286}")
    qt_sql_1287_non_strict "${const_sql_1287}"
    testFoldConst("${const_sql_1287}")
    qt_sql_1288_non_strict "${const_sql_1288}"
    testFoldConst("${const_sql_1288}")
    qt_sql_1289_non_strict "${const_sql_1289}"
    testFoldConst("${const_sql_1289}")
    qt_sql_1290_non_strict "${const_sql_1290}"
    testFoldConst("${const_sql_1290}")
    qt_sql_1291_non_strict "${const_sql_1291}"
    testFoldConst("${const_sql_1291}")
    qt_sql_1292_non_strict "${const_sql_1292}"
    testFoldConst("${const_sql_1292}")
    qt_sql_1293_non_strict "${const_sql_1293}"
    testFoldConst("${const_sql_1293}")
    qt_sql_1294_non_strict "${const_sql_1294}"
    testFoldConst("${const_sql_1294}")
    qt_sql_1295_non_strict "${const_sql_1295}"
    testFoldConst("${const_sql_1295}")
    qt_sql_1296_non_strict "${const_sql_1296}"
    testFoldConst("${const_sql_1296}")
    qt_sql_1297_non_strict "${const_sql_1297}"
    testFoldConst("${const_sql_1297}")
    qt_sql_1298_non_strict "${const_sql_1298}"
    testFoldConst("${const_sql_1298}")
    qt_sql_1299_non_strict "${const_sql_1299}"
    testFoldConst("${const_sql_1299}")
    qt_sql_1300_non_strict "${const_sql_1300}"
    testFoldConst("${const_sql_1300}")
    qt_sql_1301_non_strict "${const_sql_1301}"
    testFoldConst("${const_sql_1301}")
    qt_sql_1302_non_strict "${const_sql_1302}"
    testFoldConst("${const_sql_1302}")
    qt_sql_1303_non_strict "${const_sql_1303}"
    testFoldConst("${const_sql_1303}")
    qt_sql_1304_non_strict "${const_sql_1304}"
    testFoldConst("${const_sql_1304}")
    qt_sql_1305_non_strict "${const_sql_1305}"
    testFoldConst("${const_sql_1305}")
    qt_sql_1306_non_strict "${const_sql_1306}"
    testFoldConst("${const_sql_1306}")
    qt_sql_1307_non_strict "${const_sql_1307}"
    testFoldConst("${const_sql_1307}")
    qt_sql_1308_non_strict "${const_sql_1308}"
    testFoldConst("${const_sql_1308}")
    qt_sql_1309_non_strict "${const_sql_1309}"
    testFoldConst("${const_sql_1309}")
    qt_sql_1310_non_strict "${const_sql_1310}"
    testFoldConst("${const_sql_1310}")
    qt_sql_1311_non_strict "${const_sql_1311}"
    testFoldConst("${const_sql_1311}")
    qt_sql_1312_non_strict "${const_sql_1312}"
    testFoldConst("${const_sql_1312}")
    qt_sql_1313_non_strict "${const_sql_1313}"
    testFoldConst("${const_sql_1313}")
    qt_sql_1314_non_strict "${const_sql_1314}"
    testFoldConst("${const_sql_1314}")
    qt_sql_1315_non_strict "${const_sql_1315}"
    testFoldConst("${const_sql_1315}")
    qt_sql_1316_non_strict "${const_sql_1316}"
    testFoldConst("${const_sql_1316}")
    qt_sql_1317_non_strict "${const_sql_1317}"
    testFoldConst("${const_sql_1317}")
    qt_sql_1318_non_strict "${const_sql_1318}"
    testFoldConst("${const_sql_1318}")
    qt_sql_1319_non_strict "${const_sql_1319}"
    testFoldConst("${const_sql_1319}")
    qt_sql_1320_non_strict "${const_sql_1320}"
    testFoldConst("${const_sql_1320}")
    qt_sql_1321_non_strict "${const_sql_1321}"
    testFoldConst("${const_sql_1321}")
    qt_sql_1322_non_strict "${const_sql_1322}"
    testFoldConst("${const_sql_1322}")
    qt_sql_1323_non_strict "${const_sql_1323}"
    testFoldConst("${const_sql_1323}")
    qt_sql_1324_non_strict "${const_sql_1324}"
    testFoldConst("${const_sql_1324}")
    qt_sql_1325_non_strict "${const_sql_1325}"
    testFoldConst("${const_sql_1325}")
    qt_sql_1326_non_strict "${const_sql_1326}"
    testFoldConst("${const_sql_1326}")
    qt_sql_1327_non_strict "${const_sql_1327}"
    testFoldConst("${const_sql_1327}")
    qt_sql_1328_non_strict "${const_sql_1328}"
    testFoldConst("${const_sql_1328}")
    qt_sql_1329_non_strict "${const_sql_1329}"
    testFoldConst("${const_sql_1329}")
    qt_sql_1330_non_strict "${const_sql_1330}"
    testFoldConst("${const_sql_1330}")
    qt_sql_1331_non_strict "${const_sql_1331}"
    testFoldConst("${const_sql_1331}")
    qt_sql_1332_non_strict "${const_sql_1332}"
    testFoldConst("${const_sql_1332}")
    qt_sql_1333_non_strict "${const_sql_1333}"
    testFoldConst("${const_sql_1333}")
    qt_sql_1334_non_strict "${const_sql_1334}"
    testFoldConst("${const_sql_1334}")
    qt_sql_1335_non_strict "${const_sql_1335}"
    testFoldConst("${const_sql_1335}")
    qt_sql_1336_non_strict "${const_sql_1336}"
    testFoldConst("${const_sql_1336}")
    qt_sql_1337_non_strict "${const_sql_1337}"
    testFoldConst("${const_sql_1337}")
    qt_sql_1338_non_strict "${const_sql_1338}"
    testFoldConst("${const_sql_1338}")
    qt_sql_1339_non_strict "${const_sql_1339}"
    testFoldConst("${const_sql_1339}")
    qt_sql_1340_non_strict "${const_sql_1340}"
    testFoldConst("${const_sql_1340}")
    qt_sql_1341_non_strict "${const_sql_1341}"
    testFoldConst("${const_sql_1341}")
    qt_sql_1342_non_strict "${const_sql_1342}"
    testFoldConst("${const_sql_1342}")
    qt_sql_1343_non_strict "${const_sql_1343}"
    testFoldConst("${const_sql_1343}")
    qt_sql_1344_non_strict "${const_sql_1344}"
    testFoldConst("${const_sql_1344}")
    qt_sql_1345_non_strict "${const_sql_1345}"
    testFoldConst("${const_sql_1345}")
    qt_sql_1346_non_strict "${const_sql_1346}"
    testFoldConst("${const_sql_1346}")
    qt_sql_1347_non_strict "${const_sql_1347}"
    testFoldConst("${const_sql_1347}")
    qt_sql_1348_non_strict "${const_sql_1348}"
    testFoldConst("${const_sql_1348}")
    qt_sql_1349_non_strict "${const_sql_1349}"
    testFoldConst("${const_sql_1349}")
    qt_sql_1350_non_strict "${const_sql_1350}"
    testFoldConst("${const_sql_1350}")
    qt_sql_1351_non_strict "${const_sql_1351}"
    testFoldConst("${const_sql_1351}")
    qt_sql_1352_non_strict "${const_sql_1352}"
    testFoldConst("${const_sql_1352}")
    qt_sql_1353_non_strict "${const_sql_1353}"
    testFoldConst("${const_sql_1353}")
    qt_sql_1354_non_strict "${const_sql_1354}"
    testFoldConst("${const_sql_1354}")
    qt_sql_1355_non_strict "${const_sql_1355}"
    testFoldConst("${const_sql_1355}")
    qt_sql_1356_non_strict "${const_sql_1356}"
    testFoldConst("${const_sql_1356}")
    qt_sql_1357_non_strict "${const_sql_1357}"
    testFoldConst("${const_sql_1357}")
    qt_sql_1358_non_strict "${const_sql_1358}"
    testFoldConst("${const_sql_1358}")
    qt_sql_1359_non_strict "${const_sql_1359}"
    testFoldConst("${const_sql_1359}")
    qt_sql_1360_non_strict "${const_sql_1360}"
    testFoldConst("${const_sql_1360}")
    qt_sql_1361_non_strict "${const_sql_1361}"
    testFoldConst("${const_sql_1361}")
    qt_sql_1362_non_strict "${const_sql_1362}"
    testFoldConst("${const_sql_1362}")
    qt_sql_1363_non_strict "${const_sql_1363}"
    testFoldConst("${const_sql_1363}")
}