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


suite("test_cast_to_decimal64_18_1_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_1134 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(18, 1));"""
    qt_sql_1134_strict "${const_sql_1134}"
    testFoldConst("${const_sql_1134}")
    def const_sql_1135 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(18, 1));"""
    qt_sql_1135_strict "${const_sql_1135}"
    testFoldConst("${const_sql_1135}")
    def const_sql_1136 = """select cast("0" as decimalv3(18, 1));"""
    qt_sql_1136_strict "${const_sql_1136}"
    testFoldConst("${const_sql_1136}")
    def const_sql_1137 = """select cast("1" as decimalv3(18, 1));"""
    qt_sql_1137_strict "${const_sql_1137}"
    testFoldConst("${const_sql_1137}")
    def const_sql_1138 = """select cast("9" as decimalv3(18, 1));"""
    qt_sql_1138_strict "${const_sql_1138}"
    testFoldConst("${const_sql_1138}")
    def const_sql_1139 = """select cast("9999999999999999" as decimalv3(18, 1));"""
    qt_sql_1139_strict "${const_sql_1139}"
    testFoldConst("${const_sql_1139}")
    def const_sql_1140 = """select cast("90000000000000000" as decimalv3(18, 1));"""
    qt_sql_1140_strict "${const_sql_1140}"
    testFoldConst("${const_sql_1140}")
    def const_sql_1141 = """select cast("90000000000000001" as decimalv3(18, 1));"""
    qt_sql_1141_strict "${const_sql_1141}"
    testFoldConst("${const_sql_1141}")
    def const_sql_1142 = """select cast("99999999999999998" as decimalv3(18, 1));"""
    qt_sql_1142_strict "${const_sql_1142}"
    testFoldConst("${const_sql_1142}")
    def const_sql_1143 = """select cast("99999999999999999" as decimalv3(18, 1));"""
    qt_sql_1143_strict "${const_sql_1143}"
    testFoldConst("${const_sql_1143}")
    def const_sql_1144 = """select cast("0." as decimalv3(18, 1));"""
    qt_sql_1144_strict "${const_sql_1144}"
    testFoldConst("${const_sql_1144}")
    def const_sql_1145 = """select cast("1." as decimalv3(18, 1));"""
    qt_sql_1145_strict "${const_sql_1145}"
    testFoldConst("${const_sql_1145}")
    def const_sql_1146 = """select cast("9." as decimalv3(18, 1));"""
    qt_sql_1146_strict "${const_sql_1146}"
    testFoldConst("${const_sql_1146}")
    def const_sql_1147 = """select cast("9999999999999999." as decimalv3(18, 1));"""
    qt_sql_1147_strict "${const_sql_1147}"
    testFoldConst("${const_sql_1147}")
    def const_sql_1148 = """select cast("90000000000000000." as decimalv3(18, 1));"""
    qt_sql_1148_strict "${const_sql_1148}"
    testFoldConst("${const_sql_1148}")
    def const_sql_1149 = """select cast("90000000000000001." as decimalv3(18, 1));"""
    qt_sql_1149_strict "${const_sql_1149}"
    testFoldConst("${const_sql_1149}")
    def const_sql_1150 = """select cast("99999999999999998." as decimalv3(18, 1));"""
    qt_sql_1150_strict "${const_sql_1150}"
    testFoldConst("${const_sql_1150}")
    def const_sql_1151 = """select cast("99999999999999999." as decimalv3(18, 1));"""
    qt_sql_1151_strict "${const_sql_1151}"
    testFoldConst("${const_sql_1151}")
    def const_sql_1152 = """select cast("-0" as decimalv3(18, 1));"""
    qt_sql_1152_strict "${const_sql_1152}"
    testFoldConst("${const_sql_1152}")
    def const_sql_1153 = """select cast("-1" as decimalv3(18, 1));"""
    qt_sql_1153_strict "${const_sql_1153}"
    testFoldConst("${const_sql_1153}")
    def const_sql_1154 = """select cast("-9" as decimalv3(18, 1));"""
    qt_sql_1154_strict "${const_sql_1154}"
    testFoldConst("${const_sql_1154}")
    def const_sql_1155 = """select cast("-9999999999999999" as decimalv3(18, 1));"""
    qt_sql_1155_strict "${const_sql_1155}"
    testFoldConst("${const_sql_1155}")
    def const_sql_1156 = """select cast("-90000000000000000" as decimalv3(18, 1));"""
    qt_sql_1156_strict "${const_sql_1156}"
    testFoldConst("${const_sql_1156}")
    def const_sql_1157 = """select cast("-90000000000000001" as decimalv3(18, 1));"""
    qt_sql_1157_strict "${const_sql_1157}"
    testFoldConst("${const_sql_1157}")
    def const_sql_1158 = """select cast("-99999999999999998" as decimalv3(18, 1));"""
    qt_sql_1158_strict "${const_sql_1158}"
    testFoldConst("${const_sql_1158}")
    def const_sql_1159 = """select cast("-99999999999999999" as decimalv3(18, 1));"""
    qt_sql_1159_strict "${const_sql_1159}"
    testFoldConst("${const_sql_1159}")
    def const_sql_1160 = """select cast("-0." as decimalv3(18, 1));"""
    qt_sql_1160_strict "${const_sql_1160}"
    testFoldConst("${const_sql_1160}")
    def const_sql_1161 = """select cast("-1." as decimalv3(18, 1));"""
    qt_sql_1161_strict "${const_sql_1161}"
    testFoldConst("${const_sql_1161}")
    def const_sql_1162 = """select cast("-9." as decimalv3(18, 1));"""
    qt_sql_1162_strict "${const_sql_1162}"
    testFoldConst("${const_sql_1162}")
    def const_sql_1163 = """select cast("-9999999999999999." as decimalv3(18, 1));"""
    qt_sql_1163_strict "${const_sql_1163}"
    testFoldConst("${const_sql_1163}")
    def const_sql_1164 = """select cast("-90000000000000000." as decimalv3(18, 1));"""
    qt_sql_1164_strict "${const_sql_1164}"
    testFoldConst("${const_sql_1164}")
    def const_sql_1165 = """select cast("-90000000000000001." as decimalv3(18, 1));"""
    qt_sql_1165_strict "${const_sql_1165}"
    testFoldConst("${const_sql_1165}")
    def const_sql_1166 = """select cast("-99999999999999998." as decimalv3(18, 1));"""
    qt_sql_1166_strict "${const_sql_1166}"
    testFoldConst("${const_sql_1166}")
    def const_sql_1167 = """select cast("-99999999999999999." as decimalv3(18, 1));"""
    qt_sql_1167_strict "${const_sql_1167}"
    testFoldConst("${const_sql_1167}")
    def const_sql_1168 = """select cast(".04" as decimalv3(18, 1));"""
    qt_sql_1168_strict "${const_sql_1168}"
    testFoldConst("${const_sql_1168}")
    def const_sql_1169 = """select cast(".14" as decimalv3(18, 1));"""
    qt_sql_1169_strict "${const_sql_1169}"
    testFoldConst("${const_sql_1169}")
    def const_sql_1170 = """select cast(".84" as decimalv3(18, 1));"""
    qt_sql_1170_strict "${const_sql_1170}"
    testFoldConst("${const_sql_1170}")
    def const_sql_1171 = """select cast(".94" as decimalv3(18, 1));"""
    qt_sql_1171_strict "${const_sql_1171}"
    testFoldConst("${const_sql_1171}")
    def const_sql_1172 = """select cast(".05" as decimalv3(18, 1));"""
    qt_sql_1172_strict "${const_sql_1172}"
    testFoldConst("${const_sql_1172}")
    def const_sql_1173 = """select cast(".15" as decimalv3(18, 1));"""
    qt_sql_1173_strict "${const_sql_1173}"
    testFoldConst("${const_sql_1173}")
    def const_sql_1174 = """select cast(".85" as decimalv3(18, 1));"""
    qt_sql_1174_strict "${const_sql_1174}"
    testFoldConst("${const_sql_1174}")
    def const_sql_1175 = """select cast(".94" as decimalv3(18, 1));"""
    qt_sql_1175_strict "${const_sql_1175}"
    testFoldConst("${const_sql_1175}")
    def const_sql_1176 = """select cast("-.04" as decimalv3(18, 1));"""
    qt_sql_1176_strict "${const_sql_1176}"
    testFoldConst("${const_sql_1176}")
    def const_sql_1177 = """select cast("-.14" as decimalv3(18, 1));"""
    qt_sql_1177_strict "${const_sql_1177}"
    testFoldConst("${const_sql_1177}")
    def const_sql_1178 = """select cast("-.84" as decimalv3(18, 1));"""
    qt_sql_1178_strict "${const_sql_1178}"
    testFoldConst("${const_sql_1178}")
    def const_sql_1179 = """select cast("-.94" as decimalv3(18, 1));"""
    qt_sql_1179_strict "${const_sql_1179}"
    testFoldConst("${const_sql_1179}")
    def const_sql_1180 = """select cast("-.05" as decimalv3(18, 1));"""
    qt_sql_1180_strict "${const_sql_1180}"
    testFoldConst("${const_sql_1180}")
    def const_sql_1181 = """select cast("-.15" as decimalv3(18, 1));"""
    qt_sql_1181_strict "${const_sql_1181}"
    testFoldConst("${const_sql_1181}")
    def const_sql_1182 = """select cast("-.85" as decimalv3(18, 1));"""
    qt_sql_1182_strict "${const_sql_1182}"
    testFoldConst("${const_sql_1182}")
    def const_sql_1183 = """select cast("-.94" as decimalv3(18, 1));"""
    qt_sql_1183_strict "${const_sql_1183}"
    testFoldConst("${const_sql_1183}")
    def const_sql_1184 = """select cast("00400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1184_strict "${const_sql_1184}"
    testFoldConst("${const_sql_1184}")
    def const_sql_1185 = """select cast("01400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1185_strict "${const_sql_1185}"
    testFoldConst("${const_sql_1185}")
    def const_sql_1186 = """select cast("08400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1186_strict "${const_sql_1186}"
    testFoldConst("${const_sql_1186}")
    def const_sql_1187 = """select cast("09400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1187_strict "${const_sql_1187}"
    testFoldConst("${const_sql_1187}")
    def const_sql_1188 = """select cast("10400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1188_strict "${const_sql_1188}"
    testFoldConst("${const_sql_1188}")
    def const_sql_1189 = """select cast("11400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1189_strict "${const_sql_1189}"
    testFoldConst("${const_sql_1189}")
    def const_sql_1190 = """select cast("18400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1190_strict "${const_sql_1190}"
    testFoldConst("${const_sql_1190}")
    def const_sql_1191 = """select cast("19400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1191_strict "${const_sql_1191}"
    testFoldConst("${const_sql_1191}")
    def const_sql_1192 = """select cast("90400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1192_strict "${const_sql_1192}"
    testFoldConst("${const_sql_1192}")
    def const_sql_1193 = """select cast("91400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1193_strict "${const_sql_1193}"
    testFoldConst("${const_sql_1193}")
    def const_sql_1194 = """select cast("98400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1194_strict "${const_sql_1194}"
    testFoldConst("${const_sql_1194}")
    def const_sql_1195 = """select cast("99400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1195_strict "${const_sql_1195}"
    testFoldConst("${const_sql_1195}")
    def const_sql_1196 = """select cast("99999999999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1196_strict "${const_sql_1196}"
    testFoldConst("${const_sql_1196}")
    def const_sql_1197 = """select cast("99999999999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1197_strict "${const_sql_1197}"
    testFoldConst("${const_sql_1197}")
    def const_sql_1198 = """select cast("99999999999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1198_strict "${const_sql_1198}"
    testFoldConst("${const_sql_1198}")
    def const_sql_1199 = """select cast("99999999999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1199_strict "${const_sql_1199}"
    testFoldConst("${const_sql_1199}")
    def const_sql_1200 = """select cast("900000000000000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1200_strict "${const_sql_1200}"
    testFoldConst("${const_sql_1200}")
    def const_sql_1201 = """select cast("900000000000000001400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1201_strict "${const_sql_1201}"
    testFoldConst("${const_sql_1201}")
    def const_sql_1202 = """select cast("900000000000000008400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1202_strict "${const_sql_1202}"
    testFoldConst("${const_sql_1202}")
    def const_sql_1203 = """select cast("900000000000000009400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1203_strict "${const_sql_1203}"
    testFoldConst("${const_sql_1203}")
    def const_sql_1204 = """select cast("900000000000000010400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1204_strict "${const_sql_1204}"
    testFoldConst("${const_sql_1204}")
    def const_sql_1205 = """select cast("900000000000000011400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1205_strict "${const_sql_1205}"
    testFoldConst("${const_sql_1205}")
    def const_sql_1206 = """select cast("900000000000000018400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1206_strict "${const_sql_1206}"
    testFoldConst("${const_sql_1206}")
    def const_sql_1207 = """select cast("900000000000000019400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1207_strict "${const_sql_1207}"
    testFoldConst("${const_sql_1207}")
    def const_sql_1208 = """select cast("999999999999999980400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1208_strict "${const_sql_1208}"
    testFoldConst("${const_sql_1208}")
    def const_sql_1209 = """select cast("999999999999999981400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1209_strict "${const_sql_1209}"
    testFoldConst("${const_sql_1209}")
    def const_sql_1210 = """select cast("999999999999999988400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1210_strict "${const_sql_1210}"
    testFoldConst("${const_sql_1210}")
    def const_sql_1211 = """select cast("999999999999999989400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1211_strict "${const_sql_1211}"
    testFoldConst("${const_sql_1211}")
    def const_sql_1212 = """select cast("999999999999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1212_strict "${const_sql_1212}"
    testFoldConst("${const_sql_1212}")
    def const_sql_1213 = """select cast("999999999999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1213_strict "${const_sql_1213}"
    testFoldConst("${const_sql_1213}")
    def const_sql_1214 = """select cast("999999999999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1214_strict "${const_sql_1214}"
    testFoldConst("${const_sql_1214}")
    def const_sql_1215 = """select cast("999999999999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1215_strict "${const_sql_1215}"
    testFoldConst("${const_sql_1215}")
    def const_sql_1216 = """select cast("00500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1216_strict "${const_sql_1216}"
    testFoldConst("${const_sql_1216}")
    def const_sql_1217 = """select cast("01500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1217_strict "${const_sql_1217}"
    testFoldConst("${const_sql_1217}")
    def const_sql_1218 = """select cast("08500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1218_strict "${const_sql_1218}"
    testFoldConst("${const_sql_1218}")
    def const_sql_1219 = """select cast("09500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1219_strict "${const_sql_1219}"
    testFoldConst("${const_sql_1219}")
    def const_sql_1220 = """select cast("10500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1220_strict "${const_sql_1220}"
    testFoldConst("${const_sql_1220}")
    def const_sql_1221 = """select cast("11500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1221_strict "${const_sql_1221}"
    testFoldConst("${const_sql_1221}")
    def const_sql_1222 = """select cast("18500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1222_strict "${const_sql_1222}"
    testFoldConst("${const_sql_1222}")
    def const_sql_1223 = """select cast("19500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1223_strict "${const_sql_1223}"
    testFoldConst("${const_sql_1223}")
    def const_sql_1224 = """select cast("90500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1224_strict "${const_sql_1224}"
    testFoldConst("${const_sql_1224}")
    def const_sql_1225 = """select cast("91500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1225_strict "${const_sql_1225}"
    testFoldConst("${const_sql_1225}")
    def const_sql_1226 = """select cast("98500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1226_strict "${const_sql_1226}"
    testFoldConst("${const_sql_1226}")
    def const_sql_1227 = """select cast("99500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1227_strict "${const_sql_1227}"
    testFoldConst("${const_sql_1227}")
    def const_sql_1228 = """select cast("99999999999999990500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1228_strict "${const_sql_1228}"
    testFoldConst("${const_sql_1228}")
    def const_sql_1229 = """select cast("99999999999999991500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1229_strict "${const_sql_1229}"
    testFoldConst("${const_sql_1229}")
    def const_sql_1230 = """select cast("99999999999999998500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1230_strict "${const_sql_1230}"
    testFoldConst("${const_sql_1230}")
    def const_sql_1231 = """select cast("99999999999999999500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1231_strict "${const_sql_1231}"
    testFoldConst("${const_sql_1231}")
    def const_sql_1232 = """select cast("900000000000000000500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1232_strict "${const_sql_1232}"
    testFoldConst("${const_sql_1232}")
    def const_sql_1233 = """select cast("900000000000000001500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1233_strict "${const_sql_1233}"
    testFoldConst("${const_sql_1233}")
    def const_sql_1234 = """select cast("900000000000000008500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1234_strict "${const_sql_1234}"
    testFoldConst("${const_sql_1234}")
    def const_sql_1235 = """select cast("900000000000000009500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1235_strict "${const_sql_1235}"
    testFoldConst("${const_sql_1235}")
    def const_sql_1236 = """select cast("900000000000000010500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1236_strict "${const_sql_1236}"
    testFoldConst("${const_sql_1236}")
    def const_sql_1237 = """select cast("900000000000000011500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1237_strict "${const_sql_1237}"
    testFoldConst("${const_sql_1237}")
    def const_sql_1238 = """select cast("900000000000000018500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1238_strict "${const_sql_1238}"
    testFoldConst("${const_sql_1238}")
    def const_sql_1239 = """select cast("900000000000000019500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1239_strict "${const_sql_1239}"
    testFoldConst("${const_sql_1239}")
    def const_sql_1240 = """select cast("999999999999999980500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1240_strict "${const_sql_1240}"
    testFoldConst("${const_sql_1240}")
    def const_sql_1241 = """select cast("999999999999999981500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1241_strict "${const_sql_1241}"
    testFoldConst("${const_sql_1241}")
    def const_sql_1242 = """select cast("999999999999999988500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1242_strict "${const_sql_1242}"
    testFoldConst("${const_sql_1242}")
    def const_sql_1243 = """select cast("999999999999999989500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1243_strict "${const_sql_1243}"
    testFoldConst("${const_sql_1243}")
    def const_sql_1244 = """select cast("999999999999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1244_strict "${const_sql_1244}"
    testFoldConst("${const_sql_1244}")
    def const_sql_1245 = """select cast("999999999999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1245_strict "${const_sql_1245}"
    testFoldConst("${const_sql_1245}")
    def const_sql_1246 = """select cast("999999999999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1246_strict "${const_sql_1246}"
    testFoldConst("${const_sql_1246}")
    def const_sql_1247 = """select cast("999999999999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1247_strict "${const_sql_1247}"
    testFoldConst("${const_sql_1247}")
    def const_sql_1248 = """select cast("-00400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1248_strict "${const_sql_1248}"
    testFoldConst("${const_sql_1248}")
    def const_sql_1249 = """select cast("-01400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1249_strict "${const_sql_1249}"
    testFoldConst("${const_sql_1249}")
    def const_sql_1250 = """select cast("-08400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1250_strict "${const_sql_1250}"
    testFoldConst("${const_sql_1250}")
    def const_sql_1251 = """select cast("-09400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1251_strict "${const_sql_1251}"
    testFoldConst("${const_sql_1251}")
    def const_sql_1252 = """select cast("-10400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1252_strict "${const_sql_1252}"
    testFoldConst("${const_sql_1252}")
    def const_sql_1253 = """select cast("-11400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1253_strict "${const_sql_1253}"
    testFoldConst("${const_sql_1253}")
    def const_sql_1254 = """select cast("-18400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1254_strict "${const_sql_1254}"
    testFoldConst("${const_sql_1254}")
    def const_sql_1255 = """select cast("-19400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1255_strict "${const_sql_1255}"
    testFoldConst("${const_sql_1255}")
    def const_sql_1256 = """select cast("-90400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1256_strict "${const_sql_1256}"
    testFoldConst("${const_sql_1256}")
    def const_sql_1257 = """select cast("-91400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1257_strict "${const_sql_1257}"
    testFoldConst("${const_sql_1257}")
    def const_sql_1258 = """select cast("-98400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1258_strict "${const_sql_1258}"
    testFoldConst("${const_sql_1258}")
    def const_sql_1259 = """select cast("-99400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1259_strict "${const_sql_1259}"
    testFoldConst("${const_sql_1259}")
    def const_sql_1260 = """select cast("-99999999999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1260_strict "${const_sql_1260}"
    testFoldConst("${const_sql_1260}")
    def const_sql_1261 = """select cast("-99999999999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1261_strict "${const_sql_1261}"
    testFoldConst("${const_sql_1261}")
    def const_sql_1262 = """select cast("-99999999999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1262_strict "${const_sql_1262}"
    testFoldConst("${const_sql_1262}")
    def const_sql_1263 = """select cast("-99999999999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1263_strict "${const_sql_1263}"
    testFoldConst("${const_sql_1263}")
    def const_sql_1264 = """select cast("-900000000000000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1264_strict "${const_sql_1264}"
    testFoldConst("${const_sql_1264}")
    def const_sql_1265 = """select cast("-900000000000000001400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1265_strict "${const_sql_1265}"
    testFoldConst("${const_sql_1265}")
    def const_sql_1266 = """select cast("-900000000000000008400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1266_strict "${const_sql_1266}"
    testFoldConst("${const_sql_1266}")
    def const_sql_1267 = """select cast("-900000000000000009400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1267_strict "${const_sql_1267}"
    testFoldConst("${const_sql_1267}")
    def const_sql_1268 = """select cast("-900000000000000010400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1268_strict "${const_sql_1268}"
    testFoldConst("${const_sql_1268}")
    def const_sql_1269 = """select cast("-900000000000000011400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1269_strict "${const_sql_1269}"
    testFoldConst("${const_sql_1269}")
    def const_sql_1270 = """select cast("-900000000000000018400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1270_strict "${const_sql_1270}"
    testFoldConst("${const_sql_1270}")
    def const_sql_1271 = """select cast("-900000000000000019400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1271_strict "${const_sql_1271}"
    testFoldConst("${const_sql_1271}")
    def const_sql_1272 = """select cast("-999999999999999980400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1272_strict "${const_sql_1272}"
    testFoldConst("${const_sql_1272}")
    def const_sql_1273 = """select cast("-999999999999999981400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1273_strict "${const_sql_1273}"
    testFoldConst("${const_sql_1273}")
    def const_sql_1274 = """select cast("-999999999999999988400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1274_strict "${const_sql_1274}"
    testFoldConst("${const_sql_1274}")
    def const_sql_1275 = """select cast("-999999999999999989400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1275_strict "${const_sql_1275}"
    testFoldConst("${const_sql_1275}")
    def const_sql_1276 = """select cast("-999999999999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1276_strict "${const_sql_1276}"
    testFoldConst("${const_sql_1276}")
    def const_sql_1277 = """select cast("-999999999999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1277_strict "${const_sql_1277}"
    testFoldConst("${const_sql_1277}")
    def const_sql_1278 = """select cast("-999999999999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1278_strict "${const_sql_1278}"
    testFoldConst("${const_sql_1278}")
    def const_sql_1279 = """select cast("-999999999999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1279_strict "${const_sql_1279}"
    testFoldConst("${const_sql_1279}")
    def const_sql_1280 = """select cast("-00500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1280_strict "${const_sql_1280}"
    testFoldConst("${const_sql_1280}")
    def const_sql_1281 = """select cast("-01500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1281_strict "${const_sql_1281}"
    testFoldConst("${const_sql_1281}")
    def const_sql_1282 = """select cast("-08500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1282_strict "${const_sql_1282}"
    testFoldConst("${const_sql_1282}")
    def const_sql_1283 = """select cast("-09500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1283_strict "${const_sql_1283}"
    testFoldConst("${const_sql_1283}")
    def const_sql_1284 = """select cast("-10500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1284_strict "${const_sql_1284}"
    testFoldConst("${const_sql_1284}")
    def const_sql_1285 = """select cast("-11500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1285_strict "${const_sql_1285}"
    testFoldConst("${const_sql_1285}")
    def const_sql_1286 = """select cast("-18500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1286_strict "${const_sql_1286}"
    testFoldConst("${const_sql_1286}")
    def const_sql_1287 = """select cast("-19500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1287_strict "${const_sql_1287}"
    testFoldConst("${const_sql_1287}")
    def const_sql_1288 = """select cast("-90500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1288_strict "${const_sql_1288}"
    testFoldConst("${const_sql_1288}")
    def const_sql_1289 = """select cast("-91500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1289_strict "${const_sql_1289}"
    testFoldConst("${const_sql_1289}")
    def const_sql_1290 = """select cast("-98500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1290_strict "${const_sql_1290}"
    testFoldConst("${const_sql_1290}")
    def const_sql_1291 = """select cast("-99500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1291_strict "${const_sql_1291}"
    testFoldConst("${const_sql_1291}")
    def const_sql_1292 = """select cast("-99999999999999990500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1292_strict "${const_sql_1292}"
    testFoldConst("${const_sql_1292}")
    def const_sql_1293 = """select cast("-99999999999999991500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1293_strict "${const_sql_1293}"
    testFoldConst("${const_sql_1293}")
    def const_sql_1294 = """select cast("-99999999999999998500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1294_strict "${const_sql_1294}"
    testFoldConst("${const_sql_1294}")
    def const_sql_1295 = """select cast("-99999999999999999500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1295_strict "${const_sql_1295}"
    testFoldConst("${const_sql_1295}")
    def const_sql_1296 = """select cast("-900000000000000000500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1296_strict "${const_sql_1296}"
    testFoldConst("${const_sql_1296}")
    def const_sql_1297 = """select cast("-900000000000000001500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1297_strict "${const_sql_1297}"
    testFoldConst("${const_sql_1297}")
    def const_sql_1298 = """select cast("-900000000000000008500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1298_strict "${const_sql_1298}"
    testFoldConst("${const_sql_1298}")
    def const_sql_1299 = """select cast("-900000000000000009500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1299_strict "${const_sql_1299}"
    testFoldConst("${const_sql_1299}")
    def const_sql_1300 = """select cast("-900000000000000010500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1300_strict "${const_sql_1300}"
    testFoldConst("${const_sql_1300}")
    def const_sql_1301 = """select cast("-900000000000000011500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1301_strict "${const_sql_1301}"
    testFoldConst("${const_sql_1301}")
    def const_sql_1302 = """select cast("-900000000000000018500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1302_strict "${const_sql_1302}"
    testFoldConst("${const_sql_1302}")
    def const_sql_1303 = """select cast("-900000000000000019500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1303_strict "${const_sql_1303}"
    testFoldConst("${const_sql_1303}")
    def const_sql_1304 = """select cast("-999999999999999980500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1304_strict "${const_sql_1304}"
    testFoldConst("${const_sql_1304}")
    def const_sql_1305 = """select cast("-999999999999999981500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1305_strict "${const_sql_1305}"
    testFoldConst("${const_sql_1305}")
    def const_sql_1306 = """select cast("-999999999999999988500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1306_strict "${const_sql_1306}"
    testFoldConst("${const_sql_1306}")
    def const_sql_1307 = """select cast("-999999999999999989500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1307_strict "${const_sql_1307}"
    testFoldConst("${const_sql_1307}")
    def const_sql_1308 = """select cast("-999999999999999990400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1308_strict "${const_sql_1308}"
    testFoldConst("${const_sql_1308}")
    def const_sql_1309 = """select cast("-999999999999999991400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1309_strict "${const_sql_1309}"
    testFoldConst("${const_sql_1309}")
    def const_sql_1310 = """select cast("-999999999999999998400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1310_strict "${const_sql_1310}"
    testFoldConst("${const_sql_1310}")
    def const_sql_1311 = """select cast("-999999999999999999400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(18, 1));"""
    qt_sql_1311_strict "${const_sql_1311}"
    testFoldConst("${const_sql_1311}")
    sql "set enable_strict_cast=false;"
    qt_sql_1134_non_strict "${const_sql_1134}"
    testFoldConst("${const_sql_1134}")
    qt_sql_1135_non_strict "${const_sql_1135}"
    testFoldConst("${const_sql_1135}")
    qt_sql_1136_non_strict "${const_sql_1136}"
    testFoldConst("${const_sql_1136}")
    qt_sql_1137_non_strict "${const_sql_1137}"
    testFoldConst("${const_sql_1137}")
    qt_sql_1138_non_strict "${const_sql_1138}"
    testFoldConst("${const_sql_1138}")
    qt_sql_1139_non_strict "${const_sql_1139}"
    testFoldConst("${const_sql_1139}")
    qt_sql_1140_non_strict "${const_sql_1140}"
    testFoldConst("${const_sql_1140}")
    qt_sql_1141_non_strict "${const_sql_1141}"
    testFoldConst("${const_sql_1141}")
    qt_sql_1142_non_strict "${const_sql_1142}"
    testFoldConst("${const_sql_1142}")
    qt_sql_1143_non_strict "${const_sql_1143}"
    testFoldConst("${const_sql_1143}")
    qt_sql_1144_non_strict "${const_sql_1144}"
    testFoldConst("${const_sql_1144}")
    qt_sql_1145_non_strict "${const_sql_1145}"
    testFoldConst("${const_sql_1145}")
    qt_sql_1146_non_strict "${const_sql_1146}"
    testFoldConst("${const_sql_1146}")
    qt_sql_1147_non_strict "${const_sql_1147}"
    testFoldConst("${const_sql_1147}")
    qt_sql_1148_non_strict "${const_sql_1148}"
    testFoldConst("${const_sql_1148}")
    qt_sql_1149_non_strict "${const_sql_1149}"
    testFoldConst("${const_sql_1149}")
    qt_sql_1150_non_strict "${const_sql_1150}"
    testFoldConst("${const_sql_1150}")
    qt_sql_1151_non_strict "${const_sql_1151}"
    testFoldConst("${const_sql_1151}")
    qt_sql_1152_non_strict "${const_sql_1152}"
    testFoldConst("${const_sql_1152}")
    qt_sql_1153_non_strict "${const_sql_1153}"
    testFoldConst("${const_sql_1153}")
    qt_sql_1154_non_strict "${const_sql_1154}"
    testFoldConst("${const_sql_1154}")
    qt_sql_1155_non_strict "${const_sql_1155}"
    testFoldConst("${const_sql_1155}")
    qt_sql_1156_non_strict "${const_sql_1156}"
    testFoldConst("${const_sql_1156}")
    qt_sql_1157_non_strict "${const_sql_1157}"
    testFoldConst("${const_sql_1157}")
    qt_sql_1158_non_strict "${const_sql_1158}"
    testFoldConst("${const_sql_1158}")
    qt_sql_1159_non_strict "${const_sql_1159}"
    testFoldConst("${const_sql_1159}")
    qt_sql_1160_non_strict "${const_sql_1160}"
    testFoldConst("${const_sql_1160}")
    qt_sql_1161_non_strict "${const_sql_1161}"
    testFoldConst("${const_sql_1161}")
    qt_sql_1162_non_strict "${const_sql_1162}"
    testFoldConst("${const_sql_1162}")
    qt_sql_1163_non_strict "${const_sql_1163}"
    testFoldConst("${const_sql_1163}")
    qt_sql_1164_non_strict "${const_sql_1164}"
    testFoldConst("${const_sql_1164}")
    qt_sql_1165_non_strict "${const_sql_1165}"
    testFoldConst("${const_sql_1165}")
    qt_sql_1166_non_strict "${const_sql_1166}"
    testFoldConst("${const_sql_1166}")
    qt_sql_1167_non_strict "${const_sql_1167}"
    testFoldConst("${const_sql_1167}")
    qt_sql_1168_non_strict "${const_sql_1168}"
    testFoldConst("${const_sql_1168}")
    qt_sql_1169_non_strict "${const_sql_1169}"
    testFoldConst("${const_sql_1169}")
    qt_sql_1170_non_strict "${const_sql_1170}"
    testFoldConst("${const_sql_1170}")
    qt_sql_1171_non_strict "${const_sql_1171}"
    testFoldConst("${const_sql_1171}")
    qt_sql_1172_non_strict "${const_sql_1172}"
    testFoldConst("${const_sql_1172}")
    qt_sql_1173_non_strict "${const_sql_1173}"
    testFoldConst("${const_sql_1173}")
    qt_sql_1174_non_strict "${const_sql_1174}"
    testFoldConst("${const_sql_1174}")
    qt_sql_1175_non_strict "${const_sql_1175}"
    testFoldConst("${const_sql_1175}")
    qt_sql_1176_non_strict "${const_sql_1176}"
    testFoldConst("${const_sql_1176}")
    qt_sql_1177_non_strict "${const_sql_1177}"
    testFoldConst("${const_sql_1177}")
    qt_sql_1178_non_strict "${const_sql_1178}"
    testFoldConst("${const_sql_1178}")
    qt_sql_1179_non_strict "${const_sql_1179}"
    testFoldConst("${const_sql_1179}")
    qt_sql_1180_non_strict "${const_sql_1180}"
    testFoldConst("${const_sql_1180}")
    qt_sql_1181_non_strict "${const_sql_1181}"
    testFoldConst("${const_sql_1181}")
    qt_sql_1182_non_strict "${const_sql_1182}"
    testFoldConst("${const_sql_1182}")
    qt_sql_1183_non_strict "${const_sql_1183}"
    testFoldConst("${const_sql_1183}")
    qt_sql_1184_non_strict "${const_sql_1184}"
    testFoldConst("${const_sql_1184}")
    qt_sql_1185_non_strict "${const_sql_1185}"
    testFoldConst("${const_sql_1185}")
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
}