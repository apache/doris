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

#ifndef  DORIS_BE_SRC_COMMON_UTIL_MEM_UTIL_HPP
#define  DORIS_BE_SRC_COMMON_UTIL_MEM_UTIL_HPP

#include <string.h>

namespace doris {

template<const size_t N>
inline void fixed_size_memory_copy(void* dst, const void* src) {
    struct X {
        uint8_t byte[N];
    };

    *(reinterpret_cast<X*>(dst)) = *(reinterpret_cast<const X*>(src));
}

template<> inline void fixed_size_memory_copy<0>(void*, const void*) {}

template<> inline void fixed_size_memory_copy<1>(void* dst, const void* src) {
    *(reinterpret_cast<uint8_t*>(dst)) = * (reinterpret_cast<const uint8_t*>(src));
}

template<> inline void fixed_size_memory_copy<2>(void* dst, const void* src) {
    *(reinterpret_cast<uint16_t*>(dst)) = * (reinterpret_cast<const uint16_t*>(src));
}

template<> inline void fixed_size_memory_copy<4>(void* dst, const void* src) {
    *(reinterpret_cast<uint32_t*>(dst)) = * (reinterpret_cast<const uint32_t*>(src));
}

template<> inline void fixed_size_memory_copy<8>(void* dst, const void* src) {
    *(reinterpret_cast<uint64_t*>(dst)) = * (reinterpret_cast<const uint64_t*>(src));
}

inline void memory_copy(void* dst, const void* src, size_t size) {
// Function fixed_size_memory_copy will report a stack-use-after-scope error in ASAN mode.
#if !defined(ADDRESS_SANITIZER)
    static const void* addrs[] = {
        &&B0, &&B1, &&B2, &&B3, &&B4, &&B5, &&B6, 
        &&B7, &&B8, &&B9, &&B10, &&B11, &&B12, &&B13,
        &&B14, &&B15, &&B16, &&B17, &&B18, &&B19, 
        &&B20, &&B21, &&B22, &&B23, &&B24, &&B25,
        &&B26, &&B27, &&B28, &&B29, &&B30, &&B31, 
        &&B32, &&B33, &&B34, &&B35, &&B36, &&B37,
        &&B38, &&B39, &&B40, &&B41, &&B42, &&B43, 
        &&B44, &&B45, &&B46, &&B47, &&B48, &&B49,
        &&B50, &&B51, &&B52, &&B53, &&B54, &&B55, 
        &&B56, &&B57, &&B58, &&B59, &&B60, &&B61,
        &&B62, &&B63, &&B64, &&B65, &&B66, &&B67, 
        &&B68, &&B69, &&B70, &&B71, &&B72, &&B73,
        &&B74, &&B75, &&B76, &&B77, &&B78, &&B79, 
        &&B80, &&B81, &&B82, &&B83, &&B84, &&B85,
        &&B86, &&B87, &&B88, &&B89, &&B90, &&B91, 
        &&B92, &&B93, &&B94, &&B95, &&B96, &&B97,
        &&B98, &&B99, &&B100, &&B101, &&B102, &&B103, 
        &&B104, &&B105, &&B106, &&B107, &&B108, 
        &&B109, &&B110, &&B111, &&B112, &&B113, 
        &&B114, &&B115, &&B116, &&B117, &&B118, 
        &&B119, &&B120, &&B121, &&B122, &&B123, 
        &&B124, &&B125, &&B126, &&B127, &&B128, 
        &&B129, &&B130, &&B131, &&B132, &&B133, 
        &&B134, &&B135, &&B136, &&B137, &&B138, 
        &&B139, &&B140, &&B141, &&B142, &&B143, 
        &&B144, &&B145, &&B146, &&B147, &&B148, 
        &&B149, &&B150, &&B151, &&B152, &&B153, 
        &&B154, &&B155, &&B156, &&B157, &&B158, 
        &&B159, &&B160, &&B161, &&B162, &&B163,
        &&B164, &&B165, &&B166, &&B167, &&B168, 
        &&B169, &&B170, &&B171, &&B172, &&B173, &&B174,
        &&B175, &&B176, &&B177, &&B178, &&B179, &&B180, 
        &&B181, &&B182, &&B183, &&B184, &&B185,
        &&B186, &&B187, &&B188, &&B189, &&B190, &&B191, 
        &&B192, &&B193, &&B194, &&B195, &&B196,
        &&B197, &&B198, &&B199, &&B200, &&B201, &&B202, 
        &&B203, &&B204, &&B205, &&B206, &&B207,
        &&B208, &&B209, &&B210, &&B211, &&B212, &&B213, 
        &&B214, &&B215, &&B216, &&B217, &&B218,
        &&B219, &&B220, &&B221, &&B222, &&B223, &&B224, 
        &&B225, &&B226, &&B227, &&B228, &&B229,
        &&B230, &&B231, &&B232, &&B233, &&B234, &&B235, 
        &&B236, &&B237, &&B238, &&B239, &&B240,
        &&B241, &&B242, &&B243, &&B244, &&B245, &&B246, 
        &&B247, &&B248, &&B249, &&B250, &&B251,
        &&B252, &&B253, &&B254, &&B255,
    };

    if (size <= 255) {
        // 这里使用GOTO是为了提高性能，switch、if else均无法达到此种性能
        goto* addrs[size];
B0:
        return fixed_size_memory_copy<0>(dst, src);
B1:
        return fixed_size_memory_copy<1>(dst, src);
B2:
        return fixed_size_memory_copy<2>(dst, src);
B3:
        return fixed_size_memory_copy<3>(dst, src);
B4:
        return fixed_size_memory_copy<4>(dst, src);
B5:
        return fixed_size_memory_copy<5>(dst, src);
B6:
        return fixed_size_memory_copy<6>(dst, src);
B7:
        return fixed_size_memory_copy<7>(dst, src);
B8:
        return fixed_size_memory_copy<8>(dst, src);
B9:
        return fixed_size_memory_copy<9>(dst, src);
B10:
        return fixed_size_memory_copy<10>(dst, src);
B11:
        return fixed_size_memory_copy<11>(dst, src);
B12:
        return fixed_size_memory_copy<12>(dst, src);
B13:
        return fixed_size_memory_copy<13>(dst, src);
B14:
        return fixed_size_memory_copy<14>(dst, src);
B15:
        return fixed_size_memory_copy<15>(dst, src);
B16:
        return fixed_size_memory_copy<16>(dst, src);
B17:
        return fixed_size_memory_copy<17>(dst, src);
B18:
        return fixed_size_memory_copy<18>(dst, src);
B19:
        return fixed_size_memory_copy<19>(dst, src);
B20:
        return fixed_size_memory_copy<20>(dst, src);
B21:
        return fixed_size_memory_copy<21>(dst, src);
B22:
        return fixed_size_memory_copy<22>(dst, src);
B23:
        return fixed_size_memory_copy<23>(dst, src);
B24:
        return fixed_size_memory_copy<24>(dst, src);
B25:
        return fixed_size_memory_copy<25>(dst, src);
B26:
        return fixed_size_memory_copy<26>(dst, src);
B27:
        return fixed_size_memory_copy<27>(dst, src);
B28:
        return fixed_size_memory_copy<28>(dst, src);
B29:
        return fixed_size_memory_copy<29>(dst, src);
B30:
        return fixed_size_memory_copy<30>(dst, src);
B31:
        return fixed_size_memory_copy<31>(dst, src);
B32:
        return fixed_size_memory_copy<32>(dst, src);
B33:
        return fixed_size_memory_copy<33>(dst, src);
B34:
        return fixed_size_memory_copy<34>(dst, src);
B35:
        return fixed_size_memory_copy<35>(dst, src);
B36:
        return fixed_size_memory_copy<36>(dst, src);
B37:
        return fixed_size_memory_copy<37>(dst, src);
B38:
        return fixed_size_memory_copy<38>(dst, src);
B39:
        return fixed_size_memory_copy<39>(dst, src);
B40:
        return fixed_size_memory_copy<40>(dst, src);
B41:
        return fixed_size_memory_copy<41>(dst, src);
B42:
        return fixed_size_memory_copy<42>(dst, src);
B43:
        return fixed_size_memory_copy<43>(dst, src);
B44:
        return fixed_size_memory_copy<44>(dst, src);
B45:
        return fixed_size_memory_copy<45>(dst, src);
B46:
        return fixed_size_memory_copy<46>(dst, src);
B47:
        return fixed_size_memory_copy<47>(dst, src);
B48:
        return fixed_size_memory_copy<48>(dst, src);
B49:
        return fixed_size_memory_copy<49>(dst, src);
B50:
        return fixed_size_memory_copy<50>(dst, src);
B51:
        return fixed_size_memory_copy<51>(dst, src);
B52:
        return fixed_size_memory_copy<52>(dst, src);
B53:
        return fixed_size_memory_copy<53>(dst, src);
B54:
        return fixed_size_memory_copy<54>(dst, src);
B55:
        return fixed_size_memory_copy<55>(dst, src);
B56:
        return fixed_size_memory_copy<56>(dst, src);
B57:
        return fixed_size_memory_copy<57>(dst, src);
B58:
        return fixed_size_memory_copy<58>(dst, src);
B59:
        return fixed_size_memory_copy<59>(dst, src);
B60:
        return fixed_size_memory_copy<60>(dst, src);
B61:
        return fixed_size_memory_copy<61>(dst, src);
B62:
        return fixed_size_memory_copy<62>(dst, src);
B63:
        return fixed_size_memory_copy<63>(dst, src);
B64:
        return fixed_size_memory_copy<64>(dst, src);
B65:
        return fixed_size_memory_copy<65>(dst, src);
B66:
        return fixed_size_memory_copy<66>(dst, src);
B67:
        return fixed_size_memory_copy<67>(dst, src);
B68:
        return fixed_size_memory_copy<68>(dst, src);
B69:
        return fixed_size_memory_copy<69>(dst, src);
B70:
        return fixed_size_memory_copy<70>(dst, src);
B71:
        return fixed_size_memory_copy<71>(dst, src);
B72:
        return fixed_size_memory_copy<72>(dst, src);
B73:
        return fixed_size_memory_copy<73>(dst, src);
B74:
        return fixed_size_memory_copy<74>(dst, src);
B75:
        return fixed_size_memory_copy<75>(dst, src);
B76:
        return fixed_size_memory_copy<76>(dst, src);
B77:
        return fixed_size_memory_copy<77>(dst, src);
B78:
        return fixed_size_memory_copy<78>(dst, src);
B79:
        return fixed_size_memory_copy<79>(dst, src);
B80:
        return fixed_size_memory_copy<80>(dst, src);
B81:
        return fixed_size_memory_copy<81>(dst, src);
B82:
        return fixed_size_memory_copy<82>(dst, src);
B83:
        return fixed_size_memory_copy<83>(dst, src);
B84:
        return fixed_size_memory_copy<84>(dst, src);
B85:
        return fixed_size_memory_copy<85>(dst, src);
B86:
        return fixed_size_memory_copy<86>(dst, src);
B87:
        return fixed_size_memory_copy<87>(dst, src);
B88:
        return fixed_size_memory_copy<88>(dst, src);
B89:
        return fixed_size_memory_copy<89>(dst, src);
B90:
        return fixed_size_memory_copy<90>(dst, src);
B91:
        return fixed_size_memory_copy<91>(dst, src);
B92:
        return fixed_size_memory_copy<92>(dst, src);
B93:
        return fixed_size_memory_copy<93>(dst, src);
B94:
        return fixed_size_memory_copy<94>(dst, src);
B95:
        return fixed_size_memory_copy<95>(dst, src);
B96:
        return fixed_size_memory_copy<96>(dst, src);
B97:
        return fixed_size_memory_copy<97>(dst, src);
B98:
        return fixed_size_memory_copy<98>(dst, src);
B99:
        return fixed_size_memory_copy<99>(dst, src);
B100:
        return fixed_size_memory_copy<100>(dst, src);
B101:
        return fixed_size_memory_copy<101>(dst, src);
B102:
        return fixed_size_memory_copy<102>(dst, src);
B103:
        return fixed_size_memory_copy<103>(dst, src);
B104:
        return fixed_size_memory_copy<104>(dst, src);
B105:
        return fixed_size_memory_copy<105>(dst, src);
B106:
        return fixed_size_memory_copy<106>(dst, src);
B107:
        return fixed_size_memory_copy<107>(dst, src);
B108:
        return fixed_size_memory_copy<108>(dst, src);
B109:
        return fixed_size_memory_copy<109>(dst, src);
B110:
        return fixed_size_memory_copy<110>(dst, src);
B111:
        return fixed_size_memory_copy<111>(dst, src);
B112:
        return fixed_size_memory_copy<112>(dst, src);
B113:
        return fixed_size_memory_copy<113>(dst, src);
B114:
        return fixed_size_memory_copy<114>(dst, src);
B115:
        return fixed_size_memory_copy<115>(dst, src);
B116:
        return fixed_size_memory_copy<116>(dst, src);
B117:
        return fixed_size_memory_copy<117>(dst, src);
B118:
        return fixed_size_memory_copy<118>(dst, src);
B119:
        return fixed_size_memory_copy<119>(dst, src);
B120:
        return fixed_size_memory_copy<120>(dst, src);
B121:
        return fixed_size_memory_copy<121>(dst, src);
B122:
        return fixed_size_memory_copy<122>(dst, src);
B123:
        return fixed_size_memory_copy<123>(dst, src);
B124:
        return fixed_size_memory_copy<124>(dst, src);
B125:
        return fixed_size_memory_copy<125>(dst, src);
B126:
        return fixed_size_memory_copy<126>(dst, src);
B127:
        return fixed_size_memory_copy<127>(dst, src);
B128:
        return fixed_size_memory_copy<128>(dst, src);
B129:
        return fixed_size_memory_copy<129>(dst, src);
B130:
        return fixed_size_memory_copy<130>(dst, src);
B131:
        return fixed_size_memory_copy<131>(dst, src);
B132:
        return fixed_size_memory_copy<132>(dst, src);
B133:
        return fixed_size_memory_copy<133>(dst, src);
B134:
        return fixed_size_memory_copy<134>(dst, src);
B135:
        return fixed_size_memory_copy<135>(dst, src);
B136:
        return fixed_size_memory_copy<136>(dst, src);
B137:
        return fixed_size_memory_copy<137>(dst, src);
B138:
        return fixed_size_memory_copy<138>(dst, src);
B139:
        return fixed_size_memory_copy<139>(dst, src);
B140:
        return fixed_size_memory_copy<140>(dst, src);
B141:
        return fixed_size_memory_copy<141>(dst, src);
B142:
        return fixed_size_memory_copy<142>(dst, src);
B143:
        return fixed_size_memory_copy<143>(dst, src);
B144:
        return fixed_size_memory_copy<144>(dst, src);
B145:
        return fixed_size_memory_copy<145>(dst, src);
B146:
        return fixed_size_memory_copy<146>(dst, src);
B147:
        return fixed_size_memory_copy<147>(dst, src);
B148:
        return fixed_size_memory_copy<148>(dst, src);
B149:
        return fixed_size_memory_copy<149>(dst, src);
B150:
        return fixed_size_memory_copy<150>(dst, src);
B151:
        return fixed_size_memory_copy<151>(dst, src);
B152:
        return fixed_size_memory_copy<152>(dst, src);
B153:
        return fixed_size_memory_copy<153>(dst, src);
B154:
        return fixed_size_memory_copy<154>(dst, src);
B155:
        return fixed_size_memory_copy<155>(dst, src);
B156:
        return fixed_size_memory_copy<156>(dst, src);
B157:
        return fixed_size_memory_copy<157>(dst, src);
B158:
        return fixed_size_memory_copy<158>(dst, src);
B159:
        return fixed_size_memory_copy<159>(dst, src);
B160:
        return fixed_size_memory_copy<160>(dst, src);
B161:
        return fixed_size_memory_copy<161>(dst, src);
B162:
        return fixed_size_memory_copy<162>(dst, src);
B163:
        return fixed_size_memory_copy<163>(dst, src);
B164:
        return fixed_size_memory_copy<164>(dst, src);
B165:
        return fixed_size_memory_copy<165>(dst, src);
B166:
        return fixed_size_memory_copy<166>(dst, src);
B167:
        return fixed_size_memory_copy<167>(dst, src);
B168:
        return fixed_size_memory_copy<168>(dst, src);
B169:
        return fixed_size_memory_copy<169>(dst, src);
B170:
        return fixed_size_memory_copy<170>(dst, src);
B171:
        return fixed_size_memory_copy<171>(dst, src);
B172:
        return fixed_size_memory_copy<172>(dst, src);
B173:
        return fixed_size_memory_copy<173>(dst, src);
B174:
        return fixed_size_memory_copy<174>(dst, src);
B175:
        return fixed_size_memory_copy<175>(dst, src);
B176:
        return fixed_size_memory_copy<176>(dst, src);
B177:
        return fixed_size_memory_copy<177>(dst, src);
B178:
        return fixed_size_memory_copy<178>(dst, src);
B179:
        return fixed_size_memory_copy<179>(dst, src);
B180:
        return fixed_size_memory_copy<180>(dst, src);
B181:
        return fixed_size_memory_copy<181>(dst, src);
B182:
        return fixed_size_memory_copy<182>(dst, src);
B183:
        return fixed_size_memory_copy<183>(dst, src);
B184:
        return fixed_size_memory_copy<184>(dst, src);
B185:
        return fixed_size_memory_copy<185>(dst, src);
B186:
        return fixed_size_memory_copy<186>(dst, src);
B187:
        return fixed_size_memory_copy<187>(dst, src);
B188:
        return fixed_size_memory_copy<188>(dst, src);
B189:
        return fixed_size_memory_copy<189>(dst, src);
B190:
        return fixed_size_memory_copy<190>(dst, src);
B191:
        return fixed_size_memory_copy<191>(dst, src);
B192:
        return fixed_size_memory_copy<192>(dst, src);
B193:
        return fixed_size_memory_copy<193>(dst, src);
B194:
        return fixed_size_memory_copy<194>(dst, src);
B195:
        return fixed_size_memory_copy<195>(dst, src);
B196:
        return fixed_size_memory_copy<196>(dst, src);
B197:
        return fixed_size_memory_copy<197>(dst, src);
B198:
        return fixed_size_memory_copy<198>(dst, src);
B199:
        return fixed_size_memory_copy<199>(dst, src);
B200:
        return fixed_size_memory_copy<200>(dst, src);
B201:
        return fixed_size_memory_copy<201>(dst, src);
B202:
        return fixed_size_memory_copy<202>(dst, src);
B203:
        return fixed_size_memory_copy<203>(dst, src);
B204:
        return fixed_size_memory_copy<204>(dst, src);
B205:
        return fixed_size_memory_copy<205>(dst, src);
B206:
        return fixed_size_memory_copy<206>(dst, src);
B207:
        return fixed_size_memory_copy<207>(dst, src);
B208:
        return fixed_size_memory_copy<208>(dst, src);
B209:
        return fixed_size_memory_copy<209>(dst, src);
B210:
        return fixed_size_memory_copy<210>(dst, src);
B211:
        return fixed_size_memory_copy<211>(dst, src);
B212:
        return fixed_size_memory_copy<212>(dst, src);
B213:
        return fixed_size_memory_copy<213>(dst, src);
B214:
        return fixed_size_memory_copy<214>(dst, src);
B215:
        return fixed_size_memory_copy<215>(dst, src);
B216:
        return fixed_size_memory_copy<216>(dst, src);
B217:
        return fixed_size_memory_copy<217>(dst, src);
B218:
        return fixed_size_memory_copy<218>(dst, src);
B219:
        return fixed_size_memory_copy<219>(dst, src);
B220:
        return fixed_size_memory_copy<220>(dst, src);
B221:
        return fixed_size_memory_copy<221>(dst, src);
B222:
        return fixed_size_memory_copy<222>(dst, src);
B223:
        return fixed_size_memory_copy<223>(dst, src);
B224:
        return fixed_size_memory_copy<224>(dst, src);
B225:
        return fixed_size_memory_copy<225>(dst, src);
B226:
        return fixed_size_memory_copy<226>(dst, src);
B227:
        return fixed_size_memory_copy<227>(dst, src);
B228:
        return fixed_size_memory_copy<228>(dst, src);
B229:
        return fixed_size_memory_copy<229>(dst, src);
B230:
        return fixed_size_memory_copy<230>(dst, src);
B231:
        return fixed_size_memory_copy<231>(dst, src);
B232:
        return fixed_size_memory_copy<232>(dst, src);
B233:
        return fixed_size_memory_copy<233>(dst, src);
B234:
        return fixed_size_memory_copy<234>(dst, src);
B235:
        return fixed_size_memory_copy<235>(dst, src);
B236:
        return fixed_size_memory_copy<236>(dst, src);
B237:
        return fixed_size_memory_copy<237>(dst, src);
B238:
        return fixed_size_memory_copy<238>(dst, src);
B239:
        return fixed_size_memory_copy<239>(dst, src);
B240:
        return fixed_size_memory_copy<240>(dst, src);
B241:
        return fixed_size_memory_copy<241>(dst, src);
B242:
        return fixed_size_memory_copy<242>(dst, src);
B243:
        return fixed_size_memory_copy<243>(dst, src);
B244:
        return fixed_size_memory_copy<244>(dst, src);
B245:
        return fixed_size_memory_copy<245>(dst, src);
B246:
        return fixed_size_memory_copy<246>(dst, src);
B247:
        return fixed_size_memory_copy<247>(dst, src);
B248:
        return fixed_size_memory_copy<248>(dst, src);
B249:
        return fixed_size_memory_copy<249>(dst, src);
B250:
        return fixed_size_memory_copy<250>(dst, src);
B251:
        return fixed_size_memory_copy<251>(dst, src);
B252:
        return fixed_size_memory_copy<252>(dst, src);
B253:
        return fixed_size_memory_copy<253>(dst, src);
B254:
        return fixed_size_memory_copy<254>(dst, src);
B255:
        return fixed_size_memory_copy<255>(dst, src);
    }
#endif

    memcpy(dst, src, size);
    return;
}

}

#endif  // DORIS_BE_SRC_COMMON_SRC_UTIL_MEM_UTIL_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
