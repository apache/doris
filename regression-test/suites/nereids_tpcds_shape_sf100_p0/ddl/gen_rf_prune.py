# // Licensed to the Apache Software Foundation (ASF) under one
# // or more contributor license agreements.  See the NOTICE file
# // distributed with this work for additional information
# // regarding copyright ownership.  The ASF licenses this file
# // to you under the Apache License, Version 2.0 (the
# // "License"); you may not use this file except in compliance
# // with the License.  You may obtain a copy of the License at
# //
# //   http://www.apache.org/licenses/LICENSE-2.0
# //
# // Unless required by applicable law or agreed to in writing,
# // software distributed under the License is distributed on an
# // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# // KIND, either express or implied.  See the License for the
# // specific language governing permissions and limitations
# // under the License.


def ini_prune_count():
    counter = dict()
    counter[2] = 2
    counter[4] = 3
    counter[5] = 4
    counter[6] = 4
    counter[7] = 1
    counter[8] = 1
    counter[11] = 2
    counter[13] = 0
    counter[14] = 12
    counter[15] = 2
    counter[17] = 4
    counter[19] = 1
    counter[21] = 1
    counter[22] = 1
    counter[23] = 3
    counter[24] = 4
    counter[25] = 2
    counter[26] = 1
    counter[27] = 1
    counter[30] = 1
    counter[31] = 2
    counter[35] = 3
    counter[36] = 1
    counter[38] = 3
    counter[39] = 2
    counter[40] = 1
    counter[45] = 3
    counter[46] = 2
    counter[47] = 2
    counter[48] = 1
    counter[50] = 1
    counter[53] = 1
    counter[54] = 1
    counter[57] = 2
    counter[58] = 1
    counter[59] = 3
    counter[62] = 3
    counter[63] = 1
    counter[64] = 3
    counter[65] = 3
    counter[66] = 2
    counter[67] = 2
    counter[70] = 1
    counter[72] = 5
    counter[74] = 2
    counter[76] = 6
    counter[77] = 4
    counter[79] = 1
    counter[80] = 3
    counter[81] = 1
    counter[83] = 1
    counter[85] = 0
    counter[86] = 1
    counter[87] = 3
    counter[89] = 1
    counter[91] = 1
    counter[95] = 2
    counter[99] = 3
    return counter


if __name__ == '__main__':
    counters = ini_prune_count()
    with open('rf_prune.tmpl', 'r') as f:
        tmpl = f.read()
        for i in range(1,100):
            casei = tmpl.replace('{query_id}', str(i))
            pruned = 0
            if i in counters:
                pruned = counters[i]
            casei = casei.replace('{pruned}', str(pruned))
            with open('../rf_prune/ds'+str(i)+'_rf_prune.groovy', 'w') as out:
                out.write(casei)

