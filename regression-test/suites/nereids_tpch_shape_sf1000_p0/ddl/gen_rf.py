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
if __name__ == '__main__':
    with open('rf.tmpl', 'r') as f:
        tmpl = f.read()
        for i in range(1,23):
            with open('../../../../tools/tpch-tools/queries/q'+str(i)+'.sql', 'r') as fi:
                casei = tmpl.replace('{--}', str(i))
                casei = casei.replace('{query}', fi.read())
                # with open('../rf/h_rf'+str(i)+'.groovy', 'w') as out:
                #     out.write(casei)
                with open('rf/rf.'+str(i), 'r') as rf_file:
                    casei = casei.replace('{rfs}', rf_file.read())
                    with open('../rf/h_rf'+str(i)+'.groovy', 'w') as out:
                        out.write(casei)