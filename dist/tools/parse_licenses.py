# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import collections
import os 
import re
import sys
import time

license_dict = {}
# The Apache Software License, Version 2.0
license_dict["The Apache Software License, Version 2.0"] = "The Apache Software License, Version 2.0"
license_dict["Apache 2.0"] = "The Apache Software License, Version 2.0"
license_dict["Apache License, Version 2.0"] = "The Apache Software License, Version 2.0"
license_dict["Apache License 2.0"] = "The Apache Software License, Version 2.0"
license_dict["Apache 2"] = "The Apache Software License, Version 2.0"
license_dict["Apache v2"] = "The Apache Software License, Version 2.0"
license_dict["Apache-2.0"] = "The Apache Software License, Version 2.0"
license_dict["ASF 2.0"] = "The Apache Software License, Version 2.0"
license_dict["Apache License Version 2"] = "The Apache Software License, Version 2.0"
license_dict["Apache License Version 2.0"] = "The Apache Software License, Version 2.0"
license_dict["Apache License (v2.0)"] = "The Apache Software License, Version 2.0"
license_dict["Apache License"] = "The Apache Software License, Version 2.0"
license_dict["The Apache License, Version 2.0"] = "The Apache Software License, Version 2.0"
license_dict["Apache 2.0 License"] = "The Apache Software License, Version 2.0"
license_dict["Apache License, version 2.0"] = "The Apache Software License, Version 2.0"
# The 3-Clause BSD License
license_dict["3-Clause BSD License"] = "The 3-Clause BSD License"
license_dict["New BSD License"] = "The 3-Clause BSD License"
license_dict["The New BSD License"] = "The 3-Clause BSD License"
license_dict["Modified BSD License"] = "The 3-Clause BSD License"
license_dict["BSD Licence 3"] = "The 3-Clause BSD License"
license_dict["BSD License 3"] = "The 3-Clause BSD License"
license_dict["BSD 3-clause"] = "The 3-Clause BSD License"
license_dict["BSD-3-Clause"] = "The 3-Clause BSD License"
license_dict["The BSD 3-Clause License"] = "The 3-Clause BSD License"
# The 2-Clause BSD License
license_dict["The 2-Clause BSD License"] = "The 2-Clause BSD License"
license_dict["BSD 2-Clause License"] = "The 2-Clause BSD License"
license_dict["Simplified BSD License"] = "The 2-Clause BSD License"
license_dict["FreeBSD License"] = "The 2-Clause BSD License"
license_dict["BSD License"] = "The 2-Clause BSD License"
license_dict["BSD Licence"] = "The 2-Clause BSD License"
license_dict["BSD"] = "The 2-Clause BSD License"
license_dict["The BSD License"] = "The 2-Clause BSD License"
# The MIT License
license_dict["The MIT License"] = "The MIT License"
license_dict["MIT License"] = "The MIT License"
license_dict["MIT license"] = "The MIT License"
# Eclipse Public License - v1.0
license_dict["Eclipse Public License 1.0"] = "Eclipse Public License - v1.0"
license_dict["Eclipse Public License - v 1.0"] = "Eclipse Public License - v1.0"
# Eclipse Public License - v2.0
license_dict["Eclipse Public License 2.0"] = "Eclipse Public License - v2.0"
# GNU General Public License
license_dict["GNU General Public License"] = "GNU General Public License"
# GNU General Public License, version 2
license_dict["The GNU General Public License, Version 2"] = "GNU General Public License, version 2"
# Creative Commons Zero
license_dict["cc0"] = "Creative Commons Zero"
license_dict["CC0"] = "Creative Commons Zero"
# CDDL + GPLv2 with classpath exception
license_dict["CDDL + GPLv2 with classpath exception"] = "CDDL + GPLv2 with classpath exception"
license_dict["CDDL/GPLv2+CE"] = "CDDL + GPLv2 with classpath exception"
# Common Development and Distribution License (CDDL) v1.0
license_dict["Common Development and Distribution License (CDDL) v1.0"] = "Common Development and Distribution License (CDDL) v1.0"
license_dict["CDDL 1.0"] = "Common Development and Distribution License (CDDL) v1.0"
# The JSON License
license_dict["The JSON License"] = "The JSON License"
# CUP license
license_dict["CUP License (MIT License)"] = "CUP License"
# Eclipse Distribution License - v1.0
license_dict["Eclipse Distribution License - v 1.0"] = "Eclipse Distribution License - v1.0"
license_dict["EDL 1.0"] = "Eclipse Distribution License - v1.0"
# Public Domain
license_dict["Public Domain"] = "Public Domain"

#########################################################

license_file_dict = {}
license_file_dict["The 3-Clause BSD License"] = "licenses/LICENSE-BSD-3.txt"
license_file_dict["The 2-Clause BSD License"] = "licenses/LICENSE-BSD-2.txt"
license_file_dict["The MIT License"] = "licenses/LICENSE-MIT.txt"
license_file_dict["Eclipse Public License - v1.0"] = "licenses/LICENSE-EPL-1.0.txt"
license_file_dict["Eclipse Public License - v2.0"] = "licenses/LICENSE-EPL-2.0.txt"
license_file_dict["Eclipse Distribution License - v1.0"] = "licenses/LICENSE-EDL-1.0.txt"
license_file_dict["Creative Commons Zero"] = "licenses/LICENSE-CC0.txt"
license_file_dict["CDDL + GPLv2 with classpath exception"] = "licenses/LICENSE-CDDL.txt & licenses/LICENSE-GPLv2-CE.txt"
license_file_dict["Common Development and Distribution License (CDDL) v1.0"] = "licenses/LICENSE-CDDL.txt"
license_file_dict["CUP License"] = "licenses/LICENSE-CUP.txt"
license_file_dict["Public Domain"] = "licenses/LICENSE-PD.txt"

class LicenseParser:

    def __init__(self, license_file, out_file):
        self.license_file = license_file
        self.out_file = out_file;
        self.out_dict = dict()

    def parse_and_output(self):
        for line in open(self.license_file):
            #print line
            line = line.strip()
            if not len(line) or line.startswith("Lists of"):
                continue
            r = re.search(r'\((.+)\) (.+) \((.+) - (.+)\)', line)
            license_alias = r.group(1)
            project_name = r.group(2)
            package_name = r.group(3)
            url = r.group(4)

            #print self.convert_to_standard_name(license_alias)
            #print project_name;
            #print package_name;
            #print url

            self.assemble_output(license_alias, project_name, package_name, url);

        self.save_to_file()

    def save_to_file(self):
        f = open(self.out_file, 'w');

        sorted_out_dict = collections.OrderedDict(sorted(self.out_dict.items()))
        for license_name in sorted_out_dict.keys():
            license_file_location = license_file_dict.get(license_name, "unknown")
            f.write(license_name + " -- " + license_file_location + "\n")

            sorted_out_dict_2 = collections.OrderedDict(sorted(self.out_dict[license_name].items()))
            for project_name in sorted_out_dict_2.keys():
                f.write("    * " + project_name + ":\n")
                sorted_out_dict_3 = sorted(self.out_dict[license_name][project_name])
                for package in sorted_out_dict_3:
                    f.write("        - " + package + "\n")
            f.write("\n")
        f.close()

    def assemble_output(self, license_alias, project_name, package_name, url):
        standard_name = self.convert_to_standard_name(license_alias);
        if standard_name in self.out_dict:
            if project_name in self.out_dict[standard_name]:
                self.out_dict[standard_name][project_name].add(package_name + " (" + url + ")")
            else:
                self.out_dict[standard_name][project_name] = set()
                self.out_dict[standard_name][project_name].add(package_name + " (" + url + ")")
        else:
            self.out_dict[standard_name] = dict()
            self.out_dict[standard_name][project_name] = set()
            self.out_dict[standard_name][project_name].add(package_name + " (" + url + ")")

    def convert_to_standard_name(self, alias):
        if alias in license_dict:
            return license_dict[alias]
        else:
            license_dict[alias] = alias;
            return alias

def main():
    license_file = sys.argv[1]
    out_file = sys.argv[2]
    license_parser = LicenseParser(license_file, out_file);
    license_parser.parse_and_output();

if __name__ == '__main__':
    main()

