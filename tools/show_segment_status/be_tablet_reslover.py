#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

import json
import time 
from urllib import urlopen

class BeTabletResolver:
    def __init__(self, be_list, tablet_map):
        self.tablet_map = tablet_map
        self.tablet_infos = {}

        self.be_map = {}
        for be in be_list:
            self.be_map[be['be_id']] = be

    def debug_output(self):
        print "tablet_infos:(%s), print up to ten here:" % len(self.tablet_infos)
        self._print_list(self.tablet_infos.values()[0:10])
        print

    def _print_list(self, one_list):
        for item in one_list:
            print item

    def init(self):
        self.fetch_tablet_meta()

    def fetch_tablet_meta(self):
        print "fetching tablet metas from BEs..."
        count = 0
        for tablet in self.tablet_map.values():
            be_id = tablet['be_id']
            be = self.be_map[be_id]
            url = self._make_url(be, tablet)
            print url
            tablet_meta = self._fetch_tablet_meta_by_id(url)
            self._decode_rs_metas_of_tablet(tablet_meta)
            # slow down, do not need too fast
            count += 1
            if count % 10 == 0:
                time.sleep(0.005)
        print "finished. \n"
        return

    def _make_url(self, be, tablet):
        url_list = []
        url_list.append("http://")
        url_list.append(be["ip"])
        url_list.append(":")
        url_list.append(be["http_port"])
        url_list.append("/api/meta/header/")
        url_list.append(str(tablet["tablet_id"]))
        url_list.append("/")
        url_list.append(str(tablet["schema_hash"]))
        return "".join(url_list)

    def _fetch_tablet_meta_by_id(self, url):
        tablet_meta = urlopen(url).read()
        tablet_meta = json.loads(tablet_meta)
        return tablet_meta

    def _decode_rs_metas_of_tablet(self, tablet_meta):
        # When something wrong, may do not have rs_metas attr, so use 'get()' instead of '[]'
        rs_metas = tablet_meta.get('rs_metas')
        if rs_metas is None:
            return
        size = len(rs_metas)

        rowsets = []
        for rs_meta in rs_metas:
            rowset = {}
            rowset['tablet_id'] = rs_meta['tablet_id']
            rowset['num_rows'] = rs_meta['num_rows']
            rowset['data_disk_size'] = rs_meta['data_disk_size']
            if rs_meta['rowset_type'] == 'BETA_ROWSET':
                rowset['is_beta'] = True
            else:
                rowset['is_beta'] = False
            rowsets.append(rowset);

        self.tablet_infos[rs_meta['tablet_id']] = rowsets
        return

    def get_rowsets_by_tablet(self, tablet_id):
        return self.tablet_infos.get(tablet_id)

    def get_all_rowsets(self):
        return self.tablet_infos.values()

if __name__ == '__main__':
    main()

