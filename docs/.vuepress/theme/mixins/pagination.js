/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
export default {
  methods: {
    // 获取当前页码
    _getStoragePage () {
      const path = window.location.pathname
      const currentPage = JSON.parse(sessionStorage.getItem('currentPage'))

      if (currentPage === null || path !== currentPage.path) {
        sessionStorage.setItem('currentPage', JSON.stringify({ page: 1, path: '' }))
        return 1
      }

      return parseInt(currentPage.page)
    },
    // 设置当前页码
    _setStoragePage (page) {
      const path = window.location.pathname
      sessionStorage.setItem('currentPage', JSON.stringify({ page, path }))
    }
  }
}
