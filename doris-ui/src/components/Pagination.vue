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


<template>
  <el-pagination class="page-box" @size-change="handleSizeChange" @current-change="handleCurrentChange" background :current-page="childMsg.currentPage" :page-sizes="[10, 20, 30, 40]" :page-size="childMsg.pageSize" layout="total, sizes, prev, pager, next, jumper" :total="childMsg.total">
  </el-pagination>
</template>
<script>
export default {
  name: 'Pagination',
  props: ['childMsg'],
  data() {
    return {
      pageparm: {
        currentPage: this.childMsg.currentPage,
        pageSize: this.childMsg.pageSize
      }
    }
  },
  created() {},
  methods: {
    handleSizeChange(val) {
      this.pageparm.pageSize = val
      this.$emit('callFather', this.pageparm)
    },
    handleCurrentChange(val) {
      this.pageparm.currentPage = val
      this.$emit('callFather', this.pageparm)
    }
  }
}
</script>

<style>
.page-box {
  margin: 10px auto;
}
</style>
