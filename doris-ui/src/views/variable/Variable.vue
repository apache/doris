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

/**
 * query Page
 */
<template>
  <div>
    <!-- 面包屑导航 -->
    <el-breadcrumb separator-class="el-icon-arrow-right">
      <el-breadcrumb-item :to="{ path: '/home/home' }">Home</el-breadcrumb-item>
      <el-breadcrumb-item >Variable</el-breadcrumb-item>
    </el-breadcrumb>
   <el-form  class="user-search">
   </el-form>
     <div>
        
        <div class="container">
            <el-tabs v-model="message">
                <el-tab-pane :label="`Variable Info`" name="variableInfo">
                    <el-table :data="variableInfoList" style="width: 100%">
                        <el-table-column sortable prop="Name" label="Name">
                        </el-table-column>
                        <el-table-column sortable prop="Value" label="Value">                          
                        </el-table-column>
                    </el-table>                   
                </el-tab-pane>
                <el-tab-pane :label="`Configure Info`" name="configureInfo">
                    <template>
                        <el-table v-if="message === 'configureInfo'" :data="configureInfoList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
            </el-tabs>
        </div>
    </div>
  </div>
</template>

<script>
import { variableList} from '../../api/doris'
export default {
  name: 'TaskCore',
  data() {
    return {
      message: 'variableInfo',
      showHeader: true,
      configureInfo:'',
      variableInfoList:[],
      configureInfoList:[]
    }
  },
  created() {
    this.getdata(this.formInline)
    self = this
  },
  methods: {
    // 获取Frontend列表
    getdata(parameter) {
      this.loading = true
      variableList(parameter)
        .then(res => {
          this.loading = false
          if (res.success == false) {
  this.$message({
              type: 'info',
              message: res.msg
            })
          } else {
            this.variableInfoList = res.data.variableInfo
            this.configureInfoList = res.data.configureInfo
          }
        })
        .catch(err => {
          this.loading = false
          this.$message.error('Authentication failed, Please login again')
          this.$router.push({ path: '/' })

        })
    },
    // 分页插件事件
    callFather(parm) {
      this.formInline.page = parm.currentPage
      this.formInline.limit = parm.pageSize
      this.getdata(this.formInline)
    },
    // 搜索事件
 
    // 关闭编辑、增加弹出框
    closeDialog() {
      this.editFormVisible = false
    }
  }
}
</script>

<style scoped>

    .message-title{
        cursor: pointer;
    }
    .handle-row{
        margin-top: 30px;
    }
.user-search {
  margin-top: 20px;
}
.userRole {
  width: 100%;
}
</style>

 
 