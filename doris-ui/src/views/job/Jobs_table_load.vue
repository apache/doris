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
 * Jobs Page
 */
<template>
  <div>
    <!-- 面包屑导航 -->
    <el-breadcrumb separator-class="el-icon-arrow-right">
      <el-breadcrumb-item :to="{ path: '/home/home' }">Home</el-breadcrumb-item>
      <el-breadcrumb-item :to="{path:'/job/Jobs'}">Jobs DB</el-breadcrumb-item>
      <el-breadcrumb-item>Jobs Operation</el-breadcrumb-item>
      <el-breadcrumb-item>Jobs Load</el-breadcrumb-item>
    </el-breadcrumb>
   <el-form :inline="true" :model="formInline" class="user-search">
   </el-form>
    <!--列表-->
    <el-table size="small" :data="listData" highlight-current-row v-loading="loading" border element-loading-text="loading..." style="width: 100%;" >
      <el-table-column align="center" type="selection" width="60">
      </el-table-column>
      <el-table-column sortable prop="JobId" label="JobId">
      </el-table-column>
      <el-table-column sortable prop="Label" label="Label" >
      </el-table-column> 
      <el-table-column sortable prop="State" label="State" >
      </el-table-column> 
      <el-table-column sortable prop="Progress" label="Progress" >
      </el-table-column> 
      <el-table-column sortable prop="Type" label="Type" >
      </el-table-column> 
      <el-table-column sortable prop="EtlInfo" label="EtlInfo" >
      </el-table-column>  
      <el-table-column sortable prop="TaskInfo" label="TaskInfo" >
      </el-table-column>  
      <el-table-column sortable prop="ErrorMsg" label="ErrorMsg" >
      </el-table-column>  
      <el-table-column sortable prop="CreateTime" label="CreateTime" >
      </el-table-column>  
      <el-table-column sortable prop="EtlStartTime" label="EtlStartTime" >
      </el-table-column>  
      <el-table-column sortable prop="EtlFinishTime" label="EtlFinishTime" >
      </el-table-column>  
      <el-table-column sortable prop="LoadStartTime" label="LoadStartTime" >
      </el-table-column>  
      <el-table-column sortable prop="LoadFinishTime" label="LoadFinishTime">
      </el-table-column> 
      <el-table-column sortable prop="URL" label="URL" >
      </el-table-column> 
      <el-table-column sortable prop="JobDetails" label="JobDetails">
      </el-table-column> 
    </el-table>
    <!-- 分页组件 -->
    <Pagination v-bind:child-msg="pageparm" @callFather="callFather"></Pagination>
  </div>
</template>

<script>
import { table_info_list } from '../../api/doris'
import Pagination from '../../components/Pagination'
export default {
  data() {
    return {
      nshow: true, //switch开启
      fshow: false, //switch关闭
      loading: false, //是显示加载
      editFormVisible: false, //控制编辑页面显示与隐藏
      title: '添加',
      editForm: {
        deptId: '',
        deptName: '',
        deptNo: '',
        token: localStorage.getItem('logintoken')
      },
      formInline: {
        page: 1,
        limit: 10,
        varLable: '',
        varName: '',
        token: localStorage.getItem('logintoken')
      },
     
      userparm: [], //搜索权限
      listData: [], //用户数据
      // 分页参数
      pageparm: {
        currentPage: 1,
        pageSize: 10,
        total: 10
      }
    }
  },
  // 注册组件
  components: {
    Pagination
  },
  /**
   * 数据发生改变
   */

  /**
   * 创建完毕
   */
  created() {
    this.getdata(this.formInline)
    self = this
  },

  /**
   * 里面的方法只有被调用才会执行
   */
  methods: {
    // 获取Frontend列表
    getdata(parameter) {
      this.loading = true;
      table_info_list(self.$route.params.id.replace(new RegExp(/\\/g),'\/'))
        .then(res => {
          this.loading = false
          if (res.success == false) {
  this.$message({
              type: 'info',
              message: res.msg
            })
          } else {
            this.listData = res.data
            // 分页赋值
            this.pageparm.currentPage = this.formInline.page
            this.pageparm.pageSize = this.formInline.limit
            this.pageparm.total = res.count
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
    // 关闭编辑、增加弹出框
    closeDialog() {
      this.editFormVisible = false
    }
  }
}
</script>

<style scoped>
.user-search {
  margin-top: 20px;
}
.userRole {
  width: 100%;
}
</style>

 
 