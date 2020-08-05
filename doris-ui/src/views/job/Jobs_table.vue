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
    </el-breadcrumb>
   <el-form :inline="true" :model="formInline" class="user-search">
   </el-form>
    <!--列表-->
    <el-table size="small" :data="listData" highlight-current-row v-loading="loading" border element-loading-text="loading..." style="width: 100%;"  @row-click="goto_job_task">
      <el-table-column align="center" type="selection" width="60">
      </el-table-column>
      <el-table-column sortable prop="JobType" label="JobType">
      </el-table-column>
      <el-table-column sortable prop="Finished" label="Finished" >
      </el-table-column> 
      <el-table-column sortable prop="Running" label="Running" >
      </el-table-column> 
      <el-table-column sortable prop="Cancelled" label="Cancelled" >
      </el-table-column> 
      <el-table-column sortable prop="Pending" label="Pending" >
      </el-table-column> 
      <el-table-column sortable prop="Total" label="Total" >
      </el-table-column>  
      <el-table-column sortable prop="hrefPath" label="hrefPath" v-if="show">
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
      show: false,
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
  
    goto_job_task(row, event, column){
      console.log("hrefPath = "+ row.hrefPath)
      var routePath = "";
      if(row.JobType == "load"){
          routePath = '/job/Jobs_table_load/'+row.hrefPath.replace(new RegExp(/\//g),'\\') ;
      } else if(row.JobType == "delete"){
          routePath = '/job/Jobs_table_delete/'+row.hrefPath.replace(new RegExp(/\//g),'\\') ;
      } else if(row.JobType == "rollup"){
          routePath = '/job/Jobs_table_rollup/'+row.hrefPath.replace(new RegExp(/\//g),'\\') ;
      } else if(row.JobType == "schema_change"){
          routePath = '/job/Jobs_table_schema_change/'+row.hrefPath.replace(new RegExp(/\//g),'\\') ;
      } else if(row.JobType == "export"){
          routePath = '/job/Jobs_table_export/'+row.hrefPath.replace(new RegExp(/\//g),'\\') ;
      }
      self.$router.push({ 
         path: routePath 
      })
    },
    // 获取Frontend列表
    getdata(parameter) {
      this.loading = true
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

 
 