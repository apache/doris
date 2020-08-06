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
 * frontend Page
 */
<template>
  <div>
    <!-- 面包屑导航 -->
    <el-breadcrumb separator-class="el-icon-arrow-right">
      <el-breadcrumb-item :to="{ path: '/home/home' }">Home</el-breadcrumb-item>
      <el-breadcrumb-item :to="{ path: '/system/cluster_balance' }">Cluster Balance</el-breadcrumb-item>
      <el-breadcrumb-item >Pending Tablets</el-breadcrumb-item>
    </el-breadcrumb>
   <el-form :inline="true" :model="formInline" class="user-search">
   </el-form>
    <!--列表-->
    <el-table size="small" :data="listData" highlight-current-row v-loading="loading" border element-loading-text="loading..." style="width: 100%;">
      <el-table-column align="center" type="selection" width="60">
      </el-table-column>
      <el-table-column sortable prop="TabletId" label="TabletId">
      </el-table-column>
      <el-table-column sortable prop="Type" label="Type">
      </el-table-column>
      <el-table-column sortable prop="Medium" label="Medium">
      </el-table-column>
      <el-table-column sortable prop="Status" label="Status">
      </el-table-column>
      <el-table-column sortable prop="State" label="State">
      </el-table-column>
      <el-table-column sortable prop="OrigPrio" label="OrigPrio">
      </el-table-column>
      <el-table-column sortable prop="DynmPrio" label="DynmPrio">
      </el-table-column>
      <el-table-column sortable prop="SrcBe" label="SrcBe">
      </el-table-column>
      <el-table-column sortable prop="SrcPath" label="SrcPath">
      </el-table-column>
      <el-table-column sortable prop="DestBe" label="DestBe">
      </el-table-column>
      <el-table-column sortable prop="DestPath" label="DestPath">
      </el-table-column>
      <el-table-column sortable prop="Timeout" label="Timeout">
      </el-table-column>
      <el-table-column sortable prop="Create" label="Create">
      </el-table-column>
      <el-table-column sortable prop="LstSched" label="LstSched">
      </el-table-column>
      <el-table-column sortable prop="LstVisit" label="LstVisit">
      </el-table-column>
      <el-table-column sortable prop="Finished" label="Finished">
      </el-table-column>
      <el-table-column sortable prop="Rate" label="Rate">
      </el-table-column>
      <el-table-column sortable prop="FailedSched" label="FailedSched">
      </el-table-column>
      <el-table-column sortable prop="FailedRunning" label="FailedRunning">
      </el-table-column>
      <el-table-column sortable prop="LstAdjPrio" label="LstAdjPrio">
      </el-table-column>
      <el-table-column sortable prop="VisibleVer" label="VisibleVer">
      </el-table-column>
      <el-table-column sortable prop="VisibleVerHash" label="VisibleVerHash">
      </el-table-column>
      <el-table-column sortable prop="CmtVer" label="CmtVer">
      </el-table-column>
      <el-table-column sortable prop="CmtVerHash" label="CmtVerHash">
      </el-table-column>
      <el-table-column sortable prop="ErrMsg" label="ErrMsg">
      </el-table-column>
    </el-table>
    <!-- 分页组件 -->
    <Pagination v-bind:child-msg="pageparm" @callFather="callFather"></Pagination>
  </div>
</template>

<script>
import { cluster_balance_sub_List } from '../../api/doris'
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
        limit: 20,
        varLable: '',
        varName: '',
        token: localStorage.getItem('logintoken')
      },
     
      userparm: [], //搜索权限
      listData: [], //用户数据
      // 分页参数
      pageparm: {
        currentPage: 1,
        pageSize: 20,
        total: 20
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
      this.loading = true
      cluster_balance_sub_List(self.$route.params.id)
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
    // 搜索事件
 
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

 
 