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
      <el-breadcrumb-item >Log</el-breadcrumb-item>
    </el-breadcrumb>
   <h1 font-size="medium;"><b>Log Configuration</b></h1>
      <pre>Level: {{Level}}</pre>
      <pre>Verbose Names: {{VerboseNames}}</pre>
      <pre>Audit Names:{{AuditNames}}</pre>
   <el-form :inline="true" :model="formInline" class="user-search">
     <el-form-item label="">
        <el-input size="small" v-model="formInline.add_verbose" placeholder="new verbose name"></el-input>
      </el-form-item>
       <el-button size="small" type="primary" icon="el-icon-search"  @click="add">Add</el-button>
      <el-form-item label="">
        <el-input size="small" v-model="formInline.del_verbose"  placeholder="del verbose name"></el-input>
      </el-form-item>
       <el-button size="small" type="primary" icon="el-icon-search" @click="del">Delete</el-button>
   </el-form>

   <h1><b>Log Contents</b></h1>
      <pre>Log path is: {{logPath}}</pre>
      <pre>Show last {{Showinglast}}</pre>
      <div class="div1background" >
        <div class="div2background">
            <pre><div v-html="log"></div></pre>
        </div>
      </div>
  </div>
</template>

<script>
import { logList} from '../../api/doris'
import Pagination from '../../components/Pagination'
export default {
  data() {
    return {
      nshow: true, //switch开启
      fshow: false, //switch关闭
      loading: false, //是显示加载
      editFormVisible: false, //控制编辑页面显示与隐藏
      Level:'',
      VerboseNames:'',
      AuditNames:'',
      logPath:'',
      Showinglast:'',
      log:'',
      title: '添加',
      editForm: {
        deptId: '',
        deptName: '',
        deptNo: '',
        token: localStorage.getItem('logintoken')
      },
      formInline: {
        page: 1,
        limit: 100,
        varLable: '',
        varName: '',
        token: localStorage.getItem('logintoken')
      },
      userparm: [], //搜索权限
      listData: [], //用户数据
      // 分页参数
      pageparm: {
        currentPage: 1,
        pageSize: 100,
        total: 100
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
    this.getdata('')
    self = this
  },
  goto_profile(index,row){
        
  },
  /**
   * 里面的方法只有被调用才会执行
   */
  methods: {
    // 获取Frontend列表
    getdata(parameter) {
      this.loading = true
      logList(parameter)
        .then(res => {
          this.loading = false
          if (res.success == false) {
  this.$message({
              type: 'info',
              message: res.msg
            })
          } else {
            this.Level = res.data.LogConfiguration.Level
            this.VerboseNames = res.data.LogConfiguration.VerboseNames
            this.AuditNames = res.data.LogConfiguration.AuditNames
            this.logPath = res.data.LogContents.logPath
            this.log = res.data.LogContents.log
            this.Showinglast = res.data.LogContents.Showinglast
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
    add() {
      var param = "?add_verbose=" + this.formInline.add_verbose;
      this.getdata(param)
    },
    del(){
      var param = "?del_verbose=" + this.formInline.del_verbose;
      this.getdata(param)
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

 
 