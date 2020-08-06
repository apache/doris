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
      <el-breadcrumb-item >System Info</el-breadcrumb-item>
    </el-breadcrumb>
   <h1 font-size="medium;"><b>Version</b></h1>
   <div class="div1background" >
        <div class="div2background">
          <pre>Git: {{Git}}</pre>
          <pre>Version: {{Version}}</pre>
          <pre>Build Info:{{BuildInfo}}</pre>
          <pre>Build Time:{{BuildTime}}</pre>
      </div>
   </div>
   
   <h1><b>Hardware Info</b></h1>
   <div class="div1background" >
        <div class="div2background">
          <div background-color="#DCDCDC">
              <pre><div v-html="OS"></div></pre>
              <div class="verticalBar"></div>
              <pre><div v-html="Processor"></div></pre>
              <div class="verticalBar"></div>
              <pre><div v-html="Memory"></div></pre>
              <div class="verticalBar"></div>
              <pre><div v-html="Processes"></div></pre>
              <div class="verticalBar"></div>
              <pre><div v-html="Disk"></div></pre>
              <div class="verticalBar"></div>
              <pre><div v-html="FileSystem"></div></pre>
              <div class="verticalBar"></div>
              <pre><div v-html="NetworkInterface"></div></pre>
              <div class="verticalBar"></div>
              <pre><div v-html="NetworkParameter"></div></pre>
            </div>
        </div>
   </div>
  </div>
</template>

<script>
import { systemInfoList} from '../../api/doris'
import Pagination from '../../components/Pagination'
export default {
  data() {
    return {
        nshow: true, //switch开启
        fshow: false, //switch关闭
        loading: false, //是显示加载
        editFormVisible: false, //控制编辑页面显示与隐藏
        Git:'',
        Version:'',
        BuildInfo:'', 
        BuildTime:'',
        OS:'',
        Processor:'', 
        Memory:'',
        Disk:'',
        FileSystem:'',
        NetworkInterface:'',
        NetworkParameter:'',
        Processes:'',
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
      systemInfoList(parameter)
        .then(res => {
          this.loading = false
          if (res.success == false) {
  this.$message({
              type: 'info',
              message: res.msg
            })
          } else {
            this.Git = res.data.VersionInfo.Git
            this.Version = res.data.VersionInfo.Version
            this.BuildInfo = res.data.VersionInfo.BuildInfo
            this.BuildTime = res.data.VersionInfo.BuildTime
            this.OS = res.data.HarewareInfo.OS
            this.Processor = res.data.HarewareInfo.Processor
            this.Memory = res.data.HarewareInfo.Memory
            this.Disk = res.data.HarewareInfo.Disk
            this.Processes = res.data.HarewareInfo.Processes
            this.FileSystem = res.data.HarewareInfo.FileSystem
            this.NetworkInterface = res.data.HarewareInfo.NetworkInterface
            this.NetworkParameter = res.data.HarewareInfo.NetworkParameter
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
.verticalBar {
margin: 0 auto;
    width: 100%;
    height: 1px;
    background-color: #d4d4d4;
    text-align: center;
    font-size: 16px;
    color: rgba(101, 101, 101, 1);
    }


</style>

 
 