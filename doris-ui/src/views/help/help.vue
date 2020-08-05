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
      <el-breadcrumb-item :to="{ path: '/help/help' }">Help</el-breadcrumb-item>
    </el-breadcrumb>
    <h1 font-size="medium;"><b>Help Info</b></h1>
      <pre>This page lists the help info, like 'help contents' in Mysql client.</pre>
    <el-form :inline="true" :model="formInline" class="user-search">
     <el-form-item label="">
        <el-input size="small" v-model="formInline.query" placeholder="key"></el-input>
      </el-form-item>
       <el-button size="small" type="primary" icon="el-icon-search"  @click="search">Search</el-button>
    </el-form>

    <h3><b>Exact Matching Topic</b></h3>
      <div class="div1background" x-if="showmatching">
        <div class="div2background">
          <pre>{{matching}}</pre>
        </div>
      </div>
      <!--showmatchingTopic start-->
      <div class="div1background" v-show="showmatchingTopic">
        <div class="div2background">
          <h4>'{{matchingtopic}}'</h4>
          <strong>Description</strong>
          <pre class="topic_text" style="border: 0px;">
            <div v-html="matchingdescription"></div>
          </pre>
          <strong>Example</strong>
          <pre class="topic_text" style="border: 0px">
            <div v-html="matchingexample"></div>
          </pre>
          <strong>Keyword</strong>
          <pre class="topic_text" style="border: 0px">[{{matchingKeyword}}]</pre>
          <strong>Url</strong>
          <pre class="topic_text" style="border: 0px">{{matchingUrl}}</pre>
        </div>
      </div>
      <!--showmatchingTopic end-->
    <h3><b>Fuzzy Matching Topic(By Keyword)</b></h3>
      <div class="div1background" v-show="showfuzzy">
        <div class="div2background">
          <pre>{{fuzzy}}</pre>
        </div>
      </div>
      <!--showfuzzyTopic start-->
      <div class="div1background" v-show="showfuzzyTopic">
        <div class="div2background">
          <h4>'{{fuzzytopic}}'</h4>
          <strong>Description</strong>
          <pre class="topic_text" style="border: 0px;">
            <div v-html="fuzzydescription"></div>
          </pre>
          <strong>Example</strong>
          <pre class="topic_text" style="border: 0px">
            <div v-html="fuzzyexample"></div>
          </pre>
          <strong>Keyword</strong>
          <pre class="topic_text" style="border: 0px">[{{fuzzyKeyword}}]</pre>
          <strong>Url</strong>
          <pre class="topic_text" style="border: 0px">{{fuzzyUrl}}</pre>
        </div>
      </div>
      <!--showfuzzyTopic end-->
    <h3><b>Category Info</b></h3>
      <pre>{{cateMatchingTitle}}</pre>
      <pre v-if="showtopicSize">Find {{topicSize}} sub topics.</pre>
        <!--列表-->
    <el-table size="small" v-if="showtopicdatas" :data="topicdatas" highlight-current-row v-loading="loading" border element-loading-text="loading..." style="width: 100%;" @row-click="rowsearch">
      <el-table-column align="center" type="selection" width="60">
      </el-table-column>
      <el-table-column sortable prop="name" label="Sub Topics">
      </el-table-column>
    </el-table>   
      <pre v-if="showsubCateSize">Find {{subCateSize}} sub categories</pre>
    <!--列表-->
    <el-table size="small" v-if="showsubdatas" :data="subdatas" highlight-current-row v-loading="loading" border element-loading-text="loading..." style="width: 100%;" @row-click="rowsearch">
      <el-table-column align="center" type="selection" width="60">
      </el-table-column>
      <el-table-column sortable prop="name" label="Sub Categories">
      </el-table-column>
    </el-table>
      <pre  v-if="showsubCateSize">Find {{subCateSize}} sub categories</pre>
  </div>
</template>

<script>
import { helpList} from '../../api/doris'
import Pagination from '../../components/Pagination'
export default {
  data() {
    return {
      nshow: true, //switch开启
      fshow: false, //switch关闭
      loading: false, //是显示加载
      editFormVisible: false, //控制编辑页面显示与隐藏
      matching:'',
      fuzzy:'',
      cateMatchingTitle:'',
      topicSize:'',
      subCateSize:'',
      subdatas:[],
      topicdatas:[],
      title: '添加',
      showtopicSize:false,
      showtopicdatas:false,
      showsubCateSize:false,
      showsubdatas:false,
      showsubCateSize:false,
      showfuzzyTopic:0,
      showfuzzy:false,
      showmatchingTopic:0,
      showmatching:false,
      fuzzyKeyword:'',
      fuzzyUrl:'',
      fuzzydescription:'',
      fuzzyexample:'',
      fuzzytopic:'',
      matchingKeyword:'',
      matchingUrl:'',
      matchingdescription:'',
      matchingexample:'',
      matchingtopic:'',
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
      helpList(parameter)
        .then(res => {
          this.loading = false
          if (res.success == false) {
  this.$message({
              type: 'info',
              message: res.msg
            })
          } else {
            this.cateMatchingTitle = res.data.cateMatchingTitle
            if(res.data.fuzzy != null){
              this.fuzzy = res.data.fuzzy
              this.showfuzzy = true
            }
            if(res.data.matching != null){
              this.matching = res.data.matching
              this.showmatching = true;
            }
            if(res.data.matchingTopic != null){
                this.showmatchingTopic= 1
                this.matchingKeyword=res.data.matchingTopic.matchingKeyword
                this.matchingUrl=res.data.matchingTopic.matchingUrl
                this.matchingdescription=res.data.matchingTopic.matchingdescription
                this.matchingexample=res.data.matchingTopic.matchingexample
                this.matchingtopic=res.data.matchingTopic.matchingtopic
            }
            if(res.data.fuzzyTopic != null){
                this.showfuzzyTopic= 1
                this.fuzzyKeyword=res.data.fuzzyTopic.fuzzyKeyword
                this.fuzzyUrl=res.data.fuzzyTopic.fuzzyUrl
                this.fuzzydescription=res.data.fuzzyTopic.fuzzydescription
                this.fuzzyexample=res.data.fuzzyTopic.fuzzyexample
                this.fuzzytopic=res.data.fuzzyTopic.fuzzytopic
            }
            if( res.data.subCateSize != null ){
              this.subCateSize = res.data.subCateSize
              this.showsubCateSize=true
            }
            if(res.data.topicSize != null){
              this.topicSize = res.data.topicSize
              this.showtopicSize=true
            }
            if(res.data.subdatas != null){
              this.subdatas = res.data.subdatas
              this.showsubdatas=true;
            }
            if(res.data.topicdatas != null){
              this.topicdatas = res.data.topicdatas
              this.showtopicdatas = true;
            }

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
    search() {
      var param = "?query=" + this.formInline.query;
      this.getdata(param)
    },
    rowsearch(row, event, column) {
      var param = "?query=" + row.name;
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

 
 