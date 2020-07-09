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

    <h1><b>Exact Matching Topic</b></h1>
      <pre>{{logPath}}</pre>
    <h1><b>Fuzzy Matching Topic(By Keyword)</b></h1>
      <pre>{{logPath}}</pre>
    <h1><b>Category Info</b></h1>
      <pre>Find only one category, so show you the detail info below.</pre>
      <pre>Find 2 sub categories</pre>

  </div>
</template>

<script>
import { logList} from '../../api/userMG'
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
          this.$message.error('菜单加载失败，请稍后再试！')
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

 
 