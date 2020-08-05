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
      <el-breadcrumb-item >HA</el-breadcrumb-item>
    </el-breadcrumb>
   <el-form class="user-search">
   </el-form>
     <div>
        <div class="container">
            <el-tabs v-model="message" tabPosition="left">
                <el-tab-pane :label="`Frontend Role`" name="FrontendRole">
                    <el-table :data="variableInfoList" style="width: 100%">
                        <el-table-column sortable prop="Name" label="Name">
                        </el-table-column>
                        <el-table-column sortable prop="Value" label="Value">                          
                        </el-table-column>
                    </el-table>                   
                </el-tab-pane>
                <el-tab-pane :label="`Current JournalId`" name="CurrentJournalId">
                    <template>
                        <el-table v-if="message === 'CurrentJournalId'" :data="CurrentJournalIdList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
                <el-tab-pane :label="`Electable Nodes`" name="Electablenodes">
                    <template>
                        <el-table v-if="message === 'Electablenodes'" :data="ElectablenodesList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
                <el-tab-pane :label="`Observer Nodes`" name="Observernodes">
                    <template>
                        <el-table v-if="message === 'Observernodes'" :data="ObservernodesList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
                <el-tab-pane :label="`Can Read`" name="CanRead">
                    <template>
                        <el-table v-if="message === 'CanRead'" :data="CanReadList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
                <el-tab-pane :label="`DatabaseNames`" name="databaseNames">
                    <template>
                        <el-table v-if="message === 'databaseNames'" :data="databaseNamesList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
                <el-tab-pane :label="`CheckpointInfo`" name="CheckpointInfo">
                    <template>
                        <el-table v-if="message === 'CheckpointInfo'" :data="CheckpointInfoList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
                <el-tab-pane :label="`AllowedFrontends`" name="allowedFrontends">
                    <template>
                        <el-table v-if="message === 'allowedFrontends'" :data="allowedFrontendsList"  style="width: 100%">
                            <el-table-column sortable prop="Name" label="Name">
                            </el-table-column>
                            <el-table-column sortable prop="Value" label="Value">                          
                            </el-table-column>
                        </el-table>   
                    </template>
                </el-tab-pane>
                <el-tab-pane :label="`RemovedFronteds`" name="removedFronteds">
                    <template>
                        <el-table v-if="message === 'removedFronteds'" :data="removedFrontedsList"  style="width: 100%">
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
import { haList} from '../../api/doris'
export default {
  name: 'TaskCore',
  data() {
    return {
      message: 'FrontendRole',
      showHeader: true,
      FrontendRole:'',
      CurrentJournalId:'',
      Electablenodes:'',
      Observernodes:'',
      CanRead:'',
      CheckpointInfo:'',
      databaseNames:'',
      allowedFrontends:'',
      removedFronteds:'',
      FrontendRoleList:[],
      CurrentJournalIdList:[],
      ElectablenodesList:[],
      ObservernodesList:[],
      CanReadList:[],
      CheckpointInfoList:[],
      databaseNamesList:[],
      allowedFrontendsList:[],
      removedFrontedsList:[],
      variableInfoList:[]
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
      haList(parameter)
        .then(res => {
          this.loading = false
          if (res.success == false) {
  this.$message({
              type: 'info',
              message: res.msg
            })
          } else {
            this.FrontendRoleList=res.data.variableInfo
            this.CurrentJournalIdList=res.data.CurrentJournalId
            this.ElectablenodesList=res.data.Electablenodes
            this.ObservernodesList=res.data.Observernodes
            this.CanReadList=res.data.CanRead
            this.CheckpointInfoList=res.data.CheckpointInfo
            this.databaseNamesList=res.data.databaseNames
            this.allowedFrontendsList=res.data.allowedFrontends
            this.removedFrontedsList=res.data.removedFronteds
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

 
 