<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<template>
<Common :sidebar="false">
  <div class="case-wrap">
    <div
        v-if="data.list"
        class="wrapper cases"
      >
        <div class="title">
          {{ data.title }}
        </div>
        <div class="sub-title">
          {{ data.subTitle }}
        </div>
        <div class="list">
          <div
            v-for="(cese, index) in data.list"
            :key="index"
            class="case"
          >
            <img
              v-if="cese.logo"
              :src="$withBase(cese.logo)"
              :alt="cese.alt"
            >
          </div>
        </div>
      </div>
  </div>
</Common> 
</template>

<script>
import Common from "@theme/components/Common";
export default {
  name: 'CaseList',
  components: {Common},
  computed: {
    data() {
      const lang = this.$lang
      const userCasePages = this.$site.pages.filter(page => page.frontmatter.type === 'userCase')
      const currentUserCase = userCasePages.find(page => page.frontmatter.language === lang)
      return currentUserCase.frontmatter
    },
  }
}
</script>

<style lang="stylus">
.case-wrap
  margin-left: 0
  margin-right 0
.wrapper
  box-sizing border-box
  max-width 1280px
  padding 2em 2em 2em 2em
  margin-left auto
  margin-right auto
  width 100%
  .title
    font-size 1.9rem
    text-align center
    color #595959
    font-family PingFangSC-Medium
  .sub-title
    text-align center
    font-family PingFangSC-Light
    font-size 1.3rem
    color #999999
    padding-top 1rem
.cases
  padding 5rem 0rem
  .list
    padding 0.5rem 0
    margin-top 2.5rem
    display flex
    flex-wrap wrap
    justify-content flex-start
    .case
      flex-grow 1
      flex-basis 25%
      max-width 25%
      padding: 1rem 0rem
      text-align center
      img 
        width 80%
        margin 0 auto
</style>