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
  <div class="article-wrap">
    <Navbar />
    <div class="content-wrapper1">
      <div class="article-header">
        <h1>{{frontmatter.title}}</h1>
        <div class="article-sub-header">
          <span class="article-date article-header-item" v-if="frontmatter.date">{{$themeLocaleConfig.article.metaTime}}：
            <span class="article-header-item-content">{{frontmatter.date}}</span>
          </span>
          <span class="article-author article-header-item" v-if="frontmatter.author">{{$themeLocaleConfig.article.metaAuthor}}：
            <span class="article-header-item-content">{{frontmatter.author}}</span>
          </span>
        </div>
      </div>
      <Content></Content>
      <footer class="article-footer">
        <CustomFooter />
      </footer>
    </div>
  </div>
</template>
<script>
import CustomFooter from "@theme/components/Footer.vue";
import Navbar from "@parent-theme/components/Navbar.vue";
export default {
  name: 'Article',
  components: {
    CustomFooter,
    Navbar
  },
  computed: {
    frontmatter () {
      return this.$page.frontmatter
    }
  }
};
</script>
<style lang="stylus">
  .content-wrapper1
    padding: 60px 15px 80px
    min-height: calc(100vh - 140px)
    max-width: 80%
    margin: 0 auto
    .article-sub-header
      .article-header-item
        font-size: 13px
        color: #888888
        margin-right 16px
        .article-header-item-content
          color: #555555
</style>