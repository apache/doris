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
    <Navbar/>
    <div class="content-wrapper">
      <div class="article-list">
        <article class="article-item" v-for="item in currentArticleList" :key="item.key">
          <header class="article-item-title">
            <router-link :to="item.path" class="nav-link">{{item.frontmatter.title}}</router-link>
          </header>
          <p class="article-item-summary">{{item.frontmatter.description}}</p>
          <footer>
            <div class="article-item-meta article-item-time">
              {{$themeLocaleConfig.article.metaTime}}：<span>{{item.frontmatter.date}}</span>
            </div>
            <div class="article-item-meta article-item-author">
              {{$themeLocaleConfig.article.metaAuthor}}：<span>{{item.frontmatter.author}}</span>
            </div>
          </footer>
        </article>
        <div class="article-pagination-wrap">
          <span class="article-pagination-button pre" v-show="pageNumber > 1" @click="prePage">{{$themeLocaleConfig.article.paginationPre}}</span>
          <span class="article-pagination-button next" v-show="hasNextPaginationButton" @click="nextPage">{{$themeLocaleConfig.article.paginationNext}}</span>
        </div>
      </div>
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
  name: 'ArticleList',
  components: {
    CustomFooter,
    Navbar
  },
  data () {
    return {
      pageNumber: 1,
      pageSize: 8
    }
  },
  computed: {
    articleList () {
      const list = this.$site.pages.filter(item => item.frontmatter.isArticle && item.frontmatter.language === this.$lang)
      return list.sort((a, b) => Date.parse(b.frontmatter.date) - Date.parse(a.frontmatter.date))
    },
    currentArticleList () {
      const start = (this.pageNumber - 1) * this.pageSize
      const end = start + this.pageSize
      return this.articleList.slice(start, end)
    },
    hasNextPaginationButton () {
      return (this.pageNumber * this.pageSize) < this.articleList.length
    }
  },
  methods: {
    nextPage () {
      this.pageNumber++
    },
    prePage () {
      this.pageNumber--
    }
  }
};
</script>
<style lang="stylus">
  .content-wrapper
    padding: 100px 15px 80px
    min-height: calc(100vh - 180px)
    max-width: 80%
    margin: 0 auto
    .article-item 
      padding-bottom: 20px
      margin-bottom: 20px
      border-bottom: 1px solid rgba(0,0,0,.05)
      .article-item-title 
        font-size: 26px
        border-bottom: 0
        .nav-link
          cursor: pointer
          color: #2c3e50
          transition: all .2s
          text-decoration: none
        .nav-link:hover
          color: #3eaf7c
      .article-item-summary
        font-size: 14px
        color: #888888
        margin-bottom: 10px
      .article-item-meta
        display: inline-flex
        align-items: center
        font-size: 12px
        line-height: 12px
        color: #888888
      .article-item-meta:not(:last-child)
        margin-bottom: 3px
        margin-right: 20px
    .article-item:last-child
      border: none
    .article-pagination-wrap
      position: relative
      overflow: hidden
      height: 30px
      margin-bottom: 20px
      .article-pagination-button
        position: absolute
        top: 0
        font-size: 16px
        color: #777777
        cursor: pointer
      .article-pagination-button.pre
        left: 0
      .article-pagination-button.next
        right: 0
      .article-pagination-button:hover
        color: #3eaf7c  
</style>