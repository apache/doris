<!-- Licensed to the Apache Software Foundation (ASF) under one
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
  <!-- 公共布局 -->
  <Common class="tag-wrapper" :sidebar="false">
    <!-- 标签集合 -->
    <ModuleTransition>
      <TagList
        v-show="recoShowModule"
        class="tags"
        :currentTag="$currentTags.key"
        @getCurrentTag="tagClick"></TagList>
    </ModuleTransition>

    <!-- 博客列表 -->
    <ModuleTransition delay="0.08">
      <note-abstract
        v-show="recoShowModule"
        class="list"
        :data="posts"
        :currentTag="$currentTags.key"
        @paginationChange="paginationChange"></note-abstract>
    </ModuleTransition>
  </Common>
</template>

<script>
import { defineComponent, computed } from 'vue-demi'
import Common from '@theme/components/Common'
import NoteAbstract from '@theme/components/NoteAbstract'
import TagList from '@theme/components/TagList'
import { ModuleTransition } from '@vuepress-reco/core/lib/components'
import { sortPostsByStickyAndDate, filterPosts } from '@theme/helpers/postData'
import moduleTransitonMixin from '@theme/mixins/moduleTransiton'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  mixins: [moduleTransitonMixin],
  components: { Common, NoteAbstract, TagList, ModuleTransition },

  setup (props, ctx) {
    const instance = useInstance()

    // 时间降序后的博客列表
    const posts = computed(() => {
      let posts = instance.$currentTags.pages
      posts = filterPosts(posts)
      sortPostsByStickyAndDate(posts)
      return posts
    })

    const getCurrentTag = (tag) => {
      ctx.emit('currentTag', tag)
    }

    const tagClick = (tagInfo) => {
      if (instance.$route.path !== tagInfo.path) {
        instance.$router.push({ path: tagInfo.path })
      }
    }

    const paginationChange = (page) => {
      setTimeout(() => {
        window.scrollTo(0, 0)
      }, 100)
    }

    return { posts, getCurrentTag, tagClick, paginationChange }
  }
})
</script>

<style src="../styles/theme.styl" lang="stylus"></style>
<style src="prismjs/themes/prism-tomorrow.css"></style>
<style lang="stylus" scoped>

.tag-wrapper
  max-width: $contentWidth;
  margin: 0 auto;
  padding: 4.6rem 2.5rem 0;

@media (max-width: $MQMobile)
  .tag-wrapper
    padding: 4.6rem 1rem 0;
</style>
