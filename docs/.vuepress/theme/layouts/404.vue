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
  <section class="theme-container nopage" v-if="!noFoundPageByTencent">
    <article class="content">
      <h1>404</h1>
      <blockquote>{{ getMsg() }}</blockquote>
      <router-link to="/">Take me home.</router-link>
    </article>
  </section>
</template>

<script>
import { defineComponent, computed, onMounted } from 'vue-demi'
import { useInstance } from '@theme/helpers/composable'

const msgs = [
  `There's nothing here.`,
  `How did we get here?`,
  `That's a Four-Oh-Four.`,
  `Looks like we've got some broken links.`
]

export default defineComponent({
  setup (props, ctx) {
    const instance = useInstance()

    const noFoundPageByTencent = computed(() => {
      return instance.$themeConfig.noFoundPageByTencent !== false
    })

    const getMsg = () => {
      return msgs[Math.floor(Math.random() * msgs.length)]
    }

    onMounted(() => {
      if (noFoundPageByTencent.value) {
        const dom = document.createElement('script')
        dom.setAttribute('homePageName', '回到首页')
        dom.setAttribute('homePageUrl', instance.$site.base)
        dom.setAttribute('src', '//qzonestyle.gtimg.cn/qzone/hybrid/app/404/search_children.js')

        document.body.append(dom)
      }
    })

    return { noFoundPageByTencent, getMsg }
  }
})
</script>

<style src="../styles/theme.styl" lang="stylus"></style>

<style lang="stylus">
.content
  margin 4rem auto 0
  max-width 800px
  padding 0 2rem
.mod_404
  .desc
    .desc_link
      display: inline-block
      // margin: 20px 0
      background: #424242!important
      color: #ffffff
      padding: 6px 20px!important
      text-decoration: none!important
      border-radius: 4px

@media screen and (max-width: 720px)
  .mod_404
    .desc
      margin: 50px 0
    .wrapper
      margin 0!important
      padding-top 20px
</style>

