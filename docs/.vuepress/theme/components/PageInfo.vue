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
  <div>
    <reco-icon
      v-if="pageInfo.frontmatter.author || $themeConfig.author"
      icon="reco-account"
    >
      <span>{{ pageInfo.frontmatter.author || $themeConfig.author }}</span>
    </reco-icon>
    <reco-icon
      v-if="pageInfo.frontmatter.date"
      icon="reco-date"
    >
      <span>{{ formatDateValue(pageInfo.frontmatter.date) }}</span>
    </reco-icon>
    <reco-icon
      v-if="showAccessNumber === true"
      icon="reco-eye"
    >
      <AccessNumber :idVal="pageInfo.path" :numStyle="numStyle" />
    </reco-icon>
    <reco-icon
      v-if="pageInfo.frontmatter.tags"
      icon="reco-tag"
      class="tags"
    >
      <span
        v-for="(subItem, subIndex) in pageInfo.frontmatter.tags"
        :key="subIndex"
        class="tag-item"
        :class="{ 'active': currentTag == subItem }"
        @click.stop="goTags(subItem)"
      >{{subItem}}</span>
    </reco-icon>
  </div>
</template>

<script>
import { defineComponent } from 'vue-demi'
import { RecoIcon } from '@vuepress-reco/core/lib/components'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  components: { RecoIcon },
  props: {
    pageInfo: {
      type: Object,
      default () {
        return {}
      }
    },
    currentTag: {
      type: String,
      default: ''
    },
    showAccessNumber: {
      type: Boolean,
      default: false
    }
  },

  setup (props, ctx) {
    const instance = useInstance()

    const numStyle = {
      fontSize: '.9rem',
      fontWeight: 'normal',
      color: '#999'
    }

    const goTags = (tag) => {
      if (instance.$route.path !== `/tag/${tag}/`) {
        instance.$router.push({ path: `/tag/${tag}/` })
      }
    }

    const formatDateValue = (value) => {
      return new Intl.DateTimeFormat(instance.$lang).format(new Date(value))
    }

    return { numStyle, goTags, formatDateValue }
  }
})
</script>

<style lang="stylus" scoped>
.iconfont
  display inline-block
  line-height 1.5rem
  &:not(:last-child)
    margin-right 1rem
  span
    margin-left 0.5rem
.tags
  .tag-item
    font-family Ubuntu, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Cantarell, 'Fira Sans', 'Droid Sans', 'Helvetica Neue', sans-serif
    cursor pointer
    &.active
      color $accentColor
    &:hover
      color $accentColor
@media (max-width: $MQMobile)
  .tags
    display block
    margin-left 0 !important
</style>
