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
  <div class="tags">
    <span
      v-for="(item, index) in tags"
      v-show="!item.pages || (item.pages && item.pages.length > 0)"
      :key="index"
      :class="{'active': item.name == currentTag}"
      :style="{ 'backgroundColor': getOneColor() }"
      @click="tagClick(item)">{{item.name}}</span>
  </div>
</template>

<script>
import { defineComponent, computed } from 'vue-demi'
import { getOneColor } from '@theme/helpers/other'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  props: {
    currentTag: {
      type: String,
      default: ''
    }
  },
  setup (props, ctx) {
    const instance = useInstance()
    const tags = computed(() => {
      return [{ name: instance.$recoLocales.all, path: '/tag/' }, ...instance.$tagesList]
    })

    const tagClick = tag => {
      ctx.emit('getCurrentTag', tag)
    }

    return { tags, tagClick, getOneColor }
  }
})
</script>

<style lang="stylus" scoped>
.tags
  margin 30px 0
  span
    vertical-align: middle;
    margin: 4px 4px 10px;
    padding: 4px 8px;
    display: inline-block;
    cursor: pointer;
    border-radius: $borderRadius
    background: #fff;
    color: #fff;
    line-height 13px
    font-size: 13px;
    box-shadow var(--box-shadow)
    transition: all .5s
    &:hover
      transform scale(1.04)
    &.active
      transform scale(1.2)
</style>
