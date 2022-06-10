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
  <form
    id="search-form"
    class="algolia-search-wrapper search-box"
    role="search"
  >
    <reco-icon icon="reco-search" />
    <input
      id="algolia-search-input"
      class="search-query"
      :placeholder="placeholder"
    >
  </form>
</template>

<script>
import { defineComponent, ref, onMounted } from 'vue-demi'
import { RecoIcon } from '@vuepress-reco/core/lib/components'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  components: { RecoIcon },

  props: ['options'],

  setup (props, ctx) {
    const instance = useInstance()

    const placeholder = ref(undefined)

    const initialize = (userOptions, lang) => {
      Promise.all([
        import(/* webpackChunkName: "docsearch" */ 'docsearch.js/dist/cdn/docsearch.min.js'),
        import(/* webpackChunkName: "docsearch" */ 'docsearch.js/dist/cdn/docsearch.min.css')
      ]).then(([docsearch]) => {
        docsearch = docsearch.default
        const { algoliaOptions = {} } = userOptions
        docsearch(Object.assign(
          {},
          userOptions,
          {
            inputSelector: '#algolia-search-input',
            // #697 Make docsearch work well at i18n mode.
            algoliaOptions: Object.assign({
              'facetFilters': [`lang:${lang}`].concat(algoliaOptions.facetFilters || [])
            }, algoliaOptions),
            handleSelected: (input, event, suggestion) => {
              const { pathname, hash } = new URL(suggestion.url)
              this.$router.push(`${pathname}${hash}`)
            }
          }
        ))
      })
    }

    const update = (options, lang) => {
      instance.$el.innerHTML = '<input id="algolia-search-input" class="search-query">'
      instance.initialize(options, lang)
    }

    onMounted(() => {
      initialize(props.options, instance.$lang)
      placeholder.value = instance.$site.themeConfig.searchPlaceholder || ''
    })

    return { placeholder, initialize, update }
  },

  watch: {
    $lang (newValue) {
      this.update(this.options, newValue)
    },

    options (newValue) {
      this.update(newValue, this.$lang)
    }
  }
})
</script>

<style lang="stylus">
.algolia-search-wrapper
  & > span
    vertical-align middle
  .algolia-autocomplete
    line-height normal
    .ds-dropdown-menu
      background-color var(--background-color)
      border-radius $borderRadius
      font-size 15px
      margin 6px 0 0
      padding 4px
      text-align left
      box-shadow var(--box-shadow)
      &:before
        display none
      [class*=ds-dataset-]
        background-color var(--background-color)
        border none
        padding 0
      .ds-suggestions
        margin-top 0
      .ds-suggestion
        border-bottom 1px solid var(--border-color)
    .algolia-docsearch-suggestion--highlight
      color $accentColor
    .algolia-docsearch-suggestion
      border-color var(--border-color)
      padding 0
      .algolia-docsearch-suggestion--category-header
        padding 5px 10px
        margin-top 0
        background $accentColor
        color #fff
        font-weight 600
        .algolia-docsearch-suggestion--highlight
          background rgba(255, 255, 255, 0.6)
      .algolia-docsearch-suggestion--wrapper
        background var(--background-color)
        padding 0
      .algolia-docsearch-suggestion--title
        font-weight 600
        margin-bottom 0
        color var(--text-color)
      .algolia-docsearch-suggestion--subcategory-column
        vertical-align top
        padding 5px 7px 5px 5px
        border-color var(--border-color)
        background var(--background-color)
        &:after
          display none
      .algolia-docsearch-suggestion--subcategory-column-text
        color var(--text-color)
    .algolia-docsearch-footer
      border-color var(--border-color)
      background var(--background-color)
    .ds-cursor .algolia-docsearch-suggestion--content
      background-color #e7edf3 !important
      color $textColor

@media (min-width: $MQMobile)
  .algolia-search-wrapper
    .algolia-autocomplete
      .algolia-docsearch-suggestion
        .algolia-docsearch-suggestion--subcategory-column
          float none
          width 150px
          min-width 150px
          display table-cell
        .algolia-docsearch-suggestion--content
          float none
          display table-cell
          width 100%
          vertical-align top
        .ds-dropdown-menu
          min-width 515px !important

@media (max-width: $MQMobile)
  .algolia-search-wrapper
    .ds-dropdown-menu
      min-width calc(100vw - 4rem) !important
      max-width calc(100vw - 4rem) !important
    .algolia-docsearch-suggestion--wrapper
      padding 5px 7px 5px 5px !important
    .algolia-docsearch-suggestion--subcategory-column
      padding 0 !important
      background var(--border-color) !important
    .algolia-docsearch-suggestion--subcategory-column-text:after
      content " > "
      font-size 10px
      line-height 14.4px
      display inline-block
      width 5px
      margin -3px 3px 0
      vertical-align middle

</style>
