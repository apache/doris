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
  <div class="home">
    <div class="hero">
      <ModuleTransition>
        <img
          v-if="recoShowModule && $frontmatter.heroImage"
          :style="heroImageStyle || {}"
          :src="$withBase($frontmatter.heroImage)"
          alt="hero">
      </ModuleTransition>
      <ModuleTransition delay="0.04">
        <h1
          v-if="recoShowModule && $frontmatter.heroText !== null"
          :style="{ marginTop: $frontmatter.heroImage ? '0px' : '140px'}"
        >
          {{ $frontmatter.heroText || $title || 'vuePress-theme-reco' }}
        </h1>
      </ModuleTransition>
      <ModuleTransition delay="0.08">
        <p v-if="recoShowModule && $frontmatter.tagline !== null" class="description">
          {{ $frontmatter.tagline || $description || 'Welcome to your vuePress-theme-reco site' }}
        </p>
      </ModuleTransition>
      <ModuleTransition delay="0.16">
        <p class="action" v-if="recoShowModule && $frontmatter.actionText && $frontmatter.actionLink">
          <NavLink class="action-button" :item="actionLink"/>
        </p>
      </ModuleTransition>
    </div>

    <ModuleTransition delay="0.24">
      <div class="features" v-if="recoShowModule && $frontmatter.features && $frontmatter.features.length">
        <div v-for="(feature, index) in $frontmatter.features" :key="index" class="feature">
          <h2>{{ feature.title }}</h2>
          <p>{{ feature.details }}</p>
        </div>
      </div>
    </ModuleTransition>
    <ModuleTransition delay="0.32">
      <Content class="home-center" v-show="recoShowModule" custom/>
    </ModuleTransition>
  </div>
</template>

<script>
import { defineComponent, computed } from 'vue-demi'
import NavLink from '@theme/components/NavLink'
import { ModuleTransition } from '@vuepress-reco/core/lib/components'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  components: { NavLink, ModuleTransition },

  setup (props, ctx) {
    const instance = useInstance()
    const recoShowModule = computed(() => instance && instance.$parent.recoShowModule)
    const actionLink = computed(() => instance && {
      link: instance.$frontmatter.actionLink,
      text: instance.$frontmatter.actionText
    })
    const heroImageStyle = computed(() => instance.$frontmatter.heroImageStyle || {
      maxHeight: '200px',
      margin: '6rem auto 1.5rem'
    })

    return { recoShowModule, actionLink, heroImageStyle }
  }
})
</script>

<style lang="stylus">
.home {
  padding: $navbarHeight 2rem 0;
  max-width: 960px;
  margin: 0px auto;

  .hero {
    text-align: center;
    h1 {
      display: block;
      font-size: 2.5rem;
      color: var(--text-color);
    }

    h1, .description, .action {
      margin: 1.8rem auto;
    }

    .description {
      font-size: 1.6rem;
      line-height: 1.3;
      color: var(--text-color);
    }

    .action-button {
      display: inline-block;
      font-size: 1.2rem;
      color: #fff;
      background-color: $accentColor;
      padding: 0.2rem 1.2rem;
      border-radius: $borderRadius
      transition: background-color 0.1s ease;
      box-sizing: border-box;
      load-start()

      &:hover {
        background-color: lighten($accentColor, 10%);
      }
    }
  }

  .features {
    border-top: 1px solid var(--border-color);;
    padding: 1.2rem 0;
    margin-top: 2.5rem;
    display: flex;
    flex-wrap: wrap;
    align-items: flex-start;
    align-content: stretch;
    justify-content: space-between;
  }

  .feature {
    flex-grow: 1;
    flex-basis: 30%;
    max-width: 30%;
    transition: all .5s
    color: var(--text-color);
    h2 {
      font-size: 1.6rem;
      font-weight: 500;
      border-bottom: none;
      padding-bottom: 0;
    }

    &:hover {
      transform scale(1.05)
    }
  }
}

@media (max-width: $MQMobile) {
  .home {
    .features {
      flex-direction: column;
    }

    .feature {
      max-width: 100%;
      padding: 0 2.5rem;
    }
  }
}

@media (max-width: $MQMobileNarrow) {
  .home {
    padding-left: 1.5rem;
    padding-right: 1.5rem;

    .hero {
      img {
        max-height: 210px;
        margin: 2rem auto 1.2rem;
      }

      h1 {
        font-size: 2rem;
      }

      h1, .description, .action {
        margin: 1.2rem auto;
      }

      .description {
        font-size: 1.2rem;
      }

      .action-button {
        font-size: 1rem;
        padding: 0.6rem 1.2rem;
      }
    }

    .feature {
      h2 {
        font-size: 1.25rem;
      }
    }
  }
}
</style>
