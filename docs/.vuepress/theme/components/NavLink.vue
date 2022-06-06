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
  <RouterLink
    class="nav-link"
    :class="[item.className]"
    :to="link"
    v-if="!isExternal(link)"
    :exact="exact"
  >
    <reco-icon :icon="`${item.icon}`" />
    {{ item.text }}
  </RouterLink>
  <a
    v-else
    :href="link"
    class="nav-link external"
    :target="isMailto(link) || isTel(link) ? null : '_blank'"
    :rel="isMailto(link) || isTel(link) ? null : 'noopener noreferrer'"
  >
    <reco-icon :icon="`${item.icon}`" />
    {{ item.text }}
    <OutboundLink />
  </a>
</template>

<script>
import { defineComponent, computed, toRefs } from "vue-demi";
import { isExternal, isMailto, isTel, ensureExt } from "@theme/helpers/utils";
import { RecoIcon } from "@vuepress-reco/core/lib/components";
import { useInstance } from "@theme/helpers/composable";

export default defineComponent({
  components: { RecoIcon },

  props: {
    item: {
      required: true,
    },
  },
  methods: {
    isVersionLink(link) {
      return link.indexOf("branch") > -1;
    },
    go(link) {
      const url = window.location.origin + link;
      location.href = url;
    },
  },

  setup(props, ctx) {
    const instance = useInstance();

    const { item } = toRefs(props);

    const link = computed(() => ensureExt(item.value.link));

    const exact = computed(() => {
      if (instance.$site.locales) {
        return Object.keys(instance.$site.locales).some(
          (rootLink) => rootLink === link.value
        );
      }
      return link.value === "/";
    });

    return { link, exact, isExternal, isMailto, isTel };
  },
});
</script>
