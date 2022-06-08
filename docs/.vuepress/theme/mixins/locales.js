/*
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
*/
import { zhHans, zhHant, en, ja, ko, es } from '../locales/index'

export default {
  computed: {
    $recoLocales () {
      const recoLocales = this.$themeLocaleConfig.recoLocales || {}

      if (/^zh\-(CN|SG)$/.test(this.$lang)) {
        return { ...zhHans, ...recoLocales }
      }
      if (/^zh\-(HK|MO|TW)$/.test(this.$lang)) {
        return { ...zhHant, ...recoLocales }
      }
      if (/^ja\-JP$/.test(this.$lang)) {
        return { ...ja, ...recoLocales }
      }
      if (/^ko\-KR$/.test(this.$lang)) {
        return { ...ko, ...recoLocales }
      }
      if (/^es(\-[A-Z]+)?$/.test(this.$lang)) {
        return { ...es, ...recoLocales }
      }
      return { ...en, ...recoLocales }
    }
  }
}
