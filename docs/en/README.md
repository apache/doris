---
{
  "page": "home"
}
---

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

<div class="home-blog">
  <div class="hero">
    <div class="b">
      <div class="bText">
        <h1>Welcome to</h1> 
        <p class="description">Apache Doris</p> 
        <p class="sum">A fast MPP database for all modern analytics on big data</p> 
        <div class="bannerHref">
          <a href="/en/docs/get-starting/get-starting.html" class="button1">Quick Start</a> 
          <a href="https://github.com/apache/incubator-doris" target="_blank" class="button2"><i class="doris doris-github-fill"></i>GitHub</a>
        </div>
      </div> 
      <div class="bImg">
        <div class="bcenter">
          <img src="/blog-images/hrighting.png" alt="hero" class="hero-img"> 
          <i class="dian1"></i> 
          <i class="dian2"></i> 
          <i class="dian3"></i> 
          <i class="dian4"></i> 
          <i class="dian5"></i> 
          <i class="xbg1"></i> 
          <i class="xbg2"></i> 
          <i class="xbg3"></i> 
          <i class="xb"></i>
        </div>
      </div>
    </div>
  </div>
</div>
<div class="newsBox">
  <ul class="wow fadeInUp">
    <li>
      <a href="/blogs/ReleaseNote/release-note-1.0.0.html">
        <div class="newsboxTitle">
          <p class="t">[Release Note] Apache Doris(Incubating) 1.0.0 Release</p>
          <!-- <p class="s">[Release Note] Apache Doris(Incubating) 1.0.0 Release</p> -->
          <i class="doris doris-jiantou_xiangyouliangci"></i>
        </div>
        <div class="newsboxImg">
          <div class="newsboxImgBox">
            <img src="/blog-images/news1.jpg" alt="img">
          </div>
        </div>
      </a>
    </li>
    <li>
      <a href="/blogs/PracticalCases/flink-cdc-to-doris.html">
        <div class="newsboxTitle">
          <p class="t">Use Flink CDC to import real-time MySQL data into Apache Doris</p>
          <!-- <p class="s">Use Flink CDC to realize real-time MySQL data into Apache Doris</p> -->
          <i class="doris doris-jiantou_xiangyouliangci"></i>
        </div>
        <div class="newsboxImg">
          <div class="newsboxImgBox">
            <img src="/blog-images/news2.jpg" alt="img">
          </div>
        </div>
      </a>
    </li>
    <li>
      <a href="/blogs/ReleaseNote/release-note-0.15.0.html">
        <div class="newsboxTitle">
          <p class="t">[Release Note] Apache Doris(Incubating) 0.15.0 Release</p>
          <!-- <p class="s">5. [Release Note] Apache Doris(Incubating) 0.15.0 Release</p> -->
          <i class="doris doris-jiantou_xiangyouliangci"></i>
        </div>
        <div class="newsboxImg">
          <div class="newsboxImgBox">
            <img src="/blog-images/news3.jpg" alt="img">
          </div>
        </div>
      </a>
    </li>
  </ul>
  <div class="newsdownData wow fadeInUp">
    <div class="newsleft">
      <p class="t wow fadeInUp">A modern, high-performance and real-time analysical database based on MPP</p>
      <p class="s wow fadeInUp">It is well known for its high-performance and easy-to-use. It can return query results under massive data within only sub-seconds.It can support not only high concurrent point query scenarios, but also complex analysis scenarios with high throughput.</p>
      <div class="fataImg">
        <img src="/blog-images/data.png" alt="Data" />
        <div class="dim"><img src="/blog-images/data-1.png" alt="Data" /></div>
      </div>
      <div class="al wow fadeInUp"><a href="/en/docs/get-starting/get-starting.html" class="a">Understand More</a></div>
    </div>
  </div>
</div>

<div class="apacheDoris">
  <div class="appleft">
    <div class="apptitle">
      <p class="t wow fadeInUp">Data Import </p>
      <p class="s wow fadeInUp">Apache Doris provides rich data synchronization methods, supports fast loading of data from localhost, Hadoop, Flink, Spark, Kafka and other  systems.</p>
      <div class="al wow fadeInUp"><a href="/en/docs/data-operate/import/load-manual.html">Understand More</a></div>
    </div>
    <div class="appimg wow fadeInUp">
      <img src="/blog-images/doris1.png" alt="doris">
    </div>
  </div>
  <div class="dorissolid1 wow fadeInUp">
    <img src="/blog-images/dorisSolid1.jpg" alt="doris">
  </div>
  <div class="appright">
    <div class="apptitle">
      <p class="t wow fadeInUp">Data Access</p>
      <p class="s wow fadeInUp">Apache Doris can directly access data in MySQL, PostgreSQL, Oracle, S3, Hive, Iceberg, Elasticsearch and other systems without data replication. At the same time, the data stored in Doris can also be read by Spark and Flink, and can be output to the downstream data application for display and analysis.</p>
      <div class="al wow fadeInUp"><a href="/en/docs/ecosystem/external-table/doris-on-es.html">Understand More</a></div>
    </div>
    <div class="appimg wow fadeInUp">
      <img src="/blog-images/doris2.png" alt="doris">
    </div>
  </div>
  <div class="dorissolid2 wow fadeInUp">
    <img src="/blog-images/dorisSolid2.jpg" alt="doris">
  </div>
  <div class="appleft appleft3">
    <div class="apptitle">
      <p class="t wow fadeInUp">Data Application</p>
      <p class="s wow fadeInUp">Apache Doris supports outputting data to downstream applications through JDBC standard protocol, and also supports various BI / Client tools to connect Doris through MySQL protocol。Based on this, Apache Doris can be well applied in many business fields, such as multi-dimensional reporting, user portrait, ad-hoc query, real-time dashboard and so on.</p>
      <div class="al wow fadeInUp"><a href="/en/docs/get-starting/get-starting.html">Understand More</a></div>
    </div>
    <div class="appimg wow fadeInUp">
      <img src="/blog-images/doris3.png" alt="doris">
    </div>
  </div>
</div>
<div class="icoBox">
  <div class="icoBoxtitle">
    <p class="wow fadeInUp">Why <span>Apache Doris</span></p>
  </div>
  <div class="icoBoxico">
    <ul>
      <li class="wow fadeInUp" data-wow-delay="200ms">
        <div class="icoimg">
          <img src="/blog-images/i1.png" alt="doris" />
        </div>
        <div class="icotitle">
          <p class="t">Extreme<br>Performance</p>
          <p class="s">Efficient column storage engine and modern MPP architecture,combined with intelligent materialized view,vectorized execution and various index acceleration, achieve the ultimate query performance.</p>
        </div>
      </li>
      <li class="wow fadeInUp" data-wow-delay="400ms">
        <div class="icoimg">
          <img src="/blog-images/i2.png" alt="doris" />
        </div>
        <div class="icotitle">
          <p class="t">Easy<br>to Use</p>
          <p class="s">Fully compatible with MySQL protocol and standard SQL, user-friendly. Support online table structure change and pre aggregation rollup, and easily integrate with the existing system framework.</p>
        </div>
      </li>
      <li class="wow fadeInUp" data-wow-delay="600ms">
        <div class="icoimg">
          <img src="/blog-images/i3.png" alt="doris" />
        </div>
        <div class="icotitle">
          <p class="t">Hybrid<br>Batch-Stream</p>
          <p class="s">Support efficient import of offline batch data and real-time streaming data, and ensure second level real-time performance. Multi version polar mechanism combined with  import transaction support solves read-write conflicts and implements exactly once</p>
        </div>
      </li>
      <li class="wow fadeInUp" data-wow-delay="200ms">
        <div class="icoimg">
          <img src="/blog-images/i4.png" alt="doris" />
        </div>
        <div class="icotitle">
          <p class="t">Simple<br>Maintenance</p>
          <p class="s">Highly integrated, without any dependence on external components, and the cluster scale is elastically scalable online.The system is highly available, node failure, automatic replica switching, data fragmentation and automatic load balancing.</p>
        </div>
      </li>
      <li class="wow fadeInUp" data-wow-delay="400ms">
        <div class="icoimg">
          <img src="/blog-images/i5.png" alt="doris" />
        </div>
        <div class="icotitle">
          <p class="t">Rich<br>Ecology</p>
          <p class="s">Support loading and accessing multiple heterogeneous data sources, has extensive big data ecological compatibility, and adapts with mainstream BI tools to realize the ecological closed loop from data processing to data analysis.</p>
        </div>
      </li>
      <li class="wow fadeInUp" data-wow-delay="600ms">
        <div class="icoimg">
          <img src="/blog-images/i6.png" alt="doris" />
        </div>
        <div class="icotitle">
          <p class="t">Ultra-high<br>Concurency</p>
          <p class="s">Without concurrent bottleneck, it can support tens of thousands of users to use at the same time in thereal production environment. Through flexible resource allocations trategy, it can meet high concurrent point queries and high through put large queries at the same time.</p>
        </div>
      </li>
    </ul>
  </div>
</div>

<div class="core">
  <div class="corebox">
    <div class="coreleft">
      <p class="t wow fadeInUp">Core Features</p>
      <p class="s wow fadeInUp">As a mature analytical database project, Apache Doris has many widely recognized core features and achieves the ultimate query performance in a variety of ways.</p>
    </div>
    <div class="coreright">
      <ul>
        <li class="wow fadeInUp">
          <div class="coreimg">
            <svg t="1650610624941" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="1938" width="200" height="200"><path d="M905.6 230.4c-63.5264 0-115.2-51.6864-115.2-115.2s51.6736-115.2 115.2-115.2 115.2 51.6864 115.2 115.2-51.6736 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64zM204.8 281.6c-63.5136 0-115.2-51.6864-115.2-115.2s51.6864-115.2 115.2-115.2 115.2 51.6864 115.2 115.2-51.6864 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64z" p-id="1939"></path><path d="M204.8 281.6c-63.5136 0-115.2-51.6864-115.2-115.2s51.6864-115.2 115.2-115.2 115.2 51.6864 115.2 115.2-51.6864 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64zM537.6 614.4c-63.5136 0-115.2-51.6736-115.2-115.2 0-63.5136 51.6864-115.2 115.2-115.2 63.5264 0 115.2 51.6864 115.2 115.2 0 63.5264-51.6736 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64zM332.8 921.6c-63.5136 0-115.2-51.6736-115.2-115.2s51.6864-115.2 115.2-115.2 115.2 51.6736 115.2 115.2-51.6864 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64z" p-id="1940"></path><path d="M742.4 1024H230.4C103.36 1024 0 920.64 0 793.6c0-110.784 78.8736-206.0032 187.5328-226.3808a25.6 25.6 0 0 1 9.4336 50.3296C112.512 633.3824 51.2 707.4304 51.2 793.6c0 98.816 80.384 179.2 179.2 179.2h512c127.04 0 230.4-103.36 230.4-230.4 0-112.8704-80.6016-208.3584-191.6544-227.0464a25.6128 25.6128 0 0 1 8.4992-50.496C925.44 487.7184 1024 604.4288 1024 742.4c0 155.2768-126.3232 281.6-281.6 281.6z" p-id="1941"></path><path d="M785.4208 515.7248a25.6 25.6 0 0 1-23.9232-16.4864c-0.896-2.3808-1.8048-4.7488-2.816-7.1168a280.1408 280.1408 0 0 0-60.3648-89.6256 280.576 280.576 0 0 0-89.4336-60.2752 275.7376 275.7376 0 0 0-52.864-16.448 25.5872 25.5872 0 1 1 10.24-50.1632c21.5168 4.3904 42.5472 10.9312 62.5024 19.4432a330.8544 330.8544 0 0 1 105.7536 71.2448 330.88 330.88 0 0 1 40.6528 49.2672 330.2528 330.2528 0 0 1 30.6688 56.64c1.2032 2.7904 2.3552 5.7984 3.4944 8.8064a25.6 25.6 0 0 1-23.9104 34.7136zM192.2688 617.984h-0.5504a25.6128 25.6128 0 0 1-25.0624-26.1376 322.944 322.944 0 0 1 6.5408-57.3184 325.824 325.824 0 0 1 19.4432-62.528 330.5472 330.5472 0 0 1 30.5792-56.4608 25.6 25.6 0 1 1 42.5088 28.5312 279.7824 279.7824 0 0 0-25.92 47.808 275.8016 275.8016 0 0 0-16.448 52.864 271.4496 271.4496 0 0 0-5.5168 48.1536 25.6 25.6 0 0 1-25.5744 25.088zM474.24 461.44a25.472 25.472 0 0 1-18.0992-7.5008l-206.08-206.08a25.6 25.6 0 1 1 36.1984-36.1984l206.08 206.08a25.6 25.6 0 0 1-18.0992 43.6992zM382.4384 757.376a25.5872 25.5872 0 0 1-21.2608-39.808l105.472-157.952a25.5872 25.5872 0 1 1 42.5728 28.416l-105.472 157.952a25.5488 25.5488 0 0 1-21.312 11.392zM293.9904 185.472a25.6 25.6 0 0 1-1.8432-51.1232l522.3552-38.144c14.08-1.216 26.368 9.5616 27.4048 23.6672a25.6 25.6 0 0 1-23.68 27.392l-522.3552 38.144a23.104 23.104 0 0 1-1.8816 0.064z"  p-id="1942"></path></svg>
          </div>
          <div class="coretitle">
            <p class="t"> Vectorization execution engine</p>
            <p class="s">Give full play to the parallel planning ability of modern CPU through <br />vectorization execution engine.</p>
          </div>
        </li>
        <li class="wow fadeInUp">
          <div class="coreimg">
            <svg t="1650610662916" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="2085" width="200" height="200"><path d="M50.673152 590.181581l71.227187-33.565491 0 138.349975-71.227187 18.831872L50.673152 590.181617 50.673152 590.181581zM142.367334 734.264115l75.315405-17.19255 0 151.449317-75.315427 0L142.367312 734.264153 142.367334 734.264115zM142.367334 322.482688l75.315405-60.578714L217.682739 459.198423l-75.315405 36.839014L142.367334 322.482669 142.367334 322.482688zM142.367334 546.7904l75.315405-34.379981 0 157.99643-75.315405 19.646464L142.367334 546.790364 142.367334 546.7904zM248.795648 238.164275l103.149466-82.684211 0 239.860926-103.149466 49.93833L248.795648 238.164285 248.795648 238.164275zM50.673152 754.730291l71.227187-15.558349L121.900339 868.520903 50.67318 868.520903 50.67318 754.730277 50.673152 754.730291zM248.795648 710.519398l103.149466-22.919987L351.945114 868.520903 248.795622 868.520903 248.795622 710.519356 248.795648 710.519398zM50.673152 395.341005l71.227187-56.485478 0 166.187997-71.227187 34.379981L50.673152 395.341047 50.673152 395.341005zM248.795648 498.49129l103.149466-48.299008 0 185.834452-103.149466 26.193613L248.795648 498.491335 248.795648 498.49129zM973.326848 238.164275l-27.019469 630.356582-16.373586 0-17.193267-590.244045-311.912755-49.93833-2.459136 179.282227 140.814848 18.831872 2.454016-176.008704 44.207616 7.366758-1.634304 174.36928 134.262374 18.012262 0 53.211922-135.082086-16.372941-1.639424 166.182912 136.72151 9.00608 0 36.019355-137.536102-6.547149-1.639424 176.828314-45.027363 0 2.454016-178.467635-142.449152-6.547149-2.454016 171.095757-44.20766 0L547.613484 681.866782l-169.468621-8.18647 0-49.118698 169.468621 12.279706L547.613484 460.018092l-169.468621-18.831872 0-62.21804 169.468621 22.105498L547.613484 220.152049l-162.921267-26.193613-6.547354-33.565491L973.326848 238.164275zM596.734362 465.74551l-1.639424 173.55479 141.62944 9.820672 2.459136-167.002522L596.734362 465.74551zM596.734362 465.74551" p-id="2086"></path></svg>
          </div>
          <div class="coretitle">
            <p class="t">Intelligent materialized view</p>
            <p class="s">Users can create intelligent materialized views to accelerate pre aggregation queries, and the query will automatically match the optimal materialized views.</p>
          </div>
        </li>
        <li class="wow fadeInUp">
          <div class="coreimg">
            <svg t="1650610676123" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="2225" width="200" height="200"><path d="M498.33 90.94L73.44 441.65l-3.68 3.47c-24.19 26.22 12.47 67.98 42.32 43.34l56.55-46.7v461.67c0 16.77 13.58 30.35 30.35 30.35h637.35l5.46-0.48c14.16-2.57 24.89-14.96 24.89-29.87V441.82l56.55 46.64c31.21 25.76 69.85-21.05 38.64-46.81L536.97 90.94c-11.22-9.25-27.42-9.25-38.64 0z m19.32 62.74l288.32 238v481.4H229.33V391.62l288.32-237.94z" p-id="2226"></path><path d="M699.75 569.59c14.89 0 27.29 10.73 29.87 24.89l0.48 5.46v182.1c0 16.77-13.58 30.35-30.35 30.35-14.89 0-27.29-10.73-29.87-24.89l-0.48-5.46v-182.1c0-16.76 13.59-30.35 30.35-30.35zM335.56 690.99c14.89 0 27.29 10.73 29.87 24.89l0.48 5.46v60.7c0 16.77-13.58 30.35-30.35 30.35-14.89 0-27.29-10.73-29.87-24.89l-0.48-5.46v-60.7c0-16.76 13.59-30.35 30.35-30.35zM620.42 333.18c13.23-10.29 32.3-7.9 42.59 5.33 9.15 11.76 8.28 28.14-1.32 38.85l-4.01 3.73-130.07 101.17c-10.29 8.01-24.35 8.43-35.01 1.61l-4.34-3.37-46.55-43.41-88 65.78c-11.93 8.92-28.29 7.73-38.82-2.08l-3.65-4.08c-8.92-11.93-7.73-28.29 2.08-38.82l4.08-3.65 108.4-80.94c10.29-7.68 24.11-7.96 34.6-1.2l4.27 3.32 46.08 43.03 109.67-85.27zM517.65 630.29c14.89 0 27.29 10.73 29.87 24.89l0.48 5.46v121.4c0 16.77-13.58 30.35-30.35 30.35-14.89 0-27.29-10.73-29.87-24.89l-0.48-5.46v-121.4c0-16.76 13.59-30.35 30.35-30.35z" p-id="2227"></path></svg>
          </div>
          <div class="coretitle">
            <p class="t"> Column storage engine</p>
            <p class="s">Apache Doris is equipped with an efficient column storage engine, which not only reduces the amount of data scanning, but also realizes ultra-high data compression ratio. </p>
          </div>
        </li>
        <li class="wow fadeInUp">
          <div class="coreimg">
            <svg t="1650610850334" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="2665" width="200" height="200"><path d="M512 230.4c-63.5136 0-115.2-51.6864-115.2-115.2s51.6864-115.2 115.2-115.2c63.5264 0 115.2 51.6864 115.2 115.2s-51.6736 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64zM512 1024c-63.5136 0-115.2-51.6736-115.2-115.2s51.6864-115.2 115.2-115.2c63.5264 0 115.2 51.6736 115.2 115.2s-51.6736 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64zM908.8 627.2c-63.5264 0-115.2-51.6736-115.2-115.2 0-63.5136 51.6736-115.2 115.2-115.2s115.2 51.6864 115.2 115.2c0 63.5264-51.6736 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64zM115.2 627.2c-63.5136 0-115.2-51.6736-115.2-115.2 0-63.5136 51.6864-115.2 115.2-115.2s115.2 51.6864 115.2 115.2c0 63.5264-51.6864 115.2-115.2 115.2z m0-179.2c-35.2896 0-64 28.7104-64 64s28.7104 64 64 64 64-28.7104 64-64-28.7104-64-64-64z" p-id="2666"></path><path d="M320.5376 219.392a25.6 25.6 0 0 1-13.184-47.5648 392.256 392.256 0 0 1 116.5184-46.6688 25.6 25.6 0 0 1 11.1488 49.984 341.5552 341.5552 0 0 0-101.3376 40.6016 25.6384 25.6384 0 0 1-13.1456 3.648zM703.4624 219.4048c-4.4928 0-9.024-1.1776-13.1456-3.6608a341.2736 341.2736 0 0 0-101.3248-40.6016 25.6256 25.6256 0 0 1-19.4176-30.5664 25.664 25.664 0 0 1 30.5664-19.4176 392.3968 392.3968 0 0 1 116.5312 46.6688 25.6128 25.6128 0 0 1-13.2096 47.5776zM429.4656 899.456c-1.8432 0-3.7248-0.2048-5.5936-0.6144a392.64 392.64 0 0 1-116.5184-46.656 25.6 25.6 0 0 1 26.3296-43.9296 341.2096 341.2096 0 0 0 101.3376 40.6016 25.6 25.6 0 0 1-5.5552 50.5984zM594.5344 899.456a25.6 25.6 0 0 1-5.5424-50.5856 341.1712 341.1712 0 0 0 101.3248-40.6144 25.6 25.6 0 1 1 26.3424 43.904 392.0256 392.0256 0 0 1-116.5312 46.6816 26.0864 26.0864 0 0 1-5.5936 0.6144zM150.1696 455.04a25.6256 25.6256 0 0 1-25.024-31.1808 391.808 391.808 0 0 1 46.6816-116.5184 25.6 25.6 0 0 1 43.904 26.3424 341.184 341.184 0 0 0-40.6144 101.3376 25.5872 25.5872 0 0 1-24.9472 20.0192zM193.8176 729.088c-8.704 0-17.1776-4.4288-21.9776-12.4288a392.1664 392.1664 0 0 1-46.6816-116.5312 25.6 25.6 0 0 1 49.984-11.1488 341.0304 341.0304 0 0 0 40.6144 101.3248 25.6 25.6 0 0 1-21.9392 38.784zM830.1952 729.088a25.5744 25.5744 0 0 1-21.9392-38.7584 341.2096 341.2096 0 0 0 40.6016-101.3248 25.6768 25.6768 0 0 1 30.5536-19.4304 25.6 25.6 0 0 1 19.4304 30.5536 392.512 392.512 0 0 1-46.656 116.5312 25.6256 25.6256 0 0 1-21.9904 12.4288zM873.8304 455.04a25.6128 25.6128 0 0 1-24.96-20.032 341.4912 341.4912 0 0 0-40.6016-101.3376 25.6128 25.6128 0 0 1 43.9296-26.3296 392.64 392.64 0 0 1 46.656 116.5184 25.6 25.6 0 0 1-25.024 31.1808z" p-id="2667"></path><path d="M231.424 913.0496a102.016 102.016 0 0 1-72.3712-29.9264l-18.176-18.176c-39.9104-39.9104-39.9104-104.832 0-144.7552l34.816-34.816a25.6 25.6 0 0 1 36.2112 36.1984l-34.816 34.816a51.2256 51.2256 0 0 0 0 72.3456l18.176 18.176a51.2256 51.2256 0 0 0 72.3456 0l34.816-34.8288a25.6 25.6 0 0 1 36.2112 36.1984l-34.816 34.8288a102.0928 102.0928 0 0 1-72.3968 29.9392zM830.1952 346.112a25.6 25.6 0 0 1-18.0992-43.712l34.8288-34.816a50.7648 50.7648 0 0 0 14.9632-36.16 50.8416 50.8416 0 0 0-14.9632-36.1856l-18.176-18.176a51.2256 51.2256 0 0 0-72.3456 0l-34.816 34.816a25.6 25.6 0 1 1-36.1984-36.2112l34.816-34.816c39.8976-39.9104 104.8448-39.9104 144.7552 0l18.176 18.176a101.6832 101.6832 0 0 1 29.9648 72.384 101.632 101.632 0 0 1-29.9648 72.3712l-34.8288 34.816a25.4848 25.4848 0 0 1-18.112 7.5136zM792.576 913.0624a101.952 101.952 0 0 1-72.3712-29.9392l-34.816-34.8288a25.6 25.6 0 1 1 36.1984-36.1984l34.816 34.8288c19.3024 19.3024 53.056 19.3024 72.3456 0l18.176-18.176c9.6512-9.6512 14.9632-22.5024 14.9632-36.1728s-5.312-26.5216-14.9632-36.1728l-34.8288-34.816a25.6 25.6 0 1 1 36.1984-36.1984l34.8288 34.816c39.9104 39.9104 39.9104 104.832 0 144.7552l-18.176 18.176a101.9904 101.9904 0 0 1-72.3712 29.9264zM193.792 346.112a25.472 25.472 0 0 1-18.0992-7.5008l-34.816-34.816c-19.328-19.3152-29.9648-45.0304-29.9648-72.3712s10.6496-53.056 29.9648-72.3712l18.176-18.1632c39.9104-39.8976 104.8576-39.8976 144.7424 0l34.816 34.816a25.6 25.6 0 1 1-36.1984 36.1984l-34.816-34.816a51.2256 51.2256 0 0 0-72.3456 0l-18.176 18.1632c-9.6512 9.6512-14.9632 22.4896-14.9632 36.1728s5.312 26.5216 14.9632 36.1728l34.816 34.816a25.6 25.6 0 0 1-18.0992 43.6992zM512 665.6c-84.6976 0-153.6-68.9024-153.6-153.6s68.9024-153.6 153.6-153.6 153.6 68.9024 153.6 153.6-68.9024 153.6-153.6 153.6z m0-256c-56.4608 0-102.4 45.9392-102.4 102.4s45.9392 102.4 102.4 102.4 102.4-45.9392 102.4-102.4-45.9392-102.4-102.4-102.4z" p-id="2668"></path></svg>
          </div>
          <div class="coretitle">
            <p class="t">Rich index structure </p>
            <p class="s">Doris provides a rich index structure to speed up data reading and filtering. Using the partition and bucket cutting function, Doris can support ultra-high concurrency of online service business, and a single node can support up to thousands of QPS.</p>
          </div>
        </li>
      </ul>
    </div>
  </div>

  <!-- <div class="EventsBlog">
    <div class="ebtitleH1 wow fadeInUp">Events & Blog</div>
    <ul>
      <li class="wow fadeInUp" data-wow-delay="200ms">
        <a href="/docs/theme-reco/">
          <div class="ebimg">
            <img src="/blog-images/ebi1.jpg" alt="Doris" />
          </div>
          <div class="ebtitle">
            <p>Efficient column storage engine and modern MPP architecture, combined with intelligent</p>
          </div>
        </a>
      </li>
      <li class="wow fadeInUp" data-wow-delay="400ms">
        <a href="/docs/theme-reco/">
          <div class="ebimg">
            <img src="/blog-images/ebi2.jpg" alt="Doris" />
          </div>
          <div class="ebtitle">
            <p>Efficient column storage engine and modern MPP architecture, combined with intelligent</p>
          </div>
        </a>
      </li>
      <li class="wow fadeInUp" data-wow-delay="600ms">
        <a href="/docs/theme-reco/">
          <div class="ebimg">
            <img src="/blog-images/ebi3.jpg" alt="Doris" />
          </div>
          <div class="ebtitle">
            <p>Efficient column storage engine and modern MPP architecture, combined with intelligent</p>
          </div>
        </a>
      </li>
    </ul>
  </div> -->
</div>

<div class="footer">
  <div class="footerCenter">
    <div class="footerleft">
      <div class="f wow fadeInUp" data-wow-delay="200ms">
        <p class="t">Resource</p>
        <a href="/en/downloads/downloads.html" class="a">Downloads</a>
        <a href="/en/docs/get-starting/get-starting.html" class="a">Document</a>
      </div>
      <div class="f wow fadeInUp" data-wow-delay="400ms">
        <p class="t">ASF</p>
        <a href="https://www.apache.org/" target="_blank" class="a">Foundation</a>
        <a href="https://www.apache.org/security/" target="_blank" class="a">Security</a>
        <a href="https://www.apache.org/licenses/" target="_blank" class="a">License</a>
        <a href="https://www.apache.org/events/current-event" target="_blank" class="a">Events</a>
        <a href="https://www.apache.org/foundation/sponsorship.html" target="_blank" class="a">Sponsorship</a>
         <a href="https://www.apache.org/foundation/policies/privacy.html" target="_blank" class="a">Privacy</a>
        <a href="https://www.apache.org/foundation/thanks.html" target="_blank" class="a">Thanks</a>
      </div>
      <div class="f wow fadeInUp" data-wow-delay="600ms">
        <p class="t">Language</p>
        <a href="/en/" class="a">English</a>
        <a href="/zh-CN/" class="a">简体中文</a>
      </div>
    </div>
    <div class="footerright wow fadeInUp" data-wow-delay="800ms">
      <div class="fx">
        <p class="t">Share</p>
        <ul>
          <li><a href="mailto:dev@doris.apache.org" target="_blank"><img src="/blog-images/fx1.png" alt="Email" /></a></li>
          <li><a href="https://github.com/apache/incubator-doris" target="_blank"><img src="/blog-images/fx2.png" alt="Github" /></a></li>
          <li><a href="https://twitter.com/doris_apache" target="_blank"><img src="/blog-images/fx3.png" alt="Twitter" /></a></li>
          <li><a href="https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-18u6vjopj-Th15vTVfmCzVfhhL5rz26A" target="_blank"><img src="/blog-images/fx4.png" alt="Slack" /></a></li>
          <li><a href="https://mp.weixin.qq.com/mp/homepage?__biz=Mzg5MDEyODc1OA==&hid=1&sn=eb2d31c20d5c4fc638b897c764e11195&scene=18" target="_blank"><img src="/blog-images/fx5.png" alt="WeChart" /></a></li>
          <li><a href="https://space.bilibili.com/362350065" target="_blank"><img src="/blog-images/fx6.png" alt="bilibili" /></a></li>
        </ul>
      </div>
    </div>
    <div class="footerfoot">
      <img src="/blog-images/dorisyezi.png" alt="doris" class="wow fadeInUp" />
      <p class="wow fadeInUp">Copyright © 2022 The Apache Software Foundation. Licensed under the Apache License, Version 2.0. Apache <br/> Doris(Incubating), Apache Incubator, Apache, the Apache feather logo, the Apache Doris(Incubating) logo and the <br/>Apache Incubator project logo are trademarks of The Apache Software Foundation.</p>
      <p class="wow fadeInUp">Apache Doris(incubating) is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored <br/>by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that<br/> the infrastructure, communications, and decision making process have stabilized in a manner consistent with other<br/> successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of <br/>the code, it does indicate that the project has yet to be fully endorsed by the ASF.</p>
    </div>
  </div>
</div>
<script type="text/javascript" src="https://cdn.jsdelivr.net/npm/wowjs@1.1.2/dist/wow.min.js"></script>
<script type="text/javascript" src="/js/home.js"></script>
