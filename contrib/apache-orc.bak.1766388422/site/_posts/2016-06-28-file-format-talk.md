---
layout: news_item
title: "File format benchmark"
date: "2016-06-28 08:00:00 -0800"
author: omalley
categories: [talk]
---

I gave a talk at Hadoop Summit San Jose 2016 about a file format
benchmark that I've contributed as [ORC-72]({{site.jira}}/ORC-72). The
benchmark focuses on real data sets that are publicly available. The data
sets represent a wide variety of use cases:

* *NYC Taxi Data* - very dense data with mostly numeric types
* *Github Archives* - very sparse data with a lot of complex structure
* *Sales* - a real production schema from a sales table with a synthetic generator

The benchmarks look at a set of three very common use cases:

* *Full table scan* - read all columns and rows
* *Column projection* - read some columns, but all of the rows
* *Column projection and predicate push down* - read some columns and some rows

You can see the slides here:

[File Format Benchmarks: Avro, JSON, ORC, & Parquet](https://www.slideshare.net/oom65/file-format-benchmarks-avro-json-orc-parquet)

<iframe src="//www.slideshare.net/slideshow/embed_code/key/fSn4xuYXBXGvlx"
width="595" height="485" frameborder="0" marginwidth="0" marginheight="0"
scrolling="no" style="border:1px solid #CCC; border-width:1px;
margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe>
