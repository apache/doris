# Doris Storage File Format Optimization #

## File format ##

![](../../../resources/images/segment_v2.png)
<center>1. doris segment</center>

Documents include:
- The file starts with an 8-byte magic code to identify the file format and version
- Data Region: Used to store data information for each column, where the data is loaded on demand by pages.
- Index Region: Doris stores the index data of each column in Index Region, where the data is loaded according to column granularity, so the data information of the following column is stored separately.
- Footer信息
	- FileFooterPB: Metadata Information for Definition Files
	- Chesum of 4 bytes of footer Pb content
	- Four bytes FileFooterPB message length for reading FileFooterPB
	- The 8 byte MAGIC CODE is stored in the last bit to facilitate the identification of file types in different scenarios.

The data in the file is organized in the form of page, which is the basic unit of coding and compression. Current page types include the following:

### DataPage ###

Data Page is divided into two types: nullable and non-nullable data pages.

Nullable's data page includes:
```

                 +----------------+
                 |  value count   |
                 |----------------|
                 |  first row id  |
                 |----------------|
                 | bitmap length  |
                 |----------------|
                 |  null bitmap   |
                 |----------------|
                 |     data       |
                 |----------------|
                 |    checksum    |
                 +----------------+
```

non -zero data page32467;- 26500;- 229140;-

```
                 |----------------|
                 |   value count  |
                 |----------------|
                 |  first row id  |
                 |----------------|
                 |     data       |
                 |----------------|
                 |    checksum    |
                 +----------------+
```

The meanings of each field are as follows:

- value count
	- Represents the number of rows in a page
- First row id
	- Line number of the first line in page
- bitmap length
	- Represents the number of bytes in the next bitmap
- null bitmap
	- bitmap representing null information
- Data
	- Store data after encoding and compress
	- You need to write in the header information of the data: is_compressed
	- Various kinds of data encoded by different codes need to write some field information in the header information in order to achieve data parsing.
	- TODO: Add header information for various encodings
- Checksum
	- Store page granularity checksum, including page header and subsequent actual data


### Bloom Filter Pages ###

For each bloom filter column, a page of the bloom filter is generated corresponding to the granularity of the page and saved in the bloom filter pages area.

### Ordinal Index Page ###

For each column, a sparse index of row numbers is established according to page granularity. The content is a pointer to the block (including offset and length) for the line number of the start line of the page

### Short Key Index page ###

We generate a sparse index of short key every N rows (configurable) with the contents of short key - > line number (ordinal)

### Column's other indexes###

The format design supports the subsequent expansion of other index information, such as bitmap index, spatial index, etc. It only needs to write the required data to the existing column data, and add the corresponding metadata fields to FileFooterPB.

### Metadata Definition###
FileFooterPB is defined as:

```
message ColumnPB {
    optional uint32 column_id = 1; // 这里使用column id，不使用column name是因为计划支持修改列名
    optional string type = 2; // 列类型
    optional string aggregation = 3; // 是否聚合
    optional uint32 length = 4; // 长度
    optional bool is_key = 5; // 是否是主键列
    optional string default_value = 6; // 默认值
    optional uint32 precision = 9 [default = 27]; // 精度
    optional uint32 frac = 10 [default = 9];
    optional bool is_nullable = 11 [default=false]; // 是否有null
    optional bool is_bf_column = 15 [default=false]; // 是否有bf词典
	optional bool is_bitmap_column = 16 [default=false]; // 是否有bitmap索引
}

// page偏移
message PagePointerPB {
	required uint64 offset; // page在文件中的偏移
	required uint32 length; // page的大小
}

message MetadataPairPB {
  optional string key = 1;
  optional bytes value = 2;
}

message ColumnMetaPB {
	optional ColumnMessage encoding; // 编码方式

	optional PagePointerPB dict_page // 词典page
	repeated PagePointerPB bloom_filter_pages; // bloom filter词典信息
	optional PagePointerPB ordinal_index_page; // 行号索引数据
	optional PagePointerPB page_zone_map_page; // page级别统计信息索引数据

	optional PagePointerPB bitmap_index_page; // bitmap索引数据

	optional uint64 data_footprint; // 列中索引的大小
	optional uint64 index_footprint; // 列中数据的大小
	optional uint64 raw_data_footprint; // 原始列数据大小

	optional CompressKind compress_kind; // 列的压缩方式

	optional ZoneMapPB column_zone_map; //文件级别的过滤条件
	repeated MetadataPairPB column_meta_datas;
}

message FileFooterPB {
	optional uint32 version = 2 [default = 1]; // 用于版本兼容和升级使用
	repeated ColumnPB schema = 5; // 列Schema
    optional uint64 num_values = 4; // 文件中保存的行数
    optional uint64 index_footprint = 7; // 索引大小
    optional uint64 data_footprint = 8; // 数据大小
	optional uint64 raw_data_footprint = 8; // 原始数据大小

    optional CompressKind compress_kind = 9 [default = COMPRESS_LZO]; // 压缩方式
    repeated ColumnMetaPB column_metas = 10; // 列元数据
	optional PagePointerPB key_index_page; // short key索引page
}

```

## Read-write logic##

### Write ###

The general writing process is as follows:
1. Write magic
2. Generate corresponding Column Writer according to schema information. Each Column Writer obtains corresponding encoding information (configurable) according to different types, and generates corresponding encoder according to encoding.
3. Call encoder - > add (value) for data writing. Each K line generates a short key index entry, and if the current page satisfies certain conditions (the size exceeds 1M or the number of rows is K), a new page is generated and cached in memory.
4. Continuous cycle step 3 until data writing is completed. Brush the data of each column into the file in sequence
5. Generate FileFooterPB information and write it to the file.

Relevant issues:

- How does the index of short key be generated?
	- Now we still generate a short key sparse index according to how many rows are sparse, and keep a short sparse index generated every 1024 rows. The specific content is: short key - > ordinal

- What should be stored in the ordinal index?
	- Store the first ordinal to page pointer mapping information for pages
- What are stored in pages of different encoding types?
	- Dictionary Compression
	- plain
	- rle
	- bshuf

### Read###

1. Read the magic of the file and judge the type and version of the file.
2. Read FileFooterPB and check sum
3. Read short key index and data ordinal index information of corresponding columns according to required columns
4. Use start key and end key, locate the row number to be read through short key index, then determine the row ranges to be read through ordinal index, and filter the row ranges to be read through statistics, bitmap index and so on.
5. Then read row data through ordinal index according to row ranges

Relevant issues:
1. How to quickly locate a row within the page?

	The data inside the page is encoding, so it can not locate the row-level data quickly. Different encoding methods have different schemes for fast line number positioning in-house, which need to be analyzed concretely:
	- If it is rle-coded, skip is performed by resolving the head of RLE until the RLE block containing the row is reached, and then the reverse solution is performed.
	- binary plain encoding: offset information will be stored in the page, and offset information will be specified in the page header. When reading, offset information will be parsed into the array first, so that you can quickly locate the data of a row of block through offset data information of each row.
2. How to achieve efficient block reading? Consider merging adjacent blocks while they are being read, one-time reading?
This requires judging whether the block is continuous at the time of reading, and if it is continuous, reading it once.

## Coding##

In the existing Doris storage, plain encoding is adopted for string type encoding, which is inefficient. After comparison, it is found that in Baidu statistics scenario, data will expand more than twice because of string type coding. Therefore, it is planned to introduce dictionary-based coding compression.

## Compression##

It implements a scalable compression framework, supports a variety of compression algorithms, facilitates the subsequent addition of new compression algorithms, and plans to introduce zstd compression.

## TODO ##
1. How to implement nested types? How to locate line numbers in nested types?
2. How to optimize the downstream bitmap and column statistics statistics caused by ScanRange splitting?
