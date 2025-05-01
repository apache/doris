#include "olap/rowset/segment_v2/ann_index_reader.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <random>

#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/x_index_file_reader.h"
#include "olap/tablet_schema.h"

// Add CLucene RAM Directory header
#include <CLucene/store/RAMDirectory.h>

namespace doris::segment_v2 {

// Remove the MockDirectory class entirely since we'll use CLucene's RAMDirectory

class MockIndexFileReader : public doris::segment_v2::XIndexFileReader {
public:
    MockIndexFileReader()
            : XIndexFileReader(io::global_local_filesystem(), "",
                               InvertedIndexStorageFormatPB::V2) {}

    MOCK_CONST_METHOD2(open, doris::Result<std::unique_ptr<DorisCompoundReader>>(
                                     const doris::TabletIndex*, const doris::io::IOContext*));
};

class MockTabletSchema : public doris::TabletIndex {};

class AnnIndexReaderTest : public testing::Test {};

TEST_F(AnnIndexReaderTest, TestLoadIndex) {
    MockTabletSchema tablet_schema;
    std::shared_ptr<MockIndexFileReader> index_file_reader =
            std::make_shared<MockIndexFileReader>();
    auto ann_index_reader = std::make_unique<AnnIndexReader>(&tablet_schema, index_file_reader);

    EXPECT_TRUE(ann_index_reader->load_index().ok());
}

TEST_F(AnnIndexReaderTest, TestQuery) {
    MockTabletSchema tablet_schema;
    std::shared_ptr<MockIndexFileReader> index_file_reader =
            std::make_shared<MockIndexFileReader>();
    auto ann_index_reader = std::make_unique<AnnIndexReader>(&tablet_schema, index_file_reader);
}
} // namespace doris::segment_v2