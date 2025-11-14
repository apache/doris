#pragma once
#include <gen_cpp/FrontendService_types.h>

#include <vector>

#include "common/status.h"
#include "exec/schema_scanner.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
}

class SchemaDatabasePropertiesScanner : public SchemaScanner {
    ENABLE_FACTORY_CREATOR(SchemaDatabasePropertiesScanner);

public:
    SchemaDatabasePropertiesScanner();
    ~SchemaDatabasePropertiesScanner() override = default;

    Status start(RuntimeState* state) override;
    Status get_next_block_internal(vectorized::Block* block, bool* eos) override;

    static std::vector<SchemaScanner::ColumnDesc> _s_tbls_columns;

private:
    Status get_onedb_info_from_fe(int64_t dbId);
    bool check_and_mark_eos(bool* eos) const;
    int _block_rows_limit = 4096;
    int _row_idx = 0;
    int _total_rows = 0;
    int _db_index = 0;
    TGetDbsResult _db_result;
    std::unique_ptr<vectorized::Block> _dbproperties_block = nullptr;
    int _rpc_timeout_ms = 3000;
};
}

