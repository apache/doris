#include <gmock/gmock.h>

#include "vec/exec/scan/scanner_scheduler.h"

namespace doris::vectorized {
class MockSimplifiedScanScheduler : SimplifiedScanScheduler {
public:
    MockSimplifiedScanScheduler(std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl)
            : SimplifiedScanScheduler("ForTest", cgroup_cpu_ctl) {}

    MOCK_METHOD0(get_active_threads, int());
    MOCK_METHOD0(get_queue_size, int());
    MOCK_METHOD3(schedule_scan_task, Status(std::shared_ptr<ScannerContext> scanner_ctx,
                                            std::shared_ptr<ScanTask> current_scan_task,
                                            std::unique_lock<std::mutex>& transfer_lock));
};
} // namespace doris::vectorized
