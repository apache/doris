#include "snapshot/snapshot_manager.h"

#include "recycler/recycler.h"

namespace doris::cloud {

void SnapshotManager::begin_snapshot(std::string_view instance_id,
                                     const BeginSnapshotRequest& request,
                                     BeginSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::commit_snapshot(std::string_view instance_id,
                                      const CommitSnapshotRequest& request,
                                      CommitSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::abort_snapshot(std::string_view instance_id,
                                     const AbortSnapshotRequest& request,
                                     AbortSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::drop_snapshot(std::string_view instance_id,
                                    const DropSnapshotRequest& request,
                                    DropSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::list_snapshot(std::string_view instance_id,
                                    const ListSnapshotRequest& request,
                                    ListSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::clone_instance(std::string_view instance_id,
                                     const CloneInstanceRequest& request,
                                     CloneInstanceResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

int SnapshotManager::recycle_snapshots(InstanceRecycler* recycler) {
    return 0;
}

int SnapshotManager::recycle_snapshot_meta_and_data(StorageVaultAccessor* accessor,
                                                    Versionstamp* snapshot_version,
                                                    const SnapshotPB* snapshot_pb) {
    return 0;
}

} // namespace doris::cloud
