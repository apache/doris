# Backup/Restore — Review Guide

## BackupJob State Machine

- [ ] `PENDING → SNAPSHOTING` no-EditLog transition preserved? (Restart regenerates snapshot tasks)
- [ ] Snapshot prep writes `BarrierLog` and stores commit sequence?
- [ ] `UPLOAD_INFO` failures retry until timeout, not cancel immediately?

## RestoreJob Replay

- [ ] Early redoable stages that skip EditLog remain restart-replayable from earlier persisted state?
- [ ] `state` is persistent truth; `showState` is display-only, advanced only after finish log is durable?
- [ ] Finish ordering preserved: `logRestoreJob(this)` → snapshot cleanup → `showState` update?
