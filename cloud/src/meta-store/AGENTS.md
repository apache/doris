# Cloud Meta-Store — Review Guide

## TxnKv

- [ ] Every `TxnErrorCode` result handled?
- [ ] `TXN_MAYBE_COMMITTED` treated as ambiguous, requiring idempotent recheck/retry at caller?
- [ ] Transaction-size limit respected (no assumption TxnKv absorbs arbitrarily large writes)?

## Key Encoding

- [ ] Key families from `keys.h` helpers, not hand-built prefixes?
- [ ] Correct space: `0x01` current metadata, `0x02` system/meta-service, `0x03` versioned historical/auxiliary?
- [ ] `encode_bytes()` / `encode_int64()` with field order matching intended scan prefix?
- [ ] `0x03` versioned values use existing helpers, not open-coded KV layouts?

## Versionstamp

- [ ] Versionstamped writes use `atomic_set_ver_key()` / `atomic_set_ver_value()` helpers?
- [ ] `get_versionstamp()` called only after successful commit?
