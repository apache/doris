# External Catalog — Review Guide

## Initialization and Reset

- [ ] Code avoids relying on `ExternalCatalog.isInitializing` as reentrancy guard? (Checked but never set to `true`)
- [ ] `getFilteredDatabaseNames()` changes keep `lowerCaseToDatabaseName` update atomic/consistent with refill semantics?
- [ ] `lower_case_database_names` behavior and case-insensitive lookup mapping preserved?
- [ ] `resetToUninitialized()` avoids exposing partially torn-down state to in-flight readers?
