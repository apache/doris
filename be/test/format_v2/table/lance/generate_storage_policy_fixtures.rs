// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// SPDX-License-Identifier: Apache-2.0
// Draft example for the pinned lance-c crate; run only after its standard build finishes.
// Creates metadata-only fixtures with official Lance APIs. Never writes source data.

use std::error::Error;
use std::path::{Path, PathBuf};

use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::write::CommitBuilder;
use lance_table::format::BasePath;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const SOURCE_URI: &str = "be/test/format_v2/table/lance/data/all_types.lance";

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: generate_tvf_storage_clone <Doris repository root> <NEW absolute output directory>".into());
    }
    let repository = std::fs::canonicalize(&args[0])?;
    let output = PathBuf::from(&args[1]);
    if !output.is_absolute() || output.exists() {
        return Err(
            "output must be a new absolute directory (existing content is never removed)".into(),
        );
    }
    std::fs::create_dir_all(&output)?;
    // The committed relative base URI intentionally resolves under the test repository cwd.
    std::env::set_current_dir(repository)?;
    lance_c::runtime::block_on(generate(&output))
}

async fn row_ids(dataset: &Dataset) -> Result<Vec<i64>> {
    let mut scanner = dataset.scan();
    scanner.project(&["row_id"])?;
    let batch = scanner.try_into_batch().await?;
    let mut values = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        values.push(
            arrow::util::display::array_value_to_string(batch.column(0).as_ref(), row)?.parse()?,
        );
    }
    values.sort_unstable();
    if values != vec![1, 2, 3, 4] {
        return Err(format!("expected row_id [1,2,3,4], got {values:?}").into());
    }
    Ok(values)
}

async fn publish_manifest_only(dataset: &Dataset, destination: &Path) -> Result<()> {
    // The SDK reports the actual latest manifest; do not guess v1/v2 filename ordering.
    let location = dataset.manifest_location();
    let manifest = PathBuf::from(lance_io::local::to_local_path(&location.path));
    let filename = manifest.file_name().ok_or("manifest has no filename")?;
    let versions = destination.join("_versions");
    std::fs::create_dir_all(&versions)?;
    std::fs::copy(&manifest, versions.join(filename))?;
    // Deliberately copy neither data nor transactions. Reopen/scan proves that only the
    // primary manifest plus the unchanged existing source fixture is sufficient.
    let reopened = DatasetBuilder::from_uri(destination.to_str().ok_or("non-UTF8 destination")?)
        .load()
        .await?;
    let values = row_ids(&reopened).await?;
    let registrations = reopened
        .manifest()
        .base_paths
        .values()
        .map(|base| {
            serde_json::json!({
                "id": base.id,
                "uri": base.path,
                "is_dataset_root": base.is_dataset_root,
            })
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        serde_json::json!({
            "fixture": destination,
            "manifest": filename.to_string_lossy(),
            "row_ids": values,
            "bases": registrations,
        })
    );
    Ok(())
}

async fn generate(output: &Path) -> Result<()> {
    let mut source = DatasetBuilder::from_uri(SOURCE_URI).load().await?;
    row_ids(&source).await?;
    if source.uri() != SOURCE_URI {
        return Err(format!("SDK changed source URI identity: {:?}", source.uri()).into());
    }
    let scratch = output.join("sdk-clone-root");
    let version = source.version_id();
    let clone = source
        .shallow_clone(scratch.to_str().ok_or("non-UTF8 scratch")?, version, None)
        .await?;
    if !clone
        .manifest()
        .base_paths
        .values()
        .any(|base| base.path == SOURCE_URI && base.is_dataset_root)
    {
        return Err(
            "official shallow_clone did not retain the required relative source base URI".into(),
        );
    }
    row_ids(&clone).await?;
    publish_manifest_only(&clone, &output.join("tvf_storage_clone.lance")).await?;

    // Optional direct-directory base: reuse the exact source fragments without writing files.
    // Official Overwrite rejects inherited deletion files, so skip this optional fixture for
    // a source that has deletions rather than changing its row semantics.
    if source
        .manifest()
        .fragments
        .iter()
        .all(|fragment| fragment.deletion_file.is_none())
    {
        let base_id = 7;
        let mut fragments = source.manifest().fragments.as_ref().clone();
        for fragment in &mut fragments {
            for file in fragment.referenced_lance_files_mut() {
                file.base_id = Some(base_id);
            }
        }
        let transaction = Transaction::new(
            0,
            Operation::Overwrite {
                fragments,
                schema: source.schema().clone(),
                config_upsert_values: None,
                initial_bases: Some(vec![BasePath::new(
                    base_id,
                    format!("{SOURCE_URI}/data"),
                    None,
                    false,
                )]),
            },
            None,
        );
        let direct_scratch = output.join("sdk-clone-direct-directory");
        let direct = CommitBuilder::new(direct_scratch.to_str().ok_or("non-UTF8 scratch")?)
            .execute(transaction)
            .await?;
        row_ids(&direct).await?;
        publish_manifest_only(&direct, &output.join("tvf_storage_direct_base.lance")).await?;
    } else {
        eprintln!("optional direct-directory fixture skipped: source contains deletion files");
    }
    Ok(())
}
