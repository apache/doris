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

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;

use crate::error::{FfiError, FfiResult};

/// Configuration for opening a Lance dataset, deserialized from JSON.
/// Passed from C++ as a JSON string via the FFI.
#[derive(Debug, Default, Deserialize)]
pub struct LanceReaderConfig {
    /// Dataset URI (s3://bucket/path.lance or file:///path.lance)
    pub uri: String,
    /// Column names to project (empty = all columns)
    #[serde(default)]
    pub columns: Vec<String>,
    /// Maximum rows per batch
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Dataset version for time travel (0 = latest)
    #[serde(default)]
    pub version: u64,
    /// Storage options (S3 credentials, etc.)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    // --- Index-accelerated search ---
    /// SQL-like filter expression pushed down to Lance (uses scalar indexes if available).
    /// Example: "category = 'shoes' AND price < 100"
    #[serde(default)]
    pub filter: Option<String>,

    /// Vector nearest-neighbor search config.
    #[serde(default)]
    pub vector_search: Option<VectorSearchConfig>,

    /// Full-text search query string (uses FTS index if available).
    #[serde(default)]
    pub full_text_search: Option<String>,

    /// LIMIT pushdown (0 = no limit)
    #[serde(default)]
    pub limit: Option<i64>,

    /// OFFSET pushdown
    #[serde(default)]
    pub offset: Option<i64>,

    /// Specific fragment IDs to scan (empty = all fragments)
    #[serde(default)]
    pub fragment_ids: Vec<u64>,

    /// Fragment data file path relative to dataset root (e.g., "data/xxx.lance").
    /// When set, only the fragment whose data file matches this path is scanned.
    /// This prevents duplicate reads when TVF creates multiple scan ranges.
    #[serde(default)]
    pub fragment_file: Option<String>,
}

/// Vector ANN search configuration.
#[derive(Debug, Deserialize)]
pub struct VectorSearchConfig {
    /// Column name containing the vector
    pub column: String,
    /// Query vector as flat f32 array
    pub query: Vec<f32>,
    /// Number of nearest neighbors to return
    pub k: usize,
    /// Distance metric: "l2", "cosine", "dot"
    #[serde(default = "default_metric")]
    pub metric: String,
    /// Number of IVF probes (higher = more accurate, slower)
    #[serde(default)]
    pub nprobes: Option<usize>,
    /// HNSW ef search parameter
    #[serde(default)]
    pub ef: Option<usize>,
    /// Refine factor for re-ranking
    #[serde(default)]
    pub refine_factor: Option<u32>,
}

fn default_metric() -> String {
    "l2".to_string()
}

fn default_batch_size() -> usize {
    4096
}

/// A synchronous wrapper around lance-rs async APIs.
///
/// Each reader owns a single-threaded tokio runtime (`new_current_thread`).
/// `block_on()` is called from the Doris scanner thread, which is allowed to block.
/// This creates zero additional OS threads — the runtime is purely a
/// future-polling state machine running inline on the calling thread.
pub struct LanceReader {
    rt: tokio::runtime::Runtime,
    stream: Option<BoxStream<'static, Result<RecordBatch, lance::Error>>>,
    schema: SchemaRef,
}

impl LanceReader {
    /// Open a Lance dataset and prepare a scan stream.
    ///
    /// - `uri`: Dataset path (local file:/// or s3://)
    /// - `columns`: Column names to project (empty = all columns)
    /// - `batch_size`: Maximum rows per RecordBatch
    pub fn open(uri: &str, columns: &[String], batch_size: usize) -> FfiResult<Self> {
        Self::open_with_config(&LanceReaderConfig {
            uri: uri.to_string(),
            columns: columns.to_vec(),
            batch_size,
            ..Default::default()
        })
    }

    /// Open a Lance dataset from a full config (supports S3 creds, version, etc.)
    pub fn open_with_config(config: &LanceReaderConfig) -> FfiResult<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| FfiError::Io(format!("Failed to create tokio runtime: {}", e)))?;

        let (stream, schema) = rt.block_on(async {
            // Build dataset open params with storage options (S3 creds, etc.)
            let mut builder = lance::dataset::builder::DatasetBuilder::from_uri(&config.uri);

            // Set storage options (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc.)
            for (k, v) in &config.storage_options {
                builder = builder.with_storage_option(k, v);
            }

            // Time travel: open a specific version
            if config.version > 0 {
                builder = builder.with_version(config.version);
            }

            let dataset = builder.load().await.map_err(FfiError::from)?;

            let mut scanner = dataset.scan();
            if !config.columns.is_empty() {
                let col_refs: Vec<&str> = config.columns.iter().map(|s| s.as_str()).collect();
                scanner
                    .project(&col_refs)
                    .map_err(|e| FfiError::Lance(e.to_string()))?;
            }
            scanner.batch_size(config.batch_size);

            // Filter pushdown (uses scalar indexes: BTree, Bitmap)
            if let Some(ref filter_expr) = config.filter {
                scanner
                    .filter(filter_expr)
                    .map_err(|e| FfiError::Lance(e.to_string()))?;
            }

            // Vector ANN search (uses IVF-PQ, IVF-HNSW indexes)
            if let Some(ref vs) = config.vector_search {
                let query_array = arrow::array::Float32Array::from(vs.query.clone());
                scanner
                    .nearest(&vs.column, &query_array, vs.k)
                    .map_err(|e| FfiError::Lance(e.to_string()))?;
                // Distance metric
                let metric = match vs.metric.to_lowercase().as_str() {
                    "cosine" => lance_linalg::distance::MetricType::Cosine,
                    "dot" => lance_linalg::distance::MetricType::Dot,
                    _ => lance_linalg::distance::MetricType::L2,
                };
                scanner.distance_metric(metric);
                if let Some(n) = vs.nprobes {
                    scanner.nprobs(n);
                }
                if let Some(ef) = vs.ef {
                    scanner.ef(ef);
                }
                if let Some(rf) = vs.refine_factor {
                    scanner.refine(rf);
                }
            }

            // Full-text search (uses tantivy FTS index)
            if let Some(ref fts_query) = config.full_text_search {
                scanner
                    .full_text_search(lance_index::scalar::FullTextSearchQuery::new(
                        fts_query.clone(),
                    ))
                    .map_err(|e| FfiError::Lance(e.to_string()))?;
            }

            // LIMIT/OFFSET pushdown
            if config.limit.is_some() || config.offset.is_some() {
                scanner
                    .limit(config.limit, config.offset)
                    .map_err(|e| FfiError::Lance(e.to_string()))?;
            }

            // Fragment-level parallelism: scan only specific fragments by ID
            if !config.fragment_ids.is_empty() {
                let all_frags = dataset.get_fragments();
                let selected: Vec<_> = all_frags
                    .into_iter()
                    .filter(|f| config.fragment_ids.contains(&(f.id() as u64)))
                    .map(|f| f.metadata().clone())
                    .collect();
                if !selected.is_empty() {
                    scanner.with_fragments(selected);
                }
            }

            // Filter to a single fragment by data file path.
            // When TVF creates multiple scan ranges (one per .lance file), each range
            // passes its fragment file path so we only read that specific fragment.
            if let Some(ref frag_file) = config.fragment_file {
                let all_frags = dataset.get_fragments();
                let selected: Vec<_> = all_frags
                    .into_iter()
                    .filter(|f| {
                        f.metadata()
                            .files
                            .iter()
                            .any(|df| frag_file.ends_with(&df.path))
                    })
                    .map(|f| f.metadata().clone())
                    .collect();
                if !selected.is_empty() {
                    scanner.with_fragments(selected);
                }
            }

            let schema_ref = scanner
                .schema()
                .await
                .map_err(|e| FfiError::Lance(e.to_string()))?;

            let stream = scanner
                .try_into_stream()
                .await
                .map_err(|e| FfiError::Lance(e.to_string()))?;

            Ok::<_, FfiError>((stream.boxed(), schema_ref))
        })?;

        Ok(Self {
            rt,
            stream: Some(stream),
            schema,
        })
    }

    /// Read the next batch. Returns `None` on EOF.
    pub fn next_batch(&mut self) -> FfiResult<Option<RecordBatch>> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| FfiError::Lance("Reader stream is closed".to_string()))?;

        let result = self.rt.block_on(stream.next());
        match result {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(FfiError::from(e)),
            None => Ok(None),
        }
    }

    /// Get the schema of the scan output.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Helper: create a small Lance dataset in a temp directory and return its path.
    fn create_test_dataset(dir: &std::path::Path) -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "alice", "bob", "carol", "dave", "eve",
                ])),
                Arc::new(Float64Array::from(vec![90.5, 85.0, 92.3, 78.1, 88.7])),
            ],
        )
        .unwrap();

        let uri = dir.join("test.lance").to_string_lossy().to_string();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let batches = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
            lance::Dataset::write(batches, &uri, None::<lance::dataset::WriteParams>)
                .await
                .unwrap();
        });

        uri
    }

    #[test]
    fn test_open_and_read_all_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = create_test_dataset(tmp.path());

        let mut reader = LanceReader::open(&uri, &[], 1024).unwrap();

        // Schema should have 3 fields
        assert_eq!(reader.schema().fields().len(), 3);
        assert_eq!(reader.schema().field(0).name(), "id");
        assert_eq!(reader.schema().field(1).name(), "name");
        assert_eq!(reader.schema().field(2).name(), "score");

        // Read first batch — should contain all 5 rows
        let batch = reader.next_batch().unwrap();
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 3);

        // Verify values
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3, 4, 5]);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(4), "eve");

        // Next batch should be EOF
        let batch = reader.next_batch().unwrap();
        assert!(batch.is_none());
    }

    #[test]
    fn test_open_with_column_projection() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = create_test_dataset(tmp.path());

        let columns = vec!["name".to_string(), "score".to_string()];
        let mut reader = LanceReader::open(&uri, &columns, 1024).unwrap();

        assert_eq!(reader.schema().fields().len(), 2);
        assert_eq!(reader.schema().field(0).name(), "name");
        assert_eq!(reader.schema().field(1).name(), "score");

        let batch = reader.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 5);

        let scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((scores.value(0) - 90.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_open_with_small_batch_size() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = create_test_dataset(tmp.path());

        let mut reader = LanceReader::open(&uri, &[], 2).unwrap();

        let mut total_rows = 0;
        let mut batch_count = 0;
        loop {
            match reader.next_batch().unwrap() {
                Some(batch) => {
                    total_rows += batch.num_rows();
                    batch_count += 1;
                }
                None => break,
            }
        }
        assert_eq!(total_rows, 5);
        assert!(
            batch_count >= 2,
            "Expected multiple batches, got {}",
            batch_count
        );
    }

    #[test]
    fn test_open_nonexistent_path() {
        let result = LanceReader::open("/nonexistent/path/to/dataset.lance", &[], 1024);
        assert!(result.is_err());
        match result {
            Err(e) => assert_eq!(e.status_code(), crate::error::FFI_ERR_LANCE),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[test]
    fn test_open_invalid_column_name() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = create_test_dataset(tmp.path());

        let columns = vec!["nonexistent_column".to_string()];
        let result = LanceReader::open(&uri, &columns, 1024);
        assert!(result.is_err());
    }
}
