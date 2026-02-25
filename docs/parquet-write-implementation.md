# Parquet Write Implementation in Delta Kernel Rust

This document provides a detailed explanation of how Parquet files are written in delta-kernel-rs, including the write flow, statistics collection, and integration with the Delta transaction system.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Components](#core-components)
  - [DefaultParquetHandler](#defaultparquethandler)
  - [Statistics Collection](#statistics-collection)
  - [DataFileMetadata](#datafilemetadata)
- [Write Flow](#write-flow)
  - [1. High-Level Engine Write](#1-high-level-engine-write)
  - [2. Parquet Handler Write](#2-parquet-handler-write)
  - [3. Internal Write Implementation](#3-internal-write-implementation)
- [File Naming and Storage](#file-naming-and-storage)
- [Statistics Collection Details](#statistics-collection-details)
  - [Collected Statistics](#collected-statistics)
  - [String Truncation](#string-truncation)
  - [Nested Struct Handling](#nested-struct-handling)
- [Checkpoint Writes](#checkpoint-writes)
- [Key Files Reference](#key-files-reference)

---

## Overview

Delta Kernel Rust writes Parquet files as the primary data file format for Delta tables. The write implementation:

1. Uses Apache Arrow's `ArrowWriter` for Parquet serialization
2. Generates UUID-based filenames for uniqueness
3. Collects file statistics (min/max/nullCount) during writes
4. Returns metadata compatible with Delta's Add action schema

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      User / Transaction                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DefaultEngine::write_parquet                  │
│  - Validates partition columns                                   │
│  - Transforms logical → physical schema (column mapping)         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│              DefaultParquetHandler::write_parquet_file           │
│  - Public async API                                              │
│  - Calls internal write_parquet()                                │
│  - Converts DataFileMetadata to EngineData                       │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│               DefaultParquetHandler::write_parquet               │
│  - Collects statistics via collect_stats()                       │
│  - Serializes RecordBatch to Parquet bytes                       │
│  - Writes to object store with UUID filename                     │
│  - Returns DataFileMetadata                                      │
└─────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
         ┌──────────────────┐    ┌──────────────────┐
         │   collect_stats  │    │    ArrowWriter   │
         │  (stats.rs)      │    │  (parquet crate) │
         └──────────────────┘    └──────────────────┘
```

## Core Components

### DefaultParquetHandler

**Location:** `kernel/src/engine/default/parquet.rs`

The `DefaultParquetHandler` struct implements the `ParquetHandler` trait and manages all Parquet I/O operations.

```rust
pub struct DefaultParquetHandler<E: TaskExecutor> {
    store: Arc<DynObjectStore>,      // Object store for file operations
    task_executor: Arc<E>,           // Async task executor
    readahead: usize,                // Buffer size for read operations
}
```

Key methods:

| Method | Visibility | Purpose |
|--------|------------|---------|
| `write_parquet` | Private async | Core write implementation |
| `write_parquet_file` | Public async | API for data file writes |
| `ParquetHandler::write_parquet_file` | Trait impl | API for checkpoint writes |

### Statistics Collection

**Location:** `kernel/src/engine/default/stats.rs`

The `collect_stats()` function computes Delta-compatible statistics from a `RecordBatch`.

```rust
pub(crate) fn collect_stats(
    batch: &RecordBatch,
    stats_columns: &[ColumnName],
) -> DeltaResult<StructArray>
```

### DataFileMetadata

**Location:** `kernel/src/engine/default/parquet.rs`

Represents metadata for a written Parquet file:

```rust
pub struct DataFileMetadata {
    file_meta: FileMeta,      // path, last_modified, size
    stats: StructArray,       // numRecords, nullCount, minValues, maxValues, tightBounds
}
```

The `as_record_batch()` method converts this to the schema expected by `Transaction::add_files()`.

---

## Write Flow

### 1. High-Level Engine Write

**Location:** `kernel/src/engine/default/mod.rs` (lines 224-266)

The entry point for most write operations is `DefaultEngine::write_parquet`:

```rust
pub async fn write_parquet(
    &self,
    data: &ArrowEngineData,
    write_context: &WriteContext,
    partition_values: HashMap<String, String>,
) -> DeltaResult<Box<dyn EngineData>>
```

This method:

1. **Validates partition columns** - Ensures partition keys exist in the schema
2. **Translates logical to physical names** - Handles column mapping mode
3. **Applies schema transformation** - Uses `EvaluationHandler` to transform data
4. **Delegates to parquet handler** - Calls `write_parquet_file` with physical schema

### 2. Parquet Handler Write

**Location:** `kernel/src/engine/default/parquet.rs` (lines 214-225)

```rust
pub async fn write_parquet_file(
    &self,
    path: &url::Url,
    data: Box<dyn EngineData>,
    partition_values: HashMap<String, String>,
    stats_columns: Option<&[ColumnName]>,
) -> DeltaResult<Box<dyn EngineData>>
```

This public API:

1. Calls internal `write_parquet()` to perform the actual write
2. Converts `DataFileMetadata` to a `RecordBatch` via `as_record_batch()`
3. Returns data matching the Add file metadata schema

### 3. Internal Write Implementation

**Location:** `kernel/src/engine/default/parquet.rs` (lines 159-204)

The core implementation performs these steps:

```rust
async fn write_parquet(
    &self,
    path: &url::Url,
    data: Box<dyn EngineData>,
    stats_columns: &[ColumnName],
) -> DeltaResult<DataFileMetadata>
```

**Step-by-step breakdown:**

1. **Convert to Arrow RecordBatch**
   ```rust
   let batch: Box<_> = ArrowEngineData::try_from_engine_data(data)?;
   let record_batch = batch.record_batch();
   ```

2. **Collect statistics**
   ```rust
   let stats = collect_stats(record_batch, stats_columns)?;
   ```

3. **Serialize to Parquet bytes**
   ```rust
   let mut buffer = vec![];
   let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
   writer.write(record_batch)?;
   writer.close()?;  // Must close to write footer
   ```

4. **Generate UUID filename**
   ```rust
   let name: String = format!("{}.parquet", Uuid::new_v4());
   let path = path.join(&name)?;
   ```

5. **Write to object store**
   ```rust
   self.store
       .put(&Path::from_url_path(path.path())?, buffer.into())
       .await?;
   ```

6. **Fetch file metadata**
   ```rust
   let metadata = self.store.head(&Path::from_url_path(path.path())?).await?;
   let modification_time = metadata.last_modified.timestamp_millis();
   ```

7. **Return DataFileMetadata**
   ```rust
   let file_meta = FileMeta::new(path, modification_time, size);
   Ok(DataFileMetadata::new(file_meta, stats))
   ```

---

## File Naming and Storage

### File Path Requirements

- The target `path` URL **must** end with a trailing slash
- Files are written as `{path}/{uuid}.parquet`
- The UUID is generated using `uuid::Uuid::new_v4()`

### Example

```
Input path:  s3://my-bucket/delta-table/data/
Output file: s3://my-bucket/delta-table/data/a1b2c3d4-e5f6-7890-abcd-ef1234567890.parquet
```

### Object Store Integration

The implementation uses the `object_store` crate:

- **PUT operation:** Writes the entire Parquet buffer atomically
- **HEAD operation:** Retrieves file metadata (size, modification time)

---

## Statistics Collection Details

**Location:** `kernel/src/engine/default/stats.rs`

### Collected Statistics

The `collect_stats()` function returns a `StructArray` with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `numRecords` | Int64 | Total row count |
| `nullCount` | Struct | Nested null counts per column |
| `minValues` | Struct | Nested minimum values per column |
| `maxValues` | Struct | Nested maximum values per column |
| `tightBounds` | Boolean | Always `true` for new writes |

### Supported Data Types for Min/Max

| Category | Types |
|----------|-------|
| Integer | Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64 |
| Float | Float32, Float64 |
| Date | Date32, Date64 |
| Timestamp | All time units (Second, Millisecond, Microsecond, Nanosecond) |
| Decimal | Decimal128 |
| String | Utf8, LargeUtf8, Utf8View |

**Not supported for min/max:** Map, List, FixedSizeList (only nullCount is collected)

### String Truncation

Delta protocol requires string statistics to be truncated for efficiency:

**Min values:** Truncated to 32 characters at a valid UTF-8 boundary

**Max values:** Truncated to 32 characters plus a tie-breaker character:
- ASCII characters (< 0x7F): append `\x7F` (ASCII DEL)
- Non-ASCII characters: append `\u{10FFFF}` (max Unicode code point)

```rust
const STRING_PREFIX_LENGTH: usize = 32;
const ASCII_MAX_CHAR: char = '\x7F';
const UTF8_MAX_CHAR: char = '\u{10FFFF}';
```

### Nested Struct Handling

Statistics for nested structs propagate struct-level nulls to child fields:

```rust
// Example: struct { a: int, b: string } with struct-level null
// Row 0: struct=NULL (a=1, b="x" are masked)
// Row 1: struct=VALID, a=2, b="y"

// Result:
// - nullCount.a = 1 (struct null counts as null)
// - nullCount.b = 1
// - minValues.a = 2 (only visible values)
// - maxValues.a = 2
```

---

## Checkpoint Writes

**Location:** `kernel/src/engine/default/parquet.rs` (lines 319-355)

Checkpoint writes use a different code path via the `ParquetHandler` trait:

```rust
fn write_parquet_file(
    &self,
    location: url::Url,
    data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
) -> DeltaResult<()>
```

Key differences from data file writes:

| Aspect | Data Files | Checkpoints |
|--------|------------|-------------|
| Writer | `ArrowWriter` (sync, buffered) | `AsyncArrowWriter` |
| Input | Single `Box<dyn EngineData>` | `Iterator` of batches |
| Output | `DataFileMetadata` with stats | `()` (no return value) |
| Filename | UUID-generated | Explicit path provided |
| Overwrites | No (unique filenames) | Yes (always overwrites) |

```rust
// Checkpoint write implementation
let object_writer = ParquetObjectWriter::new(store, path);
let mut writer = AsyncArrowWriter::try_new(object_writer, schema, None)?;

// Write first batch
writer.write(&first_record_batch).await?;

// Write remaining batches
for result in data {
    let batch: RecordBatch = /* convert */;
    writer.write(&batch).await?;
}

writer.finish().await?;
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `kernel/src/engine/default/parquet.rs` | Core Parquet handler implementation |
| `kernel/src/engine/default/stats.rs` | Statistics collection for Delta files |
| `kernel/src/engine/default/mod.rs` | DefaultEngine with high-level write_parquet API |
| `kernel/src/transaction/mod.rs` | Transaction API that uses parquet writes |
| `kernel/src/lib.rs` | ParquetHandler trait definition |

### Related Traits

- **`ParquetHandler`** (`kernel/src/lib.rs`): Trait for Parquet read/write operations
- **`Engine`** (`kernel/src/lib.rs`): Main engine trait providing `parquet_handler()`
- **`TaskExecutor`** (`kernel/src/engine/default/executor/mod.rs`): Async task execution

---

## Example Usage

### Writing a Data File

```rust
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::transaction::WriteContext;

// Get write context from transaction
let write_context = transaction.get_write_context();

// Write data using the engine
let file_metadata = engine
    .write_parquet(&arrow_data, &write_context, partition_values)
    .await?;

// Add to transaction
transaction.add_files(file_metadata);
```

### Direct Parquet Handler Usage

```rust
use delta_kernel::engine::default::parquet::DefaultParquetHandler;
use delta_kernel::table_properties::ParquetFormatVersion;

let handler = DefaultParquetHandler::new(object_store, task_executor);

let metadata = handler
    .write_parquet_file(
        &target_url,
        data,
        partition_values,
        Some(&stats_columns),
        ParquetFormatVersion::V1_0_0, // or write_context.parquet_format_version()
    )
    .await?;
```

---

## Parquet Format Version

The Parquet format version used during writes is controlled by the `delta.parquet.format.version` table property:

| Version | Parquet Writer | Features |
|---------|----------------|----------|
| `1.0.0` (default) | `PARQUET_1_0` | Maximum compatibility |
| `2.12.0` | `PARQUET_2_0` | Delta encodings, DataPageV2, INT64 timestamps |

### Accessing Format Version

```rust
// From WriteContext (recommended)
let format_version = write_context.parquet_format_version();

// The format version is automatically applied when using DefaultEngine::write_parquet()
engine.write_parquet(&data, &write_context, partition_values).await?;
```

### Checkpoints

Checkpoint writes always use Parquet V1 format (`ParquetFormatVersion::V1_0_0`) for maximum compatibility across all Delta readers.

---

## Notes

- **Parquet Version:** Configurable via `delta.parquet.format.version` table property (default: V1)
- **Compression:** Default compression settings from Arrow (typically Snappy for data pages)
- **Row Groups:** Single row group per file (entire RecordBatch written at once)
- **Statistics:** Column-level statistics are collected by kernel, not relying on Parquet's built-in stats
