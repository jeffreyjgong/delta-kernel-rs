# Writing Parquet V2 with Arrow-rs

This guide explains how to configure the `parquet` crate (arrow-rs) to write Parquet files using the V2 format, including DataPageV2 and other V2 features.

## Table of Contents

- [Overview](#overview)
- [Parquet V1 vs V2](#parquet-v1-vs-v2)
- [Configuring WriterVersion](#configuring-writerversion)
- [Basic Example](#basic-example)
- [Advanced Configuration](#advanced-configuration)
- [Integration with Delta Kernel](#integration-with-delta-kernel)
- [Compatibility Considerations](#compatibility-considerations)

---

## Overview

Apache Parquet has two major format versions:

- **Parquet 1.0 (V1)**: The original format, widely supported
- **Parquet 2.0 (V2)**: Enhanced format with performance improvements

By default, arrow-rs writes Parquet 1.0 files. This guide shows how to enable Parquet 2.0 features.

## Parquet V1 vs V2

### Key Differences

| Feature | V1 (DataPage) | V2 (DataPageV2) |
|---------|---------------|-----------------|
| **Variable-length encoding** | Interleaved with other data | Separated for better performance |
| **Row boundaries** | Flexible | Strict (rows terminate at page boundaries) |
| **Delta encodings** | Not available | Supported |
| **Performance** | Baseline | 5-10x faster for variable-length reads |

### DataPageV2 Benefits

1. **Faster decoding**: Variable-length encoding is separated, reducing CPU cost per cell
2. **Better page skipping**: Strict row boundaries enable more efficient predicate pushdown
3. **Delta encodings**: Support for `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY`

### Compatibility

Most modern Parquet readers support both V1 and V2. However, some older tools may not fully support V2 features. Test with your target readers before adopting V2 in production.

---

## Configuring WriterVersion

The `WriterVersion` enum in arrow-rs controls the Parquet format version:

```rust
use parquet::file::properties::WriterVersion;

pub enum WriterVersion {
    PARQUET_1_0,  // Default - DataPage V1
    PARQUET_2_0,  // DataPage V2 with enhanced features
}
```

### Default Behavior

```rust
// Default is PARQUET_1_0
pub const DEFAULT_WRITER_VERSION: WriterVersion = WriterVersion::PARQUET_1_0;
```

---

## Basic Example

### Writing Parquet V2 with ArrowWriter

```rust
use std::sync::Arc;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};

fn write_parquet_v2() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Create sample data
    let col1: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let col2: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
    
    let batch = RecordBatch::try_from_iter(vec![
        ("id", col1),
        ("name", col2),
    ])?;

    // Configure WriterProperties for Parquet V2
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    // Write with V2 format
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut buffer,
        batch.schema(),
        Some(props),  // Pass properties here
    )?;
    
    writer.write(&batch)?;
    writer.close()?;

    Ok(buffer)
}
```

### Writing to a File

```rust
use std::fs::File;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};

fn write_parquet_v2_to_file(
    batch: &RecordBatch,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    Ok(())
}
```

### Async Writing with AsyncArrowWriter

```rust
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use tokio::fs::File;

async fn write_parquet_v2_async(
    batch: &RecordBatch,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path).await?;
    
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch).await?;
    writer.close().await?;

    Ok(())
}
```

---

## Advanced Configuration

### Full WriterProperties Example

```rust
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterVersion, EnabledStatistics};
use parquet::schema::types::ColumnPath;

let props = WriterProperties::builder()
    // Parquet version
    .set_writer_version(WriterVersion::PARQUET_2_0)
    
    // Compression (applies to all columns)
    .set_compression(Compression::ZSTD(Default::default()))
    
    // Encoding (default for all columns)
    .set_encoding(Encoding::PLAIN)
    
    // Column-specific encoding (V2 enables delta encodings)
    .set_column_encoding(
        ColumnPath::from("timestamp_col"),
        Encoding::DELTA_BINARY_PACKED,  // Only available in V2
    )
    
    // Dictionary encoding
    .set_dictionary_enabled(true)
    
    // Statistics level
    .set_statistics_enabled(EnabledStatistics::Page)
    
    // Page size limits
    .set_data_page_size_limit(1024 * 1024)  // 1MB
    .set_data_page_row_count_limit(20_000)
    
    // Row group size
    .set_max_row_group_row_count(Some(1_000_000))
    
    // Custom created_by string
    .set_created_by("my-application v1.0".to_string())
    
    .build();
```

### V2-Specific Encodings

These encodings are particularly useful with Parquet V2:

```rust
use parquet::basic::Encoding;

// Delta encodings (V2 only)
Encoding::DELTA_BINARY_PACKED    // For INT32, INT64
Encoding::DELTA_LENGTH_BYTE_ARRAY // For BYTE_ARRAY
Encoding::DELTA_BYTE_ARRAY       // For BYTE_ARRAY (prefix + suffix)
```

### Column-Specific Configuration

```rust
use parquet::schema::types::ColumnPath;

let props = WriterProperties::builder()
    .set_writer_version(WriterVersion::PARQUET_2_0)
    
    // Different compression per column
    .set_column_compression(
        ColumnPath::from("large_text"),
        Compression::ZSTD(Default::default()),
    )
    .set_column_compression(
        ColumnPath::from("small_int"),
        Compression::UNCOMPRESSED,
    )
    
    // Different encoding per column
    .set_column_encoding(
        ColumnPath::from("timestamp"),
        Encoding::DELTA_BINARY_PACKED,
    )
    
    .build();
```

---

## Integration with Delta Kernel

### Table Property Configuration

Delta Kernel Rust supports configuring the Parquet format version via the `delta.parquet.format.version` table property:

| Property Value | Parquet Version | Dictionary Encoding | Page Format |
|----------------|-----------------|---------------------|-------------|
| `1.0.0` (default) | `PARQUET_1_0` | Enabled | DataPageV1 |
| `2.12.0` | `PARQUET_2_0` | Disabled | DataPageV2 |

### Implementation

The format version is applied during writes via `WriteContext`:

```rust
// kernel/src/engine/default/parquet.rs
let writer_properties = Some(build_writer_properties(parquet_format_version));
let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), writer_properties)?;
```

The `build_writer_properties` function (in `kernel/src/engine/default/parquet_properties.rs`) creates appropriate `WriterProperties`:

```rust
pub(crate) fn build_writer_properties(format_version: ParquetFormatVersion) -> WriterProperties {
    let mut builder = WriterProperties::builder();

    match format_version {
        ParquetFormatVersion::V1_0_0 => {
            builder = builder.set_writer_version(WriterVersion::PARQUET_1_0);
        }
        ParquetFormatVersion::V2_12_0 => {
            builder = builder
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(false);
        }
    }

    builder.build()
}
```

### Usage

```rust
// The format version is automatically read from table properties
let write_context = transaction.get_write_context();
let format_version = write_context.parquet_format_version();

// DefaultEngine::write_parquet automatically applies the format version
engine.write_parquet(&data, &write_context, partition_values).await?;
```

### Checkpoints

Checkpoint writes always use Parquet V1 (`ParquetFormatVersion::V1_0_0`) for maximum compatibility, regardless of the table property setting.

### Delta Encodings

Note: To use delta encodings (DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY), 
you need to explicitly configure them per-column using `WriterPropertiesBuilder::set_column_encoding()`.
The V2 configuration disables dictionary encoding, which is a prerequisite for using delta encodings,
but the actual encoding selection requires additional per-column configuration.

### Considerations for Delta Tables

1. **Reader compatibility**: Ensure all readers of your Delta tables support Parquet V2
2. **Statistics**: Delta collects its own statistics; Parquet-level stats are supplementary
3. **Compression**: Delta tables commonly use Snappy or ZSTD compression
4. **Opt-in**: V2 support is opt-in via the `delta.parquet.format.version` table property

---

## Compatibility Considerations

### Reader Support

| Reader | V2 Support |
|--------|------------|
| Apache Arrow (all languages) | Full support |
| Apache Spark 3.x+ | Full support |
| Databricks Runtime | Full support |
| DuckDB | Full support |
| Polars | Full support |
| pandas (via pyarrow) | Full support |
| Older Hive versions | May have issues |

### When to Use V2

**Recommended for V2:**
- New applications with modern readers
- Performance-critical workloads with string/binary data
- Use cases benefiting from delta encodings

**Stick with V1:**
- Compatibility with legacy systems
- When readers are unknown or untested
- Conservative production environments

### Testing Compatibility

```rust
// Write test file with V2
let props = WriterProperties::builder()
    .set_writer_version(WriterVersion::PARQUET_2_0)
    .build();

// Verify it can be read back
let file = File::open("test.parquet")?;
let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
    .build()?;

for batch in reader {
    // Verify data integrity
}
```

---

## Summary

### Using arrow-rs Directly

To write Parquet V2 with arrow-rs:

1. **Import the types:**
   ```rust
   use parquet::file::properties::{WriterProperties, WriterVersion};
   ```

2. **Build properties with V2:**
   ```rust
   let props = WriterProperties::builder()
       .set_writer_version(WriterVersion::PARQUET_2_0)
       .build();
   ```

3. **Pass to ArrowWriter:**
   ```rust
   let writer = ArrowWriter::try_new(output, schema, Some(props))?;
   ```

### Using Delta Kernel Rust

To enable Parquet V2 in Delta Kernel:

1. **Set the table property** on your Delta table:
   ```
   delta.parquet.format.version = "2.12.0"
   ```

2. **Write data normally** - the format version is automatically applied:
   ```rust
   let write_context = transaction.get_write_context();
   engine.write_parquet(&data, &write_context, partition_values).await?;
   ```

This enables DataPageV2, delta encodings, and INT64 timestamps for improved performance with compatible readers.
