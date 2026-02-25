# Delta Table Properties in Delta Kernel Rust

This document explains how Delta Kernel Rust understands, parses, and uses Delta table properties from table metadata.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Table Properties Struct](#table-properties-struct)
- [Property Parsing](#property-parsing)
- [Accessing Table Properties](#accessing-table-properties)
- [Property Categories](#property-categories)
  - [Checkpointing Properties](#checkpointing-properties)
  - [Column Mapping Properties](#column-mapping-properties)
  - [Data Skipping Properties](#data-skipping-properties)
  - [Retention Properties](#retention-properties)
  - [Feature Enablement Properties](#feature-enablement-properties)
  - [Row Tracking Properties](#row-tracking-properties)
  - [In-Commit Timestamps Properties](#in-commit-timestamps-properties)
- [How Properties Affect Behavior](#how-properties-affect-behavior)
- [Unknown Properties](#unknown-properties)
- [Key Files Reference](#key-files-reference)

---

## Overview

Delta table properties are configuration settings stored in the `configuration` field of the table's `Metadata` action. These properties control various aspects of table behavior including:

- Checkpointing frequency and format
- Column mapping mode
- Data skipping statistics
- File retention policies
- Feature enablement (CDF, deletion vectors, row tracking, etc.)

Delta Kernel Rust parses these key-value pairs into a strongly-typed `TableProperties` struct for safe access throughout the codebase.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Delta Log (JSON)                              │
│   { "metaData": { "configuration": { "delta.xxx": "yyy" } } }   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Metadata Action                               │
│         configuration: HashMap<String, String>                   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼ parse_table_properties()
┌─────────────────────────────────────────────────────────────────┐
│                    TableProperties                               │
│   Strongly-typed struct with parsed values                       │
│   + unknown_properties: HashMap<String, String>                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                  TableConfiguration                              │
│   Combines Protocol + Metadata + TableProperties                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Snapshot                                    │
│   table_properties() -> &TableProperties                         │
└─────────────────────────────────────────────────────────────────┘
```

## Table Properties Struct

**Location:** `kernel/src/table_properties.rs`

The `TableProperties` struct contains all recognized Delta table properties:

```rust
pub struct TableProperties {
    // Checkpointing
    pub checkpoint_interval: Option<NonZero<u64>>,
    pub checkpoint_write_stats_as_json: Option<bool>,
    pub checkpoint_write_stats_as_struct: Option<bool>,
    pub checkpoint_policy: Option<CheckpointPolicy>,
    
    // Column Mapping
    pub column_mapping_mode: Option<ColumnMappingMode>,
    
    // Data Skipping
    pub data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,
    pub data_skipping_stats_columns: Option<Vec<ColumnName>>,
    
    // Retention
    pub deleted_file_retention_duration: Option<Duration>,
    pub log_retention_duration: Option<Duration>,
    pub enable_expired_log_cleanup: Option<bool>,
    pub set_transaction_retention_duration: Option<Duration>,
    
    // Feature Enablement
    pub append_only: Option<bool>,
    pub enable_change_data_feed: Option<bool>,
    pub enable_deletion_vectors: Option<bool>,
    pub enable_row_tracking: Option<bool>,
    pub enable_in_commit_timestamps: Option<bool>,
    
    // Row Tracking
    pub row_tracking_suspended: Option<bool>,
    pub materialized_row_id_column_name: Option<String>,
    pub materialized_row_commit_version_column_name: Option<String>,
    
    // In-Commit Timestamps
    pub in_commit_timestamp_enablement_version: Option<Version>,
    pub in_commit_timestamp_enablement_timestamp: Option<i64>,
    
    // Auto-optimization
    pub auto_compact: Option<bool>,
    pub optimize_write: Option<bool>,
    
    // Other
    pub randomize_file_prefixes: Option<bool>,
    pub random_prefix_length: Option<i32>,
    pub isolation_level: Option<IsolationLevel>,
    
    // Unrecognized properties
    pub unknown_properties: HashMap<String, String>,
}
```

## Property Parsing

**Location:** `kernel/src/table_properties/deserialize.rs`

Parsing is infallible - invalid or unrecognized properties are stored in `unknown_properties`:

```rust
impl<K, V, I> From<I> for TableProperties
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn from(unparsed: I) -> Self {
        let mut props = TableProperties::default();
        let unparsed = unparsed.into_iter().filter(|(k, v)| {
            // Only keep elements that fail to parse
            try_parse(&mut props, k.as_ref(), v.as_ref()).is_none()
        });
        props.unknown_properties = unparsed.map(|(k, v)| (k.into(), v.into())).collect();
        props
    }
}
```

### Parsing Helpers

The deserialize module provides type-specific parsers:

| Helper | Purpose | Example |
|--------|---------|---------|
| `parse_bool` | Boolean values | `"true"`, `"false"` |
| `parse_positive_int` | Positive integers | `"100"` |
| `parse_int` | Signed integers | `"-1"`, `"32"` |
| `parse_interval` | Duration intervals | `"interval 7 days"` |
| `parse_column_names` | Comma-separated columns | `"col1,col2,col3"` |

## Accessing Table Properties

### From Metadata

```rust
// In kernel/src/actions/mod.rs
impl Metadata {
    pub(crate) fn parse_table_properties(&self) -> TableProperties {
        TableProperties::from(self.configuration.iter())
    }
}
```

### From Snapshot

```rust
// User-facing API
let snapshot = table.snapshot(engine, None)?;
let properties = snapshot.table_properties();

// Check specific properties
if properties.enable_change_data_feed == Some(true) {
    // CDF is enabled
}
```

### From TableConfiguration

```rust
// Internal API
let table_config = TableConfiguration::try_new(
    metadata, protocol, schema, table_root, version
)?;
let properties = table_config.table_properties();
```

---

## Property Categories

### Checkpointing Properties

| Property Key | Field | Type | Default | Description |
|--------------|-------|------|---------|-------------|
| `delta.checkpointInterval` | `checkpoint_interval` | `Option<NonZero<u64>>` | 10 | Checkpoint every N commits |
| `delta.checkpoint.writeStatsAsJson` | `checkpoint_write_stats_as_json` | `Option<bool>` | true | Write stats as JSON string |
| `delta.checkpoint.writeStatsAsStruct` | `checkpoint_write_stats_as_struct` | `Option<bool>` | false | Write stats as struct |
| `delta.checkpointPolicy` | `checkpoint_policy` | `Option<CheckpointPolicy>` | classic | `classic` or `v2` |

**Usage in code:**

```rust
// kernel/src/checkpoint/stats_transform.rs
impl StatsTransformConfig {
    pub(super) fn from_table_properties(properties: &TableProperties) -> Self {
        Self {
            write_stats_as_json: properties.should_write_stats_as_json(),
            write_stats_as_struct: properties.should_write_stats_as_struct(),
        }
    }
}
```

### Column Mapping Properties

| Property Key | Field | Type | Description |
|--------------|-------|------|-------------|
| `delta.columnMapping.mode` | `column_mapping_mode` | `Option<ColumnMappingMode>` | `none`, `id`, or `name` |
| `delta.columnMapping.maxColumnId` | (parsed separately) | `i64` | Maximum assigned column ID |

**Column Mapping Modes:**

```rust
pub enum ColumnMappingMode {
    None,  // No column mapping (default)
    Id,    // Match columns by field ID
    Name,  // Match columns by physical name
}
```

**Usage:**

```rust
// kernel/src/table_features/column_mapping.rs
pub(crate) fn column_mapping_mode(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> ColumnMappingMode {
    match (table_properties.column_mapping_mode, protocol.min_reader_version()) {
        (Some(mode), 2) => mode,
        (Some(mode), 3) if protocol.has_table_feature(&TableFeature::ColumnMapping) => mode,
        _ => ColumnMappingMode::None,
    }
}
```

### Data Skipping Properties

| Property Key | Field | Type | Default | Description |
|--------------|-------|------|---------|-------------|
| `delta.dataSkippingNumIndexedCols` | `data_skipping_num_indexed_cols` | `Option<DataSkippingNumIndexedCols>` | 32 | Number of columns for stats (-1 = all) |
| `delta.dataSkippingStatsColumns` | `data_skipping_stats_columns` | `Option<Vec<ColumnName>>` | None | Explicit list of stats columns |

**Priority:** If `dataSkippingStatsColumns` is set, it takes precedence over `dataSkippingNumIndexedCols`.

### Retention Properties

| Property Key | Field | Type | Default | Description |
|--------------|-------|------|---------|-------------|
| `delta.deletedFileRetentionDuration` | `deleted_file_retention_duration` | `Option<Duration>` | 7 days | Time before VACUUM can remove files |
| `delta.logRetentionDuration` | `log_retention_duration` | `Option<Duration>` | 30 days | History retention period |
| `delta.enableExpiredLogCleanup` | `enable_expired_log_cleanup` | `Option<bool>` | true | Auto-cleanup expired logs |
| `delta.setTransactionRetentionDuration` | `set_transaction_retention_duration` | `Option<Duration>` | None | Retention for app transaction IDs |

**Interval Format:**

```
"interval 7 days"
"interval 30 days"
"interval 168 hours"
```

### Feature Enablement Properties

| Property Key | Field | Type | Description |
|--------------|-------|------|-------------|
| `delta.appendOnly` | `append_only` | `Option<bool>` | Table only supports appends |
| `delta.enableChangeDataFeed` | `enable_change_data_feed` | `Option<bool>` | Enable Change Data Feed |
| `delta.enableDeletionVectors` | `enable_deletion_vectors` | `Option<bool>` | Enable deletion vectors |
| `delta.enableRowTracking` | `enable_row_tracking` | `Option<bool>` | Enable row tracking |
| `delta.enableInCommitTimestamps` | `enable_in_commit_timestamps` | `Option<bool>` | Enable in-commit timestamps |

**Feature Check Example:**

```rust
// kernel/src/table_features/mod.rs
EnablementCheck::EnabledIf(|props: &TableProperties| {
    props.enable_change_data_feed == Some(true)
})
```

### Row Tracking Properties

| Property Key | Field | Type | Description |
|--------------|-------|------|-------------|
| `delta.enableRowTracking` | `enable_row_tracking` | `Option<bool>` | Enable row tracking feature |
| `delta.rowTrackingSuspended` | `row_tracking_suspended` | `Option<bool>` | Temporarily suspend tracking |
| `delta.rowTracking.materializedRowIdColumnName` | `materialized_row_id_column_name` | `Option<String>` | Custom row ID column name |
| `delta.rowTracking.materializedRowCommitVersionColumnName` | `materialized_row_commit_version_column_name` | `Option<String>` | Custom commit version column |

**Row Tracking Active Check:**

```rust
props.enable_row_tracking == Some(true) && props.row_tracking_suspended != Some(true)
```

### In-Commit Timestamps Properties

| Property Key | Field | Type | Description |
|--------------|-------|------|-------------|
| `delta.enableInCommitTimestamps` | `enable_in_commit_timestamps` | `Option<bool>` | Enable ICT feature |
| `delta.inCommitTimestampEnablementVersion` | `in_commit_timestamp_enablement_version` | `Option<Version>` | Version when ICT was enabled |
| `delta.inCommitTimestampEnablementTimestamp` | `in_commit_timestamp_enablement_timestamp` | `Option<i64>` | Timestamp when ICT was enabled |

### Parquet Format Properties

| Property Key | Field | Type | Default | Description |
|--------------|-------|------|---------|-------------|
| `delta.parquet.format.version` | `parquet_format_version` | `Option<ParquetFormatVersion>` | `1.0.0` | Parquet format version for writes |

**Supported Values:**
- `1.0.0`: Parquet V1 format (default, maximum compatibility)
- `2.12.0`: Parquet V2 format with DataPageV2 headers, disabled dictionary encoding

**Parquet Format Enum:**

```rust
pub enum ParquetFormatVersion {
    V1_0_0,   // Parquet V1 (default)
    V2_12_0,  // Parquet V2
}
```

**Usage:**

The parquet format version is accessed through `WriteContext` during writes:

```rust
let write_context = transaction.get_write_context();
let format_version = write_context.parquet_format_version();
// format_version is used to configure WriterProperties for parquet-rs
```

**V1 vs V2 Configuration:**

| Feature | V1 (`1.0.0`) | V2 (`2.12.0`) |
|---------|--------------|---------------|
| Writer Version | `PARQUET_1_0` | `PARQUET_2_0` |
| Dictionary Encoding | Enabled | Disabled |
| Page Format | DataPageV1 | DataPageV2 |
| Timestamp Type | INT64 (arrow-rs default) | INT64 |

**Note:** Checkpoint writes always use Parquet V1 format for maximum compatibility across all Delta readers.

---

## How Properties Affect Behavior

### Checkpoint Format

```rust
// Properties control stats format in checkpoints
let config = StatsTransformConfig::from_table_properties(properties);
// config.write_stats_as_json -> writes "stats" field
// config.write_stats_as_struct -> writes "stats_parsed" field
```

### Transaction Validation

```rust
// CDF enabled tables have restrictions
if properties.enable_change_data_feed == Some(true) {
    // Cannot add and remove in same transaction
}
```

### Retention Calculations

```rust
// Determine when files can be vacuumed
fn deleted_file_retention_timestamp(&self) -> DeltaResult<i64> {
    let retention = self.table_properties().deleted_file_retention_duration;
    // Calculate cutoff timestamp
}
```

### Stats Column Selection

```rust
// Determine which columns get statistics
if let Some(columns) = properties.data_skipping_stats_columns {
    // Use explicit list
} else if let Some(num) = properties.data_skipping_num_indexed_cols {
    // Use first N columns (-1 = all)
} else {
    // Default: first 32 columns
}
```

---

## Unknown Properties

Properties that don't match any known key are preserved in `unknown_properties`:

```rust
pub unknown_properties: HashMap<String, String>
```

This allows:
- Forward compatibility with new properties
- Custom application-specific properties
- Properties from other Delta implementations

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `kernel/src/table_properties.rs` | `TableProperties` struct and constants |
| `kernel/src/table_properties/deserialize.rs` | Parsing logic |
| `kernel/src/actions/mod.rs` | `Metadata.parse_table_properties()` |
| `kernel/src/table_configuration.rs` | `TableConfiguration` integration |
| `kernel/src/snapshot.rs` | `Snapshot::table_properties()` |
| `kernel/src/table_features/mod.rs` | Feature enablement checks |
| `kernel/src/table_features/column_mapping.rs` | Column mapping mode resolution |
| `kernel/src/checkpoint/stats_transform.rs` | Checkpoint stats configuration |
| `kernel/src/action_reconciliation/mod.rs` | Retention timestamp calculations |

---

## Property Key Constants

All property keys are defined as constants in `kernel/src/table_properties.rs`:

```rust
pub(crate) const APPEND_ONLY: &str = "delta.appendOnly";
pub(crate) const CHECKPOINT_INTERVAL: &str = "delta.checkpointInterval";
pub(crate) const COLUMN_MAPPING_MODE: &str = "delta.columnMapping.mode";
pub(crate) const ENABLE_CHANGE_DATA_FEED: &str = "delta.enableChangeDataFeed";
pub(crate) const ENABLE_DELETION_VECTORS: &str = "delta.enableDeletionVectors";
pub(crate) const ENABLE_ROW_TRACKING: &str = "delta.enableRowTracking";
pub(crate) const PARQUET_FORMAT_VERSION: &str = "delta.parquet.format.version";
// ... and more
```

This ensures consistency across the codebase and makes property keys easily discoverable.
