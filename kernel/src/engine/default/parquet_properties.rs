//! Parquet writer properties configuration based on Delta table properties.

use crate::parquet::file::properties::{WriterProperties, WriterVersion};
use crate::table_properties::ParquetFormatVersion;

/// Build `WriterProperties` based on the parquet format version specified in table properties.
///
/// This function translates the Delta table property `delta.parquet.format.version` into
/// the appropriate parquet-rs `WriterProperties` configuration.
///
/// # Version Mapping
///
/// | Delta Version | Parquet Writer Version | Dictionary |
/// |---------------|------------------------|------------|
/// | `1.0.0`       | `PARQUET_1_0`          | Enabled    |
/// | `2.12.0`      | `PARQUET_2_0`          | Disabled   |
///
/// For `2.12.0`, the writer is configured with Parquet V2 which enables:
/// - DataPageV2 headers (via `WriterVersion::PARQUET_2_0`)
/// - Disabled dictionary encoding (allows PLAIN encoding to be used directly)
/// - INT64 timestamps (default in arrow-rs)
///
/// Note: Dictionary encoding is disabled for V2 to align with the Parquet V2 specification
/// which recommends using other encodings. The actual encoding used will depend on the
/// column type and parquet-rs's encoder selection.
pub(crate) fn build_writer_properties(format_version: ParquetFormatVersion) -> WriterProperties {
    let mut builder = WriterProperties::builder();

    match format_version {
        ParquetFormatVersion::V1_0_0 => {
            builder = builder.set_writer_version(WriterVersion::PARQUET_1_0);
        }
        ParquetFormatVersion::V2_12_0 => {
            builder = builder
                .set_writer_version(WriterVersion::PARQUET_2_0)
                // Disable dictionary encoding for V2
                .set_dictionary_enabled(false);
        }
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::schema::types::ColumnPath;

    #[test]
    fn test_build_writer_properties_v1() {
        let props = build_writer_properties(ParquetFormatVersion::V1_0_0);
        assert_eq!(props.writer_version(), WriterVersion::PARQUET_1_0);
        // V1 uses default dictionary encoding (enabled)
        let test_col = ColumnPath::from("test_column");
        assert!(props.dictionary_enabled(&test_col));
    }

    #[test]
    fn test_build_writer_properties_v2() {
        let props = build_writer_properties(ParquetFormatVersion::V2_12_0);
        assert_eq!(props.writer_version(), WriterVersion::PARQUET_2_0);
        // V2 disables dictionary encoding
        let test_col = ColumnPath::from("test_column");
        assert!(!props.dictionary_enabled(&test_col));
    }

    #[test]
    fn test_default_is_v1() {
        let props = build_writer_properties(ParquetFormatVersion::default());
        assert_eq!(props.writer_version(), WriterVersion::PARQUET_1_0);
    }
}
