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
/// | Delta Version | Parquet Writer Version |
/// |---------------|------------------------|
/// | `1.0.0`       | `PARQUET_1_0`          |
/// | `2.12.0`      | `PARQUET_2_0`          |
///
/// For `2.12.0`, the writer is configured with Parquet V2 which enables:
/// - DataPageV2 headers
/// - Delta encodings (DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY)
/// - INT64 timestamps (default in arrow-rs)
pub(crate) fn build_writer_properties(format_version: ParquetFormatVersion) -> WriterProperties {
    let mut builder = WriterProperties::builder();

    match format_version {
        ParquetFormatVersion::V1_0_0 => {
            builder = builder.set_writer_version(WriterVersion::PARQUET_1_0);
        }
        ParquetFormatVersion::V2_12_0 => {
            builder = builder.set_writer_version(WriterVersion::PARQUET_2_0);
        }
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_writer_properties_v1() {
        let props = build_writer_properties(ParquetFormatVersion::V1_0_0);
        assert_eq!(props.writer_version(), WriterVersion::PARQUET_1_0);
    }

    #[test]
    fn test_build_writer_properties_v2() {
        let props = build_writer_properties(ParquetFormatVersion::V2_12_0);
        assert_eq!(props.writer_version(), WriterVersion::PARQUET_2_0);
    }

    #[test]
    fn test_default_is_v1() {
        let props = build_writer_properties(ParquetFormatVersion::default());
        assert_eq!(props.writer_version(), WriterVersion::PARQUET_1_0);
    }
}
