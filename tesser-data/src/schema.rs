use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

/// Precision used for canonical decimal columns.
pub const CANONICAL_DECIMAL_PRECISION: u8 = 38;
/// Scale used for canonical decimal columns.
pub const CANONICAL_DECIMAL_SCALE: i8 = 18;
pub const CANONICAL_DECIMAL_SCALE_U32: u32 = 18;

/// Arrow schema shared by normalized candle data sets.
pub fn canonical_candle_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("interval", DataType::Utf8, false),
        Field::new("open", canonical_decimal_type(), false),
        Field::new("high", canonical_decimal_type(), false),
        Field::new("low", canonical_decimal_type(), false),
        Field::new("close", canonical_decimal_type(), false),
        Field::new("volume", canonical_decimal_type(), true),
    ]))
}

/// Helper that returns the decimal type definition shared by OHLCV columns.
pub fn canonical_decimal_type() -> DataType {
    DataType::Decimal128(CANONICAL_DECIMAL_PRECISION, CANONICAL_DECIMAL_SCALE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_contains_expected_fields() {
        let schema = canonical_candle_schema();
        assert_eq!(schema.fields().len(), 8);
        let open_field = schema.field_with_name("open").unwrap();
        assert_eq!(open_field.data_type(), &canonical_decimal_type());
        let ts_field = schema.field_with_name("timestamp").unwrap();
        assert_eq!(ts_field.data_type(), &DataType::Int64);
        let interval = schema.field_with_name("interval").unwrap();
        assert_eq!(interval.data_type(), &DataType::Utf8);
    }
}
