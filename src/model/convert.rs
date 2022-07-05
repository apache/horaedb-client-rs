// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use avro_rs::{types::Value, Schema as AvroSchema};
use common_types::{bytes::Bytes, datum::Datum, string::StringBytes, time::Timestamp};

use crate::model::row::{QueriedRows, Row, Schema};

/// Convert the avro `Value` into the `Datum`.
///
/// Some types defined by avro are not used and the conversion rule is totally
/// based on the implementation in the server.
fn value_to_datum(value: Value) -> Result<Datum, String> {
    let datum = match value {
        Value::Null => Datum::Null,
        Value::TimestampMillis(v) => Datum::Timestamp(Timestamp::new(v)),
        Value::Double(v) => Datum::Double(v),
        Value::Float(v) => Datum::Float(v),
        Value::Bytes(v) => Datum::Varbinary(Bytes::from(v)),
        Value::String(v) => Datum::String(StringBytes::from(v)),
        // FIXME: Now the server converts both uint64 and int64 into`Value::Long` because uint64 is
        // not supported by avro, that is to say something may go wrong in some corner case.
        Value::Long(v) => Datum::Int64(v),
        Value::Int(v) => Datum::Int32(v),
        Value::Boolean(v) => Datum::Boolean(v),
        Value::Union(inner_val) => value_to_datum(*inner_val)?,
        Value::Fixed(_, _)
        | Value::Enum(_, _)
        | Value::Array(_)
        | Value::Map(_)
        | Value::Record(_)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::TimeMillis(_)
        | Value::TimeMicros(_)
        | Value::TimestampMicros(_)
        | Value::Duration(_)
        | Value::Uuid(_) => return Err(format!("Unsupported value type:{:?}", value)),
    };

    Ok(datum)
}

fn parse_one_row(schema: &AvroSchema, mut raw: &[u8], row: &mut Row) -> Result<(), String> {
    let record = avro_rs::from_avro_datum(schema, &mut raw, None).map_err(|e| e.to_string())?;
    if let Value::Record(cols) = record {
        for (_, column_value) in cols {
            let datum = value_to_datum(column_value)?;
            row.datums.push(datum);
        }

        Ok(())
    } else {
        Err(format!("invalid avro row:{:?}, expect record", record))
    }
}

/// Parse the raw rows according to the schema and the (de)serialization
/// protocol is avro.
pub fn parse_queried_rows(raw_schema: &str, rows: &[Vec<u8>]) -> Result<QueriedRows, String> {
    let avro_schema = AvroSchema::parse_str(raw_schema).map_err(|e| e.to_string())?;
    let schema = Schema::try_from(&avro_schema)?;

    let mut queried_rows = QueriedRows::with_capacity(schema, rows.len());
    for raw_row in rows {
        let mut row = Row::with_column_num(queried_rows.schema.num_cols());
        parse_one_row(&avro_schema, &*raw_row, &mut row)?;
        queried_rows.rows.push(row);
    }

    Ok(queried_rows)
}
