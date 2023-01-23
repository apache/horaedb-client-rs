// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// [Row] in sql query

use std::collections::BTreeMap;

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, Time32MillisecondArray, TimestampMillisecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use paste::paste;

use crate::{model::value::Value, Error, Result};

#[derive(Debug)]
pub struct Row {
    // It is better to iterate in a fixed order, also can save memory.
    values: BTreeMap<String, Value>,
}

impl Row {
    pub fn column(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.values.iter().map(|(name, _)| name.clone()).collect()
    }
}

macro_rules! fill_column {
    ($arrow_column:expr, $arrow_array_type:ty, $value_type:ty, $rows:expr, $col_idx:expr) => {
        paste! {
            let row_count = $rows.len();
            let cast_arrow_column = $arrow_column
                .as_any()
                .downcast_ref::<$arrow_array_type>().unwrap();
            for row_idx in 0..row_count {
                let value = cast_arrow_column.value(row_idx).to_owned();
                let row = $rows.get_mut(row_idx).unwrap();
                let col = row.get_mut($col_idx).unwrap();
                *col = $value_type(value)
            }
        }
    };
}

#[derive(Clone, Debug)]
pub struct RowBuilder {
    pub col_idx_to_name: Vec<String>,
    pub row_values: Vec<Vec<Value>>,
}

impl RowBuilder {
    pub fn build(self) -> Vec<Row> {
        self.row_values
            .into_iter()
            .map(|row| {
                let row_value_with_names = row
                    .into_iter()
                    .enumerate()
                    .map(|(col_idx, value)| {
                        // Find its name.
                        let col_name = self.col_idx_to_name[col_idx].clone();
                        (col_name, value)
                    })
                    .collect::<BTreeMap<String, Value>>();

                Row {
                    values: row_value_with_names,
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn with_arrow_record_batch(record_batch: RecordBatch) -> Result<Self> {
        // Build `col_idx_to_name`.
        let col_idx_to_name = record_batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();

        // Build `rows`.
        let col_count = record_batch.num_columns();
        let row_count = record_batch.num_rows();

        let mut rows = vec![vec![Value::Null; col_count]; row_count];

        // Fill row row batch column by column.
        for col_idx in 0..col_count {
            let arrow_column = record_batch.column(col_idx);
            Self::fill_column_in_row_batch(&mut rows, col_idx, arrow_column)?;
        }

        Ok(RowBuilder {
            col_idx_to_name,
            row_values: rows,
        })
    }

    fn fill_column_in_row_batch(
        rows: &mut [Vec<Value>],
        col_idx: usize,
        arrow_column: &ArrayRef,
    ) -> Result<()> {
        let row_count = rows.len();
        let arrow_type = arrow_column.data_type();
        // TODO: may we can make it simpler with macro.
        match arrow_type {
            // Because `rows` will be initialized with `Value::Null`, just do nothing while
            // encounter `DataType::Null`.
            DataType::Null => {}
            DataType::Boolean => {
                fill_column!(arrow_column, BooleanArray, Value::Boolean, rows, col_idx);
            }
            DataType::Int8 => {
                fill_column!(arrow_column, Int8Array, Value::Int8, rows, col_idx);
            }
            DataType::Int16 => {
                fill_column!(arrow_column, Int16Array, Value::Int16, rows, col_idx);
            }
            DataType::Int32 => {
                fill_column!(arrow_column, Int32Array, Value::Int32, rows, col_idx);
            }
            DataType::Int64 => {
                fill_column!(arrow_column, Int64Array, Value::Int64, rows, col_idx);
            }
            DataType::UInt8 => {
                fill_column!(arrow_column, UInt8Array, Value::UInt8, rows, col_idx);
            }
            DataType::UInt16 => {
                fill_column!(arrow_column, UInt16Array, Value::UInt16, rows, col_idx);
            }
            DataType::UInt32 => {
                fill_column!(arrow_column, UInt32Array, Value::UInt32, rows, col_idx);
            }
            DataType::UInt64 => {
                fill_column!(arrow_column, UInt64Array, Value::UInt64, rows, col_idx);
            }
            DataType::Float32 => {
                fill_column!(arrow_column, Int8Array, Value::Int8, rows, col_idx);
            }
            DataType::Float64 => {
                fill_column!(arrow_column, Int8Array, Value::Int8, rows, col_idx);
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                fill_column!(arrow_column, StringArray, Value::String, rows, col_idx);
            }
            DataType::Binary | DataType::LargeBinary => {
                fill_column!(arrow_column, BinaryArray, Value::Varbinary, rows, col_idx);
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                fill_column!(
                    arrow_column,
                    TimestampMillisecondArray,
                    Value::Timestamp,
                    rows,
                    col_idx
                );
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                let cast_arrow_column = arrow_column
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap();
                for row_idx in 0..row_count {
                    let value = cast_arrow_column.value(row_idx);
                    let row = rows.get_mut(row_idx).unwrap();
                    let col = row.get_mut(col_idx).unwrap();
                    *col = Value::Timestamp(value as i64)
                }
            }
            // Encounter unsupported type.
            _ => {
                return Err(Error::BuildRows(format!(
                    "found unsupported arrow type:{}",
                    arrow_type
                )));
            }
        }
        Ok(())
    }
}
