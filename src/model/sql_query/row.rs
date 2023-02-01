// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// [Row] in sql query

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

#[derive(Debug, PartialEq)]
pub struct Row {
    // It is better to iterate in a fixed order, also can save memory.
    columns: Vec<Column>,
}

impl Row {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub value: Value,
}

impl Column {
    pub(crate) fn new(name: String, value: Value) -> Self {
        Self { name, value }
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
                let columns = row
                    .into_iter()
                    .enumerate()
                    .map(|(col_idx, value)| {
                        // Find its name.
                        let col_name = self.col_idx_to_name[col_idx].clone();

                        Column::new(col_name, value)
                    })
                    .collect::<Vec<Column>>();

                Row { columns }
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
                    "Unsupported arrow type:{}",
                    arrow_type
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{
            BinaryArray, Int32Array, StringArray, Time32MillisecondArray, TimestampMillisecondArray,
        },
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::{Row, RowBuilder};
    use crate::model::{sql_query::row::Column, value::Value};

    #[test]
    fn test_build_row() {
        let int_values = vec![1, 2, 3];
        let string_values = vec![
            "test1".to_string(),
            "test2".to_string(),
            "test3".to_string(),
        ];
        let binary_values = vec![b"0.1".as_slice(), b"0.2".as_slice(), b"0.3".as_slice()];
        let timestamp_values = vec![1001, 1002, 1003];
        let timestamp32_values = vec![1004, 1005, 1006];
        // Built rows.
        let int_array = Int32Array::from(int_values.clone());
        let string_array = StringArray::from(string_values.clone());
        let binary_array = BinaryArray::from(binary_values.clone());
        let timestamp_array = TimestampMillisecondArray::from(timestamp_values.clone());
        let timestamp32_array = Time32MillisecondArray::from(timestamp32_values.clone());
        let schema = Schema::new(vec![
            Field::new("int", DataType::Int32, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("varbinary", DataType::Binary, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "timestamp32",
                DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
                false,
            ),
        ]);
        let arrow_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(int_array),
                Arc::new(string_array),
                Arc::new(binary_array),
                Arc::new(timestamp_array),
                Arc::new(timestamp32_array),
            ],
        )
        .unwrap();

        let built_rows = RowBuilder::with_arrow_record_batch(arrow_batch)
            .unwrap()
            .build();

        // Expected rows
        let int_col_values = int_values.into_iter().map(Value::Int32).collect::<Vec<_>>();
        let string_col_values = string_values
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>();
        let binary_col_values = binary_values
            .into_iter()
            .map(|v| Value::Varbinary(v.to_vec()))
            .collect::<Vec<_>>();
        let timestamp_col_values = timestamp_values
            .into_iter()
            .map(|v| Value::Timestamp(v as i64))
            .collect::<Vec<_>>();
        let timestamp32_col_values = timestamp32_values
            .into_iter()
            .map(|v| Value::Timestamp(v as i64))
            .collect::<Vec<_>>();
        let row1 = Row {
            columns: vec![
                Column::new("int".to_string(), int_col_values[0].clone()),
                Column::new("string".to_string(), string_col_values[0].clone()),
                Column::new("varbinary".to_string(), binary_col_values[0].clone()),
                Column::new("timestamp".to_string(), timestamp_col_values[0].clone()),
                Column::new("timestamp32".to_string(), timestamp32_col_values[0].clone()),
            ],
        };
        let row2 = Row {
            columns: vec![
                Column::new("int".to_string(), int_col_values[1].clone()),
                Column::new("string".to_string(), string_col_values[1].clone()),
                Column::new("varbinary".to_string(), binary_col_values[1].clone()),
                Column::new("timestamp".to_string(), timestamp_col_values[1].clone()),
                Column::new("timestamp32".to_string(), timestamp32_col_values[1].clone()),
            ],
        };
        let row3 = Row {
            columns: vec![
                Column::new("int".to_string(), int_col_values[2].clone()),
                Column::new("string".to_string(), string_col_values[2].clone()),
                Column::new("varbinary".to_string(), binary_col_values[2].clone()),
                Column::new("timestamp".to_string(), timestamp_col_values[2].clone()),
                Column::new("timestamp32".to_string(), timestamp32_col_values[2].clone()),
            ],
        };
        let expected_rows = vec![row1, row2, row3];

        assert_eq!(built_rows, expected_rows);
    }
}
