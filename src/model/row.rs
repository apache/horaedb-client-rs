// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use common_types::datum::Datum;

#[derive(Clone, Debug)]
pub struct Row {
    pub datums: Vec<Datum>,
}

impl Row {
    pub fn with_column_num(n: usize) -> Self {
        Self {
            datums: Vec::with_capacity(n),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnDataType {
    Null = 0,
    TimestampMillis,
    Double,
    Float,
    Bytes,
    String,
    Int64,
    Int32,
    Boolean,
}

impl TryFrom<&avro_rs::Schema> for ColumnDataType {
    type Error = String;

    /// Convert the basic schema defined by avro into the ColumnDataType.
    fn try_from(schema: &avro_rs::Schema) -> Result<Self, Self::Error> {
        let data_type = match schema {
            avro_rs::Schema::Null => ColumnDataType::Null,
            avro_rs::Schema::Boolean => ColumnDataType::Boolean,
            avro_rs::Schema::Int => ColumnDataType::Int32,
            avro_rs::Schema::Long => ColumnDataType::Int64,
            avro_rs::Schema::Float => ColumnDataType::Float,
            avro_rs::Schema::Double => ColumnDataType::Double,
            avro_rs::Schema::Bytes => ColumnDataType::Bytes,
            avro_rs::Schema::String => ColumnDataType::String,
            avro_rs::Schema::TimestampMillis => ColumnDataType::TimestampMillis,
            avro_rs::Schema::Union(v) => {
                let variants = v.variants();
                if variants.len() != 2 {
                    return Err(format!(
                        "invalid avro union schema:{:?}, expect at least two columns",
                        schema,
                    ));
                }

                if let avro_rs::Schema::Null = &variants[0] {
                    Self::try_from(&variants[1])?
                } else {
                    return Err(format!(
                        "invalid avro union schema:{:?}, expect the first column is null",
                        schema
                    ));
                }
            }
            avro_rs::Schema::Array(_)
            | avro_rs::Schema::Map(_)
            | avro_rs::Schema::Record { .. }
            | avro_rs::Schema::Enum { .. }
            | avro_rs::Schema::Fixed { .. }
            | avro_rs::Schema::Decimal { .. }
            | avro_rs::Schema::Uuid
            | avro_rs::Schema::Date
            | avro_rs::Schema::TimeMillis
            | avro_rs::Schema::TimeMicros
            | avro_rs::Schema::TimestampMicros
            | avro_rs::Schema::Duration => {
                return Err(format!("invalid avro basic schema:{:?}", schema))
            }
        };

        Ok(data_type)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub data_type: ColumnDataType,
    pub name: String,
}

#[derive(Debug, Default, Clone)]
pub struct Schema {
    pub column_schemas: Vec<ColumnSchema>,
    pub lookup: HashMap<String, usize>,
}

impl Schema {
    #[inline]
    pub fn num_cols(&self) -> usize {
        self.column_schemas.len()
    }

    #[inline]
    pub fn col_idx(&self, name: &str) -> Option<usize> {
        self.lookup.get(&*name).copied()
    }
}

impl TryFrom<&avro_rs::Schema> for Schema {
    type Error = String;

    fn try_from(avro_schema: &avro_rs::Schema) -> Result<Self, Self::Error> {
        if let avro_rs::Schema::Record { fields, lookup, .. } = avro_schema {
            let mut column_schemas = Vec::with_capacity(fields.len());
            for field in fields {
                let column_schema = ColumnSchema {
                    data_type: ColumnDataType::try_from(&field.schema)?,
                    name: field.name.clone(),
                };
                column_schemas.push(column_schema);
            }

            Ok(Schema {
                column_schemas,
                lookup: lookup.clone(),
            })
        } else {
            Err(format!("Unsupported schema:{:?}", avro_schema))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct QueriedRows {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl QueriedRows {
    pub fn with_capacity(schema: Schema, n: usize) -> Self {
        Self {
            schema,
            rows: Vec::with_capacity(n),
        }
    }
}
