// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::fmt::Display;

use crate::model::QueryResponse;

/// Display [QueryResponse] in csv format.
pub struct CsvFormatter {
    pub resp: QueryResponse,
}

impl Display for CsvFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for column in &self.resp.schema.column_schemas {
            f.write_fmt(format_args!("{},", column.name))?;
        }
        f.write_str("\n")?;

        for row in &self.resp.rows {
            for datum in &row.datums {
                f.write_fmt(format_args!("{:?},", datum))?;
            }
            f.write_str("\n")?;
        }

        Ok(())
    }
}
