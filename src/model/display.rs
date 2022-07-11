// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::fmt::Display;

use crate::model::QueriedRows;

/// Display [QueriedRows] in csv format.
pub struct CsvFormatter {
    pub rows: QueriedRows,
}

impl Display for CsvFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for column in &self.rows.schema.column_schemas {
            f.write_fmt(format_args!("{},", column.name))?;
        }
        f.write_str("\n")?;

        for row in &self.rows.rows {
            for datum in &row.datums {
                f.write_fmt(format_args!("{:?},", datum))?;
            }
            f.write_str("\n")?;
        }

        Ok(())
    }
}
