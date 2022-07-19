use std::fmt::Display;

use crate::model::QueryResponse;

/// Display [QueryResponse] in csv format.
pub struct CsvFormatter {
    pub rows: QueryResponse,
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
