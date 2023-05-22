// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Display for sql query response

use std::fmt::Display;

use crate::model::sql_query::response::Response;

/// Display [`SqlQueryResponse`](Response) in csv format.
pub struct CsvFormatter {
    pub resp: Response,
}

impl Display for CsvFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Just print while returned `rows` in not empty.
        if !self.resp.rows.is_empty() {
            // Get and output column names, unwrap is safe here.
            let first_row = self.resp.rows.first().unwrap();
            let col_names = first_row
                .columns()
                .iter()
                .map(|col| col.name().to_string())
                .collect::<Vec<_>>();
            for col_name in &col_names {
                f.write_fmt(format_args!("{},", col_name))?;
            }
            f.write_str("\n")?;

            // Get and output rows.
            for row in &self.resp.rows {
                for column in row.columns() {
                    f.write_fmt(format_args!("{:?},", column.value()))?;
                }
                f.write_str("\n")?;
            }
        }

        Ok(())
    }
}
