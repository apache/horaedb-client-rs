// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
                f.write_fmt(format_args!("{col_name},"))?;
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
