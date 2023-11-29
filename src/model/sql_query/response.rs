// Copyright 2023 The HoraeDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Cursor;

use arrow::{ipc::reader::StreamReader, record_batch::RecordBatch};
use ceresdbproto::storage::{
    arrow_payload::Compression, sql_query_response::Output as OutputPb, ArrowPayload,
    SqlQueryResponse,
};

use crate::{
    errors::{Error, Result},
    model::sql_query::row::{Row, RowBuilder},
};

/// The response for [`SqlQueryRequest`](crate::model::sql_query::Request).
#[derive(Debug, Default)]
pub struct Response {
    /// The affected rows by the query sql.
    pub affected_rows: u32,
    /// The rows of the sql result.
    pub rows: Vec<Row>,
}

#[derive(Debug)]
enum Output {
    AffectedRows(u32),
    Rows(Vec<Row>),
}

impl TryFrom<SqlQueryResponse> for Response {
    type Error = Error;

    fn try_from(sql_resp_pb: SqlQueryResponse) -> std::result::Result<Self, Self::Error> {
        let output_pb = sql_resp_pb
            .output
            .ok_or_else(|| Error::Unknown("output is empty in sql query response".to_string()))?;
        let output = Output::try_from(output_pb)?;

        let resp = match output {
            Output::AffectedRows(affected) => Response {
                affected_rows: affected,
                ..Default::default()
            },
            Output::Rows(rows) => Response {
                rows,
                ..Default::default()
            },
        };

        Ok(resp)
    }
}

impl TryFrom<OutputPb> for Output {
    type Error = Error;

    fn try_from(output_pb: OutputPb) -> std::result::Result<Self, Self::Error> {
        let output = match output_pb {
            OutputPb::AffectedRows(affected) => Output::AffectedRows(affected),
            OutputPb::Arrow(arrow_payload) => {
                let arrow_record_batches = decode_arrow_payload(arrow_payload)?;
                let rows_group = arrow_record_batches
                    .into_iter()
                    .map(|record_batch| {
                        let row_builder = match RowBuilder::with_arrow_record_batch(record_batch) {
                            Ok(builder) => builder,
                            Err(e) => return Err(e),
                        };
                        Ok(row_builder.build())
                    })
                    .collect::<Result<Vec<_>>>()?;
                let rows = rows_group.into_iter().flatten().collect::<Vec<_>>();

                Output::Rows(rows)
            }
        };

        Ok(output)
    }
}

pub fn decode_arrow_payload(arrow_payload: ArrowPayload) -> Result<Vec<RecordBatch>> {
    let compression = arrow_payload.compression();
    let byte_batches = arrow_payload.record_batches;

    // Maybe unzip payload bytes firstly.
    let unzip_byte_batches = byte_batches
        .into_iter()
        .map(|bytes_batch| match compression {
            Compression::None => Ok(bytes_batch),
            Compression::Zstd => zstd::stream::decode_all(Cursor::new(bytes_batch))
                .map_err(|e| Error::DecodeArrowPayload(Box::new(e))),
        })
        .collect::<Result<Vec<Vec<u8>>>>()?;

    // Decode the byte batches to record batches, multiple record batches may be
    // included in one byte batch.
    let record_batches_group = unzip_byte_batches
        .into_iter()
        .map(|byte_batch| {
            // Decode bytes to `RecordBatch`.
            let stream_reader = match StreamReader::try_new(Cursor::new(byte_batch), None)
                .map_err(|e| Error::DecodeArrowPayload(Box::new(e)))
            {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            stream_reader
                .into_iter()
                .map(|decode_result| {
                    decode_result.map_err(|e| Error::DecodeArrowPayload(Box::new(e)))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<Vec<_>>>>()?;

    let record_batches = record_batches_group
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(record_batches)
}
