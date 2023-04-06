// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::ops::Bound;

use serde::{Deserialize, Serialize};
use tantivy::schema::{Schema, Type};

use super::QueryAst;
use crate::json_literal::InterpretUserInput;
use crate::quickwit_query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::quickwit_query_ast::IntoTantivyAst;
use crate::{InvalidQuery, JsonLiteral};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RangeQuery {
    pub field: String,
    pub lower_bound: Bound<JsonLiteral>,
    pub upper_bound: Bound<JsonLiteral>,
}

fn convert_bound<'a, T>(
    bound: &'a Bound<JsonLiteral>,
    field_name: &str,
) -> Result<Bound<T>, InvalidQuery>
where
    T: InterpretUserInput<'a>,
{
    match bound {
        Bound::Included(val) => {
            let val = T::interpret(val).ok_or_else(|| InvalidQuery::InvalidBoundary {
                expected_value_type: T::name(),
                field_name: field_name.to_string(),
            })?;
            Ok(Bound::Included(val))
        }
        Bound::Excluded(val) => {
            let val = T::interpret(val).ok_or_else(|| InvalidQuery::InvalidBoundary {
                expected_value_type: T::name(),
                field_name: field_name.to_string(),
            })?;
            Ok(Bound::Excluded(val))
        }
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}

impl From<RangeQuery> for QueryAst {
    fn from(range_query: RangeQuery) -> Self {
        QueryAst::Range(range_query)
    }
}

impl IntoTantivyAst for RangeQuery {
    fn into_tantivy_ast_impl(
        &self,
        schema: &Schema,
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (_field, field_entry, _path) =
            super::utils::find_field_or_hit_dynamic(&self.field, schema)?;
        if !field_entry.is_fast() {
            return Err(InvalidQuery::SchemaError(format!(
                "Range queries are only supported for fast fields. (`{}` is not a fast field)",
                field_entry.name()
            )));
        }
        Ok(match field_entry.field_type() {
            tantivy::schema::FieldType::Str(_) => {
                let lower_bound = convert_bound(&self.lower_bound, field_entry.name())?;
                let upper_bound = convert_bound(&self.upper_bound, field_entry.name())?;
                tantivy::query::RangeQuery::new_str_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::U64(_) => {
                let lower_bound = convert_bound(&self.lower_bound, field_entry.name())?;
                let upper_bound = convert_bound(&self.upper_bound, field_entry.name())?;
                tantivy::query::RangeQuery::new_u64_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::I64(_) => {
                let lower_bound = convert_bound(&self.lower_bound, field_entry.name())?;
                let upper_bound = convert_bound(&self.upper_bound, field_entry.name())?;
                tantivy::query::RangeQuery::new_i64_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::F64(_) => {
                let lower_bound = convert_bound(&self.lower_bound, field_entry.name())?;
                let upper_bound = convert_bound(&self.upper_bound, field_entry.name())?;
                tantivy::query::RangeQuery::new_f64_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::Bool(_) => {
                return Err(InvalidQuery::RangeQueryNotSupportedForField {
                    value_type: "bool",
                    field_name: field_entry.name().to_string(),
                });
            }
            tantivy::schema::FieldType::Date(_) => {
                let lower_bound = convert_bound(&self.lower_bound, field_entry.name())?;
                let upper_bound = convert_bound(&self.upper_bound, field_entry.name())?;
                tantivy::query::RangeQuery::new_date_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::Facet(_) => {
                return Err(InvalidQuery::RangeQueryNotSupportedForField {
                    value_type: "facet",
                    field_name: field_entry.name().to_string(),
                });
            }
            tantivy::schema::FieldType::Bytes(_) => todo!(),
            tantivy::schema::FieldType::JsonObject(_) => todo!(),
            tantivy::schema::FieldType::IpAddr(_) => {
                let lower_bound = convert_bound(&self.lower_bound, field_entry.name())?;
                let upper_bound = convert_bound(&self.upper_bound, field_entry.name())?;
                tantivy::query::RangeQuery::new_ip_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
        }
        .into())
    }
}
