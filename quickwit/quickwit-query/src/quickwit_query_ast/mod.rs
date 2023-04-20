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

use serde::{Deserialize, Serialize};
use tantivy::query::BoostQuery;
use tantivy::schema::Schema;

mod bool_query;
mod phrase_query;
mod range_query;
mod tantivy_query_ast;
mod term_query;
mod term_set_query;
mod user_text_query;
pub(crate) mod utils;
mod visitor;

pub use bool_query::BoolQuery;
pub use phrase_query::PhraseQuery;
pub use range_query::RangeQuery;
use tantivy_query_ast::TantivyQueryAst;
pub use term_query::TermQuery;
pub use term_set_query::TermSetQuery;
pub use user_text_query::UserTextQuery;
pub use visitor::QueryAstVisitor;

use crate::{InvalidQuery, NotNaNf32};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum QueryAst {
    Bool(BoolQuery),
    Term(TermQuery),
    TermSet(TermSetQuery),
    Phrase(PhraseQuery),
    Range(RangeQuery),
    UserText(UserTextQuery),
    MatchAll,
    MatchNone,
    Boost {
        underlying: Box<QueryAst>,
        boost: NotNaNf32,
    },
}

trait IntoTantivyAst {
    /// Transforms a query Ast node into a TantivyQueryAst.
    ///
    /// This function is supposed to return an error if it detects a problem in the schema.
    /// It can call `into_tantivy_ast_call_me` but should never call `into_tantivy_ast_impl`.
    fn into_tantivy_ast_impl(
        &self,
        schema: &Schema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery>;

    /// This method is meant to be called, but should never be overloaded.
    fn into_tantivy_ast_call(
        &self,
        schema: &Schema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let tantivy_ast_res = self.into_tantivy_ast_impl(schema, search_fields, with_validation);
        if !with_validation && tantivy_ast_res.is_err() {
            return Ok(TantivyQueryAst::match_none());
        }
        tantivy_ast_res
    }
}

impl IntoTantivyAst for QueryAst {
    fn into_tantivy_ast_impl(
        &self,
        schema: &Schema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        match self {
            QueryAst::Bool(bool_query) => {
                bool_query.into_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::Term(term_query) => {
                term_query.into_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::Range(range_query) => {
                range_query.into_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::MatchAll => Ok(TantivyQueryAst::match_all()),
            QueryAst::MatchNone => Ok(TantivyQueryAst::match_none()),
            QueryAst::Boost { boost, underlying } => {
                let underlying =
                    underlying.into_tantivy_ast_call(schema, search_fields, with_validation)?;
                let boost_query = BoostQuery::new(underlying.into(), (*boost).into());
                Ok(boost_query.into())
            }
            QueryAst::TermSet(term_set) => {
                term_set.into_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::Phrase(phrase_query) => {
                phrase_query.into_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::UserText(user_text_query) => {
                user_text_query.into_tantivy_ast_call(schema, search_fields, with_validation)
            }
        }
    }
}

impl QueryAst {
    pub fn build_tantivy_query(
        &self,
        schema: &Schema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<Box<dyn crate::TantivyQuery>, InvalidQuery> {
        let tantivy_query_ast =
            self.into_tantivy_ast_call(schema, search_fields, with_validation)?;
        Ok(tantivy_query_ast.simplify().into())
    }
}

fn parse_user_query_in_asts(
    asts: Vec<QueryAst>,
    default_search_fields: &[String],
) -> anyhow::Result<Vec<QueryAst>> {
    asts.into_iter()
        .map(|ast| parse_user_query(ast, default_search_fields))
        .collect::<anyhow::Result<_>>()
}

pub fn parse_user_query(
    query_ast: QueryAst,
    default_search_fields: &[String],
) -> anyhow::Result<QueryAst> {
    match query_ast {
        QueryAst::Bool(BoolQuery {
            must,
            must_not,
            should,
            filter,
        }) => {
            let must = parse_user_query_in_asts(must, default_search_fields)?;
            let must_not = parse_user_query_in_asts(must_not, default_search_fields)?;
            let should = parse_user_query_in_asts(should, default_search_fields)?;
            let filter = parse_user_query_in_asts(filter, default_search_fields)?;
            Ok(BoolQuery {
                must,
                must_not,
                should,
                filter,
            }
            .into())
        }
        ast @ QueryAst::Term(_)
        | ast @ QueryAst::TermSet(_)
        | ast @ QueryAst::Phrase(_)
        | ast @ QueryAst::MatchAll
        | ast @ QueryAst::MatchNone
        | ast @ QueryAst::Range(_) => Ok(ast),
        QueryAst::UserText(user_text_query) => {
            user_text_query.parse_user_query(default_search_fields)
        }
        QueryAst::Boost { underlying, boost } => {
            let underlying = parse_user_query(*underlying, default_search_fields)?;
            Ok(QueryAst::Boost {
                underlying: Box::new(underlying),
                boost,
            })
        }
    }
}
