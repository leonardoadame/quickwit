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
use tantivy::schema::Schema;

use crate::quickwit_query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::quickwit_query_ast::{IntoTantivyAst, QueryAst};
use crate::{parse_user_query, DefaultOperator, InvalidQuery};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UserTextQuery {
    pub user_text: String,
    // Set of search fields to search into for text not specifically
    // targetting a field.
    //
    // If None, the default search fields, as defined in the DocMapper
    // will be used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_fields: Option<Vec<String>>,
    pub default_operator: DefaultOperator,
}

impl UserTextQuery {
    /// Parse the user query to generate a structure QueryAST.
    pub fn parse_user_query(&self, default_search_fields: &[String]) -> anyhow::Result<QueryAst> {
        let search_fields = self
            .default_fields
            .as_ref()
            .map(|search_fields| &search_fields[..])
            .unwrap_or(default_search_fields);
        parse_user_query(&self.user_text, search_fields, self.default_operator)
    }
}

impl From<UserTextQuery> for QueryAst {
    fn from(user_text_query: UserTextQuery) -> Self {
        QueryAst::UserText(user_text_query)
    }
}

impl IntoTantivyAst for UserTextQuery {
    fn into_tantivy_ast_impl(
        &self,
        _schema: &Schema,
        _default_search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, crate::InvalidQuery> {
        Err(InvalidQuery::UserQueryNotParsed)
    }
}
