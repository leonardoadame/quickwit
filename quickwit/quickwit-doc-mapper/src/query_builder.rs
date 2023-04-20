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

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;

use anyhow::{bail, Context};
use quickwit_proto::SearchRequest;
use quickwit_query::quickwit_query_ast::{QueryAst, QueryAstVisitor, RangeQuery};
use tantivy::query::Query;
use tantivy::query_grammar::{UserInputAst, UserInputLeaf, UserInputLiteral};
use tantivy::schema::{Field, FieldEntry, FieldType, Schema};

use crate::{QueryParserError, WarmupInfo};

#[derive(Default)]
struct RangeQueryFields {
    range_query_field_names: HashSet<String>,
}

impl<'a> QueryAstVisitor<'a> for RangeQueryFields {
    type Err = Infallible;

    fn visit_range(&mut self, range_query: &'a RangeQuery) -> Result<(), Infallible> {
        self.range_query_field_names
            .insert(range_query.field.to_string());
        Ok(())
    }
}

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    request: &SearchRequest,
    schema: Schema,
    with_validation: bool,
) -> Result<(Box<dyn Query>, WarmupInfo), QueryParserError> {
    let query_ast: QueryAst = serde_json::from_str(&request.query_ast)?;
    let mut range_query_fields = RangeQueryFields::default();
    range_query_fields.visit(&query_ast).unwrap();
    let fast_field_names: HashSet<String> = range_query_fields.range_query_field_names;

    // TODO identify if a default field is needed and missing.

    // TODO
    // validate requested snippet fields:
    // - snippet fields must be in the query
    // - snippet fields must be text fields.

    // resolve the query using the default fields given in the query if any, or using hte ones in
    // the docmapper. -----
    // validate sort by fields.
    // parse phrase query if needed.
    // extract term set

    // validate_requested_snippet_fields(&schema, request, &user_input_ast, default_field_names)?;

    if let Some(sort_by_field) = &request.sort_by_field {
        validate_sort_by_field(sort_by_field, &schema)?;
    }

    let query = query_ast.build_tantivy_query(&schema, with_validation)?;

    let term_set_query_fields = extract_term_set_query_fields(&query_ast);

    let mut terms_grouped_by_field: HashMap<Field, HashMap<_, bool>> = Default::default();
    query.query_terms(&mut |term, need_position| {
        let field = term.field();
        *terms_grouped_by_field
            .entry(field)
            .or_default()
            .entry(term.clone())
            .or_default() |= need_position;
    });

    let warmup_info = WarmupInfo {
        term_dict_field_names: term_set_query_fields.clone(),
        posting_field_names: term_set_query_fields,
        terms_grouped_by_field,
        fast_field_names,
        ..WarmupInfo::default()
    };

    Ok((query, warmup_info))
}

#[derive(Default)]
struct ExtractTermSetFields {
    term_dict_fields_to_warm_up: HashSet<String>,
}

impl<'a> QueryAstVisitor<'a> for ExtractTermSetFields {
    type Err = anyhow::Error;

    fn visit(&mut self, query_ast: &'a QueryAst) -> Result<(), Self::Err> {
        if let QueryAst::TermSet(term_set) = query_ast {
            for field in term_set.terms_per_field.keys() {
                self.term_dict_fields_to_warm_up.insert(field.to_string());
            }
        }
        Ok(())
    }
}

fn extract_term_set_query_fields(query_ast: &QueryAst) -> HashSet<String> {
    let mut visitor = ExtractTermSetFields::default();
    visitor
        .visit(query_ast)
        .expect("Extracting term set queries's field should never return an error.");
    visitor.term_dict_fields_to_warm_up
}

pub(crate) fn validate_sort_by_field(field_name: &str, schema: &Schema) -> anyhow::Result<()> {
    if field_name == "_score" {
        return Ok(());
    }
    let sort_by_field = schema
        .get_field(field_name)
        .with_context(|| format!("Unknown sort by field: `{field_name}`"))?;
    let sort_by_field_entry = schema.get_field_entry(sort_by_field);

    if matches!(sort_by_field_entry.field_type(), FieldType::Str(_)) {
        bail!(
            "Sort by field on type text is currently not supported `{}`.",
            field_name
        )
    }
    if !sort_by_field_entry.is_fast() {
        bail!(
            "Sort by field must be a fast field, please add the fast property to your field `{}`.",
            field_name
        )
    }

    Ok(())
}

fn validate_sort_by_score(
    schema: &Schema,
    search_fields_opt: Option<&Vec<Field>>,
) -> anyhow::Result<()> {
    if let Some(fields) = search_fields_opt {
        for field in fields {
            if !schema.get_field_entry(*field).has_fieldnorms() {
                bail!(
                    "Fieldnorms for field `{}` is missing. Fieldnorms must be stored for the \
                     field to compute the BM25 score of the documents.",
                    schema.get_field_name(*field)
                )
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use quickwit_proto::{query_string, SearchRequest};
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};

    use super::build_query;
    use crate::{DYNAMIC_FIELD_NAME, SOURCE_FIELD_NAME};

    enum TestExpectation {
        Err(&'static str),
        Ok(&'static str),
    }

    fn make_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("desc", TEXT | STORED);
        schema_builder.add_text_field("server.name", TEXT | STORED);
        schema_builder.add_text_field("server.mem", TEXT);
        schema_builder.add_bool_field("server.running", FAST | STORED | INDEXED);
        schema_builder.add_text_field(SOURCE_FIELD_NAME, TEXT);
        schema_builder.add_json_field(DYNAMIC_FIELD_NAME, TEXT);
        schema_builder.add_ip_addr_field("ip", FAST | STORED);
        schema_builder.add_ip_addr_field("ips", FAST);
        schema_builder.add_ip_addr_field("ip_notff", STORED);
        schema_builder.add_date_field("dt", FAST);
        schema_builder.add_u64_field("u64_fast", FAST | STORED);
        schema_builder.add_i64_field("i64_fast", FAST | STORED);
        schema_builder.add_f64_field("f64_fast", FAST | STORED);
        schema_builder.build()
    }

    #[track_caller]
    fn check_build_query(user_query: &str, search_fields: Vec<String>, expected: TestExpectation) {
        let request = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query_ast: quickwit_proto::query_string(user_query).unwrap(),
            search_fields,
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
        };

        let query_result = build_query(&request, make_schema(), true);
        match expected {
            TestExpectation::Err(sub_str) => {
                assert!(
                    query_result.is_err(),
                    "Expected error {sub_str}, but got a success on query parsing {user_query}"
                );
                let query_err = query_result.err().unwrap();
                let query_err_msg = query_err.to_string();
                assert!(
                    query_err_msg.contains(sub_str),
                    "Query error received is {query_err_msg}. It should contain {sub_str}"
                );
            }
            TestExpectation::Ok(sub_str) => {
                assert!(
                    query_result.is_ok(),
                    "Expected a success when parsing {sub_str}, but got an error: {:?}",
                    query_result.err()
                );
                let (query, _) = query_result.unwrap();
                assert!(
                    format!("{query:?}").contains(sub_str),
                    "Error query parsing {query:?} should contain {sub_str}"
                );
            }
        }
    }

    #[test]
    fn test_build_query() {
        check_build_query("*", Vec::new(), TestExpectation::Ok("All"));
        check_build_query(
            "foo:bar",
            Vec::new(),
            TestExpectation::Err("Field does not exist: 'foo'"),
        );
        check_build_query(
            "server.type:hpc server.mem:4GB",
            Vec::new(),
            TestExpectation::Err("Field does not exist: 'server.type'"),
        );
        check_build_query(
            "title:[a TO b]",
            Vec::new(),
            TestExpectation::Err(
                "Field `title` is of type `Str`. Range queries are only supported on boolean, \
                 datetime, IP, and numeric fields",
            ),
        );
        check_build_query(
            "title:{a TO b} desc:foo",
            Vec::new(),
            TestExpectation::Err(
                "Field `title` is of type `Str`. Range queries are only supported on boolean, \
                 datetime, IP, and numeric fields",
            ),
        );
        check_build_query(
            "title:>foo",
            Vec::new(),
            TestExpectation::Err(
                "Field `title` is of type `Str`. Range queries are only supported on boolean, \
                 datetime, IP, and numeric fields",
            ),
        );
        check_build_query(
            "title:foo desc:bar _source:baz",
            Vec::new(),
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query(
            "title:foo desc:bar",
            vec!["url".to_string()],
            TestExpectation::Err("field does not exist: 'url'"),
        );
        check_build_query(
            "server.name:\".bar:\" server.mem:4GB",
            vec!["server.name".to_string()],
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query(
            "server.name:\"for.bar:b\" server.mem:4GB",
            Vec::new(),
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query(
            "foo",
            Vec::new(),
            TestExpectation::Err("No default field declared and no field specified in query."),
        );
        check_build_query(
            "bar",
            Vec::new(),
            TestExpectation::Err("No default field declared and no field specified in query."),
        );
        check_build_query(
            "title:hello AND (Jane OR desc:world)",
            Vec::new(),
            TestExpectation::Err("No default field declared and no field specified in query."),
        );
        check_build_query(
            "server.running:true",
            Vec::new(),
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query(
            "title: IN [hello]",
            Vec::new(),
            TestExpectation::Ok("TermSetQuery"),
        );
        check_build_query(
            "IN [hello]",
            Vec::new(),
            TestExpectation::Err("Unsupported query: Set query need to target a specific field."),
        );
    }

    #[test]
    fn test_datetime_range_query() {
        check_build_query(
            "dt:[2023-01-10T15:13:35Z TO 2023-01-10T15:13:40Z]",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"dt\", value_type: Date"),
        );
        check_build_query(
            "dt:<2023-01-10T15:13:35Z",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"dt\", value_type: Date"),
        );
    }

    #[test]
    fn test_ip_range_query() {
        check_build_query(
            "ip:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { field: \"ip\", value_type: IpAddr, left_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 1, 1, 1])",
            ),
        );
        check_build_query(
            "ip:>127.0.0.1",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { field: \"ip\", value_type: IpAddr, left_bound: Excluded([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Unbounded",
            ),
        );
    }

    #[test]
    fn test_f64_range_query() {
        check_build_query(
            "f64_fast:[7.7 TO 77.7]",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"f64_fast\", value_type: F64"),
        );
        check_build_query(
            "f64_fast:>7",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"f64_fast\", value_type: F64"),
        );
    }

    #[test]
    fn test_i64_range_query() {
        check_build_query(
            "i64_fast:[-7 TO 77]",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"i64_fast\", value_type: I64"),
        );
        check_build_query(
            "i64_fast:>7",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"i64_fast\", value_type: I64"),
        );
    }

    #[test]
    fn test_u64_range_query() {
        check_build_query(
            "u64_fast:[7 TO 77]",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"u64_fast\", value_type: U64"),
        );
        check_build_query(
            "u64_fast:>7",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"u64_fast\", value_type: U64"),
        );
    }

    #[test]
    fn test_range_query_ip_fields_multivalued() {
        check_build_query(
            "ips:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { field: \"ips\", value_type: IpAddr, left_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 1, 1, 1])",
            ),
        );
    }

    #[test]
    fn test_range_query_no_fast_field() {
        check_build_query(
            "ip_notff:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Err("`ip_notff` is not a fast field"),
        );
    }

    #[track_caller]
    fn check_snippet_fields_validation(
        query_str: &str,
        search_fields: Vec<String>,
        snippet_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let schema = make_schema();
        let request = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query_ast: query_string(query_str).unwrap(),
            search_fields,
            snippet_fields,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
        };
        todo!();
        // let user_input_ast = tantivy::query_grammar::parse_query(request.query.as_ref().unwrap())
        //     .map_err(|_| QueryParserError::SyntaxError(request.query.clone().unwrap()))
        //     .unwrap();
        // let default_field_names =
        //     default_search_fields.unwrap_or_else(|| vec!["title".to_string(),
        // "desc".to_string()]);

        // validate_requested_snippet_fields(&schema, &request, &user_input_ast,
        // &default_field_names)
    }

    #[test]
    fn test_build_query_not_bool_should_fail() {
        check_build_query(
            "server.running:not a bool",
            Vec::new(),
            TestExpectation::Err("Expected a `bool` search value for field `server.running`"),
        );
    }

    #[test]
    fn test_validate_requested_snippet_fields() {
        let validation_result =
            check_snippet_fields_validation("foo", Vec::new(), vec!["desc".to_string()]);
        assert!(validation_result.is_ok());
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec!["foo".to_string()],
            vec!["desc".to_string()],
        );
        assert!(validation_result.is_ok());
        let validation_result =
            check_snippet_fields_validation("desc:foo", Vec::new(), vec!["desc".to_string()]);
        assert!(validation_result.is_ok());
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec!["desc".to_string()],
            vec!["desc".to_string()],
        );
        assert!(validation_result.is_ok());

        // Non existing field
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec!["summary".to_string()],
            vec!["summary".to_string()],
        );
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The field does not exist: 'summary'"
        );
        // Unknown searched field
        let validation_result =
            check_snippet_fields_validation("foo", Vec::new(), vec!["server.name".to_string()]);
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The snippet field `server.name` should be a default search field or appear in the \
             query."
        );
        // Search field in query
        let validation_result = check_snippet_fields_validation(
            "server.name:foo",
            Vec::new(),
            vec!["server.name".to_string()],
        );
        assert!(validation_result.is_ok());
        // Not stored field
        let validation_result =
            check_snippet_fields_validation("foo", Vec::new(), vec!["title".to_string()]);
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The snippet field `title` must be stored."
        );
        // Non text field
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec!["server.running".to_string()],
            vec!["server.running".to_string()],
        );
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The snippet field `server.running` must be of type `Str`, got `Bool`."
        );
    }

    #[test]
    fn test_build_query_warmup_info() -> anyhow::Result<()> {
        let request_with_set = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query_ast: query_string("title: IN [hello]").unwrap(),
            search_fields: Vec::new(),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
        };
        let request_without_set = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query_ast: query_string("title:hello").unwrap(),
            search_fields: Vec::new(),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
        };

        let (_, warmup_info) = build_query(&request_with_set, make_schema(), true)?;
        assert_eq!(warmup_info.term_dict_field_names.len(), 1);
        assert_eq!(warmup_info.posting_field_names.len(), 1);
        assert!(warmup_info.term_dict_field_names.contains("title"));
        assert!(warmup_info.posting_field_names.contains("title"));

        let (_, warmup_info) = build_query(&request_without_set, make_schema(), true)?;
        assert!(warmup_info.term_dict_field_names.is_empty());
        assert!(warmup_info.posting_field_names.is_empty());

        Ok(())
    }
}
