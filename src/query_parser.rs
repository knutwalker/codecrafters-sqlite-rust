use anyhow::{anyhow, Result};
use nom::{
    branch::alt,
    bytes::complete::{escaped_transform, tag, tag_no_case, take_while1},
    character::complete::{i64, multispace0, multispace1, none_of, one_of},
    combinator::{all_consuming, cut, map, opt, value},
    multi::separated_list1,
    number::complete::double,
    sequence::{delimited, preceded, terminated, tuple},
    Finish, IResult,
};

use crate::{
    operators::Op,
    planner::{Filter, LogicalOp, Plan},
    values::{TotalFloat, Val},
};

pub fn parse_query(q: &str) -> Result<Plan<'_>> {
    Ok(query(q)
        .finish()
        .map_err(|e| anyhow!("Invalid query: {}", e))?
        .1)
}

fn query(query: &str) -> IResult<&str, Plan<'_>> {
    all_consuming(map(
        tuple((
            terminated(tag_no_case("SELECT"), multispace1),
            alt((
                value(LogicalOp::Count, tag_no_case("COUNT(*)")),
                value(LogicalOp::SelectAll, tag("*")),
                map(
                    separated_list1(delimited(multispace0, tag(","), multispace0), name),
                    LogicalOp::Select,
                ),
            )),
            delimited(multispace1, tag_no_case("FROM"), multispace1),
            name,
            opt(preceded(
                delimited(multispace1, tag_no_case("WHERE"), multispace1),
                filter,
            )),
        )),
        |(_, op, _, table, filter)| Plan::new(op, table, filter),
    ))(query)
}

fn name(s: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_ascii_alphanumeric() || c == '_')(s)
}

fn filter(s: &str) -> IResult<&str, Filter<'_>> {
    map(
        tuple((name, delimited(multispace0, op, multispace0), expr)),
        |(column, op, value)| Filter::new(op, column, value),
    )(s)
}

fn op(s: &str) -> IResult<&str, Op> {
    alt((
        value(Op::Eq, tuple((tag("="), opt(tag("="))))),
        value(Op::Ne, alt((tag("!="), tag("<>")))),
        value(Op::Le, tag("<=")),
        value(Op::Lt, tag("<")),
        value(Op::Ge, tag(">=")),
        value(Op::Gt, tag(">")),
    ))(s)
}

fn expr(s: &str) -> IResult<&str, Val> {
    alt((
        value(Val::Null, tag_no_case("NULL")),
        value(Val::Bool(true), tag_no_case("TRUE")),
        value(Val::Bool(false), tag_no_case("FALSE")),
        map(i64, Val::Int),
        map(double, |o| Val::Float(TotalFloat(o))),
        map(string, Val::Text),
    ))(s)
}

fn string(s: &str) -> IResult<&str, String> {
    alt((
        preceded(
            tag("\""),
            cut(terminated(
                escaped_transform(none_of(r#"\""#), '\\', one_of(r#""n\"#)),
                tag("\""),
            )),
        ),
        preceded(
            tag("'"),
            cut(terminated(
                escaped_transform(none_of(r#"\'"#), '\\', one_of(r#"'n\"#)),
                tag("'"),
            )),
        ),
    ))(s)
}
