import type { AnyColumn, GetColumnData } from "drizzle-orm"

export type ComparisonOperator = "eq" | "gt" | "gte" | "lt" | "lte" | "ne"

export type PredicateInput =
  | {
      readonly column: AnyColumn
      readonly kind: "comparison"
      readonly operator: ComparisonOperator
      readonly value: unknown
    }
  | {
      readonly column: AnyColumn
      readonly kind: "in"
      readonly values: readonly unknown[]
    }
  | { readonly column: AnyColumn; readonly kind: "is-null"; readonly negated: boolean }
  | {
      readonly kind: "logical"
      readonly operator: "and" | "or"
      readonly terms: readonly PredicateInput[]
    }

function comparison<TColumn extends AnyColumn>(
  operator: ComparisonOperator,
  column: TColumn,
  value: GetColumnData<TColumn, "query">,
): PredicateInput {
  return Object.freeze({ column, kind: "comparison", operator, value })
}

export function eq<TColumn extends AnyColumn>(
  column: TColumn,
  value: GetColumnData<TColumn, "query">,
): PredicateInput {
  return comparison("eq", column, value)
}

export function ne<TColumn extends AnyColumn>(
  column: TColumn,
  value: GetColumnData<TColumn, "query">,
): PredicateInput {
  return comparison("ne", column, value)
}

export function gt<TColumn extends AnyColumn>(
  column: TColumn,
  value: GetColumnData<TColumn, "query">,
): PredicateInput {
  return comparison("gt", column, value)
}

export function gte<TColumn extends AnyColumn>(
  column: TColumn,
  value: GetColumnData<TColumn, "query">,
): PredicateInput {
  return comparison("gte", column, value)
}

export function lt<TColumn extends AnyColumn>(
  column: TColumn,
  value: GetColumnData<TColumn, "query">,
): PredicateInput {
  return comparison("lt", column, value)
}

export function lte<TColumn extends AnyColumn>(
  column: TColumn,
  value: GetColumnData<TColumn, "query">,
): PredicateInput {
  return comparison("lte", column, value)
}

export function inArray<TColumn extends AnyColumn>(
  column: TColumn,
  values: readonly GetColumnData<TColumn, "query">[],
): PredicateInput {
  return Object.freeze({ column, kind: "in", values: Object.freeze([...values]) })
}

export function isNull(column: AnyColumn): PredicateInput {
  return Object.freeze({ column, kind: "is-null", negated: false })
}

export function isNotNull(column: AnyColumn): PredicateInput {
  return Object.freeze({ column, kind: "is-null", negated: true })
}

function logical(operator: "and" | "or", terms: readonly PredicateInput[]): PredicateInput {
  return Object.freeze({ kind: "logical", operator, terms: Object.freeze([...terms]) })
}

export function and(...terms: readonly PredicateInput[]): PredicateInput {
  return logical("and", terms)
}

export function or(...terms: readonly PredicateInput[]): PredicateInput {
  return logical("or", terms)
}
