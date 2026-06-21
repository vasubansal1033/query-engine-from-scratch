# What we built: a query engine, step by step

A hands-on walkthrough of how a SQL engine works — from one Python function per query to a pipelined executor that parses SQL, builds an operator tree, and runs it row-by-row (then batch-by-batch). Sample data: 10k fake users in Parquet (`id`, `name`, `age`, `country`). Tests live under `tests/`.

---

## The arc

| Stage | File | Idea |
|-------|------|------|
| 1 | `stage1_handwritten.py` | Handwritten queries; load whole file, loop, return a list |
| 2 | `stage2_table_scan.py` | Stream one row at a time via `TableScan.next()` |
| 3 | `stage3_volcano.py` | Volcano model — operators pull from children |
| 4 | `stage4_volcano_2.py` | Add `Filter`; plan is `Projection → Filter → TableScan` |
| 5 | `stage5_volcano_3.py` | Parse SQL (sqloxide) + `execute_expr` interpreter |
| 6 | `stage6_volcano_4.py` | `build_plan` wires parse tree → operator tree |
| 7 | `stage7_aggregate.py` | Blocking `SumAggregate`; `run()` = plan + execute |
| 8 | `stage8_vectorized.py` | Same plan shape, 8192-row columnar batches + benchmark |

```
Stage 1  materialize everything
  → 2–4  volcano pipelining (row at a time)
  → 5    parse + interpret expressions (fused with execution)
  → 6–8  build_plan (plan) + next() loop (execute)
  → 8    vectorized operators
```

---

## Planning vs execution

From stage 6 on, the engine splits into two phases:

**Planning — `build_plan(sql)`** parses SQL, inspects the AST, and wires operators bottom-up (`TableScan` → optional `Filter` → `Projection` or `SumAggregate`). Expression ASTs are stored on operators as recipes. No data is read.

**Execution — `plan.next()` loop** pulls rows (or batches) through the tree. `execute_expr` runs here, once per row/batch, not during planning.

```python
plan = build_plan("select sum(age) from users where age > 35")
try:
    while (row := plan.next()) is not None:
        print(row)
finally:
    plan.close()
```

Stage 5's `run()` still fuses both: parse, materialize the table, evaluate — no reusable operator tree.

| Phase | When | Reads data? |
|-------|------|-------------|
| Planning | `build_plan(sql)` | No |
| Execution | `plan.next()` loop | Yes |

> **Note: this is not a real planner.** In Postgres, DuckDB, etc., planning is a separate subsystem — logical plan generation, cost-based optimization, rewrite rules, choice of join algorithm, index vs seq scan, and so on. Our `build_plan` is **static, hand-written logic**: it always produces the same operator chain for the query shapes we support (one table, optional `WHERE`, `SELECT` list or `SUM`). There is no plan generator abstraction, no cost model, no alternative plans to pick from. It *demonstrates* the plan-vs-execute split, but don't mistake it for how production engines plan queries.

---

## Stages in brief

**1 — Handwritten.** Five functions (scan, project, filter, avg, group-by). Clear, but every query is bespoke code and `read_table().to_pylist()` loads everything upfront.

**2 — Streaming read.** `iter_batches(1)` + `next()`/`close()`. You don't need all rows in memory to start.

**3 — Volcano.** `Operator` interface; `TableScan` and `Projection`. Parents pull from children; memory stays bounded.

**4 — Filter.** `WHERE` sits below projection (predicate runs on raw columns). Filter recursively calls `child.next()` until a match.

**5 — Expression interpreter.** `execute_expr` walks the parse tree: literals, column refs, `BinaryOp`, nested parens. `run()` still loads the full table — no `build_plan` yet.

**6 — `build_plan`.** SQL → operator tree. Operators call `execute_expr` at execution time. Full loop: parse → plan → pull rows.

**7 — Aggregates.** `SumAggregate` is *blocking*: first `next()` drains the child, returns one row, then `None`. Plan: `SumAggregate → Filter → TableScan`.

**8 — Vectorization.** Same `build_plan` wiring; operators process `{col: [values]}` batches of 8192. `execute_expr_vectorized` + boolean masks for filter. `benchmark()` compares row vs vector speed.

---

## Key concepts

- **Pipelining** — demand-driven `next()` calls; work flows up the operator tree.
- **Query plan** — SQL becomes a chain of operators; order matters (filter early, project late).
- **Expression evaluation** — AST interpreted per row/batch; powers projection, filter, and `SUM(age + 1)`.
- **Streaming vs blocking** — scan/filter/project stream; aggregates block until input is exhausted.
- **Vectorization** — batch columnar data to amortize Python overhead; orthogonal to pipelining.

---

## What we skipped

Joins, `GROUP BY`, sorting, transactions, optimization (indexes, predicate pushdown, join reordering), and a proper planner/optimizer layer. This is a teaching engine — enough to *feel* pipelining, plans, expression evaluation, aggregates, and vectorization in a few hundred lines of Python.
