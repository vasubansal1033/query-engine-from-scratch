import time
from typing import Any, TYPE_CHECKING
from unittest import result

from sqloxide import parse_sql
import pyarrow.parquet as pq

from engine.stage7_aggregate import build_plan as build_row_plan

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"

BATCH_SIZE = 8192


Batch = dict[str, list]

def execute_binary_op_vectorized(operation: str, left_operand: list[int], right_operand: list[int], batch_size: int) -> list[int]:
    if operation == "Plus":
        return [left_operand[i] + right_operand[i] for i in range(batch_size)]
    elif operation == "Multiply":
        return [left_operand[i] * right_operand[i] for i in range(batch_size)]
    elif operation == "Minus":
        return [left_operand[i] - right_operand[i] for i in range(batch_size)]
    elif operation == "Divide":
        return [left_operand[i] / right_operand[i] for i in range(batch_size)]
    elif operation == "Gt":
        return [left_operand[i] > right_operand[i] for i in range(batch_size)]
    elif operation == "GtEq":
        return [left_operand[i] >= right_operand[i] for i in range(batch_size)]
    elif operation == "Lt":
        return [left_operand[i] < right_operand[i] for i in range(batch_size)]
    elif operation == "LtEq":
        return [left_operand[i] <= right_operand[i] for i in range(batch_size)]
    elif operation == "Eq":
        return [left_operand[i] == right_operand[i] for i in range(batch_size)]
    elif operation == "NotEq":
        return [left_operand[i] != right_operand[i] for i in range(batch_size)]
    else:
        raise Exception(f"unknown binary op: {operation}")

def parse_number_string(value: list[str]):
    try:
        # try converting to int
        return int(value)
    except ValueError:
        # if that fails, try converting to float
        return float(value)

def execute_expr_vectorized(batch: Batch, expr: "Expr") -> list:
    batch_size = len(next(iter(batch.values())))

    if "Value" in expr:
        scalar = None
        value = expr["Value"]["value"]
        if "Number" in value:
            value = value["Number"][0]
            scalar = parse_number_string(value)
        elif "SingleQuotedString" in value:
            # handle quoted string and number string
            if type(value["SingleQuotedString"]) == list:
                scalar = value["SingleQuotedString"][0]
            else:
                scalar = value["SingleQuotedString"]
        elif "Boolean" in value:
            scalar = value["Boolean"]
        elif "Null" in value:
            scalar = None

        # broadcast the scalar to the batch size
        return [scalar] * batch_size
    elif "Identifier" in expr:
        column_name = expr["Identifier"]["value"]
        return batch[column_name]
    elif "Nested" in expr:
        nested = expr["Nested"]
        return execute_expr_vectorized(batch, nested)
    elif "BinaryOp" in expr:
        binary_op = expr["BinaryOp"]
        operation = binary_op["op"]

        left_operand = execute_expr_vectorized(batch, binary_op["left"])
        right_operand = execute_expr_vectorized(batch, binary_op["right"])
        return execute_binary_op_vectorized(
            operation,
            left_operand,
            right_operand,
            batch_size
        )

    raise Exception(f"unknown expr: {expr}")

class Operator:
    def next(self) -> Batch | None:
        raise NotImplementedError(f"next not implemented for {self}")

    def close(self):
        raise NotImplementedError(f"close not implemented for {self}")


class TableScan(Operator):
    def __init__(self, filename: str):
        super().__init__()
        self._file = pq.ParquetFile(filename)
        # Read BATCH_SIZE rows at a time, as columns.
        self._iter = self._file.iter_batches(BATCH_SIZE)

    def next(self) -> Batch | None:
        record_batch = next(self._iter, None)
        if record_batch is None:
            return None
        # to_pydict() hands us {col_name: [values...]} -- columnar already.
        return record_batch.to_pydict()

    def close(self):
        self._file.close()

class Filter(Operator):
    def __init__(self, expr: "Expr", child: Operator) -> None:
        super().__init__()
        self._child = child
        self._expr = expr

    def next(self) -> dict[str, Any] | None:
        maybe_batch = self._child.next()
        if not maybe_batch:
            return None

        # mask is a list of booleans
        mask = execute_expr_vectorized(maybe_batch, self._expr)

        # filter the batch based on the boolean mask
        return {
            col: [v for v, keep in zip(values, mask) if keep]
            for col, values in maybe_batch.items()
        }

    def close(self):
        self._child.close()


class SumAggregate(Operator):
    def __init__(self, alias: str, arg: "Expr", child: Operator) -> None:
        super().__init__()
        self._alias = alias
        self._arg = arg
        self._child = child
        self._done = False

    def next(self) -> Batch | None:
        if self._done:
            return None
        
        self._done = True

        result = 0
        while maybe_batch := self._child.next():
            result += sum(execute_expr_vectorized(maybe_batch, self._arg))
 
        return {
            self._alias: [result]
        }

    def close(self):
        self._child.close()


def build_plan(sql: str) -> Operator:
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    plan: Operator = TableScan(FILE_NAME)

    if tree["selection"] is not None:
        plan = Filter(tree["selection"], plan)

    item = tree["projection"][0]
    if "ExprWithAlias" in item:
        func = item["ExprWithAlias"]["expr"]["Function"]
        alias = item["ExprWithAlias"]["alias"]["value"]
    else:
        func = item["UnnamedExpr"]["Function"]
        alias = None

    name = func["name"][0]["Identifier"]["value"].lower()
    if name != "sum":
        raise Exception(f"only sum() is supported in this stage, got {name!r}")

    arg = func["args"]["List"]["args"][0]["Unnamed"]["Expr"]
    return SumAggregate(alias or "sum", arg, plan)


def run_vectorized(sql: str):
    plan = build_plan(sql)
    try:
        result = None
        while (batch := plan.next()) is not None:
            result = {k: v[0] for k, v in batch.items()}
        return result
    finally:
        plan.close()


def run_row(sql: str):
    plan = build_row_plan(sql)
    try:
        result = None
        while (row := plan.next()) is not None:
            result = row
        return result
    finally:
        plan.close()


def _time(fn, sql: str, repeats: int) -> float:
    start = time.perf_counter()
    for _ in range(repeats):
        fn(sql)
    return (time.perf_counter() - start) / repeats


def benchmark(sql: str, repeats: int = 20) -> None:
    row_result = run_row(sql)
    vec_result = run_vectorized(sql)
    assert row_result == vec_result, (row_result, vec_result)

    row_time = _time(run_row, sql, repeats)
    vec_time = _time(run_vectorized, sql, repeats)

    print(f"=== {sql}")
    print(f"  result          : {vec_result}")
    print(f"  row    (batch=1) : {row_time * 1000:8.3f} ms")
    print(f"  vector (batch={BATCH_SIZE}): {vec_time * 1000:8.3f} ms")
    print(f"  speedup          : {row_time / vec_time:6.2f}x\n")


if __name__ == "__main__":
    benchmark("select sum(age) from users")
    benchmark("select sum(age) from users where age > 35")
    benchmark("select sum(age + 1) from users")
