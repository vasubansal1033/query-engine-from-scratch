from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq
import string

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"

def execute_binary_op(operation: str, left_operand: int, right_operand: int) -> int:
    if operation == "Plus":
        return left_operand + right_operand
    elif operation == "Multiply":
        return left_operand * right_operand
    elif operation == "Minus":
        return left_operand - right_operand
    elif operation == "Divide":
        if right_operand == 0:
            raise Exception(f"division by zero: {left_operand} / {right_operand}")
        return left_operand / right_operand
    elif operation == "Gt":
        if left_operand > right_operand:
            return True
        else:
            return False
    elif operation == "GtEq":
        if left_operand >= right_operand:
            return True
        else:
            return False
    elif operation == "Lt":
        if left_operand < right_operand:
            return True
        else:
            return False
    elif operation == "LtEq":
        if left_operand <= right_operand:
            return True
        else:
            return False
    elif operation == "Eq":
        if left_operand == right_operand:
            return True
        else:
            return False
    elif operation == "NotEq":
        if left_operand != right_operand:
            return True
        else:
            return False
    else:
        raise Exception(f"unknown binary op: {operation}")

def parse_number_string(value: string):
    try:
        # try converting to int
        return int(value)
    except ValueError:
        # if that fails, try converting to float
        return float(value)

def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    if "Value" in expr:
        value = expr["Value"]["value"]
        if "Number" in value:
            value = value["Number"][0]
            return parse_number_string(value)
        elif "SingleQuotedString" in value:
            # handle quoted string and number string
            if type(value["SingleQuotedString"]) == list:
                return value["SingleQuotedString"][0]
            else:
                return value["SingleQuotedString"]
        elif "Boolean" in value:
            return value["Boolean"]
        elif "Null" in value:
            return None
    elif "Identifier" in expr:
        column_name = expr["Identifier"]["value"]
        return row[column_name]
    elif "Nested" in expr:
        nested = expr["Nested"]
        return execute_expr(row, nested)
    elif "BinaryOp" in expr:
        binary_op = expr["BinaryOp"]
        operation = binary_op["op"]

        left_operand = execute_expr(row, binary_op["left"])
        right_operand = execute_expr(row, binary_op["right"])
        return execute_binary_op(
            operation,
            left_operand,
            right_operand
        )

    raise Exception(f"unknown expr: {expr}")


class Operator:
    def next(self) -> dict[str, Any] | None:
        raise NotImplementedError(f"next not implemented for {self}")

    def close(self):
        raise NotImplementedError(f"close not implemented for {self}")


class TableScan(Operator):
    def __init__(self, filename: str = FILE_NAME):
        super().__init__()
        self._file = pq.ParquetFile(filename)
        self._iter = self._file.iter_batches(1)

    def next(self) -> dict[str, Any] | None:
        maybe_rows = next(self._iter, None)
        if not maybe_rows:
            return None
        return maybe_rows.to_pylist()[0]

    def close(self):
        self._file.close()

class Filter(Operator):
    def __init__(self, expr: "Expr", child: Operator) -> None:
        super().__init__()
        self._child = child
        self._expr = expr

    def next(self) -> dict[str, Any] | None:
        maybe_row = self._child.next()
        if not maybe_row:
            return None

        if execute_expr(maybe_row, self._expr) <= 0:
            return self.next()

        return maybe_row

    def close(self):
        self._child.close()


# SUM is our first *aggregate*: it folds many rows into a single value.
#
# Every operator so far has been streaming -- one row in, (maybe) one row out.
# An aggregate can't do that: to know SUM(age) it must see EVERY row first.
# So it's a *blocking* operator. On the first next() call it drains its child
# completely, accumulating the running total, then emits one result row. Every
# call after that returns None (there's only one row of output).
#
# We evaluate the inner expression per row with the same execute_expr, so
# SUM(age + 1) or SUM(age * 2) work too, not just SUM(age).
class SumAggregate(Operator):
    def __init__(self, alias: str, arg: "Expr", child: Operator) -> None:
        super().__init__()
        self._alias = alias
        self._arg = arg
        self._child = child
        self._done = False

    def next(self) -> dict[str, Any] | None:
        if self._done:
            return None
        
        self._done = True

        sum = 0
        while maybe_row := self._child.next():
            sum += execute_expr(maybe_row, self._arg)
 
        return {
            self._alias: sum
        }

    def close(self):
        self._child.close()


def build_plan(sql: str) -> Operator:
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    plan: Operator = TableScan()

    if tree["selection"] is not None:
        plan = Filter(tree["selection"], plan)

    # No GROUP BY yet: we assume the SELECT is a single aggregate like
    # `sum(age)`. Pull the function name, its argument expression, and an
    # alias for the output column.
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

    # Unwrap the single argument: sum(<arg>) -> <arg>.
    arg = func["args"]["List"]["args"][0]["Unnamed"]["Expr"]
    alias = alias or "sum"

    return SumAggregate(alias, arg, plan)


def run(sql: str) -> None:
    plan = build_plan(sql)
    print(f"=== {sql}")
    try:
        while (row := plan.next()) is not None:
            print(row)
    finally:
        plan.close()


if __name__ == "__main__":
    run("select sum(age) from users")
    run("select sum(age) from users where age > 35")
    run("select sum(age + 1) from users")
