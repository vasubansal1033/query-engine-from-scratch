import string
from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr


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
    def __init__(self, filename: str):
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


class Projection(Operator):
    def __init__(self, exprs: dict[str, "Expr"], child: Operator) -> None:
        super().__init__()
        self._child = child
        self._exprs = exprs

    def next(self) -> dict[str, Any] | None:
        maybe_row = self._child.next()
        if not maybe_row:
            return None
        
        result = {}
        for key, expr in self._exprs.items():

            result[key] = execute_expr(maybe_row, expr)
        return result

    def close(self):
        self._child.close()

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


def build_plan(sql: str):
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    # Bottom of the plan: read every row from the table.
    plan: Operator = TableScan("data/sample_1.parquet")

    # WHERE goes *below* the projection, on the raw rows -- the predicate
    # `age > 35` references columns before projection may rename or drop them.
    # `selection` is None when there's no WHERE clause, so we skip Filter then.
    if tree["selection"] is not None:
        plan = Filter(tree["selection"], plan)

    # SELECT list: unwrap each item into an (alias -> expr) pair, the shape
    # Projection wants. Two shapes come out of the parser:
    #   `name`            -> {"UnnamedExpr": <expr>}
    #   `age + 1 as age`  -> {"ExprWithAlias": {"expr": <expr>, "alias": ...}}
    exprs: dict[str, "Expr"] = {}
    for item in tree["projection"]:
        if "UnnamedExpr" in item:
            expr = item["UnnamedExpr"]
            alias = expr.get("Identifier", {}).get("value", "?column?")
        else:
            expr = item["ExprWithAlias"]["expr"]
            alias = item["ExprWithAlias"]["alias"]["value"]
        exprs[alias] = expr
    plan = Projection(exprs, plan)

    num_rows = 0
    row = plan.next()
    while row is not None:
        print(row)
        num_rows += 1
        row = plan.next()
    print(num_rows)


if __name__ == "__main__":
    # We're now running actual SQL: parse it, build a plan of operators, and
    # the operators lean entirely on execute_expr -- nothing is hardcoded.
    build_plan("select name, age + 1 as age from users")

    build_plan("select name, age from users where age > 35")
