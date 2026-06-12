from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"

def execute_binary_op(operation: str, left_operand: float, right_operand: float) -> float:
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
    else:
        raise Exception(f"unknown binary op: {operation}")

def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    if "Value" in expr:
        value = expr["Value"]["value"]["Number"][0]
        return float(value)
    elif "Identifier" in expr:
        column_name = expr["Identifier"]["value"]
        return float(row[column_name])
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

def run(sql: str) -> None:
    select = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    # Each item in the SELECT list comes in one of two shapes:
    #   `name`            -> {"UnnamedExpr": <expr>}
    #   `age + 1 as age`  -> {"ExprWithAlias": {"expr": <expr>, "alias": ...}}
    # We unwrap both into (alias, expr) pairs and hand each expr to our
    # interpreter, once per row.
    output_exprs: dict[str, "Expr"] = {}
    for item in select["projection"]:
        if "UnnamedExpr" in item:
            expr = item["UnnamedExpr"]
            alias = expr.get("Identifier", {}).get("value", "?column?")
        elif "ExprWithAlias" in item:
            expr = item["ExprWithAlias"]["expr"]
            alias = item["ExprWithAlias"]["alias"]["value"]
        output_exprs[alias] = expr

    print(f"=== {sql}")
    for row in pq.read_table(FILE_NAME).to_pylist():
        output_row = {
            alias: execute_expr(row, expr) for alias, expr in output_exprs.items()
        }
        print(output_row)


if __name__ == "__main__":
    run("select name, age + 1 as age from users")
    run("select name, age * 2 - 1 as score from users")  # precedence: (age*2) - 1
