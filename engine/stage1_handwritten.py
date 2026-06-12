from typing import Any
import pyarrow.parquet as pq

FILE_NAME = "data/sample_1.parquet"

# result should be a list of dictionaries, e.g.:
# [
#     {"col1": "a", "col2": 1, "col3": True},
#     {"col1": "b", "col2": 2, "col3": False},
#     {"col1": "c", "col2": 3, "col3": True},
# ]


# select * from users
def query1() -> list[dict[str, Any]]:
    data = pq.read_table(FILE_NAME).to_pylist()
    result = []
    for row in data:
        result.append(row)
    return result


# select name, age + 1 as age from users
def query2() -> list[dict[str, Any]]:
    data = pq.read_table(FILE_NAME).to_pylist()
    result = []
    for row in data:
        result.append({"name": row["name"], "age": row["age"] + 1})

    return result


# select name, age + 1 as age from users where age > 25
def query3() -> list[dict[str, Any]]:
    data = pq.read_table(FILE_NAME).to_pylist()
    result = []
    for row in data:
        if row["age"] > 25:
            result.append({"name": row["name"], "age": row["age"] + 1})

    return result

# select avg(age) as avg_age from users
def query4() -> list[dict[str, Any]]:
    data = pq.read_table(FILE_NAME).to_pylist()
    sum_age, num_rows = 0, 0
    for row in data:
        sum_age += row["age"]
        num_rows += 1
    return [{"avg_age": sum_age / num_rows}]

# select avg(age) from users group by country
def query5() -> list[dict[str, Any]]:
    data = pq.read_table(FILE_NAME).to_pylist()
    country_avg_age = {}
    country_count = {}
    for row in data:
        if row["country"] not in country_avg_age:
            country_avg_age[row["country"]] = row["age"]
        else:
            country_avg_age[row["country"]] += row["age"]
            country_count[row["country"]] += 1

    return [{
        "country": country,
        "avg_age": country_avg_age[country] / country_count[country]
        } for country in country_avg_age
    ]
    
