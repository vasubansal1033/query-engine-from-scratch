from typing import Any
import pyarrow.parquet as pq

FILE_NAME = "data/sample_1.parquet"


def scan_table_full(filename: str) -> list[dict[str, Any]]:
    data = pq.read_table(filename)
    return data.to_pylist()


# we don't want to read the whole file at once and then select, filter, aggregate, etc.
# what if we could read one row at a time?
def scan_table() -> list[dict[str, Any]]:
    file = pq.ParquetFile(FILE_NAME)
    iter = file.iter_batches(1)
    result = []
    
    for batch in iter:
        result.append(batch.to_pylist()[0])

    return result


class TableScan:
    def __init__(self):
        self.file = pq.ParquetFile(FILE_NAME)
        self.iter = self.file.iter_batches(1)

    def next(self) -> dict[str, Any] | None:
        next_row = next(self.iter, None)
        if next_row is None:
            return None
            
        row = next_row.to_pylist()[0]
        return row

    def close(self):
        self.file.close()
