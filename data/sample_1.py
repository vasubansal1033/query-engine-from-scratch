import pyarrow as pa
import pyarrow.parquet as pq
import random
import string

# Define the number of rows to generate
num_rows = 10_000

# Pre-define the character set for the string generation
chars = string.ascii_letters + string.digits

# Generate the data using pure Python list comprehensions
# Column A: Random integers from 0 to 1000
a_data = [random.randint(0, 1000) for _ in range(num_rows)]

# Column B: Random integers from -1,000,000 to 1,000,000
b_data = [random.randint(-1_000_000, 1_000_000) for _ in range(num_rows)]

# Column C: 10-letter random alphanumeric strings
c_data = [''.join(random.choices(chars, k=10)) for _ in range(num_rows)]

# Convert the Python lists to PyArrow arrays
# Explicitly declaring the integer types as int32 keeps the Parquet file size optimized
a_array = pa.array(a_data, type=pa.int32())
b_array = pa.array(b_data, type=pa.int32())
c_array = pa.array(c_data, type=pa.string())

# Construct the PyArrow Table
table = pa.Table.from_arrays(
    [a_array, b_array, c_array],
    names=['a', 'b', 'c']
)

# Write the table to a Parquet file
output_filename = "data/sample_1.parquet"
pq.write_table(table, output_filename)

print(f"Successfully wrote {num_rows} rows to '{output_filename}'!")
