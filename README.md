# Build a SQL query engine from scratch

Hands-on workshop at the [Rootconf Topical Edition on Databases](https://hasgeek.com/rootconf/topical-edition-on-databases/).

Workshop details: https://hasgeek.com/rootconf/build-a-sql-query-engine-from-scratch-workshop/

## What we built

For a walkthrough of each stage in `engine/` — from handwritten queries to pipelined, vectorized execution — see **[UNDERSTANDING.md](UNDERSTANDING.md)**.

## Set up

Please have the following ready before attending the workshop. No additional time will be provided for setup.

### Step 1: install uv

Install `uv` by following the steps [here](https://docs.astral.sh/uv/getting-started/installation/).

This is important. `uv` will be the main tool for installations and running tests. We cannot help with issues arising from raw `pip` or system python usage.

To confirm that your set up is working, run:
```
uv run pytest tests/test_setup_working.py
```

These tests should pass.

### Step 2: get a Python editor

Choose your own editor, we have no preference. Could be VS code, neovim, PyCharm or anything else.

Open the `tests/test_setup_working.py` script in your editor and make sure it shows no errors in imports.

## Editing and running tests

As we progress through the workshop, we will be editing files in `engine/` directory.

There are tests for each stage under `tests/`. We will run tests one stage at a time like so:
```
uv run pytest tests/test_stage1.py
```

## Presenters

[Aayush](https://github.com/naikaayush) and [Samyak](https://github.com/Samyak2).
