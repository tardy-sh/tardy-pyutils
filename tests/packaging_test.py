"""
Check if we can properly import the package
"""

import polars as pl

from tardy_pyutils.data import unnest_dataframe

data = pl.DataFrame({"level1": {"level2": {"level3": "done"}}})

data_unnested: pl.DataFrame = unnest_dataframe(data)

if data_unnested.columns[0] != "level1.level2.level3":
    raise ValueError("Column name unnesting failed")
