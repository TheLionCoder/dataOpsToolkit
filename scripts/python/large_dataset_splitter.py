# *-* encoding: utf-8 *-*
from pathlib import Path
from typing import List

import polars as pl


def split_and_save_by_category(
    file_path, *, separator: str, category_col: str, output_dir: Path, **kwargs
):
    file_name = file_path.stem
    lazy_df = pl.scan_csv(
        file_path, separator=separator, infer_schema_length=0, **kwargs
    )
    categories = extract_unique_categories(lazy_df, category_col=category_col)

    for cat in categories:
        fout_dir = output_dir.joinpath(cat)
        fout_dir.mkdir(parents=True, exist_ok=True)
        lazy_filtered = lazy_df.filter(pl.col(category_col).eq(cat)).select(
            pl.all().exclude(category_col)
        )
        df = lazy_filtered.collect()
        df.write_csv(fout_dir.joinpath(f"{file_name}.csv"), separator="|")


@pl.StringCache()
def extract_unique_categories(lazy_df: pl.LazyFrame, *, category_col: str) -> List[str]:
    return (
        lazy_df.select(pl.col(category_col).cast(pl.Categorical))
        .unique()
        .collect()
        .get_column(category_col)
        .to_list()
    )
