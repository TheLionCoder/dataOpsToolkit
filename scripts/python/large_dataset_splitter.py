# *-* encoding: utf-8 *-*
from pathlib import Path
from typing import List, Literal

import click
import polars as pl
from tqdm import tqdm

from colorama import Fore, Style

from scripts.utils.utils import to_path, setup_logger


def split_and_save_by_category(
    lazy_df: pl.LazyFrame,
    *,
    categories: List[str],
    file_name: str,
    category_col: str,
    output_dir: Path,
    keep_category_col: bool = False,
    output_extension: Literal["csv", "txt"],
    **kwargs,
):
    for cat in tqdm(categories,
                    desc="Saving files",
                    colour="green"):
        fout_dir = output_dir.joinpath(cat)
        fout_dir.mkdir(parents=True, exist_ok=True)

        lazy_filtered = lazy_df.filter(pl.col(category_col).eq(cat))
        if not keep_category_col:
            lazy_filtered = lazy_df.filter(pl.col(category_col).eq(cat)).select(
                pl.all().exclude(category_col)
            )
        df = lazy_filtered.collect()
        df.write_csv(fout_dir.joinpath(f"{file_name}.{output_extension}"), **kwargs)


@pl.StringCache()
def _extract_unique_categories(
    lazy_df: pl.LazyFrame, *, category_col: str
) -> List[str]:
    return (
        lazy_df.select(pl.col(category_col).cast(pl.Categorical))
        .unique()
        .collect()
        .get_column(category_col)
        .to_list()
    )


@click.command()
@click.option(
    "-p",
    "--input-path",
    type=str,
    required=True,
    help="Path to the directory containing the large dataset",
)
@click.option(
    "-e",
    "--extension",
    type=click.Choice(["csv", "txt"]),
    default="csv",
    help="Extension of the files to process",
)
@click.option(
    "-s", "--separator", type=str, default=",", help="Separator for the dataset"
)
@click.option(
    "-c",
    "--category-col",
    type=str,
    required=True,
    help="Column to split the dataset by",
)
@click.option(
    "--keep-category-col",
    is_flag=True,
    default=False,
    help="Either to keep the category column in the output files",
)
@click.option("--output-extension", type=click.Choice(["csv", "txt"]), default="csv",
              help="Extension of the output files")
@click.option(
    "--output-separator", type=str, default="|", help="Separator for the output files"
)
@click.option("--output-dir", type=str, required=True)
def main(
    input_path: str | Path,
    extension: Literal["csv", "txt"],
    separator: str,
    category_col: str,
    keep_category_col: bool,
    output_separator: str,
    output_extension: Literal["csv", "txt"],
    output_dir: str | Path,
):
    input_path = to_path(input_path)
    output_dir = to_path(output_dir)
    logger = setup_logger()
    try:
        files: List[Path] = list(input_path.rglob(f"*.{extension}")) if input_path.is_dir() else [input_path]
        for file_path in tqdm(
            files,
            desc="Processing files",
            colour="yellow",
        ):
            logger.info(
                f"{Fore.BLUE} Reading file {file_path}... {Style.RESET_ALL}"
            )
            file_name: str = file_path.stem
            lazy_df: pl.LazyFrame = pl.scan_csv(
                file_path,
                separator=separator,
                infer_schema_length=0,
            )
            categories: List[str] = _extract_unique_categories(
                lazy_df, category_col=category_col
            )
            logger.info(
                f"{Fore.LIGHTBLUE_EX} Splitting  {file_name} in {categories}... {Style.RESET_ALL}"
            )
            split_and_save_by_category(
                lazy_df,
                separator=output_separator,
                categories=categories,
                file_name=file_name,
                category_col=category_col,
                output_dir=output_dir,
                keep_category_col=keep_category_col,
                output_extension=output_extension
            )
            logger.info(
                f"{Fore.LIGHTBLUE_EX} {len(categories)} files saved in {output_dir}... {Style.RESET_ALL}"
            )
    except Exception as e:
        logger.error(f"{Fore.LIGHTRED_EX} Error: {e} {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
