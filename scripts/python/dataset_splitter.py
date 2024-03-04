# *-* encoding: utf-8 *-*
# !/usr/bin/env python3

from pathlib import Path
from typing import TypeVar

import click
import pandas as pd
from tqdm import tqdm

from utils.utils import setup_logger

FileType = TypeVar("FileType", "csv", "txt", "xlsx")


def read_data(file_path: Path, file_format: FileType, **kwargs) -> pd.DataFrame:
    """
    Read data from a file.

    :param file_path: File path to the dataset.
    :param file_format: File format of the dataset.
    :param kwargs: Additional arguments to pass to the read function

    .e.g.:
    >>> 'read_data(/some_path/some_file.csv, file_format="csv", sep="|")'
    >>> 'read_data(/some_path/some_file.txt, file_format="txt", delimiter="|")'
    >>> 'read_data(/some_path/some_file.xlsx, file_format="xlsx")'
    """
    read_functions = {"csv": pd.read_csv, "txt": pd.read_csv, "xlsx": pd.read_excel}

    try:
        read_function = read_functions[file_format]
        return read_function(file_path, **kwargs)
    except KeyError:
        raise ValueError(f"File format {file_format} not supported.")
    except FileNotFoundError:
        raise FileNotFoundError(f"File {file_path} not found.")
    except Exception as e:
        raise ValueError(f"Error reading file {file_path}: {e}")


def split_and_save_data(
    dataset: pd.DataFrame,
    column_category: str,
    output_dir: Path | str,
    file_name: str,
    output_format: FileType,
    keep_column: bool,
) -> None:
    """
    Split a dataset into multiple files based on a category column.
    :param dataset: Pandas DataFrame to split.
    :param column_category: Column name to use as category.
    :param output_dir: Directory to save the files.
    :param output_format: Format to save the files.
    :param file_name: Name of the files to save.
    :param keep_column: Whether to keep the category column in the files.
    """

    if isinstance(output_dir, str):
        output_dir = Path(output_dir)

    dataset = dataset.astype({column_category: "category"})
    groups = dataset.groupby(by=column_category, observed=True)

    save_functions = {
        "csv": pd.DataFrame.to_csv,
        "txt": pd.DataFrame.to_csv,
        "xlsx": pd.DataFrame.to_excel,
    }

    for group_name, group_data in tqdm(groups):
        category_dr = output_dir.joinpath(str(group_name))
        category_dr.mkdir(parents=True, exist_ok=True)

        if not keep_column:
            group_data = group_data.drop(columns=[column_category])
        file_path = category_dr / f"{file_name}_{group_name}.{output_format}"
        try:
            save_function = save_functions[output_format]
            save_function(group_data, file_path, index=False)
        except KeyError:
            raise ValueError(f"File format {output_format} not supported.")
        except Exception as e:
            raise ValueError(f"Error saving file {file_path}: {e}")


def process_pipeline(
    source_path: Path,
    file_format: FileType,
    output_dir: Path,
    column_category: str,
    file_name: str,
    output_format: FileType,
    keep_column: bool,
    **kwargs,
) -> None:
    """
    Process pipeline to split a dataset into multiple files based on a category column.
    Args:
        source_path: Path to the dataset.
        file_format: File format of the dataset.
        output_dir: Path to save the files.
        column_category: Column name to use as category.
        file_name: Name of the files to save.
        output_format: Format to save the files.
        keep_column: Whether to keep the category column in the files.
        **kwargs: Additional arguments to pass to the read function.
    """
    try:
        dataset = read_data(source_path, file_format, **kwargs)
        split_and_save_data(
            dataset=dataset,
            column_category=column_category,
            output_dir=output_dir,
            file_name=file_name,
            output_format=output_format,
            keep_column=keep_column,
        )
    except Exception as e:
        raise ValueError(f"Error processing pipeline: {e}")


@click.command()
@click.option("--source_path", type=Path, required=True)
@click.option("--file_format", type=str, required=False, default="csv")
@click.option("--output_dir", type=Path, required=True)
@click.option("--column_category", type=str, required=True)
@click.option("--output_format", type=str, required=False, default="csv")
@click.option("--keep_column", type=bool, required=False, default=False)
@click.option("--sep", type=str, required=False)
@click.option("--delimiter", type=str, required=False)
@click.option("--dtype", type=str, required=False, default="str")
@click.option(
    "--process_all",
    type=bool,
    required=False,
    default=False,
    help="Whether to process all files in the source directory.",
)
def main(
    source_path: Path | str,
    file_format: FileType,
    output_dir: Path,
    column_category: str,
    output_format: FileType,
    keep_column: bool,
    sep: str,
    delimiter: str,
    dtype: str,
    process_all: bool,
) -> None:
    """
    Main function to split a dataset into multiple files based on a category column.
    :param source_path: File path to the dataset.
    :param file_format: File format of the dataset.
    :param output_dir: Directory to save the files.
    :param column_category: Column name to use as category.
    :param output_format: Format to save the files.
    :param keep_column: Whether to keep the category column in the files.
    :param sep: Delimiter to use when reading a csv file.
    :param delimiter: Delimiter to use when reading a txt file.
    :param dtype: Data type to use when reading the file.
    :param process_all: Whether to process all files in the source directory.
    """
    kwargs = {"sep": sep, "delimiter": delimiter, "dtype": dtype}
    logger = setup_logger()

    if isinstance(source_path, str):
        source_path = Path(source_path)

    if source_path.is_dir() and process_all:
        for file_path in tqdm(source_path.rglob(f"*.{file_format}")):
            file_name = file_path.stem
            logger.info(f"\033[94m Reading data from {file_name}.{file_format} \033[0m")
            process_pipeline(
                source_path=file_path,
                file_format=file_format,
                output_dir=output_dir,
                column_category=column_category,
                file_name=file_name,
                output_format=output_format,
                keep_column=keep_column,
                **kwargs,
            )
            logger.info("\033[92m Done! \033[0m")
    if source_path.is_file():
        logger.info(f"\033[94m Reading data from {source_path} \033[0m")
        process_pipeline(
            source_path=source_path,
            file_format=file_format,
            output_dir=output_dir,
            column_category=column_category,
            file_name=source_path.stem,
            output_format=output_format,
            keep_column=keep_column,
            **kwargs,
        )
        logger.info("\033[92m Done! \033[0m")


if __name__ == "__main__":
    main()
