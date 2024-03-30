# *-* encoding: utf-8 *-*
# !/usr/bin/env python3
from os.path import expanduser

from pathlib import Path
from typing import Literal

from colorama import Fore, Style
import click
import pandas as pd
from tqdm import tqdm

from ..utils.utils import setup_logger


class Config:
    def __init__(self):
        self.read_functions = {
            "csv": pd.read_csv,
            "txt": pd.read_csv,
            "xlsx": pd.read_excel,
        }
        self.save_functions = {
            "csv": pd.DataFrame.to_csv,
            "txt": pd.DataFrame.to_csv,
            "xlsx": pd.DataFrame.to_excel,
        }


def read_data(
    config: Config,
    file_path: Path,
    file_format: Literal["csv", "txt", "xlsx"],
    **kwargs,
) -> pd.DataFrame:
    """
    Read data from a file.
    :param config: Config object with read functions.
    :param file_path: File path to the dataset.
    :param file_format: File format of the dataset.
    :param kwargs: Additional arguments to pass to the read function

    .e.g.:
    >>> 'read_data(/some_path/some_file.csv, file_format="csv", sep="|")'
    >>> 'read_data(/some_path/some_file.txt, file_format="txt", delimiter="|")'
    >>> 'read_data(/some_path/some_file.xlsx, file_format="xlsx")'
    """
    try:
        read_function = config.read_functions[file_format]
        return read_function(file_path, **kwargs)
    except KeyError:
        raise ValueError(f"File format {file_format} not supported.")
    except FileNotFoundError:
        raise FileNotFoundError(f"File {file_path} not found.")
    except Exception as e:
        raise ValueError(f"Error reading file {file_path}: {e}")


def split_and_save_data(
    config: Config,
    dataset: pd.DataFrame,
    column_category: str,
    output_dir: Path,
    fout_name: str,
    output_format: Literal["csv", "txt", "xlsx"],
    keep_column: bool,
    **kwargs,
) -> None:
    """
    Split a dataset into multiple files based on a category column.
    :param config: Config object with save functions.
    :param dataset: Pandas DataFrame to split.
    :param column_category: Column name to use as category.
    :param output_dir: Directory to save the files.
    :param output_format: Format to save the files.
    :param fout_name: Name of the files to save.
    :param keep_column: Whether to keep the category column in the files.
    :param kwargs: Additional arguments to pass to the save function.
    """
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    dataset = dataset.astype({column_category: "category"})
    groups = dataset.groupby(by=column_category, observed=True)

    for group_name, group_data in tqdm(
        groups, desc="Saving files...", colour="#E84855"
    ):
        if not keep_column:
            group_data = group_data.drop(columns=[column_category])
        file_path = output_dir.joinpath(f"{fout_name}_{group_name}.{output_format}")
        try:
            save_function = config.save_functions[output_format]
            save_function(group_data, file_path, index=False, **kwargs)
        except KeyError:
            raise ValueError(f"File format {output_format} not supported.")
        except Exception as e:
            raise ValueError(f"Error saving file {file_path}: {e}")


def _to_path(path: str) -> Path:
    """
    Convert a string path to a Path object.
    :param path: String path to convert.
    :return: Path object.
    """
    return Path(expanduser(path)) if isinstance(path, str) else path


@click.command()
@click.option("--input-path", type=str, required=True, help="Path to the dataset.")
@click.option("--file-format", type=str, required=False, default="csv")
@click.option("--output-dir", type=str, required=True)
@click.option("--column_category", type=str, required=True)
@click.option("--output-format", type=str, required=False, default="csv")
@click.option("--keep-column", type=bool, required=False, default=False)
@click.option("--sep", type=str, required=False, default="|")
@click.option("--delimiter", type=str, required=False)
@click.option("--dtype", type=str, required=False, default="str")
@click.option(
    "--output-sep",
    type=str,
    required=False,
    default="|",
    help="Delimiter to use when saving a csv file.",
)
def main(
    input_path: str,
    file_format: Literal["csv", "txt", "xlsx"],
    output_dir: str,
    column_category: str,
    output_format: Literal["csv", "txt", "xlsx"],
    keep_column: bool,
    sep: str,
    delimiter: str,
    dtype: str,
    output_sep: str,
) -> None:
    """
    Main function to split a dataset into multiple files based on a category column.
    :param input_path: File path to the dataset.
    :param file_format: File format of the dataset.
    :param output_dir: Directory to save the files.
    :param column_category: Column name to use as category.
    :param output_format: Format to save the files.
    :param keep_column: Whether to keep the category column in the files.
    :param sep: Delimiter to use when reading a csv file.
    :param delimiter: Delimiter to use when reading a txt file.
    :param dtype: Data type to use when reading the file.
    :param output_sep: Delimiter to use when saving a csv file.
    """
    kwargs = {"sep": sep, "delimiter": delimiter, "dtype": dtype}
    logger = setup_logger()

    input_path = _to_path(input_path)
    output_dir = _to_path(output_dir)

    config = Config()

    logger.info(f"{Fore.BLUE}Reading data from {input_path} {Style.RESET_ALL}")
    if input_path.is_dir():
        datasets = [
            (file.stem, read_data(config, file, file_format, **kwargs))
            for file in input_path.glob(f"*.{file_format}")
        ]
        for file_name, dataset in datasets:
            logger.info(
                f"{Fore.MAGENTA}Splitting data and saving file"
                f"to {output_dir}."
                f" {Style.RESET_ALL}"
            )
            split_and_save_data(
                config=config,
                dataset=dataset,
                column_category=column_category,
                output_dir=output_dir,
                fout_name=file_name,
                output_format=output_format,
                keep_column=keep_column,
                sep=output_sep,
            )
        else:
            file_name = input_path.stem
            dataset = read_data(config, input_path, file_format, **kwargs)
            logger.info(
                f"{Fore.BLUE}Splitting data and saving file to {output_dir}. {Style.RESET_ALL}"
            )
            split_and_save_data(
                config=config,
                dataset=dataset,
                column_category=column_category,
                output_dir=output_dir,
                fout_name=file_name,
                output_format=output_format,
                keep_column=keep_column,
                sep=output_sep,
            )
            logger.info(f"{Fore.GREEN}Done! {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
