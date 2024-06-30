# *-* encoding: utf-8 *-*
# !/usr/bin/env python3
from pathlib import Path
from typing import Literal

from colorama import Fore, Style
import click
import pandas as pd
from tqdm import tqdm

from scripts.utils.utils import setup_logger, read_data, to_path


class Config:
    def __init__(self):
        self.save_functions = {
            "csv": pd.DataFrame.to_csv,
            "txt": pd.DataFrame.to_csv,
            "xlsx": pd.DataFrame.to_excel,
        }
        self.read_functions = {
            "csv": pd.read_csv,
            "txt": pd.read_csv,
            "xlsx": pd.read_excel,
        }


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
    :param column_category: Column name to use as a category.
    :param output_dir: Directory to save the files.
    :param output_format: Format to save the files.
    :param fout_name: Name of the files to save.
    :param keep_column: Whether to keep the category column in the files.
    :param kwargs: Additional arguments to pass to the save function.
    """

    dataset = dataset.astype({column_category: "category"})
    groups = dataset.groupby(by=column_category, observed=True)

    for group_name, group_data in tqdm(
        groups, desc="Saving files...", colour="#E84855"
    ):
        if not keep_column:
            group_data = group_data.drop(columns=[column_category])
        fout_dir = output_dir.joinpath(str(group_name))
        if not fout_dir.exists():
            fout_dir.mkdir(parents=True)
        file_path = fout_dir.joinpath(
            f"{fout_name}_{group_name}.{output_format}")
        try:
            save_function = config.save_functions[output_format]
            save_function(group_data, file_path,
                          index=False, **kwargs)
        except KeyError:
            raise ValueError(f"File format {output_format} not supported.")
        except Exception as e:
            raise ValueError(f"Error saving file {file_path.joinpath(str(group_name))}: {e}")


@click.command()
@click.option("--input-path", "-p", type=str, required=True,
              help="Path to the dataset.")
@click.option("--file-format", type=str, required=False, default="csv")
@click.option("--output-dir", "-o", type=str, required=True)
@click.option("--column-category", "-c", type=str, required=True,
              help="Column name to use as category to split the data.")
@click.option("--output-format", type=str, required=False, default="csv")
@click.option("--keep-column", is_flag=True, required=False, default=False)
@click.option("--sep", "-s", type=str, required=False, default="|")
@click.option("--delimiter", "-d",  type=str, required=False)
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
    Main function to split a dataset into multiple files based on a 
    category column.
    :param input_path: File path to the dataset.
    :param file_format: File format of the dataset.
    :param output_dir: Directory to save the files.
    :param column_category: Column name to use as a category.
    :param output_format: Format to save the files.
    :param keep_column: Whether to keep the category column in the files.
    :param sep: Delimiter to use when reading a csv file.
    :param delimiter: Delimiter to use when reading a txt file.
    :param dtype: Data type to use when reading the file.
    :param output_sep: Delimiter to use when saving a csv file.
    """
    kwargs = {"sep": sep, "delimiter": delimiter, "dtype": dtype}
    logger = setup_logger()

    input_path = to_path(input_path)
    output_dir = to_path(output_dir)

    config = Config()

    logger.info(f"{Fore.BLUE}Reading data from {input_path} {Style.RESET_ALL}")
    if input_path.is_dir():
        datasets = [
            (file.stem, read_data(config, file, file_format, **kwargs))
            for file in input_path.glob(f"*.{file_format}")
            if file.is_file()
        ]
        for file_name, dataset in datasets:
            logger.info(
                f"{Fore.MAGENTA}Splitting data and saving file "
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
                f"{Fore.BLUE}Splitting data and saving file"
                f"to {output_dir}. {Style.RESET_ALL}"
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
