# *-* encoding: utf-8 *-*
# !/usr/bin/env python3

from pathlib import Path
from typing import Callable, Dict, Literal

from colorama import Fore, Style
from tqdm import tqdm
from ydata_profiling import ProfileReport
import click
import pandas as pd

from ..utils.utils import setup_logger, read_data, to_path


class Config:
    def __init__(self):
        self.read_functions: Dict[str, Callable] = {
            "csv": pd.read_csv,
            "txt": pd.read_csv,
            "xlsx": pd.read_excel}


def make_profile_file(dataframe: pd.DataFrame, *, output_dir: Path,
                      title: str = None, input_col: str = None) -> None:
    """
    Create a profiler for a dataframe
    :dataframe: pd.DataFrame: dataframe
    :input_col: str: column to profile
    """

    if input_col is None:
        profiler = ProfileReport(df=dataframe, minimal=True, explorative=True,
                                 dark_mode=True, orange_mode=True,
                                 title=title)
        profiler.to_file(output_dir.joinpathf(f"{title}_profile.html"))
    else:
        dataframe: pd.DataFrame = dataframe.astype({input_col: "category"})
        grouped_data = pd.Grouper = dataframe.groupby(by=input_col, observed=True)

        for _, group in tqdm(grouped_data, desc="Profiling...", colour="red", unit="group"):
            profiler = ProfileReport(df=group, minimal=True, explorative=True,
                                     dark_mode=True, orange_mode=True,
                                     title=f"{input_col} profile")
            profiler.to_file(f"{output_dir.joinpath(input_col)}_profile.html")


@click.command()
@click.option("--file-kind", "-k", type=str, required=False, help="File kind",
              default="csv")
@click.option("--file-path", "-f", type=str, required=True, help="File path")
@click.option("--input-col", "-c", type=str, required=False,
              help="Either the column to profile or None to profile the entire dataframe")
@click.option("--title", "-t", type=str, required=False, help="Title of the profile")
@click.option("output-dir", "-o", type=str, required=False, default=".", help="Output directory")
@click.option("sep", "-s", type=str, required=False, default=",", help="Separator")
@click.option("delimiter", "d", type=str, required=False, help="txt delimiter")
def main(file_path: str, file_kind: Literal["csv", "txt", "xlsx"], input_col: str, title: str,
         output_dir: str, sep: str, delimiter: str) -> None:
    """
    Main function
    :file_path: str: file path
    :file_format: str: file format
    :input_col: str: column to profile
    :title: str: title of the profile
    :output_dir: str: output directory
    :sep: str: separator
    :delimiter: str: txt delimiter
    """
    kwargs: Dict[str, str] = {"sep": sep, "delimiter": delimiter}
    logger = setup_logger()

    file_path: Path = to_path(file_path)
    output_dir: Path = to_path(output_dir)

    config = Config()

    try:
        logger.info(f"{Fore.BLUE} Reading data from {file_path}{Style.RESET_ALL}")
        dataset = read_data(config, file_path, file_kind, **kwargs)
        logger.info(f"{Fore.GREEN} Data read successfully!{Style.RESET_ALL}")
    except FileNotFoundError:
        logger.error(f"{Fore.RED}Error: {FileNotFoundError}")
        return
    except Exception as e:
        logger.error(f"{Fore.RED}Error: {e} {Style.RESET_ALL}")
        return

    logger.info(f"{Fore.CYAN} Profiling data...{Style.RESET_ALL}")
    make_profile_file(dataset, output_dir=output_dir,
                      title=title, input_col=input_col)
    logger.info(f"{Fore.GREEN} Profiling complete!{Style.RESET_ALL}")


if __name__ == '__main__':
    main()
