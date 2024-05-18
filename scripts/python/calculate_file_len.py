# *-*encoding: utf-8 *-*

from collections import defaultdict
from pathlib import Path
from typing import Dict

from colorama import Fore, Style
from tqdm import tqdm
import click
import polars as pl

from ..utils.utils import setup_logger, to_path


def calculate_file_len(file_path: Path) -> int:
    """Calculate the number of lines in a file
    :param file_path: Path to the file
    :return: Number of lines in the file"""
    try:
        with open(file_path, "r", encoding="utf-8") as fin:
            buf_gen = (buf for buf in fin)
            return sum(buf.count("\n") for buf in buf_gen)
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="iso-8859-1") as fin:
                buf_gen = (buf for buf in fin)
                return sum(buf.count("\n") for buf in fin)
        except UnicodeDecodeError:
            with open(file_path, "r", encoding="cp1252") as fin:
                buf_gen = (buf for buf in fin)
                return sum(buf.count("\n") for buf in fin)


@click.command()
@click.option("--path", "-p", type=str, required=True,
              help="Path to the directory")
@click.option("--extension", "-e", type=str, default="txt", 
              help="File extension")
def main(path: Path, extension: str) -> None:
    """List files with a specific extension in a directory
    :param path: Path to the directory
    :param extension: File extension"""
    logger = setup_logger()
    dir_path = to_path(path)

    count_dict: Dict[str, int] = defaultdict(int)
    file_alias_dict = defaultdict(int)
    try:
        logger.info(
            f"{Fore.BLUE} listing files in {dir_path} with extension"
            f" {extension}. {Style.RESET_ALL}"
        )
        for file in tqdm(
            dir_path.rglob(f"*.{extension}"), desc="Listing files", 
            colour="#e2a0ff"
        ):
            if file.is_file() and file.suffix == f".{extension}":
                file_kind = file.name.upper()[:2]
                file_len = calculate_file_len(file)
                file_alias_dict[file_kind] = (file_alias_dict.get(
                    file_kind, 0) + 1)
                count_dict[file_kind] = count_dict.get(file_kind, 0) + file_len
        df_file_count = pl.DataFrame(file_alias_dict)       
        df_file_len = pl.DataFrame(count_dict)
        df = df_file_count.vstack(df_file_len)
        logger.info(f"{Fore.BLUE} {df} {Style.RESET_ALL}")
        df.write_excel(dir_path.joinpath("file_count.xlsx"))
    except FileNotFoundError:
        logger.error(f"{Fore.RED} Directory {dir_path} "
                     f"not Found {Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED} Error: {e} {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
