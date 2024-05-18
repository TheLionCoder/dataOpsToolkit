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
    with open(file_path, "r") as fin:
        return sum(1 for _ in fin)


@click.command()
@click.option("--path", "-p", type=str,
              required=True, help="Path to the directory")
@click.option("--extension", "-e", type=str,
              default="txt", help="File extension")
def main(path: Path, extension: str) -> None:
    """ List files with a specific extension in a directory
    :param path: Path to the directory
    :param extension: File extension"""
    logger = setup_logger()
    dir_path = to_path(path)

    count_dict: Dict[str, int] = defaultdict(int)
    try:
        logger.info(f"{Fore.BLUE} listing files in {dir_path} with extension {extension}. {Style.RESET_ALL}")
        for file in tqdm(dir_path.rglob(f"*.{extension}"), desc="Listing files", colour="#e2a0ff"):
            if file.is_file() and file.suffix == f".{extension}":
                file_kind = file.name.upper()[:2]
                file_len = calculate_file_len(file)
                count_dict[file_kind] = (
                    count_dict.get(file_kind, 0) + file_len
                )
        df = pl.DataFrame(count_dict)
        logger.info(f"{Fore.BLUE} {df} {Style.RESET_ALL}")
        df.write_excel("file_count.xlsx")
    except FileNotFoundError:
        logger.error(f"{Fore.RED} Directory {dir_path} not Found {Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED} Error: {e} {Style.RESET_ALL}")


if __name__ == '__main__':
    main()
