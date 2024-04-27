# *-* encoding: utf-8 *-*
# !/usr/bin/env python3

from pathlib import Path
from typing import Dict

from colorama import Fore, Style
from tqdm import tqdm
import click

from ..utils.utils import setup_logger


def log_file_paths(directory: Path) -> None:
    """
    List all files in a directory
    :directory: str: directory path
    """
    with open("file_paths.log", "w") as log_file:
        for file in tqdm(directory.rglob("*"), desc="Listing files",
                         unit="files", colour="#e84855"):
            log_file.write(f"{file}\n")


def count_files_by_type(directory: Path) -> Dict[str, int]:
    """
    Count the number of files by type in a directory
    :directory: str: directory path
    """
    file_types_count: Dict[str, int] = {}
    for file in tqdm(directory.rglob("*"), desc="Counting files",
                     unit="files", colour="#e84855"):
        file_extension: str = file.suffix
        file_types_count[file_extension] = file_types_count.get(file_extension, 0) + 1
    return file_types_count


@click.command()
@click.option("--directory", "-d", required=True, type=click.Path(exists=True),
              help="Directory path")
@click.option("--verbose", "-v", is_flag=True, help="Verbose mode", default=False)
def main(directory: str, verbose: bool) -> True:
    """
    Main function
    :directory: str: directory path
    :verbose: bool: verbose mode
    """
    logger = setup_logger()
    logger.info(f"{Fore.GREEN} Listing files in {directory} {Style.RESET_ALL}")

    directory_path: Path = Path(directory).expanduser() \
        if isinstance(directory, str) else directory

    try:
        if verbose:
            file_types_count = count_files_by_type(directory_path)
            logger.info(f"{Fore.MAGENTA} {file_types_count} {Style.RESET_ALL}")

            log_file_paths(directory_path)
        else:
            log_file_paths(directory_path)
        logger.info(f"{Fore.MAGENTA} File paths logged in file_paths.log {Style.RESET_ALL}")
    except FileNotFoundError:
        logger.error(f"{Fore.RED} Directory not found {Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED} {e} {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
