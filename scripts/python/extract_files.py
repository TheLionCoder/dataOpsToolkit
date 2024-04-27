# *-* encoding: utf-8 *-*
# !/usr/bin/env python3

from pathlib import Path
import zipfile

from colorama import Fore, Style
from rarfile import RarFile
from tqdm import tqdm
import click

from ..utils.utils import setup_logger, to_path


def extract_files(directory: str, remove_unpacked_dir: bool = False) -> None:
    """
    Unzip all files in a directory
    :directory: str: directory path
    :remove_unzipped_dr: bool: remove the unzipped directory
    """
    logger = setup_logger()
    dir_path: Path = to_path(directory)

    try:
        logger.info(f"{Fore.BLUE} Extracting files from {dir_path}{Style.RESET_ALL}")
        for file_path in tqdm(
            dir_path.rglob("*.{zip,rar}"),
            desc="Extracting files...",
            colour="green",
            unit="file",
        ):
            if file_path.suffix == ".zip":
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(file_path.parent)
                    logger.warning(f"Extracted {file_path.name} \n")
            else:
                with RarFile(file_path, "r") as rar_ref:
                    rar_ref.extractall(file_path.parent)
                    logger.warning(f"Extracted {file_path.name} \n")

            if remove_unpacked_dir:
                file_path.unlink()
                logger.warning(f"Removed {file_path.name} \n")
        logger.info(f"{Fore.GREEN} Extraction complete!{Style.RESET_ALL}")
    except FileNotFoundError:
        logger.error(f"{Fore.RED}Error: {FileNotFoundError}")

    except Exception as e:
        logger.error(f"{Fore.RED}Error: {e} {Style.RESET_ALL}")
        return None


@click.command()
@click.option("--directory", "-d", required=True, type=str, help="Directory path")
@click.option(
    "--remove_unpacked_dir",
    "-r",
    is_flag=True,
    required=False,
    default=False,
)
def main(directory: str, remove_unpacked_dir: bool) -> None:
    """ "
    Main function
    :directory: str: directory path
    :remove_unzipped_dir: bool: remove the unzipped directory
    """
    extract_files(directory, remove_unpacked_dir)


if __name__ == "__main__":
    main()
