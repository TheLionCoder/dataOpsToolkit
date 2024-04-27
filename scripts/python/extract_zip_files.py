# *-* encoding: utf-8 *-*
# !/usr/bin/env python3

from pathlib import Path
import zipfile

from colorama import Fore, Style
from tqdm import tqdm
import click


from ..utils.utils import setup_logger


def extract_zip_files(directory: str,
                      remove_unzipped_dir: bool = False) -> None:
    """
    Unzip all files in a directory
    :directory: str: directory path
    :remove_unzipped_dr: bool: remove the unzipped directory
    """
    logger = setup_logger()
    dir_path: Path = Path(directory).expanduser() if (
        isinstance(directory, str)) else directory

    try:
        logger.info(f"{Fore.BLUE} Extracting files from {dir_path}{Style.RESET_ALL}")
        for zip_path in tqdm(dir_path.rglob("*.zip"), desc="Extracting files...",
                             colour="green", unit="file"):
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(zip_path.parent)
                logger.warning(f"Extracted {zip_path.name}")
            if remove_unzipped_dir:
                zip_path.unlink()
                logger.warning(f"Removed {zip_path.name}")
        logger.info(f"{Fore.GREEN} Extraction complete!{Style.RESET_ALL}")
    except FileNotFoundError:
        logger.error(f"{Fore.RED}Error: {FileNotFoundError}")

    except Exception as e:
        logger.error(f"{Fore.RED}Error: {e} {Style.RESET_ALL}")
        return None


@click.command()
@click.option("--directory", "-d", required=True, type=str,
              help="Directory path")
@click.option("--remove_unzipped_dir", "-r", is_flag=True,
              required=False, default=False,)
def main(directory: str, remove_unzipped_dir: bool) -> None:
    """"
    Main function
    :directory: str: directory path
    :remove_unzipped_dir: bool: remove the unzipped directory
    """
    extract_zip_files(directory, remove_unzipped_dir)


if __name__ == '__main__':
    main()
