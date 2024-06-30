# *-* encoding: utf-8 *-*
# !/usr/bin/env python3
from datetime import datetime
from pathlib import Path
import zipfile

from colorama import Fore, Style
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

    if not dir_path.exists():
        logger.error(f"{Fore.RED}Error: {dir_path}"
                     f"does not exist{Style.RESET_ALL}")
        raise FileNotFoundError(f"{dir_path} does not exist")
    try:
        logger.info(
            f"{Fore.BLUE} Extracting files from {dir_path}{Style.RESET_ALL}"
        )
        for file_path in tqdm(
            dir_path.rglob("*.zip"),
            desc="Extracting files...",
            colour="green",
        ):
            try:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                unpack_dir = file_path.parent.joinpath(
                    f"{file_path.stem}_{timestamp}")
                unpack_dir.mkdir(exist_ok=True)

                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(unpack_dir)
                    logger.warning(f"Extracted {file_path.name} \n")

                if remove_unpacked_dir:
                    file_path.unlink()
                    logger.warning(f"Removed {file_path.name} \n")

            except zipfile.BadZipFile:
                logger.error(
                    f"{Fore.RED}Error:{file_path} - "
                    f"{zipfile.BadZipFile}{Style.RESET_ALL}"
                )
                bad_zip_dir = file_path.parent.joinpath(
                    f"bad_zip_{file_path.stem}")
                bad_zip_dir.mkdir(exist_ok=True)
                file_path.rename(bad_zip_dir.joinpath(file_path.name))
                logger.warning(
                    f"Moved BadZip {file_path.name} to {bad_zip_dir} \n"
                )

            except FileNotFoundError:
                logger.error(f"{Fore.RED}Error: {FileNotFoundError}-"
                             f"{file_path.absolute()} {Style.RESET_ALL} \n")
                continue

        logger.info(f"{Fore.GREEN} Extraction complete!{Style.RESET_ALL}")

    except Exception as e:
        logger.error(f"{Fore.RED}Error: {e} {Style.RESET_ALL}")
        return None


@click.command()
@click.option(
    "--directory",
    "-d",
    required=True,
    type=str,
    help="Directory path"
)
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
