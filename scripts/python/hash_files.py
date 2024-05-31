# *-* coding: utf-8 *-*
# !/usr/bin/env python3
import click
from pathlib import Path

from colorama import Fore, Style
from tqdm import tqdm

from ..utils.utils import compute_hash, read_file_chunks, setup_logger


def create_hash_file(directory_path: Path, file_pattern: str) -> None:
    """
    Create a hash file that contains a list of files and hashes.
    Args:
        directory_path: Path to the directory where the files are located.
        file_pattern: Pattern of the files to be hashed.
    """
    hash_file_name: str = f"01-hashes_{directory_path.name}.txt"
    hash_file_path = directory_path.joinpath(hash_file_name)

    # Create a hash file that contains a list of files and hashes
    with hash_file_path.open("w") as hash_output:
        for file in directory_path.glob(file_pattern):
            if file.name == hash_file_name:
                continue
            file_content_gen = read_file_chunks(file)
            computed_hash = compute_hash(file_content_gen, hash_name="blake2b")
            hash_output.write(f"{file.name} {computed_hash}\n")


@click.command()
@click.option(
    "--source_dir",
    "-d",
    type=str,
    required=True,
    help="Path to the directory where the files are located.",
)
@click.option(
    "--file_pattern",
    "-fp",
    type=str,
    required=True,
    default="*.txt",
    help="Pattern of the files to be hashed.",
)
@click.option(
    "--sub_folders",
    "-sf",
    is_flag=True,
    default=False,
    help="Add hash files for sub folders.",
)
def main(source_dir: str, file_pattern: str, sub_folders: bool) -> None:
    """
    Main function.
    """
    logger = setup_logger()
    try:
        source_path = Path(source_dir).expanduser()
        if not source_path.exists() or not source_path.is_dir():
            logger.error(
                f"{Fore.RED}{source_dir} directory doesn't exist or "
                f"is not readable. {Style.RESET_ALL}"
            )
            return

        all_files = list(source_path.rglob("*"))
        directories = (file for file in all_files if file.is_dir())

        if sub_folders:
            for dir_path in tqdm(
                directories, desc="Hashing files...", colour="#E84855"
            ):
                create_hash_file(directory_path=dir_path,
                                 file_pattern=file_pattern)

        create_hash_file(directory_path=source_path, file_pattern=file_pattern)
        logger.info(f"{Fore.GREEN} Files hashed successfully."
                    f"{Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED} Error in main function:"
                     f"{e} {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
