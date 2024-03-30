# -*_ coding: utf-8 -*-
# ! /usr/bin/env python3
from pathlib import Path

from colorama import Fore, Style
import click

from ..utils.utils import compute_hash, read_file_chunks, setup_logger


def validate_hash(file_path: Path, expected_hash: str, hash_name: str) -> bool:
    """
    Validate the hash of a file against expected hash
    :file_path: path to the file.
    :expected_hash: expected hash of the file.
    :hash_name: hashing algorithm to use.
    :return: True if the hash matches, False otherwise.
    """
    if hash_name is None:
        raise ValueError(
            f"{Fore.RED}Hash name is required."
            f"Please provide a valid hash name. {Style.RESET_ALL}"
        )

    file_content = read_file_chunks(file_path)
    computed_hash = compute_hash(file_content, hash_name)
    return computed_hash == expected_hash


@click.command()
@click.option(
    "--file-path", type=str, required=True, help="Path to the file to validate."
)
@click.option("--hash", type=str, required=True, help="Expected hash of the file.")
def main(file_path: str, file_hash: str) -> None:
    """
    Main function to validate the hash of a file.
    """
    logger = setup_logger()
    file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"{Fore.RED} File {file_path} does not exist. {Style.RESET_ALL}")
        return

    if validate_hash(file_path, file_hash):
        logger.info(f"{Fore.MAGENTA} Hash of {file_path} is valid. {Style.RESET_ALL}")
    else:
        logger.error(f"{Fore.RED} Hash of {file_path} is invalid. {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
