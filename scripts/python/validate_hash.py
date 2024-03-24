# -*_ coding: utf-8 -*-
# ! /usr/bin/env python3
from pathlib import Path

import click

from ..utils.utils import compute_hash, read_file_chunks, setup_logger


def validate_hash(file_path: Path, expected_hash: str) -> bool:
    """
    Validate the hash of a file against expected hash
    :file_path: path to the file.
    :expected_hash: expected hash of the file.
    :return: True if the hash matches, False otherwise.
    """
    file_content = read_file_chunks(file_path)
    computed_hash = compute_hash(file_content)
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
        logger.error(f"\033[91mFile {file_path} does not exist.\033[0m")
        return

    if validate_hash(file_path, file_hash):
        logger.info(f"\033[92mHash of {file_path} is valid.\033[0m")
    else:
        logger.error(f"\033[91mHash of {file_path} is invalid.\033[0m")


if __name__ == "__main__":
    main()
