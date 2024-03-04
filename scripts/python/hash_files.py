# *-* coding: utf-8 *-*
# !/usr/bin/env python3
import click
import hashlib
from pathlib import Path
from typing import Generator

from tqdm import tqdm

from ..utils.utils import setup_logger


def create_hash_file(directory_path: str, file_pattern: str) -> None:
    """
    Create a hash file that contains a list of files and hashes.
    Args:
        directory_path: Path to the directory where the files are located.
        file_pattern: Pattern of the files to be hashed.
    """
    src_path: Path = Path(directory_path)
    hash_file_name: str = "01-hashes.txt"
    hash_file_path = src_path.joinpath(hash_file_name)

    # Create a hash file that contains a list of files and hashes
    with open(hash_file_path, "w") as hash_output:
        for file in tqdm(src_path.rglob(file_pattern)):
            if file.name == hash_file_name:
                continue
            file_content_gen = read_file_chunks(file)
            computed_hash = compute_hash(file_content_gen)
            hash_output.write(f"{file.name} {computed_hash}\n")


def compute_hash(file_content: Generator[bytes, None, None]) -> str:
    """
    Compute SHA-256 hash.
    Args:
        file_content: Bytes of file content.

    Returns:
        The computed SHA-256 hash.
    """
    hasher = hashlib.blake2b()
    for chunk in file_content:
        hasher.update(chunk)
    return hasher.hexdigest()


def read_file_chunks(file_path: Path) -> Generator[bytes, None, None]:
    """
    Read a file and yield its content in chunks
    of 4086 bytes.
    Args:
        file_path: file path-

    Returns:
           Bytes
    """
    with open(file_path, "rb") as file:
        while chunk := file.read(4096):
            yield chunk


@click.command()
@click.option(
    "--source_dir",
    type=str,
    required=True,
    help="Path to the directory where the files are located.",
)
@click.option(
    "--file_pattern",
    type=str,
    required=True,
    default="*.txt",
    help="Pattern of the files to be hashed.",
)
def main(source_dir: str, file_pattern: str) -> None:
    """
    Main function.
    """
    logger = setup_logger()
    try:
        logger.info("\033[94m Hashing files... \033^0m")
        create_hash_file(directory_path=source_dir, file_pattern=file_pattern)
        logger.info("\033^92m Files hashed successfully. \033[0m")
    except Exception as e:
        logger.error(f"\033[91m Error in main function: {e} \033[0m")


if __name__ == "__main__":
    main()
