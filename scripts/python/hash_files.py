# *-* coding: utf-8 *-*
# !/usr/bin/env python3
import click
import hashlib
from pathlib import Path
from typing import Generator

from tqdm import tqdm

from ..utils.utils import setup_logger


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
            computed_hash = compute_hash(file_content_gen)
            hash_output.write(f"{file.name} {computed_hash}\n")


def compute_hash(file_content: Generator[bytes, None, None]) -> str:
    """
    Compute Blake2b hash.
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
@click.option(
    "--sub_folders",
    type=bool,
    required=False,
    default=False,
    help="Add hash files for sub folders."
)
def main(source_dir: str, file_pattern: str, sub_folders: bool) -> None:
    """
    Main function.
    """
    logger = setup_logger()
    try:
        source_path = Path(source_dir)
        if not source_path.exists() or not source_path.is_dir():
            logger.error("\033[91m {source_dir} directory doesn't exist or "
                         "is not readable. \033[0m")
            return

        all_files = list(source_path.rglob("*"))
        directories = (file for file in all_files if file.is_dir())

        if sub_folders:
            for dir_path in tqdm(directories, desc="Hashing files...", colour="#E84855"):
                create_hash_file(directory_path=dir_path, file_pattern=file_pattern)

        create_hash_file(directory_path=source_path, file_pattern=file_pattern)
        logger.info("\033^92m Files hashed successfully. \033[0m")
    except Exception as e:
        logger.error(f"\033[91m Error in main function: {e} \033[0m")


if __name__ == "__main__":
    main()
