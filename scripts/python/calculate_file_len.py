# *-*encoding: utf-8 *-*

from collections import defaultdict
from pathlib import Path
from typing import List, Tuple

from colorama import Fore, Style
from tqdm import tqdm
import click
import polars as pl

from ..utils.utils import setup_logger, to_path


def calculate_file_len(file_path: Path) -> int:
    """Calculate the number of lines in a file
    :param file_path: Path to the file
    :return: Number of lines in the file"""
    encodings: List[str] = ["utf-8", "iso-8859-1", "cp1252"]
    for encode in encodings:
        try:
            with open(file_path, "r", encoding=encode) as fin:
                buf_gen = (buf for buf in fin)
                return sum(buf.count("\n") for buf in buf_gen)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Unable to decode file {file_path}")


def _validate_slice_range(
    ctx: click.Context, param, value: str | None
) -> None | Tuple[int, int]:
    """Validate the slice range
    :param ctx: Click context
    :param param: Click parameters metadata
    :param value: Value to validate
    :return: Tuple of integers representing the slice range.
    """
    if value is None:
        return None
    try:
        start, end = (int(part) if part else None for part in value.split(","))
        return start, end
    except ValueError:
        raise click.BadParameter(
            "Slice range must be two integers separated by a comma"
        )


@click.command()
@click.option("-p", "--path", type=str, required=True, help="Path to the directory")
@click.option("-e", "--extension", type=str, default="txt", help="File extension")
@click.option(
    "--slice-name-range",
    type=str,
    callback=_validate_slice_range,
    default=None,
    help="Tuple indicating start and end of file name e.g. 0, 2",
)
def main(path: Path, extension: str, slice_name_range: str) -> None:
    """List files with a specific extension in a directory
    :param path: Path to the directory
    :param extension: File extension
    :param slice_name_range: string indicating the start and end of the filename._Default None
    """
    logger = setup_logger()
    dir_path = to_path(path)

    data = defaultdict(list)
    try:
        logger.info(
            f"{Fore.BLUE} listing files in {dir_path} with extension"
            f" {extension}. {Style.RESET_ALL}"
        )
        for file in tqdm(
            dir_path.rglob("*csv"),
            desc="Listing files",
            colour="yellow",
            dynamic_ncols=True,
        ):
            if slice_name_range is None:
                file_name = file.stem.upper()
            else:
                assert len(slice_name_range) == 2, "Invalid slice range"
                file_name = file.stem.upper()[slice_name_range[0] : slice_name_range[1]]
            file_root = file.parents[1].stem
            file_dir = file.parent.stem
            file_len = calculate_file_len(file)
            data["file"].append(file_name)
            data["root"].append(file_root)
            data["dir"].append(file_dir)
            data["file_count"].append(1)
            data["file_len"].append(file_len)
        raw_data = pl.DataFrame(
            data,
            schema={
                "file": pl.String,
                "root": pl.String,
                "dir": pl.String,
                "file_count": pl.UInt8,
                "file_len": pl.UInt32,
            },
        )
        df = raw_data.group_by(["file", "root", "dir"]).agg(
            pl.sum("file_count"), pl.sum("file_len")
        )
        logger.info(f"{Fore.BLUE} {df} {Style.RESET_ALL}")
        df.write_excel(dir_path.joinpath("file_count.xlsx"))
    except FileNotFoundError:
        logger.error(f"{Fore.RED} Directory {dir_path} " f"not Found {Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED} Error: {e} {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
