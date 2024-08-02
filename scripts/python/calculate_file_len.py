# *-*encoding: utf-8 *-*

from collections import defaultdict
from pathlib import Path
from typing import List, Tuple

from colorama import Fore, Style
from tqdm import tqdm
import click
import polars as pl

from ..utils.utils import setup_logger, to_path


def calculate_file_len(file_path: Path, has_header: bool = True) -> int:
    """Calculate the number of lines in a file
    :param file_path: Path to the file
    :param has_header: Whether the file has a header.
    :return: Number of lines in the file"""
    encodings: List[str] = ["utf-8", "iso-8859-1", "cp1252"]
    for encode in encodings:
        try:
            with open(file_path, "r", encoding=encode) as fin:
                if has_header:
                    next(fin)
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
@click.option("--has-header", is_flag=True, help="Whether the file has a header")
@click.option("-v", "--verbose", is_flag=True, help="Verbose output")
@click.option(
    "--include-main-dir", is_flag=True, help="Include the main directory in the output"
)
@click.option(
    "--slice-name-range",
    type=str,
    callback=_validate_slice_range,
    default=None,
    help="Tuple indicating start and end of file name e.g. 0, 2",
)
def main(
    path: str | Path,
    extension: str,
    has_header: bool,
    verbose: bool,
    include_main_dir: bool,
    slice_name_range: str,
) -> None:
    """List files with a specific extension in a directory
    :param path: Path to the directory
    :param extension: File extension
    :param has_header: Whether the file has a header
    :param verbose: Whether to print verbose output
    :param include_main_dir: Whether to include the main directory in the output
    :param slice_name_range: string indicating the start and end of the filename._Default None
    """
    logger = setup_logger()
    dir_path = to_path(path)
    fout: Path = dir_path.joinpath(f"file_count_{dir_path.stem}.xlsx")

    data = defaultdict(lambda: {"file_count": 0, "file_len": 0})
    processed_data = {
        "file": [],
        "dir": [],
        "parent": [],
        "file_count": [],
        "file_len": [],
    }
    dataframe_schema = {
        "file": pl.String,
        "dir": pl.String,
        "parent": pl.String,
        "file_count": pl.UInt8,
        "file_len": pl.UInt32,
    }

    try:
        logger.info(
            f"{Fore.BLUE} listing files in {dir_path} with extension"
            f" {extension}. {Style.RESET_ALL}"
        )
        for file in tqdm(
            dir_path.rglob(f"*.{extension}"),
            desc="Listing files",
            colour="yellow",
            dynamic_ncols=True,
        ):
            file_name = (
                file.stem.upper()[slice_name_range[0] : slice_name_range[1]]
                if slice_name_range
                else file.stem.upper()
            )
            file_dir = file.parents[1].stem
            file_parent = file.parent.stem
            file_len = calculate_file_len(file, has_header)

            key = (file_name, file_dir, file_parent)
            data[key]["file_count"] += 1
            data[key]["file_len"] += file_len

        for (file_name, file_dir, file_parent), counts in data.items():
            processed_data["file"].append(file_name)
            processed_data["dir"].append(file_dir)
            processed_data["parent"].append(file_parent)
            processed_data["file_count"].append(counts.get("file_count", 0))
            processed_data["file_len"].append(counts.get("file_len", 0))
        raw_data = pl.DataFrame(processed_data, schema=dataframe_schema)
        grouped_df = raw_data.group_by(["parent", "file"]).agg(
            pl.sum("file_count"), pl.sum("file_len")
        )

        df: pl.DataFrame = raw_data if include_main_dir else grouped_df

        if verbose:
            logger.info(f"{Fore.BLUE} {df} {Style.RESET_ALL}")

        df.write_excel(fout)

    except FileNotFoundError:
        logger.error(f"{Fore.RED} Directory {dir_path} " f"not Found {Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED} Error: {e} {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
