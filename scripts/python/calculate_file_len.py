# *-*encoding: utf-8 *-*

from collections import defaultdict
from pathlib import Path
from typing import List

from colorama import Fore, Style
from tqdm import tqdm
import click
import polars as pl

from ..utils.utils import setup_logger, to_path

pl.Config.set_tbl_cols(11)


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


@click.command()
@click.option("-p", "--path", type=str, required=True, help="Path to the directory")
@click.option("-e", "--extension", type=str, default="txt", help="File extension")
def main(path: Path, extension: str) -> None:
    """List files with a specific extension in a directory
    :param path: Path to the directory
    :param extension: File extension"""
    logger = setup_logger()
    dir_path = to_path(path)

    data: defaultdict = defaultdict(defaultdict)
    try:
        logger.info(
            f"{Fore.BLUE} listing files in {dir_path} with extension"
            f" {extension}. {Style.RESET_ALL}"
        )
        for model in dir_path.iterdir():
            if model.is_dir():
                model_data: defaultdict = defaultdict()
                for file_path in tqdm(model.iterdir(),
                                      desc=f"Processing {model.name}",
                                      colour="blue"):
                    if file_path.is_file() and file_path.suffix == f".{extension}":
                        try:
                            file_name = file_path.stem
                            model_data[file_name[:2]] = calculate_file_len(file_path)
                        except Exception as e:
                            logger.error(f"{Fore.RED} Error: {e} {Style.RESET_ALL}")
                data[model.name] = model_data
        print(data)
        #df = pl.DataFrame(data)
        #logger.info(f"{Fore.BLUE} {df} {Style.RESET_ALL}")
        #df.write_excel(dir_path.joinpath("file_count.xlsx"))
    except FileNotFoundError:
        logger.error(f"{Fore.RED} Directory {dir_path} " f"not Found {Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED} Error: {e} {Style.RESET_ALL}")


if __name__ == "__main__":
    main()
