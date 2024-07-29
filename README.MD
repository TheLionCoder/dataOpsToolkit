# DATA TOOLKIT

## Description

*Scripts* for managing data featuring and other repetitive tasks.

## Installation

1. Clone the repository:
    ```sh
    git clone git@github.com:TheLionCoder/dataOpsToolkit.git
    cd dataOpsToolkit
    ```    
    *Command line alternative*:
    ```sh
    gh repo clone TheLionCoder/dataOpsToolkit
    cd dataOpsToolkit
    ```

2. Clone the submodule Utils:
    ```sh
    git submodule add git@github.com:TheLionCoder/dataScienceUtils.git
    ```

    *Check the README in the dataScienceUtils repo*

3. Create a virtual environment:
    ```sh
    conda env create -f environment.yml
    conda activate ml-env
    ```

## Usage

To split a dataset, use the following command:

*Get help*:
    ```sh
    python3 -m src.scripts.python.dataset_splitter --help
    ```

### Arguments

- `-p, --input-path TEXT`: Path to the directory containing the large dataset [required]
- `-e, --extension [csv|txt]`: Extension of the files to process
- `-s, --separator TEXT`: Separator for the dataset
- `-c, --category-col TEXT`: Column to split the dataset by [required]
- `--keep-category-col`: Whether to keep the category column in the output files
- `--output-format [csv|txt|parquet|xlsx]`: Format of the output files
- `--output-separator TEXT`: Separator for the output files
- `--output-dir TEXT`: Output directory [required]
- `--help`: Show this message and exit

### Example

Split a CSV by city:
    ```sh
    python3 -m src.scripts.python.dataset_splitter -p "/users/projects/somedata/" -e "csv" -c "city" --output-format "parquet"
    ```

## Logging

The tool uses a logger to provide detailed information about the validation process.

## Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b ft-branch`)
3. Make your changes
4. Commit your changes (`git commit -m "add some feature."`)
5. Push to the branch (`git push origin ft-branch`)
6. Open a pull request

## License

This project is licensed under the MIT license. See the LICENSE file for details.