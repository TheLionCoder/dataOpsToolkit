# DATA TOOLKIT

## Description

*Scripts* for manage data featuring and other repetitive tasks.

## Instalation
1. Clone the repository:
```sh
git clone git@github.com:TheLionCoder/dataOpsToolkit.git
cd dataOpsToolkit
```    
*command line alternative* :
```sh
gh repo clone TheLionCoder/dataOpsToolkit
cd dataOpsToolkit
```
2. Clone the submodule Utils
```sh
git submodule add git@github.com:TheLionCoder/dataScienceUtils.git
```

*Check the README in dataScienceUtils repo*
3. Create a virtual enviroment:
```sh
conda env create -f environment.yml
conda activate ml-env
```
## Usage
e.g. To split a dataset, use the following command
*get help
```sh
python3 -m src.scripts.python.dataset_splitter --
help
```
Arguments:


Example: 
* Split a csv by city.
```sh
python3 -m src.scripts.python.dataset_splitter -p "/users/projects/somedata/"
- e "csv" -c "city" --output-format "parquet"
```

Logging
The tool uses a logger to provide detailed information about the validation process.

Contributing:

    1. Fork the repository
    2. Create a new branch (git checkout -b ft-branch)
    3. Make your changes
    4. Commit your changes (git commit -m "add some feature.")
    5. Push to the branch (git push origin ft-branch)
    6. Open a pull request.
    
License:
This project is licended under MIT license. See LICENSE file for details.













