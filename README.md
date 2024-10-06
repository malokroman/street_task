# This repos is the solution for an interview question for Street Group

# Development environment
This is developed using python 3.12.6 and should work with this version or above.

Create a virtual environment and install the requirements using `pip install -r requirements.txt`. It is recommended to use direnv to load and unload the virtual environment automatically. But you can create a virtual environment however you want.

# This code base is formatted with ruff and isort


# Codebase description

## Pipeline
A subset data from the government property transactions data is included in this repos (data/subset.csv). You may also use the full data. 
Run the pipeline by the following command in your virtual environment: 

``` sh
python main.py --csv-file "path/to/data.csv"
```


## Notebook
Some data explorations and thought process is documented in data_exploration.ipynb. 
