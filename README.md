A Python client for the SQLShare REST API.
==========================================

Documentation pasted here from:
http://escience.washington.edu/get-help-now/sqlshare-python-api

# Overview: getting started

1. Download the source and install the API

       git clone git://github.com/uwescience/sqlshare-pythonclient.git

2. Install the Python libraries.

       sudo python setup.py install

3. Make sure your own API key is configured in `$HOME/.sqlshare/config`

       [sqlshare]
       user=your-sql-share-account-name
       password=your-sql-share-account-key
       
4. Use the command line tools in `tools/*`, or write your own Python programs using these tools as examples.

The remainder of this document provides some additional details.

# The source code

The SQL Share Python API and clients are under src/python/sqlshare/ in the SVN repository and have following structure.

    sqlshare/ : the directory contains the Python module code
    tools/
          fetchdata.py   : download a dataset or the answer to a SQL query
          multiupload.py : upload multiple CSV files
          permissions.py : manage ACL of SQL share datasets

# Setup
## Download the code

The SQLShare Python API code is stored on GitHub.

       git clone git://github.com/uwescience/sqlshare-pythonclient.git

## Install the SQLShare Python library

To use the Python API, you need to either 1) install the module as a Python library or 2) add the directory that contains the Python module to the `PYTHONPATH` environment variable.

1. To install as a Python module, use `python setup.py` just like you install other 3rd party modules.

       python setup.py install
       
   If the above does not work, you may need to use `sudo python setup.py install` to install the code as a user with the right to modify system files.
   
2. Otherwise, add the path to the Python code to the `PYTHONPATH` environment variable.
    
   From the cloned `sqlshare-pythonclient` directory:
    
       export PYTHONPATH=$PYTHONPATH:`pwd`

## Configuring your SQLShare API Key

### Create a SQLShare account, if you don't already have one

Visit the [SQLShare web site](https://sqlshare.escience.washington.edu) and sign in using either UW NetID or Google Account.

### Create an API key, if you don't already have one

To obtain or create your API key, visit [https://sqlshare.escience.washington.edu/sqlshare/#s=credentials](https://sqlshare.escience.washington.edu/sqlshare/#s=credentials).

### Create the API key configuration File

The Python API reads login information from a configuration file, stored in your home directory at `$HOME/.sqlshare/config`. A sample content of config file:

    [sqlshare]
    user=your-sql-share-account-name
    password=your-sql-share-API-key

Note that password is your API key, not your UW NetID or Google Account password.

# Example uses

### Download a dataset

The following command downloads the dataset `[sqlshare@uw.edu].[periodic_table]` and stores it in the output file `output.csv`.

    python fetchdata.py -d "[sqlshare@uw.edu].[periodic_table]" -o periodic.csv
    
To get tab-separated values instead:

    python fetchdata.py -d "[sqlshare@uw.edu].[periodic_table]" -f tsv -o periodic.txt

### Download the answer to a query

This uses the same `fetchdata.py` program as above. Here, we supply the `-s` option (SQL) instead of `-d` (dataset).

To fetch the noble gases in the periodic table:

    python fetchdata.py -s "SELECT * FROM [sqlshare@uw.edu].[periodic_table] WHERE [group]=18" -o noble.csv

### Upload CSV files

Uploading a CSV file is easy

    python multiupload.py csvfile1 csvfile2 ... csvfileN

### Manage dataset permissions

    python permissions.py -t TABLENAME print
    python permissions.py -t TABLENAME add user1 user2 ... userN
    python permissions.py -t TABLENAME remove user1 user2 ... userN

### Append CSV files to an existing dataset

    python append.py datasetName csvfile1 csvfile2 ... csvfileN

* `datasetName` is created if the dataset does not exist in SQLShare.
* Each CSV file must have column header in the first line of the file.