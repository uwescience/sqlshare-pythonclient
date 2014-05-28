A Python client for the SQLShare REST API.
==========================================

# Quick Example of Use

[IPython Notebook Example of using the SQLShare Python Client](http://nbviewer.ipython.org/github/uwescience/sqlshare-pythonclient/blob/dd91fa90f0a22cf24e93f3ee0b8fd5d372621ce3/ipnb_html_wrapper_demo/SQLShare%20for%20python.ipynb)

# Quick Setup Instructions


These quick instructions are provided for experts comfortable with the command-line environments and open-source tools. For more detailed instructions, see [below](#setup-instructions).

1. Download the source and install the API

        git clone git://github.com/uwescience/sqlshare-pythonclient.git

2. Install the Python libraries.

        sudo python setup.py install

3. Make sure your own API key is configured in your home directory, in the file `.sqlshare/config`.

    On Mac or Linux platforms, you can create the directory: `mkdir -p ~/.sqlshare`.
    
    Then create a file called `config` in that directory using your favorite editor (e.g., `vim ~/.sqlshare/config`) with the following contents:

        [sqlshare]
        user=your-sql-share-account-name
        password=your-sql-share-account-key
             
4. Use the command line tools in `tools/*`, or write your own Python programs using these tools as examples.

The remainder of this document provides some additional details.

# Setup Instructions
## Download the code

### Option 1: Download the raw source code
The SQLShare Python API code is stored on GitHub. To check it out, use `git`.

        git clone git://github.com/uwescience/sqlshare-pythonclient.git
       
Now switch to the directory containing the code for further steps.

        cd sqlshare-pythonclient

### Option 2: Download a zip file of the source

You can download a zip of the code from [this link](https://github.com/uwescience/sqlshare-pythonclient/archive/master.zip). After unzipping it, open a command-line terminal in the newly unzipped directory `sqlshare-pythonclient-master`.

For example, if the file unzipped to your `Downloads` directory on OS X:

        cd ~/Downloads/sqlshare-pythonclient-master

## Install the SQLShare Python library

To use the Python API, you need to either 1) install the module as a Python library or 2) add the directory that contains the Python module to the `PYTHONPATH` environment variable.

### Option 1: Install the Python module system-wide

To install as a Python module, use `python setup.py` just like you install other 3rd party modules.

        python setup.py install
       
If the above does not work, you may need to use `sudo python setup.py install` to install the code as a user with the right to modify system files.
   
### Option 2. Put the Python module in your `PYTHONPATH` environment variable.
    
From the cloned `sqlshare-pythonclient` or unzipped `sqlshare-pythonclient-master` directory:
    
        export PYTHONPATH=$PYTHONPATH:`pwd`
        
You will need this command to be run every time you open a terminal. The way to do this in an OS X/Linux environment is to install this command in your `profile` file. For example, on OS X, you need to put this line in the file `$HOME/.bash_profile`.

        echo 'export PYTHONPATH=$PYTHONPATH':`pwd` >> ~/.bash_profile

## Configuring your SQLShare API Key

### Create a SQLShare account, if you don't already have one

Visit the [SQLShare web site](https://sqlshare.escience.washington.edu) and sign in using either UW NetID or Google Account.

### Create an API key, if you don't already have one

To obtain or create your API key, visit [https://sqlshare.escience.washington.edu/sqlshare/#s=credentials](https://sqlshare.escience.washington.edu/sqlshare/#s=credentials).

### Create the API key configuration File

The Python API reads login information from a configuration file, stored in your home directory at `$HOME/.sqlshare/config`.

1. Create the directory

        mkdir -p $HOME/.sqlshare
        
2. Here is a sample of the text that should be in the file `config` in that directory (`$HOME/.sqlshare/config`).

        [sqlshare]
        user=your-sql-share-account-name
        password=your-sql-share-API-key

Note that password is your API key, not your UW NetID or Google Account password.

Your user name is shown at the top right in SQLShare. It is typically your email address.

# The source code

The SQL Share Python API and clients are under src/python/sqlshare/ in the SVN repository and have following structure.

    sqlshare/ : the directory contains the Python module code
    tools/
          fetchdata.py   : download a dataset or the answer to a SQL query
          multiupload.py : upload multiple CSV files
          permissions.py : manage ACL of SQL share datasets

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

# Troubleshooting

### NotImplementedError: fetchdata.py requires Python 2.7.

If you see this error message, you are using an old version of Python. This tool requires Python 2.7. (_Note_: This is shorthand for Python 2.7.**anything** --- as long as the first two numbers are 2.7. At the time of writing, the newest version is 2.7.5)

Let's see if you have a newer version of Python on your computer:

    python2.7 --version

a. If this check works, you will see a message like
    
        Python 2.7.5
    
   In this case, all you need to do is invoke the tool using `python2.7 fetchdata.py` instead of `python fetchdata.py`.

b. If this check fails, you need to [download and install Python 2.7](http://www.python.org/download/) for your computer. At the time of writing, the latest version is Python 2.7.5.

   After you download and install Python 2.7, try the above check again. You should be good to go!
   
### Troubleshooting other problems

Please [create a new issue](https://github.com/uwescience/sqlshare-pythonclient/issues/new) in this project's issues tracker. Try to describe the problem in as much detail as possible, and someone will get back to you soon.
