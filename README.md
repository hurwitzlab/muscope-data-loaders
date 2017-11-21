# muscope-data-loaders
Load and validate muSCOPE data.


## Requirements
Python 3.6+ is required. In addition an iRODS client must be present.

## Install
Use a virtual environment!

```
$ python3.6 -m venv ~/mudl
$ source ~/mudl/bin/activate
(mudl) $ pip install -r requirements.txt
(mudl) $ write_models -o muscope_loader/models.py -u mysql+pymysql://imicrobe:<password>@localhost/muscope
```

## Usage

```
(mudl) $ python muscope_loader/cruise/HL2A.py
```
