# NcaaGameChecker

This repo is forked from UVAMobileDev/NcaaGameChecker
It includes informations about how to run cron jobs using crontab built in on Mac/Linux

## Download

General

```sh
git clone https://github.com/UVAMobileDev/NcaaGameChecker.git
cd NcaaGameChecker
```

### activate virtual environemnt if you are on VScode

`source venv/bin/activate`

## Setup

### MySQL Script

```sh
pip install setuptools
python -m pip install version
python setup.py develop
```

if this doesn't work then make sure to install the modules in the requirements.txt file manually

### Run lambda_function.py

verify if it is working

### Set up cron jobs

[TUTORIAL LINK](https://www.jcchouinard.com/python-automation-with-cron-on-mac/)

Basic steps


1.In the terminal enter `crontab -e`

2.press i to go into edit mode

3.type in `* * * * * /usr/bin/python /path/to/file/<FILENAME>.py /path/to/file/<FILENAME>.log`

4.press esc

5.enter: `:wq`(shortcut for write and quit)

6.After writing the crontab, you will get this message: crontab: installing new crontab,which tells you created the crontab.
