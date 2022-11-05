# SALARY PER HOUR

## Pre Requisites
Before proceeding make sure you have following apps installed in your device
- PostgreSQL
- Python v3.xx

## Execution
Move to `Python Files` directory
```sh
#Windows
$ cd '.\Python Files\'

#Linux
$ cd Python\ Files/
```

Install the required libraries by running the following command

```sh
$ pip3 install -r requirements.txt
```

Then update `.env` files based on your DB configuration

```sh
DB_USER=<user>
DB_PASSWORD=<password>
DB_HOST=<host>
DB_SCHEMA=<schema>
```

Last execute the python script
```sh
#initial/replace
$ python3 load_csv_files.py

#incremental
$ python3 load_csv_files_incremental.py
```