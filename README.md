# ZotStreaming RDBMS

## CS122A project: Python and MySQL
This project implements a command-line interface for managing the ZotStreaming platform using Python and MySQL for the CS122A database systems course.

## Setup Instructions

### Install required packages:
```sh
python -m pip install mysql-connector-python
```

### Install MySQL:
- Download from [MySQL website](https://dev.mysql.com/downloads/)

### Setup MySQL Database:
```sql
\connect root@localhost

SET GLOBAL local_infile = 1;
CREATE DATABASE IF NOT EXISTS cs122a; 
USE cs122a;
CREATE USER IF NOT EXISTS 'test'@'%' IDENTIFIED BY 'password'; 
GRANT ALL PRIVILEGES ON cs122a.* TO 'test'@'%'; 
FLUSH PRIVILEGES;
```

### To view database after running commands:
```sql
\connect root@localhost
USE cs122a;
```

Run commands through the Python interface, then use MySQL shell to verify data has been properly updated.
