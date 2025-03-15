# CS122A-Python-and-MySQL

- python -m pip install mysql-connector-python

STEPS TO TEST test_data AND USE LOCAL MySQL DATABASE
1. Install MySQL (https://dev.mysql.com/downloads/)
2. Setup commands in MySQL Shell
    - \connect root@localhost
    - \sql
    - SET GLOBAL local_infile = 1;
    - CREATE DATABASE IF NOT EXISTS cs122a;
      USE cs122a;
    - CREATE USER IF NOT EXISTS 'test'@'%' IDENTIFIED BY 'password';
      GRANT ALL PRIVILEGES ON cs122a.* TO 'test'@'%';
      FLUSH PRIVILEGES;