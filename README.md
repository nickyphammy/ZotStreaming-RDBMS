# CS122A-Python-and-MySQL

- python -m pip install mysql-connector-python

STEPS TO TEST test_data AND USE LOCAL MySQL DATABASE
1. Install MySQL (https://dev.mysql.com/downloads/)
2. Setup commands to create MySQL Database
    - \connect root@localhost
    - \sql
    - SET GLOBAL local_infile = 1;
    - CREATE DATABASE IF NOT EXISTS cs122a;
      USE cs122a;
    - CREATE USER IF NOT EXISTS 'test'@'%' IDENTIFIED BY 'password';
      GRANT ALL PRIVILEGES ON cs122a.* TO 'test'@'%';
      FLUSH PRIVILEGES;
3. Loading the database (database already created), opening up the MySQL shell again
    - \connect root@localhost
    - \sql
    - USE cs122a;
4. After running command in source code command line, run MySQL command in the MySQL shell to see if data has been updated.