# Hadoop Log Miner
==================


## Pre-install
  - MySQL-python : `# pip-2.7 install MySQL-python` or `# easy_install-2.7 install MySQL-python`

## Usage
  `./hadoop_log_miner.py loglevel logdate`

  - example:
  `./hadoop_log_miner.py INFO 2012-12-08`

  if you want all loglevels or dates, just set the cli argument as 'all',
  eg: `./hadoop_log_miner.py all all`

## Note
 - This script uses Hive's Python Thrift interface to interact with Hive, so before play with it 
you should start a Hive Thrift server with command `hive --service hiveserver` (set the port
to 10000 or leave it as default). 

 - The script also connects to MySQL server via `MySQL-python`.
Remember to start `mysqld` on *localhost:3306* (for me, `# rc.d start mysqld` will work, most system
can use `# systemctl start mysqld`)

 - This script assumes your hadoop logs are placed in `$HADOOP_HOME/logs/`. Change the source code 
 if this does not suit you.

 - MySQL configurations are also hard-coded. Change the source code if the default values do not work.


