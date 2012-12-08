#!/usr/bin/env python
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from MySQLdb import escape_string
import MySQLdb
import sys
import os
import time

HADOOP_LOG_PATH = os.environ['HADOOP_HOME'] + '/logs/'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['INFO', 'WARN', 'ERROR']

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'hadoop',
    'passwd': 'hadoop',
    'db': 'hadoop',
    'charset': 'latin1',
    }

MYSQL_CREATE_TABLE = "CREATE TABLE %s (\
                        id INT NOT NULL AUTO_INCREMENT, \
                        logtime datetime, \
                        level varchar(5), \
                        class varchar(255), \
                        msg varchar(255), \
                        PRIMARY KEY (id) \
                        ) ENGINE = MyISAM"
MYSQL_INSERT_LOG = "INSERT INTO %(table)s (logtime, level, class, msg) VALUES\
                  ('%(logtime)s', '%(level)s', '%(class)s', '%(msg)s')"

HIVE_HOST = 'localhost'
HIVE_PORT = 10000
HIVE_TABLE = 'hadooplog'
HIVE_CREATE_TABLE = "CREATE TABLE %s (\
                logdate STRING, \
                logtime STRING, \
                level STRING, \
                class STRING, \
                msg0 STRING, \
                msg1 STRING, \
                msg2 STRING, \
                msg3 STRING, \
                msg4 STRING, \
                msg5 STRING) \
                ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' \
                STORED AS TEXTFILE"
HIVE_LOAD = "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s"
HIVE_QUERY = "SELECT * FROM %s"

class HiveClientError(Exception):
  pass

class MySQLClientError(Exception):
  pass


class HadoopLogMiner:
  def __init__(self, hive_table = HIVE_TABLE):
    self.hive_table = hive_table
    pass

  def connect_mysql(self):
    try:
      if self.mysql:
        return
    except AttributeError:
      pass
    self.mysql = MySQLdb.connect(**MYSQL_CONFIG)
    self.cursor = self.mysql.cursor()

  def init_mysql_table(self, mysql_table):
    try:
      if self.mysql:
        pass
    except AttributeError:
      raise MySQLClientError("Please connect to MySQL then execute MySQL queries")
    if self.cursor.execute('show tables') > 0:
      results = self.cursor.fetchall()
      for tables in results:
        if mysql_table in tables:
          self.mysql_table = mysql_table
          return
    self.mysql_table = mysql_table
    print('mysql> %s' % MYSQL_CREATE_TABLE % self.mysql_table)
    result = self.cursor.execute(MYSQL_CREATE_TABLE % self.mysql_table)
    result = self.cursor.fetchone()
    self.mysql.commit()

  def save_hive_to_mysql(self, date = None, level = None):
    try:
      if self.mysql:
       pass
    except AttributeError:
      raise MySQLClientError("Please connect to MySQL then execute MySQL queries")
    table = ""
    if date == None:
      table += "alldate_"
    else:
      table += date + '_'
    if level == None:
      table += "all"
    else:
      table += level
    self.init_mysql_table(table.replace('-', '_'))
    for log in self.query_hive_logs(date, level):
      log['table'] = self.mysql_table
      print('mysql> %s' % MYSQL_INSERT_LOG % log)
      self.cursor.execute(MYSQL_INSERT_LOG % log)
    self.mysql.commit()
    print("All matching logs are committed to MySQL")

  def connect_hive(self):
    try:
      if self.hive != None:
        return
    except AttributeError:
      self.hive = HiveClient(HIVE_HOST, HIVE_PORT)
      self.hive.connect()

  def init_hive_table(self, hive_table = HIVE_TABLE):
    try:
      if self.hive != None:
        pass
    except AttributeError:
      raise HiveClientError("Please connect to Hive then execute Hive queries")

    self.hive_table = hive_table
    tables = self.hive.execute("show tables")
    if tables != None and self.hive_table in tables:
      print('Hive table %s already exists' % self.hive_table)
      return False
    else:
      result = self.hive.execute(HIVE_CREATE_TABLE % self.hive_table, n = 1)
      print('Create Hive table %s, return: %s' % (self.hive_table, result))
      return True

  def load_all_logfiles(self, path = HADOOP_LOG_PATH):
    for filepath in os.listdir(path):
      if os.path.isdir(os.path.join(filepath)):
        pass
      elif filepath.find('log') >= 0:
        try:
          self.load_logfile(os.path.join(path, filepath))
        except HiveServerException:
          print("Failed to load %s" % os.path.join(path, filepath))

  def load_logfile(self, path):
    try:
      if self.hive and self.hive_table:
        pass
    except AttributeError:
      raise HiveClientError("Please connect to hive and specify Hive table name")
    result = self.hive.execute(HIVE_LOAD % (path, self.hive_table))

  def query_hive_logs(self, date = None, level = None):
    try:
      if self.hive and self.hive_table:
        pass
    except AttributeError:
      raise HiveClientError("Please connect to hive and specify Hive table name")
    query = HIVE_QUERY % self.hive_table
    if level and date:
      query += " WHERE logdate='%s' AND level='%s'" % (date, level)
    elif level:
      query += " WHERE level='%s'" % level
    elif date:
      query += " WHERE logdate='%s'" % date
    results = self.hive.execute(query)
    for result in results:
      log = self.extract_log(result)
      if log != None:
        yield log

  def extract_log(self, raw):
    log = {}
    try:
      fields = raw.split('\t')
      # convert to time and back to make time valid
      logtime = time.strptime(fields[0] + ' ' + fields[1].split(',')[0], TIME_FORMAT)
      log['logtime'] = time.strftime( TIME_FORMAT, logtime)
      if fields[2] in LOG_LEVELS:
        log['level'] = escape_string(fields[2])
      else:
        raise ValueError()
      log['class'] = escape_string(fields[3])
      log['msg'] = escape_string(' '.join([m for m in fields[4:] if m != "NULL"]))
    except Exception as e:
      # print(e.message)
      return None
    return log


class HiveClient:
  def __init__(self, host, port):
    self.host = host
    self.port = port

  def connect(self):
    try:
      if self.client:
        return
    except AttributeError:
      pass
    try:
      socket = TSocket.TSocket(self.host, self.port)
      transport =  TTransport.TBufferedTransport(socket)
      protocol = TBinaryProtocol.TBinaryProtocol(transport)
      self.client = ThriftHive.Client(protocol)
      transport.open()
    except Thrift.TException as te:
      raise HiveClientError('Failed to connect to Thrift server\n' +  te.message)

  def disconnect(self):
    # TODO
    self.client = None

  def execute(self, query, n = 0):
    try:
      if self.client:
        pass
    except AttributeError:
      raise HiveClientError("Client is not connected to any Thrift server")
    print("hive> %s;" % query)
    self.client.execute(query)
    if n <= 0:
      return self.client.fetchAll()
    else:
      return self.client.fetchN(n)


if __name__ == '__main__':
  # hive = HiveClient('localhost', 10000)
  # hive.connect()
  # for query in sys.stdin:
  #   print(hive.execute(query))
  if len(sys.argv) < 3:
    sys.exit("Usage: %s loglevel(or all)  logdate(or all)" % sys.argv[0])
  if sys.argv[1] not in LOG_LEVELS:
    level = None
  else:
    level = sys.argv[1]

  if sys.argv[2] == "all":
    date = None
  else:
    date = sys.argv[2]

  miner = HadoopLogMiner()
  miner.connect_hive()
  if miner.init_hive_table():
    miner.load_all_logfiles()
  miner.connect_mysql()
  miner.save_hive_to_mysql(date = date, level = level)

