import socketserver
import platform

if platform.system() == "Windows":
    # We "fake" the UnixStreamServer by pointing it to the TCP version
    socketserver.UnixStreamServer = socketserver.TCPServer
    socketserver.UnixDatagramServer = socketserver.UDPServer

import pytest
from pyspark.sql import SparkSession



@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()