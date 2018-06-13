from gevent import monkey
monkey.patch_all()

import sqlite3

import gevent
import json
import os
import requests
import socket
import sys
from base64 import b32decode
from binascii import hexlify, unhexlify
from collections import Counter
from ConfigParser import ConfigParser
from ipaddress import ip_network

from protocol import (
    ONION_PREFIX,
    TO_SERVICES,
    Connection,
    ConnectionError,
    ProtocolError,
)
from utils import new_redis_conn, get_keys, ip_to_network

def create_table (database):
    c = database.cursor()
    c.execute('''DROP TABLE IF EXISTS NODES''')
    c.execute('''CREATE TABLE NODES (ip text, port text, UNIQUE(ip, port))''')
    

def call_crawler (addr, port, magic, timeout, database):
    """
    Establishes connection with a node to:
    1) Send version message
    2) Receive version and verack message
    3) Send getaddr message
    4) Receive addr message containing list of peering nodes
    Stores state and height for node in Redis.
    """
    c = database.cursor()

    handshake_msgs = []
    addr_msgs = []

    services = 0
    proxy = ""
    height = 1

    conn = Connection(
                      (str(addr), int(port)),
                      ("0.0.0.0", 0),
                      magic_number = magic, 
                      socket_timeout=timeout,
                      proxy=proxy,
                      protocol_version=70015,
                      to_services=services,
                      from_services=0,
                      user_agent="hanchon",
                      height=height,
                      relay=0)
    try:
        print "Connecting to %s" % (str(conn.to_addr))
        conn.open()
        handshake_msgs = conn.handshake()
    except (ProtocolError, ConnectionError, socket.error) as err:
        print("%s: %s", conn.to_addr, err)

    if len(handshake_msgs) > 0:
        try:
            conn.getaddr(block=False)
        except (ProtocolError, ConnectionError, socket.error) as err:
            print "%s: %s" % (str(conn.to_addr), str(err))
        else:
            addr_wait = 0
            while addr_wait < 30:
                addr_wait += 1
                gevent.sleep(0.3)
                try:
                    msgs = conn.get_messages(commands=['addr'])
                except (ProtocolError, ConnectionError, socket.error) as err:
                    print "%s: %s" % (str(conn.to_addr), str(err))
                    break
                if msgs and any([msg['count'] > 1 for msg in msgs]):
                    addr_msgs = msgs
                    break

        version_msg = handshake_msgs[0]
        from_services = version_msg.get('services', 0)
        if from_services != services:
            print "%s Expected %d, got %d for services" % (str(conn.to_addr), services, from_services)
        print "Start insertion addrs from:", str(addr)
        try:
            for one_addr in addr_msgs[0]['addr_list']:
                if one_addr['ipv4'] != "":
                    c.execute("INSERT OR IGNORE INTO NODES VALUES ('"+one_addr['ipv4']+"','"+str(one_addr['port'])+"')")
        except:
            pass

        database.commit()
        print "End insertion addrs from:", str(addr)

    conn.close()

if __name__ == '__main__':

    database_name = 'nodes.db'
    starting_addr = "190.123.23.15"
    starting_port = "8333"
    magic_number = "\xe3\xe1\xf3\xe8" # BCH
    # magic_numer = "f9beb4d9", # BTC
    timeout = 120

    conn = sqlite3.connect(database_name)

    create_table(conn)

    call_crawler(starting_addr, starting_port, magic_number, timeout, conn)

    c = conn.cursor()

    rows = c.execute("SELECT * FROM NODES")

    data = []
    for row in rows:
        data.append(row)

    # TODO: call the crawlers for each node in different threads
    for row in data:
        call_crawler(row[0], row[1], magic_number, timeout, conn)

    conn.close()

    