#!/usr/bin/python
import time
import socket
import struct
import random

VERSION = 3

def encode_msg(is_atom, path, time, value):
   is_atom_int = 1 if is_atom else 0
   pck_tail = struct.pack(">BBQQ", VERSION, is_atom_int, time, value) + path
   pck_head = struct.pack(">H", len(pck_tail))
   pck = pck_head + pck_tail
   return pck

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 2011))
#s.connect(("yawnhost", 2011))
#msg = encode_msg("stats-foobar-main-%s" % random.randint(1, 1000), 13, 999)
#msg = encode_msg("stats-foobar-main-125", 13, 999)
#while True:
for i in xrange(1, 100000):
    msg = encode_msg(False, "stats-a-foo", 130 * i, 777000)
    s.send(msg)

for i in xrange(1, 100000):
   msg = encode_msg(False, "stats-b-foo", 130 * i, 555000)
   s.send(msg)

for i in xrange(1, 100000):
   msg = encode_msg(False, "stats-c-foo", 130 * i, 333000)
   s.send(msg)


# for i in xrange(1, 4):
#     msg = encode_msg(True, "stats-b-foo", 130 * i, 777)
#     s.send(msg)
#     time.sleep(0.0001)

# for i in xrange(1, 4):
#     msg = encode_msg(True, "stats-c-foo", 130 * i, 1)
#     s.send(msg)
#     time.sleep(0.0001)
# msg = encode_msg(False, "stats-c-foo", 130 * 2, 13)
# s.send(msg)

# for i in xrange(1, 4):
#     msg = encode_msg(True, "stats-d-foo", 130 * i, 2)
#     s.send(msg)
#     time.sleep(0.0001)
# msg = encode_msg(False, "stats-d-foo", 130 * 2, 13)
# s.send(msg)
