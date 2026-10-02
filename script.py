import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
try:
    s.connect(('127.0.0.1', 6379))
    print("connected")
    s.sendall(b"*1\r\n$4\r\nPING\r\n")
    print("sent PING, waiting for reply...")
    print(s.recv(1024))
except socket.timeout:
    print("TIMED OUT")
s.close()