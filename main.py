import socket
from datetime import datetime, timedelta
from threading import Thread, Lock


def server():
    hostname = '0.0.0.0'
    port = 6379

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((hostname, port))
    server_socket.listen(5)

    try:
        while True:
            conn, _ = server_socket.accept()

            # Mark thread as daemon so it won't block server shutdown
            t = Thread(target=handle_client, args=(conn,), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        server_socket.close()


def handle_client(conn):
    should_exit = False
    buffer = b""

    with conn:
        while not should_exit:
            try:
                data = conn.recv(1024)
                if not data:
                    break

                buffer += data

                while buffer:
                    response, should_exit, new_buffer = command_parser(buffer, conn)

                    if should_exit:
                        break

                    if response:
                        conn.sendall(response)

                    # Incomplete message: no progress, wait for more data
                    if new_buffer == buffer:
                        break

                    buffer = new_buffer

            except Exception:
                break




def command_parser(buffer, conn):
    """
    Evaluates command elements and generates the appropriate RESP response.
    Returns: (response_bytes, should_exit, remaining_buffer)
    """
    if not buffer:
        return None, False, b""

    elements, parsed_bytes, is_error, is_incomplete = parse_resp(buffer)

    if is_incomplete:
        return None, False, buffer

    if is_error:
        return b"-Parsing error!\r\n", False, b""

    remaining_buffer = buffer[parsed_bytes:]

    if not elements:
        return b"-Parsing error!\r\n", False, remaining_buffer

    command = elements[0].upper()

    if command == b"PING":
        if len(elements) != 1:
            return b"-Parsing error!\r\n", False, remaining_buffer
        return b"+PONG\r\n", False, remaining_buffer

    elif command == b"ECHO":
        if len(elements) != 2:
            return b"-Parsing error!\r\n", False, remaining_buffer
        echo_message = elements[1]
        response = b"$" + str(len(echo_message)).encode() + b"\r\n" + echo_message + b"\r\n"
        return response, False, remaining_buffer

    elif command == b"SET":

        if len(elements) < 3:
            return b"-Parsing error!\r\n", False, remaining_buffer

        key = elements[1]
        value = elements[2]
        expire_at = None

        if len(elements) >= 5 and elements[3].upper() == b"EX":
            try:
                exp_seconds = int(elements[4].decode())
                if exp_seconds <= 0:
                    return b"-ERR invalid expire time in set\r\n", False, remaining_buffer
                expire_at = datetime.now() + timedelta(seconds=exp_seconds)
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n", False, remaining_buffer

        response = b"+OK\r\n"
        set_command(key, value, expire_at)
        return response, False, remaining_buffer

    elif command == b"GET":
        if len(elements) != 2:
            return b"-Parsing error!\r\n", False, remaining_buffer
        key = elements[1]
        response = get_command(key)
        return response, False, remaining_buffer

    elif command == b"DEL":
        if len(elements) != 2:
            return b"-Parsing error!\r\n", False, remaining_buffer
        key = elements[1]
        response = del_command(key)
        return response, False, remaining_buffer

    elif command == b"SUBSCRIBE":
        if len(elements) != 2:
            return b"-Parsing error!\r\n", False, remaining_buffer
        channel = elements[1]
        # response = b"+OK\r\n"
        count = subscribe(channel, conn)

        response = (
                b"*3\r\n"
                b"$9\r\nsubscribe\r\n"
                b"$" + str(len(channel)).encode() + b"\r\n" + channel + b"\r\n"
                b":" + str(count).encode() + b"\r\n"
        )
        return response, False, remaining_buffer

    elif command == b"PUBLISH":
        if len(elements) != 3:
            return b"-Parsing error!\r\n", False, remaining_buffer
        channel = elements[1]
        data = elements[2]
        response = publish(channel, data)
        return response, False, remaining_buffer

    elif command == b"EXIT":
        if len(elements) != 1:
            return b"-Parsing error!\r\n", False, remaining_buffer
        return None, True, remaining_buffer

    else:
        return b"-Unknown command!\r\n", False, remaining_buffer





def parse_resp(buffer):
    """
    Parses a RESP array from a raw byte buffer.
    Returns: (elements, parsed_bytes_count, is_error, is_incomplete)
    """
    if not buffer.startswith(b"*"):
        return None, 0, True, False

    crlf = buffer.find(b"\r\n")
    if crlf == -1:
        return None, 0, False, True

    try:
        num_elements = int(buffer[1:crlf])
        if num_elements <= 0:
            return None, 0, True, False
    except ValueError:
        return None, 0, True, False

    elements = []
    pos = crlf + 2

    for _ in range(num_elements):
        if pos >= len(buffer):
            return None, 0, False, True

        if buffer[pos:pos + 1] != b"$":
            return None, 0, True, False

        next_crlf = buffer.find(b"\r\n", pos)
        if next_crlf == -1:
            return None, 0, False, True

        try:
            str_len = int(buffer[pos + 1:next_crlf])
            if str_len < 0:
                return None, 0, True, False
        except ValueError:
            return None, 0, True, False

        pos = next_crlf + 2

        if pos + str_len + 2 > len(buffer):
            return None, 0, False, True

        value = buffer[pos:pos + str_len]

        if buffer[pos + str_len:pos + str_len + 2] != b"\r\n":
            return None, 0, True, False

        elements.append(value)
        pos += str_len + 2

    return elements, pos, False, False


data_store = {}
store_lock = Lock()

pubsub_channels = {}

def subscribe(channel, conn):
    with store_lock:
        if channel not in pubsub_channels:
            pubsub_channels[channel] = []

        if conn not in pubsub_channels[channel]:
            pubsub_channels[channel].append(conn)

        return len(pubsub_channels[channel])
def publish(channel, data):
    with store_lock:
        count = 0
        if channel in pubsub_channels:
            push_msg = (
                    b"*3\r\n"
                    b"$7\r\nmessage\r\n"
                    b"$" + str(len(channel)).encode() + b"\r\n" + channel + b"\r\n"
                    b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"
            )
            for conn in list(pubsub_channels[channel]):
                try:
                    conn.sendall(push_msg)
                    count += 1
                except Exception:
                    pubsub_channels[channel].remove(conn)

        return b":" + str(count).encode() + b"\r\n"


def set_command(key: bytes, value: bytes, expire_at) -> None:
    with store_lock:
        data_store[key] = (value, expire_at)


def get_command(key: bytes) -> bytes | None:
    with store_lock:
        if key not in data_store:
            return b"$-1\r\n"
        value, expire_at = data_store[key]

        if expire_at is not None and datetime.now() >= expire_at:
            data_store.pop(key)
            return b"$-1\r\n"

        return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"



def del_command(key: bytes):
    with store_lock:
        if key in data_store.keys():
            data_store.pop(key)
        return b"+OK\r\n"



if __name__ == "__main__":
    server()