import socket
import struct


def calculate_crc(data):
    """Calculate Modbus CRC-16."""
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for _ in range(8):
            if crc & 0x0001:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    return crc


def build_modbus_request(slave_id, function_code, start_address, quantity):
    """Build Modbus RTU request."""
    # Construct the request (without CRC)
    request = struct.pack('>BBHH', slave_id, function_code, start_address, quantity)

    # Calculate CRC
    crc = calculate_crc(request)

    # Append CRC (low byte first, then high byte)
    crc_low = crc & 0xFF
    crc_high = (crc >> 8) & 0xFF

    request += struct.pack('BB', crc_low, crc_high)

    return request


def establish_connection(ip, port):
    """Establish connection"""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Connect to the Modbus server
        client_socket.connect((ip, port))
        return client_socket
    except Exception as e:
        print(f"Error: {e}")


def send_modbus_request(client_socket, request):
    """Send the Modbus request over TCP."""
    try:
        # Send the request
        client_socket.send(request)

        # Receive the response
        response = client_socket.recv(1024)  # Buffer size

        return response
    except Exception as e:
        print(f"Error: {e}")


def parse_float_values(response, data_type):
    # Extract the data bytes (exclude Slave ID, Function Code, Byte Count, CRC)
    data = response[3:-2]

    # Unpack the data in 4-byte chunks (each float is 4 bytes)
    num_floats = len(data) // 4 if data_type == 'f' else 1  # The number of floats (10 registers = 5 floats)
    floats = struct.unpack('>' + data_type * num_floats, data)  # Big-endian float unpacking

    values = []

    for j, float_value in enumerate(floats):
        values.append(format(float_value, '.1f'))

    return values
