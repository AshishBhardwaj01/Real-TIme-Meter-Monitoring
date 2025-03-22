# Import required libraries
import time
from datetime import datetime
import numpy as np
from modbus import build_modbus_request, establish_connection, send_modbus_request, parse_float_values
from utils import decimal_to_hex
from dashboard import start_monitoring

# Main program entry point
if __name__ == "__main__":
    try:
        # MySQL configuration
        mysql_config = {
            'host': '127.0.0.1',
            'user': 'root',
            'password': 'Ashish#4226',
            'database': 'solar'
        }

        # Initialize dashboard and data manager with MySQL configuration
        data_manager = start_monitoring(mysql_config=mysql_config)
        print("\n=== Meter Monitoring System ===")
        print("Dashboard started at http://localhost:8050")

        slave_ids = [1, 2, 3, 4, 5, 6, 7]
        function_code = decimal_to_hex(3)

        start_addresses = [
            [decimal_to_hex(2999), 8, 'f'],   # Currents registers
            [decimal_to_hex(3019), 14, 'f'],  # Voltages registers
            [decimal_to_hex(3053), 8, 'f'],   # Powers registers
            [decimal_to_hex(3109), 2, 'f'],   # Frequency registers
            [decimal_to_hex(3191), 2, 'f'],   # Power Factor registers
            [decimal_to_hex(2699), 2, 'f']    # Active Energy registers
        ]

        server_ip = "10.36.8.108"
        server_port = 502
        interval = 1

        print("\n=== System Configuration ===")
        print(f"Server Address: {server_ip}:{server_port}")
        print(f"Database: MySQL ({mysql_config['host']})")
        print(f"Update Interval: {interval} seconds")
        print(f"Number of Meters: {len(slave_ids)}")
        print("\nPress Ctrl+C to stop the system\n")
        print("=== Starting Data Collection ===")
            
        # Main loop 
        while True:
            try:
                # Establish new connection for each iteration
                client_socket = establish_connection(server_ip, server_port)
                
                # Iterate through each slave device
                for slave_id in slave_ids:
                    # Initialize array for all parameters
                    all_values = []
                    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    try:
                        # Collect all values first
                        for config in start_addresses:
                            start_address = config[0]
                            quantity = config[1]
                    
                            request = build_modbus_request(slave_id, function_code, start_address, quantity)
                            response = send_modbus_request(client_socket, request)
                            
                            if response:
                                float_values = parse_float_values(response, config[2])
                                all_values.extend([float(v) for v in float_values])
                        
                        # Insert all parameters at once if we have complete data
                        if len(all_values) == 18:  # Ensure we have all parameters
                            data_manager.insert_data_batch(slave_id, current_timestamp, all_values)
                            print(f"Meter {slave_id} - Complete data set inserted at {current_timestamp}")
                        else:
                            print(f"Meter {slave_id} - Incomplete data set ({len(all_values)}/18 parameters)")
                            
                    except Exception as e:
                        print(f"Error processing meter {slave_id}: {e}")
                        continue

                # Close connection and wait before next iteration
                client_socket.close()
                time.sleep(interval)
                
            except ConnectionError as e:
                print(f"\nConnection Error: {e}")
                print("Retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            except Exception as e:
                print(f"\nError during data collection: {e}")
                print("Continuing to next iteration...")
                continue
            
    except KeyboardInterrupt:
        print("\n\n=== Stopping Meter Monitoring System ===")
        print("Closing connections and shutting down...")
    
    except Exception as e:
        print(f"\nCritical Error: {e}")
    
    finally:
        print("System shutdown complete.")