import mysql.connector
from datetime import datetime
import pandas as pd
import logging

class ModbusDataManager:
    def __init__(self, mysql_config=None):
        self.mysql_config = mysql_config or {
            'host': 'localhost',
            'user': 'root',
            'password': 'Ashish#4226',
            'database': 'solar'
        }
        self.parameter_descriptions = {
            'parameter_1': 'Current L1',
            'parameter_2': 'Current L2',
            'parameter_3': 'Current L3',
            'parameter_4': 'Current N',
            'parameter_5': 'Voltage L1-L2',
            'parameter_6': 'Voltage L2-L3',
            'parameter_7': 'Voltage L3-L1',
            'parameter_8': 'Voltage Average L-L',
            'parameter_9': 'Voltage L1-N',
            'parameter_10': 'Voltage L2-N',
            'parameter_11': 'Voltage L3-N',
            'parameter_12': 'Active Power L1',
            'parameter_13': 'Active Power L2',
            'parameter_14': 'Active Power L3',
            'parameter_15': 'Active Power Total',
            'parameter_16': 'Frequency',
            'parameter_17': 'Power Factor L1',
            'parameter_18': 'Active Energy'
        }

    def get_connection(self):
        try:
            return mysql.connector.connect(**self.mysql_config)
        except mysql.connector.Error as err:
            logging.error(f"Error connecting to MySQL: {err}")
            raise

    def ensure_table_exists(self, cursor, table_name):
        """Ensure the meter table exists"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp DATETIME NOT NULL,
            meter_id INT NOT NULL,
            {', '.join([f'parameter_{i} FLOAT' for i in range(1, 19)])},
            PRIMARY KEY (timestamp, meter_id)
        )
        """
        cursor.execute(create_table_query)

    def insert_data_batch(self, slave_id, timestamp, parameter_values):
        """Insert all parameters for a timestamp at once"""
        if len(parameter_values) != 18:
            logging.error(f"Expected 18 parameters, got {len(parameter_values)}")
            return

        conn = self.get_connection()
        cursor = conn.cursor()
        table_name = f"meter_{slave_id}"
        
        try:

            self.ensure_table_exists(cursor, table_name)
            

            parameters = [f"parameter_{i+1}" for i in range(18)]
            placeholders = ", ".join(["%s"] * 20)  
            
            
            columns = ["timestamp", "meter_id"] + parameters
            update_stmt = ", ".join([f"{p} = VALUES({p})" for p in parameters])
            
            query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_stmt}
            """
            
            
            values = [timestamp, slave_id] + parameter_values
            cursor.execute(query, values)
            conn.commit()
            
        except Exception as e:
            logging.error(f"Error batch inserting data: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def get_latest_data_matrix(self):
        """Get the latest complete data in matrix format from all meter tables"""
        conn = self.get_connection()
        latest_data = []

        try:
            for meter_id in range(1, 8):
                table_name = f"meter_{meter_id}"


                cursor = conn.cursor()
                check_table_query = f"SHOW TABLES LIKE '{table_name}'"
                cursor.execute(check_table_query)
                if not cursor.fetchone():
                    continue


                query = f"""
                SELECT * FROM {table_name}
                WHERE timestamp = (
                    SELECT MAX(timestamp) FROM {table_name}
                    WHERE {' AND '.join([f'parameter_{i} IS NOT NULL' for i in range(1, 19)])}
                )
                """
                
                df = pd.read_sql_query(query, conn)
                cursor.close()

                if not df.empty:
                    for parameter_num in range(1, 19):
                        parameter_name = f"parameter_{parameter_num}"
                        parameter_key = f"Parameter_{parameter_num-1}"  
                        desc = self.parameter_descriptions.get(parameter_name, '')
                        if parameter_name in df.columns:
                            latest_data.append({
                                'slave_id': meter_id,
                                'parameter_name': parameter_key,
                                'description': desc,
                                'value': df[parameter_name].iloc[0]
                            })

            if latest_data:
                df = pd.DataFrame(latest_data)
                matrix = df.pivot(index=['parameter_name', 'description'], 
                                columns='slave_id', 
                                values='value')

            
                matrix = matrix.sort_index(level=['parameter_name', 'description'])  
                matrix = matrix.sort_index(axis=1)  
                return matrix

            return pd.DataFrame()

        finally:
            conn.close()
