def decimal_to_hex(decimal_number):
    return int(hex(decimal_number), 16)

# import mysql.connector
# from datetime import datetime
# import pandas as pd
# import logging

# class ModbusDataManager:
#     def __init__(self, mysql_config=None):
#         self.mysql_config = mysql_config or {
#             'host': 'localhost',
#             'user': 'root',
#             'password': 'Ashish#4226',
#             'database': 'solar'
#         }
#         self.parameter_descriptions = {
#             'parameter_1': 'Current L1',
#             'parameter_2': 'Current L2',
#             'parameter_3': 'Current L3',
#             'parameter_4': 'Current N',
#             'parameter_5': 'Voltage L1-L2',
#             'parameter_6': 'Voltage L2-L3',
#             'parameter_7': 'Voltage L3-L1',
#             'parameter_8': 'Voltage Average L-L',
#             'parameter_9': 'Voltage L1-N',
#             'parameter_10': 'Voltage L2-N',
#             'parameter_11': 'Voltage L3-N',
#             'parameter_12': 'Active Power L1',
#             'parameter_13': 'Active Power L2',
#             'parameter_14': 'Active Power L3',
#             'parameter_15': 'Active Power Total',
#             'parameter_16': 'Frequency',
#             'parameter_17': 'Power Factor L1',
#             'parameter_18': 'Active Energy'
#         }

#     def get_connection(self):
#         try:
#             return mysql.connector.connect(**self.mysql_config)
#         except mysql.connector.Error as err:
#             logging.error(f"Error connecting to MySQL: {err}")
#             raise

#     def insert_data(self, slave_id, parameter_name, value):
#         """Insert new data point into appropriate meter table"""
#         conn = self.get_connection()
#         cursor = conn.cursor()

#         table_name = f"meter_{slave_id}"
#         # Convert from Param_X to parameter_X format
#         parameter_num = int(parameter_name.split('_')[1]) + 1
#         parameter_name = f"parameter_{parameter_num}"
#         timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#         try:
#             # Ensure the table exists for the slave ID
#             create_table_query = f"""
#             CREATE TABLE IF NOT EXISTS {table_name} (
#                 timestamp DATETIME NOT NULL,
#                 meter_id INT NOT NULL,
#                 {', '.join([f'parameter_{i} FLOAT' for i in range(1, 19)])},
#                 PRIMARY KEY (timestamp, meter_id)
#             )
#             """
#             cursor.execute(create_table_query)

#             # Insert or update the specific parameter
#             upsert_query = f"""
#             INSERT INTO {table_name} (timestamp, meter_id, {parameter_name})
#             VALUES (%s, %s, %s)
#             ON DUPLICATE KEY UPDATE {parameter_name} = VALUES({parameter_name})
#             """
#             cursor.execute(upsert_query, (timestamp, slave_id, value))

#             conn.commit()
#         except Exception as e:
#             logging.error(f"Error inserting data: {e}")
#             conn.rollback()
#         finally:
#             cursor.close()
#             conn.close()


#     def get_latest_data_matrix(self):
#         """Get the latest data in matrix format from all meter tables"""
#         conn = self.get_connection()
#         latest_data = []

#         try:
#             for meter_id in range(1, 8):
#                 table_name = f"meter_{meter_id}"

#                 # Ensure the table exists
#                 cursor = conn.cursor()
#                 check_table_query = f"SHOW TABLES LIKE '{table_name}'"
#                 cursor.execute(check_table_query)
#                 if not cursor.fetchone():
#                     continue

#                 # Get the latest data from the table
#                 query = f"""
#                 SELECT * FROM {table_name} 
#                 WHERE timestamp = (SELECT MAX(timestamp) FROM {table_name})
#                 """
#                 df = pd.read_sql_query(query, conn)

#                 if not df.empty:
#                     for parameter_num in range(1, 19):
#                         parameter_name = f"parameter_{parameter_num}"
#                         parameter_key = f"Parameter_{parameter_num}"
#                         desc = self.parameter_descriptions.get(parameter_name, '')
#                         if parameter_name in df.columns:
#                             latest_data.append({
#                                 'slave_id': meter_id,
#                                 'parameter_name': parameter_key,
#                                 'description': desc,
#                                 'value': df[parameter_name].iloc[0]
#                             })

#             if latest_data:
#                 df = pd.DataFrame(latest_data)
#                 matrix = df.pivot(index=['parameter_name', 'description'], 
#                                 columns='slave_id', 
#                                 values='value')
#                 return matrix

#             return pd.DataFrame()
#          # printing dataframe
         
#         except Exception as e:
#             logging.error(f"Error fetching latest data: {e}")
#             return pd.DataFrame()

#         finally:
#             conn.close()





 # Load Analysis Tab
            # dbc.Tab([
            #     dbc.Row([
            #         dbc.Col([
            #             html.H4("Load Meter Comparison", className="text-center mb-3"),
            #             dcc.Graph(id='load-comparison-box')
            #         ], width=6),
            #         dbc.Col([
            #             html.H4("Load Distribution Over Time", className="text-center mb-3"),
            #             dcc.Graph(id='load-distribution-area')
            #         ], width=6)
            #     ])
            # ], label="Load Analysis"),

            # Anomaly Detection Tab
            # dbc.Tab([
            #     dbc.Row([
            #         dbc.Col([
            #             html.H4("Power Anomaly Heatmap", className="text-center mb-3"),
            #             dcc.Graph(id='anomaly-heatmap')
            #         ], width=12)
            #     ], className="mb-4"),
            #     dbc.Row([
            #         dbc.Col([
            #             html.H4("Power Deviation Analysis", className="text-center mb-3"),
            #             dcc.Graph(id='power-deviation-scatter')
            #         ], width=12)
            #     ])
            # ], label="Anomaly Detection"),
    # @app.callback(
    #     Output('load-comparison-box', 'figure'),
    #     Input('interval-component', 'n_intervals')
    # )
    # def update_load_comparison(n):
    #     try:
    #         # Get historical data for the last hour
    #         conn = data_manager.get_connection()
    #         end_time = datetime.now()
    #         start_time = end_time - timedelta(hours=1)
            
    #         # Collect data for all load meters
    #         data = []
    #         labels = []
    #         for meter_id in range(1, 7):
    #             query = f"""
    #             SELECT parameter_15 as power
    #             FROM meter_{meter_id}
    #             WHERE timestamp BETWEEN %s AND %s
    #             AND parameter_15 IS NOT NULL
    #             """
    #             df = pd.read_sql_query(query, conn, params=[start_time, end_time])
    #             data.append(df['power'].values)
    #             labels.extend([f'Meter {meter_id}'] * len(df))
            
    #         conn.close()

    #         fig = go.Figure()
    #         fig.add_trace(
    #             go.Histogram(
    #                 x=np.concatenate(data),  # Flatten the data
    #                 name='Power Distribution',
    #                 marker=dict(color='blue'),
    #                 opacity=0.75
    #             )
    #         )

    #         fig.update_layout(
    #             title='Power Distribution Across Load Meters',
    #             xaxis_title='Power (kW)',
    #             yaxis_title='Frequency',
    #             height=500,
    #             template='plotly_white'
    #         )

    #         return fig
    #     except Exception as e:
    #         print(f"Error updating load comparison: {e}")
    #         return go.Figure()

    # @app.callback(
    #     Output('load-distribution-area', 'figure'),
    #     Input('interval-component', 'n_intervals')
    # )
    # def update_load_distribution_area(n):
    #     try:
    #         conn = data_manager.get_connection()
    #         end_time = datetime.now()
    #         start_time = end_time - timedelta(hours=1)
            
    #         dfs = []
    #         for meter_id in range(1, 7):
    #             query = f"""
    #             SELECT timestamp, parameter_15 as power
    #             FROM meter_{meter_id}
    #             WHERE timestamp BETWEEN %s AND %s
    #             AND parameter_15 IS NOT NULL
    #             ORDER BY timestamp
    #             """
    #             df = pd.read_sql_query(query, conn, params=[start_time, end_time])
    #             df['Meter'] = f'Meter {meter_id}'
    #             dfs.append(df)
            
    #         conn.close()
            
    #         # Combine all data
    #         combined_df = pd.concat(dfs)

    #         fig = px.area(combined_df, x='timestamp', y='power', color='Meter',
    #                      title='Load Distribution Over Time')

    #         fig.update_layout(
    #             xaxis_title='Time',
    #             yaxis_title='Power (kW)',
    #             height=500
    #         )

    #         return fig
    #     except Exception as e:
    #         print(f"Error updating load distribution area: {e}")
    #         return go.Figure()

    # @app.callback(
    #     Output('anomaly-heatmap', 'figure'),
    #     Input('interval-component', 'n_intervals')
    # )
    # def update_anomaly_heatmap(n):
    #     try:
    #         matrix = data_manager.get_latest_data_matrix()
    #         if matrix.empty:
    #             return go.Figure()

    #         # Calculate z-scores for anomaly detection
    #         z_scores = (matrix - matrix.mean()) / matrix.std()

    #         fig = go.Figure(data=go.Heatmap(
    #             z=z_scores.values,
    #             x=[f'Meter {i}' for i in matrix.columns],
    #             y=[desc for _, desc in matrix.index],
    #             colorscale='RdYlBu',
    #             zmid=0
    #         ))

    #         fig.update_layout(
    #             title='Parameter Anomaly Heatmap (Z-scores)',
    #             height=800,
    #             yaxis_title='Parameters',
    #             xaxis_title='Meters'
    #         )

    #         return fig
    #     except Exception as e:
    #         print(f"Error updating anomaly heatmap: {e}")
    #         return go.Figure()
