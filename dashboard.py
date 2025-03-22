import sqlite3
from datetime import datetime, timedelta
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import pandas as pd
import threading
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from database import ModbusDataManager

def create_dashboard(data_manager):
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

    app.layout = dbc.Container([

        dbc.Row([
            dbc.Col([
                html.H1('Meter Monitoring System', 
                       className='text-primary text-center mb-4'),
                html.Hr()
            ])
        ]),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Total Active Power", className="card-title"),
                        html.H2(id="total-power", className="text-primary"),
                        html.P("Kilowatts (kW)", className="card-text")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Power Factor", className="card-title"),
                        html.H2(id="avg-pf", className="text-success"),
                        html.P("System Average", className="card-text")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Active Energy", className="card-title"),
                        html.H2(id="total-energy", className="text-info"),
                        html.P("Kilowatt Hours (kWh)", className="card-text")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Frequency", className="card-title"),
                        html.H2(id="system-freq", className="text-warning"),
                        html.P("Hertz (Hz)", className="card-text")
                    ])
                ])
            ], width=3),
        ], className="mb-4"),

        dbc.Tabs([

            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        html.H3('Live Data Monitor', className='text-primary mb-3'),
                        html.Div(id='live-table', className='table-responsive')
                    ])
                ], className='mb-4')
            ], label="Monitor"),


            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        html.H4("Power Flow Balance", className="text-center mb-3"),
                        dcc.Graph(id='power-flow-chart')
                    ], width=6),
                    dbc.Col([
                        html.H4("Load Distribution", className="text-center mb-3"),
                        dcc.Graph(id='load-distribution-chart')
                    ], width=6)
                ], className="mb-4"),
                dbc.Row([
                    dbc.Col([
                        html.H4("Power Flow Time Series", className="text-center mb-3"),
                        dcc.Graph(id='power-flow-time-series')
                    ])
                ])
            ], label="Power Flow"),


            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        html.H4("Power Factor Analysis", className="text-center mb-3"),
                        dcc.Graph(id='power-factor-polar')
                    ], width=6),
                    dbc.Col([
                        html.H4("Power Factor Distribution", className="text-center mb-3"),
                        dcc.Graph(id='power-factor-histogram')
                    ], width=6)
                ])
            ], label="Power Quality"),

            # Energy Trends Tab
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        html.H4("Energy Usage Trends", className="text-center mb-3"),
                        dcc.Graph(id='energy-trends-line')
                    ], width=12)
                ], className="mb-4"),
                dbc.Row([
                    dbc.Col([
                        html.H4("Cumulative Energy Usage", className="text-center mb-3"),
                        dcc.Graph(id='cumulative-energy-line')
                    ], width=12)
                ])
            ], label="Energy Trends"),
            
            # Analytics Tab
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H4("Parameter Trends", className="text-center mb-3"),
                                dbc.Row([
                                    dbc.Col([
                                        html.Label('Parameter'),
                                        dcc.Dropdown(
                                            id='parameter-dropdown',
                                            className='mb-2',
                                            value=None
                                        )
                                    ], width=4),
                                    dbc.Col([
                                        html.Label('Meter'),
                                        dcc.Dropdown(
                                            id='meter-dropdown',
                                            className='mb-2',
                                            value=None
                                        )
                                    ], width=4),
                                    dbc.Col([
                                        html.Label('Time Range'),
                                        dcc.Dropdown(
                                            id='timerange-dropdown',
                                            options=[
                                                {'label': 'Last 30 min', 'value': '30M'},
                                                {'label': 'Last 1 hour', 'value': '1H'},
                                                {'label': 'Last 4 hours', 'value': '4H'},
                                                {'label': 'Last 24 hours', 'value': '24H'}
                                            ],
                                            value='1H',
                                            className='mb-2'
                                        )
                                    ], width=4)
                                ]),
                                dcc.Graph(id='line-plot')
                            ])
                        ])
                    ])
                ])
            ], label="Analytics"),
        ]),

        dcc.Interval(id='interval-component', interval=2000)
    ], fluid=True)
    
    @app.callback(
        [Output('parameter-dropdown', 'options'),
         Output('meter-dropdown', 'options')],
        Input('interval-component', 'n_intervals')
    )
    def update_dropdowns(n):
        try:
            df = data_manager.get_latest_data_matrix()
            if df.empty:
                return [], []

            # Create parameter options with descriptive labels
            parameter_options = []
            for param, desc in df.index:
                # Convert Parameter_X to parameter_Y format (adding 1 to match database)
                param_num = int(param.split('_')[1]) + 1
                param_value = f"parameter_{param_num}"
                parameter_options.append({
                    'label': f"{desc}",
                    'value': param_value
                })

            meter_options = [
                {'label': f"Meter {col}", 'value': col} 
                for col in df.columns
            ]

            return parameter_options, meter_options
        except Exception as e:
            print(f"Error updating dropdowns: {e}")
            return [], []

    @app.callback(
        Output('line-plot', 'figure'),
        [Input('parameter-dropdown', 'value'),
         Input('meter-dropdown', 'value'),
         Input('timerange-dropdown', 'value'),
         Input('interval-component', 'n_intervals')]
    )
    def update_line_plot(parameter, meter, timerange, n):
        if not parameter or not meter:
            return go.Figure(
                layout=go.Layout(
                    title="Select a parameter and meter to display the plot",
                    xaxis_title="Time",
                    yaxis_title="Value"
                )
            )

        try:
            conn = data_manager.get_connection()
            
            # Calculate time range
            end_time = datetime.now()
            if timerange == '30M':
                start_time = end_time - timedelta(minutes=30)
            elif timerange == '1H':
                start_time = end_time - timedelta(hours=1)
            elif timerange == '4H':
                start_time = end_time - timedelta(hours=4)
            else:  # 24H
                start_time = end_time - timedelta(hours=24)

            # Query using the parameter value directly (it's already in parameter_X format)
            query = f"""
            SELECT timestamp, {parameter} as value
            FROM meter_{meter}
            WHERE timestamp BETWEEN %s AND %s
            AND {parameter} IS NOT NULL
            ORDER BY timestamp
            """
            
            df = pd.read_sql_query(query, conn, params=[start_time, end_time])
            conn.close()

            if df.empty:
                return go.Figure(
                    layout=go.Layout(
                        title="No data available for the selected parameters",
                        xaxis_title="Time",
                        yaxis_title="Value"
                    )
                )

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Get parameter description from the data manager's parameter descriptions
            param_desc = data_manager.parameter_descriptions.get(parameter, parameter)

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['value'],
                mode='lines+markers',
                name=param_desc,
                line=dict(width=2),
                marker=dict(size=6)
            ))
            
            fig.update_layout(
                title=f"{param_desc} - Meter {meter}",
                xaxis_title="Time",
                yaxis_title=f"Value",
                hovermode='x unified',
                template="plotly_white",
                height=500
            )
            
            # Add range slider
            fig.update_xaxes(rangeslider_visible=True)
            
            return fig
            
        except Exception as e:
            print(f"Error updating line plot: {str(e)}")
            return go.Figure(
                layout=go.Layout(
                    title=f"Error loading data: {str(e)}",
                    xaxis_title="Time",
                    yaxis_title="Value"
                )
            )
    
    @app.callback(
        [Output('total-power', 'children'),
         Output('avg-pf', 'children'),
         Output('total-energy', 'children'),
         Output('system-freq', 'children')],
        Input('interval-component', 'n_intervals')
    )
    def update_stats(n):
        try:
            matrix = data_manager.get_latest_data_matrix()
            total_power = matrix.loc[('Parameter_14', 'Active Power Total')][0:6].sum()
            pf = matrix.loc[('Parameter_16', 'Power Factor L1')].mean()
            energy = matrix.loc[('Parameter_17', 'Active Energy')][0:6].sum()
            freq = matrix.loc[('Parameter_15', 'Frequency')].mean()
            
            return [
                f"{total_power:.2f}",
                f"{pf:.2f}",
                f"{energy:.2f}",
                f"{freq:.1f}"
            ]
        except Exception as e:
            print(f"Error updating stats: {e}")
            return ["N/A", "N/A", "N/A", "N/A"]

    @app.callback(
        Output('live-table', 'children'),
        Input('interval-component', 'n_intervals')
    )
    def update_table(n):
        try:
            df = data_manager.get_latest_data_matrix()
            if df.empty:
                return html.Div("No data available")
            df = df.sort_index(axis=1)
            df = df.reset_index()

            df['parameter_name'] = df['parameter_name'].str.split('_').str[-1].astype(int)
            df = df.sort_values(by='parameter_name')

            df = df.set_index(['parameter_name', 'description'])

            table_header = [
                html.Thead(html.Tr(
                    [html.Th('ID'), html.Th('Description')] +
                    [html.Th(f'Meter {col}') for col in df.columns]
                ))
            ]

            rows = []
            for idx in df.index:
                param_id, desc = idx
                row_data = [html.Td(param_id), html.Td(desc)]

                for col in df.columns:
                    value = df.loc[idx, col]
                    if pd.notnull(value):
                        style = {}

                        # Apply conditional formatting
                        # if 'Power Factor' in desc and value < 0.85:
                        #     style['color'] = 'red'
                        # elif 'Voltage' in desc and (value < 210 or value > 250):
                        #     style['color'] = 'orange'
                        
                        formatted_value = f"{value:.2f}" if pd.api.types.is_number(value) else value
                        row_data.append(html.Td(formatted_value, style=style))
                    else:
                        row_data.append(html.Td("-"))  

                rows.append(html.Tr(row_data))

            table_body = [html.Tbody(rows)]

            return dbc.Table(
                table_header + table_body,
                bordered=True,
                hover=True,
                responsive=True,
                striped=True,
                className='table-sm'
            )
        except Exception as e:
            print(f"Error updating table: {e}")
            return html.Div(f"Error loading data: {str(e)}")

    @app.callback(
        Output('power-flow-chart', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_power_flow_chart(n):
        try:
            matrix = data_manager.get_latest_data_matrix()
            if ('Parameter_14', 'Active Power Total') not in matrix.index:
                return go.Figure()

            power_data = matrix.loc[('Parameter_14', 'Active Power Total')]
            
            # Get the load powers from Meter 1 to Meter 6
            load_powers = power_data[0:6].values  # Changed to include Meter 6
            incoming_power = power_data[7]

            # Define a set of contrasting colors
            meter_colors = ['#FF6347', '#FFD700', '#4682B4', '#32CD32', '#8A2BE2', '#FF4500']

            load_bars = []
            for i, (power, color) in enumerate(zip(load_powers, meter_colors), start=1):
                load_bars.append(
                    go.Bar(name=f'Meter {i}', x=['Load Powers'], y=[power], marker_color=color)
                )

            fig = go.Figure(data=[
                go.Bar(name='Incoming Power', x=['Incoming'], y=[incoming_power], marker_color='blue'),
                *load_bars
            ])

            fig.update_layout(
                barmode='stack',
                title='Power Flow Balance: Incoming vs Individual Loads',
                yaxis_title='Power (kW)',
                showlegend=True,
                height=500
            )

            total_load = load_powers.sum()
            fig.add_shape(
                type="line",
                x0=-0.5,
                x1=0.5,
                y0=total_load,
                y1=total_load,
                line=dict(color="red", width=2, dash="dash"),
            )

            difference = incoming_power - total_load
            fig.add_annotation(
                text=f"Difference: {difference:.2f} kW",
                x=0.5,
                y=max(incoming_power, total_load),
                showarrow=True,
                arrowhead=1
            )

            return fig
        except Exception as e:
            print(f"Error updating power flow chart: {e}")
            return go.Figure()

    @app.callback(
        Output('power-flow-time-series', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_power_flow_time_series(n):
        try:
            conn = data_manager.get_connection()
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=1)
            
            incoming_query = """
            SELECT timestamp, parameter_15 as power
            FROM meter_7
            WHERE timestamp BETWEEN %s AND %s
            AND parameter_15 IS NOT NULL
            ORDER BY timestamp
            """
            incoming_df = pd.read_sql_query(incoming_query, conn, params=[start_time, end_time])
            
            load_dfs = []
            for meter_id in range(1, 7):
                load_query = f"""
                SELECT timestamp, parameter_15 as power
                FROM meter_{meter_id}
                WHERE timestamp BETWEEN %s AND %s
                AND parameter_15 IS NOT NULL
                ORDER BY timestamp
                """
                df = pd.read_sql_query(load_query, conn, params=[start_time, end_time])
                load_dfs.append(df)
            
            conn.close()

            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=incoming_df['timestamp'],
                y=incoming_df['power'],
                name='Incoming Power',
                line=dict(color='blue', width=2)
            ))
            
            total_load = pd.concat([df['power'] for df in load_dfs], axis=1).sum(axis=1)
            fig.add_trace(go.Scatter(
                x=load_dfs[0]['timestamp'],
                y=total_load,
                name='Total Load Power',
                line=dict(color='red', width=2)
            ))

            fig.update_layout(
                title='Power Flow Time Series',
                xaxis_title='Time',
                yaxis_title='Power (kW)',
                height=500,
                showlegend=True,
                hovermode='x unified'
            )

            return fig
        except Exception as e:
            print(f"Error updating power flow time series: {e}")
            return go.Figure()

    @app.callback(
        Output('load-distribution-chart', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_load_distribution(n):
        try:
            matrix = data_manager.get_latest_data_matrix()
            if ('Parameter_14', 'Active Power Total') not in matrix.index:
                return go.Figure()

            power_data = matrix.loc[('Parameter_14', 'Active Power Total')]
            load_powers = power_data[:7]  # Include Meter 6

            # Define colors for meters
            meter_colors = ['#FF6347', '#FFD700', '#4682B4', '#32CD32', '#8A2BE2', '#FF4500']

            fig = go.Figure(data=[go.Pie(
                labels=[f"Meter {i}" for i in range(1, 7)],
                values=load_powers.values,
                hole=.3,
                marker=dict(colors=meter_colors)  # Apply the same color scheme here
            )])

            fig.update_layout(
                title='Load Power Distribution',
                height=500
            )

            return fig
        except Exception as e:
            print(f"Error updating load distribution: {e}")
            return go.Figure()

    @app.callback(
        Output('power-factor-polar', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_power_factor_polar(n):
        try:
            matrix = data_manager.get_latest_data_matrix()
            if ('Parameter_16', 'Power Factor L1') not in matrix.index:
                return go.Figure()

            pf_data = matrix.loc[('Parameter_16', 'Power Factor L1')]
            
            fig = go.Figure()
            
            theta = np.linspace(0, 2*np.pi, len(pf_data))
            
            fig.add_trace(go.Scatterpolar(
                r=pf_data.values,
                theta=theta * 180/np.pi,
                mode='lines+markers',
                name='Power Factor',
                line_color='blue'
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(range=[0, 1], ticksuffix=""),
                    angularaxis=dict(tickmode="array", 
                                   ticktext=[f"Meter {i}" for i in pf_data.index],
                                   tickvals=theta * 180/np.pi)
                ),
                title='Power Factor Distribution by Meter',
                height=500,
                showlegend=False
            )

            return fig
        except Exception as e:
            print(f"Error updating power factor polar: {e}")
            return go.Figure()

    @app.callback(
        Output('power-factor-histogram', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_power_factor_histogram(n):
        try:
            conn = data_manager.get_connection()
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=1)
            
            pf_data = []
            for meter_id in range(1, 8):
                query = f"""
                SELECT parameter_17 as pf
                FROM meter_{meter_id}
                WHERE timestamp BETWEEN %s AND %s
                AND parameter_17 IS NOT NULL
                """
                df = pd.read_sql_query(query, conn, params=[start_time, end_time])
                pf_data.extend(df['pf'].values)
            
            conn.close()

            fig = go.Figure(data=[go.Histogram(
                x=pf_data,
                nbinsx=30,
                name='Power Factor'
            )])

            fig.update_layout(
                title='Power Factor Distribution Histogram',
                xaxis_title='Power Factor',
                yaxis_title='Count',
                height=500,
                showlegend=False
            )

            fig.add_vline(x=0.95, line_dash="dash", line_color="red",
                         annotation_text="Ideal PF (0.95)")

            return fig
        except Exception as e:
            print(f"Error updating power factor histogram: {e}")
            return go.Figure()
        
    @app.callback(
        [Output('energy-trends-line', 'figure'),
         Output('cumulative-energy-line', 'figure')],
        Input('interval-component', 'n_intervals')
    )
    def update_energy_trends(n):
        try:
            conn = data_manager.get_connection()
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)
            
            
            energy_dfs = []
            for meter_id in range(1, 8):
                query = f"""
                SELECT timestamp, parameter_18 as energy
                FROM meter_{meter_id}
                WHERE timestamp BETWEEN %s AND %s
                AND parameter_18 IS NOT NULL
                ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=[start_time, end_time])
                df['Meter'] = f'Meter {meter_id}'
                energy_dfs.append(df)
            
            conn.close()
            combined_df = pd.concat(energy_dfs)
            trends_fig = px.line(combined_df, x='timestamp', y='energy', 
                               color='Meter', title='Energy Usage Trends')
            
            trends_fig.update_layout(
                xaxis_title='Time',
                yaxis_title='Energy (kWh)',
                height=500
            )
            
            cumulative_df = combined_df.copy()
            for meter in cumulative_df['Meter'].unique():
                mask = cumulative_df['Meter'] == meter
                cumulative_df.loc[mask, 'energy'] = \
                    cumulative_df.loc[mask, 'energy'].cumsum()
            
            cumulative_fig = px.line(cumulative_df, x='timestamp', y='energy',
                                   color='Meter', title='Cumulative Energy Usage')
            
            cumulative_fig.update_layout(
                xaxis_title='Time',
                yaxis_title='Cumulative Energy (kWh)',
                height=500)
            return trends_fig, cumulative_fig
        except Exception as e:
            print(f"Error updating power factor histogram: {e}")
            return go.Figure()
    return app

def run_dashboard(data_manager):
    app = create_dashboard(data_manager)
    app.run_server(debug=False)

def start_monitoring(mysql_config=None):
    data_manager = ModbusDataManager(mysql_config=mysql_config)
    dashboard_thread = threading.Thread(target=run_dashboard, args=(data_manager,))
    dashboard_thread.daemon = True
    dashboard_thread.start()
    return data_manager