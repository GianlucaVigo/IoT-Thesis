import pandas as pd
import json
import plotly.express as px

from collections import Counter
from dateutil.parser import parse

CHUNK_SIZE = 10000

def detect_server_version(payload):

    # Eclipse Californium
    if payload.find('Californium') != -1:

        version_index_start = payload.find('(c)')+4
        version_index_end = payload.find('Institute')-1

        version = payload[version_index_start:version_index_end]

        if version.endswith(','):
            version = version[:-1]

        return(f"Eclipse Californium/{version}")
    
    elif payload.find('libcoap') != -1:

        version_index = payload.find('(C)') + 4

        return(f"Libcoap/{payload[version_index: version_index + 10]}")

    return None


def detect_format(s: str):
    # 1. Number first
    try:
        int(s)
        return "int"
    except ValueError:
        try:
            float(s)
            return "float"
        except ValueError:
            try:
                complex(s)
                return "complex"
            except ValueError:
                pass

    # 2. Boolean
    if s.lower() in {"true", "false"}:
        return "boolean"

    # 3. Datetime
    try:
        parse(s)
        return "datetime"
    except Exception:
        pass

    # 4. JSON
    try:
        json.loads(s)
        return "json"
    except (json.JSONDecodeError, TypeError):
        pass

    # 5. Fallback: just a string
    return "string"


def analysis(data_paths, mode):

    match mode:

        # 0
        case 'Data Format':
            
            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[3][:-4]

                formats = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having not None code
                    chunk = chunk[chunk['code'] != None]
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    formats += Counter(chunk['data'].apply(detect_format))

                to_plot.extend(
                    {'date': current_date, 'format': format, 'count': count}
                    for format, count in formats.items()
                )             

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'format', 'count'])
            df_plot = df_plot.groupby(['date', 'format'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'format'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='format',
                y='count',
                color="date",
                barmode='group',
                title="Data Format Distribution over Time"
            )
            fig.show()
    

        # 1
        case 'Payload Size':

            to_plot = []
            
            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[3][:-4]

                sizes = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'data_length', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having not None code
                    chunk = chunk[chunk['code'] != None]
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    sizes += Counter(chunk['data_length'])

                to_plot.extend(
                    {'date': current_date, 'size': size, 'count': count}
                    for size, count in sizes.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'size', 'count'])
            df_plot = df_plot.groupby(['date', 'size'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'size'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.scatter(
                df_plot, 
                x="date", 
                y="size",
                size="count",
                color="date",
                hover_name="size", 
                log_y=True, 
                size_max=60
            )
            fig.show()

        # 2
        case 'Response Code':

            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[3][:-4]

                response_codes = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['code']): 

                    # DATA CLEANING
                    # keeping only entries having not None code
                    chunk = chunk[chunk['code'] != None]

                    ##############################

                    response_codes += Counter(chunk['code'])

                to_plot.extend(
                    {'date': current_date, 'code': code, 'count': count}
                    for code, count in response_codes.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'code', 'count'])
            df_plot = df_plot.groupby(['date', 'code'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'code'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='code',
                y='count',
                color="date",
                barmode='group',
                title="Response Code Distribution over Time"
            )
            fig.show()

        # 3
        case 'Options':

            to_plot = []
            
            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[3][:-4]

                options = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['options']): 

                    # DATA CLEANING
                    # deleting rows with code field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['options'])

                    ##############################

                    # Parse JSON safely
                    for raw in chunk['options']:
                        try:
                            parsed = json.loads(raw.replace('""', '"'))
                            # only option keys and NOT values
                            options.update(parsed.keys())

                        # ignore malformed entries
                        except Exception:
                            continue 

                to_plot.extend(
                    {'date': current_date, 'option': option, 'count': count}
                    for option, count in options.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'option', 'count'])
            df_plot = df_plot.groupby(['date', 'option'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'option'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='option',
                y='count',
                color="date",
                barmode='group',
                title="Options Distribution over Time"
            )
            fig.show()

        # 4
        case 'Server Specifications':

            to_plot = []
            
            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[3][:-4]

                server_specs = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code', 'uri']): 

                    # DATA CLEANING
                    chunk = chunk[(chunk['code'] == '2.05 Content') & (chunk['uri'] == '/')]
                    # deleting rows with code field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    server_specs += Counter(chunk['data'].apply(detect_server_version))

                to_plot.extend(
                    {'date': current_date, 'server': spec, 'count': count}
                    for spec, count in server_specs.items()
                )

            ##############################
        
            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'server', 'count'])
            df_plot = df_plot.groupby(['date', 'server'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'server'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='server',
                y='count',
                color="date",
                barmode='group',
                title="Server Specs Distribution over Time"
            )
            fig.show()


        # 5
        case 'OBS Resources':

            to_plot = []
            
            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[3][:-4]

                obs_types = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['observable']): 

                    # DATA CLEANING
                    # deleting rows with code field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['observable'])

                    ##############################

                    obs_types += Counter(chunk['observable'])
                
                to_plot.extend(
                    {'date': current_date, 'obs_type': obs_type, 'count': count}
                    for obs_type, count in obs_types.items()
                )

            ##############################
        
            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'obs_type', 'count'])
            df_plot = df_plot.groupby(['date', 'obs_type'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'obs_type'], ascending=[True, False]).reset_index(drop=True)
            
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='obs_type',
                y='count',
                color="date",
                barmode='group',
                title="Observable Types Distribution over Time"
            )
            fig.show()



    return