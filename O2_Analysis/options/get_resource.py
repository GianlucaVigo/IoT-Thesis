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
            
            formats = Counter()

            for path in data_paths:
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data_format']): 

                    # DATA CLEANING
                    # deleting rows with data_format field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data_format'])

                    ##############################

                    formats += Counter(chunk['data_format'])


            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame.from_dict(formats, orient='index').reset_index() 
            df_plot = df_plot.rename(columns={'index':'format', 0:'count'})            
            df_plot = df_plot.sort_values('count', ascending=False).reset_index(drop=True)
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='format',
                y='count',
                title="Data Format Distribution over Time"
            )
            fig.show()
    

        # 1
        case 'Payload Size':
            
            sizes = Counter()
            
            for path in data_paths:
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data_length']): 

                    # DATA CLEANING
                    # deleting rows with data_length field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data_length'])
                    # keeping only entries having not None code
                    chunk = chunk[chunk['data_length'] != 0]

                    ##############################

                    sizes += Counter(chunk['data_length'])

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame.from_dict(sizes, orient='index').reset_index() 
            df_plot = df_plot.rename(columns={'index':'size', 0:'count'})            
            df_plot = df_plot.sort_values('size', ascending=True).reset_index(drop=True)
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.histogram(
                df_plot,
                nbins=100,
                x='size',
                y='count',
                barmode='group',
                title="Payload Size Distribution"
            )
            fig.show()

        # 2
        case 'Response Code':

            response_codes = Counter()

            for path in data_paths:
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['uri','code', 'user_inserted']): 

                    # DATA CLEANING
                    # deleting rows with code field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['code'])
                    
                    # NB: inserted '/' resource inserted by hand
                    # keep rows considering 3 cases:
                    # 1) if uri is '/', the code is '2.05 Content' (there was an actual result) and it was inserted by hand
                    # 2) if uri is '/' and it wasn't inserted by hand
                    # 3) every uri different from '/' (because they were not inserted by hand)
                    #
                    # -> i should delete all entries with uri '/', it was inserted by hand and the code is not '2.05 Content'
                    #
                    chunk = chunk[
                        (chunk['uri'] == '/') & (chunk['code'] == '2.05 Content') & (chunk['user_inserted'] == True) |
                        (chunk['uri'] == '/') & (chunk['user_inserted'] == False) |
                        (chunk['uri'] != '/')
                    ]

                    ##############################

                    response_codes += Counter(chunk['code'])

            ##############################
            
            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame.from_dict(response_codes, orient='index').reset_index() 
            df_plot = df_plot.rename(columns={'index':'code', 0:'count'})            
            df_plot = df_plot.sort_values('count', ascending=False).reset_index(drop=True)
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='code',
                y='count',
                title="Response Code Distribution"
            )
            fig.show()

        # 3
        case 'Options':

            options = Counter()
            
            for path in data_paths:

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


            ##############################
            
            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame.from_dict(options, orient='index').reset_index() 
            df_plot = df_plot.rename(columns={'index':'option', 0:'count'})            
            df_plot = df_plot.sort_values('count', ascending=False).reset_index(drop=True)
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='option',
                y='count',
                title="Options Distribution"
            )
            fig.show()

        # 4
        case 'Server Specifications':

            server_specs = Counter()
            
            for path in data_paths:
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code', 'uri']): 

                    # DATA CLEANING
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])
                    # keep only rows with code equal to 2.05 and where the uri is home path ('/')
                    chunk = chunk[(chunk['code'] == '2.05 Content') & (chunk['uri'] == '/')]
                    
                    ##############################

                    server_specs += Counter(chunk['data'].apply(detect_server_version))

            ##############################
            
            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame.from_dict(server_specs, orient='index').reset_index() 
            df_plot = df_plot.rename(columns={'index':'server_spec', 0:'count'})            
            df_plot = df_plot.sort_values('count', ascending=False).reset_index(drop=True)
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='server_spec',
                y='count',
                title="Server Specs Distribution"
            )
            fig.show()


        # 5
        case 'OBS Resources':

            obs_types = Counter()
            
            for path in data_paths:
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['uri', 'code', 'user_inserted', 'observable']): 

                    # DATA CLEANING
                    # deleting rows with code field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['observable'])
                    
                    # NB: inserted '/' resource inserted by hand
                    # keep rows considering 3 cases:
                    # 1) if uri is '/', the code is '2.05 Content' (there was an actual result) and it was inserted by hand
                    # 2) if uri is '/' and it wasn't inserted by hand
                    # 3) every uri different from '/' (because they were not inserted by hand)
                    #
                    # -> i should delete all entries with uri '/', it was inserted by hand and the code is not '2.05 Content'
                    #
                    chunk = chunk[
                        (chunk['uri'] == '/') & (chunk['code'] == '2.05 Content') & (chunk['user_inserted'] == True) |
                        (chunk['uri'] == '/') & (chunk['user_inserted'] == False) |
                        (chunk['uri'] != '/')
                    ]

                    ##############################

                    obs_types += Counter(chunk['observable'])

            ##############################
            
            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame.from_dict(obs_types, orient='index').reset_index() 
            df_plot = df_plot.rename(columns={'index':'obs_type', 0:'count'})            
            df_plot = df_plot.sort_values('count', ascending=False).reset_index(drop=True)
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='obs_type',
                y='count',
                title="Observable Types Distribution"
            )
            fig.show()


        # 6
        # About the home path (/) resource, it could be interesting to focus on the comparison between
        # the ones available in the resource list collected from discovery (user_inserted == False)
        # and those that were inserted by hand (user_inserted == False) + gave a valid result (code == 2.05 Content)
        case 'Home Path Info':

            home_path_infos = Counter()

            for path in data_paths:
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['uri','code', 'user_inserted']): 

                    # DATA CLEANING
                    # deleting rows with code field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['code'])
                    
                    # NB: inserted '/' resource inserted by hand
                    # keep rows considering 3 cases:
                    # 1) if uri is '/', the code is '2.05 Content' (there was an actual result) and it was inserted by hand
                    # 2) if uri is '/' and it wasn't inserted by hand
                    # 3) every uri different from '/' (because they were not inserted by hand)
                    #
                    # -> i should delete all entries with uri '/', it was inserted by hand and the code is not '2.05 Content'
                    #
                    chunk = chunk[
                        (chunk['uri'] == '/') & (chunk['code'] == '2.05 Content') & (chunk['user_inserted'] == True) |
                        (chunk['uri'] == '/') & (chunk['user_inserted'] == False) |
                        (chunk['uri'] != '/')
                    ]

                    ##############################

                    home_path_infos += Counter(zip(chunk['user_inserted'], chunk['code']))
            
            home_path_infos = [
                    {'user_inserted': key[0], 'code': key[1], 'count': count}
                    for key, count in home_path_infos.items()
            ]
            
            ##############################
            
            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(home_path_infos, columns=['user_inserted', 'code', 'count'])         
            df_plot = df_plot.sort_values(['user_inserted', 'count'], ascending=[False, False]).reset_index(drop=True)
            print(df_plot)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='code',
                y='count',
                color='user_inserted',
                title="Home Path Info Distribution"
            )
            fig.show()
            
            
    return