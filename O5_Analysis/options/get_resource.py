import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import json
import plotly.express as px

from collections import Counter
from dateutil.parser import parse

from utils import files_handling


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


def parse_csv(data_path, mode):

    cols = []

    match mode:

        # 'udp_pkt_size' column
        case 'Data Format':
            cols.extend(['code', 'payload'])
        # 'success' column
        case 'Payload Size':
            cols.extend(['code', 'payload_size'])
        # 'data' column
        case 'Response Code' :
            cols.append('code')
        # 'options' column
        case 'Options' :
            cols.append('options')
        
    data_df = pd.read_csv(files_handling.path_dict_to_str(data_path), usecols=cols)

    return data_df


def instant_analysis(data_paths, mode):

    # Plot Figure Size
    plt.figure(figsize=(8, 5))

    match mode:

        # 0
        case 'Data Format':

            # FORMAT DICTIONARY
            # instantiating a Counter Dictionary
            format_dict = Counter()

            for partition in data_paths['partition']:

                # GETTING A DATE DATASET
                part_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'experiment': data_paths['experiment'],
                             'date': data_paths['date'],
                             'partition': partition}
                part_df = parse_csv(part_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                part_df.dropna(ignore_index=True, inplace=True, subset='payload')
                # keeping only entries associated to a 2.05 Content result
                part_df = part_df[part_df["code"] == "2.05 Content"]

                ##############################

                # iterating over data_df rows
                for index, row in part_df.iterrows():

                    # extracting payload to be parsed
                    payload = row['payload']
                    print(f"Payload: {payload}")

                    # parsing payload to get format
                    payload_format = detect_format(payload)
                    print(f"Payload Format: {payload_format}")

                    # format already present in the counter
                    if payload_format in format_dict.keys():
                        format_dict[payload_format] += 1
                    else:
                        format_dict[payload_format] = 1
                    
                    print("£"*50)


            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(format_dict.items(), columns = ['data_format', 'count'])
            df_plot = df_plot.sort_values('count').reset_index(drop=True)

            ##############################

            # define the %
            valid_payloads = df_plot['count'].sum()
            df_plot['percentage'] = (df_plot['count'] / valid_payloads) * 100

            ##############################

            # STATS
            print(df_plot)

            ##############################

            # PLOTTING
            sns.barplot(data=df_plot,
                        y="percentage",
                        x="data_format",
                        hue="data_format",
                        palette=sns.color_palette("bright"))

            # Plot Labels
            plt.xlabel('Payload Type')
            plt.ylabel('Percentage of Resources')

            # Plot Title
            plt.title("Payload Type Distribution")

            plt.show()


        # 1
        case 'Payload Size':

            # PAYLOAD SIZE DICTIONARY
            # instantiating a Dictionary
            payload_sizes_dict = {}

            for partition in data_paths['partition']:

                print(partition)

                # GETTING A DATE DATASET
                part_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'experiment': data_paths['experiment'],
                             'date': data_paths['date'],
                             'partition': partition}
                part_df = parse_csv(part_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                part_df.dropna(ignore_index=True, inplace=True, subset='payload_size')

                ##############################

                # iterating over data_df rows
                for index, row in part_df.iterrows():

                    # payload size
                    payload_size = row['payload_size']
                    print(payload_size)

                    # response code
                    code = row['code']
                    print(code)
                    
                    if code in payload_sizes_dict.keys():

                        if payload_size in payload_sizes_dict[code].keys():
                            payload_sizes_dict[code][payload_size] += 1
                        else:
                            payload_sizes_dict[code][payload_size] = 1

                    else:
                        payload_sizes_dict[code] = {payload_size: 1}

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame.from_dict(payload_sizes_dict)
            df_plot.sort_index(inplace=True)
            df_plot.index.name = 'size'

            ##############################

            # STATS
            print(df_plot)
        
            ##############################

            # PLOTTING
            df_plot.plot.bar(subplots=True, layout=(-1, 2), sharex=True, sharey=False)
            plt.suptitle("Distributions of Payload Sizes by Status Code")
            plt.show()

            # melt df into long format
            df_long = df_plot.reset_index().melt(
                id_vars="size",
                var_name="status",
                value_name="count"
            ).dropna()

            fig = px.histogram(
                            df_long,
                            x="size",
                            y="count",
                            color="status",
                            title="Restaurant data"
            )
            fig.show()

            fig = px.sunburst(df_long,
                  path=["status", "size", "count"],
                  values="count",
                  title="Sunburst plot")
            
            fig.show()


        # 2
        case 'Response Code':

            # PAYLOAD SIZE DICTIONARY
            # instantiating a Dictionary
            response_codes_dict = {}

            for partition in data_paths['partition']:

                # GETTING A DATE DATASET
                part_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'experiment': data_paths['experiment'],
                             'date': data_paths['date'],
                             'partition': partition}
                part_df = parse_csv(part_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                part_df.dropna(ignore_index=True, inplace=True, subset='code')

                ##############################

                # iterating over data_df rows
                for index, row in part_df.iterrows():
                    
                    code = row['code']
                    # format already present in the counter
                    if code in response_codes_dict.keys():
                        response_codes_dict[code] += 1
                    else:
                        response_codes_dict[code] = 1

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(response_codes_dict.items(), columns = ['code', 'count'])
            df_plot = df_plot.sort_values('count').reset_index(drop=True)

            ##############################

            # STATS
            print(df_plot)
        
            ##############################

            # PLOTTING
            colors = sns.color_palette('pastel')[0:5]
            plt.pie(df_plot['count'], labels = df_plot['code'], colors = colors, autopct='%.0f%%')
            plt.show()

        
        # 3
        case 'Options':

            # PAYLOAD SIZE DICTIONARY
            # instantiating a Dictionary
            options_dict = Counter()

            for partition in data_paths['partition']:

                # GETTING A DATE DATASET
                part_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'experiment': data_paths['experiment'],
                             'date': data_paths['date'],
                             'partition': partition}
                part_df = parse_csv(part_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                part_df.dropna(ignore_index=True, inplace=True, subset='options')

                ##############################

                # iterating over data_df rows
                for index, row in part_df.iterrows():
                    
                    options = eval(row['options'])

                    options_dict.update(options.keys())
                    

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(options_dict.items(), columns = ['option', 'count'])

            ##############################

            # STATS
            print(df_plot)
        
            ##############################

            # PLOTTING
            # PLOTTING
            fig = px.bar(
                df_plot,
                x="option",
                y="count",
                color="option",
                title="Options",
                text="count"           # show count on bars
            )

            fig.show()

            

    return



def time_analysis(data_df, mode):

    match mode:

        # 1
        case 'Payload Size':

            plt.figure(figsize=(12,6))
            sns.boxplot(data=data_df, x="Date", y="Payload Size (bytes)")
            plt.xticks(rotation=45)
            plt.title("Payload Size Distribution per Date")
            plt.show()

            plt.figure(figsize=(12,6))
            sns.stripplot(data=data_df, x="Date", y="Payload Size (bytes)", hue="country", jitter=True, alpha=0.6)
            plt.xticks(rotation=45)
            plt.title("Payload Size Distribution per Date")
            plt.show()

            fig = px.scatter(
                data_df,
                x="Date",
                y="Payload Size (bytes)",
                color="country",
                title="Payload Size Distribution per Date",
                opacity=0.7
            )

            fig.show()

        # 2
        case 'Response Code':

            # empty list to collect results
            all_resources = []
            
            for date in sorted(data_df['Date'].unique()):
                date_df = data_df[data_df['Date'] == date]

                response_codes_dict = Counter(date_df['Code'])

                # dictionary -> pd DataFrame
                response_codes_df = pd.DataFrame(response_codes_dict.items(), columns=['response_code', 'count'])
                response_codes_df['Date'] = date # add the date
                all_resources.append(response_codes_df)

            # concatenate all dates
            all_dates_df = pd.concat(all_resources, ignore_index=True)

            fig = px.bar(
                all_dates_df,
                x="response_code",
                y="count",
                color="response_code",
                animation_frame="Date",
                title="Active CoAP Machine per Dates",
                text="count"           # show count on bars
            )

            fig.show()

    return