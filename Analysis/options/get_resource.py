import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
import plotly.express as px

from collections import Counter
from utils import payload_handling
from dateutil.parser import parse


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




def instant_analysis(data_df, mode):

    # Plot Figure Size
    plt.figure(figsize=(8, 5))

    stat_rows = []

    match mode:

        # 0
        case 'Data Format':

            data_df.dropna(subset=['Payload'], inplace=True)
            
            format_counter = Counter()

            for index, row in data_df[data_df["Code"] == "2.05 Content"].iterrows():

                # extracting payload to be parsed
                payload = row['Payload']
                print(f"Payload: {payload}")

                # parsing payload to get format
                payload_format = detect_format(payload)
                print(f"Payload Format: {payload_format}")

                # format already present in the counter
                if payload_format in format_counter.keys():
                    format_counter[payload_format] += 1
                else:
                    format_counter[payload_format] = 1
                
                print("£"*50)
            
            # dictionary -> pd DataFrame
            format_counter_df = pd.DataFrame(format_counter.items(), columns=['data_format', 'count'])

            # define the %
            valid_payloads = format_counter_df['count'].sum()
            format_counter_df['percentage'] = (format_counter_df['count'] / valid_payloads) * 100

            # Type of plot
            sns.barplot(data=format_counter_df,
                        y="percentage",
                        x="data_format",
                        hue="data_format",
                        palette=sns.color_palette("bright"))

            # Plot Labels
            plt.xlabel('Payload Type')
            plt.ylabel('Percentage of Resources')

            # Plot Title
            plt.title("Payload Type Distribution")

            stat_rows.append(f"Payload Type Distribution:\n {format_counter_df}")


        # 1
        case 'Payload Size':
            
            # Type of plot
            sns.countplot(data=data_df, x='Payload Size (bytes)', color="lightgreen")
            
            # Plot Labels
            plt.xlabel('Payload Size (Bytes)')
            plt.ylabel("# CoAP Resources")

            # Plot Title
            plt.title("[PAYLOAD] Size Distribution")

            # Additional stats
            stat_rows.append(f"\tPayload size average value: {data_df['Payload Size (bytes)'].mean()}")
            stat_rows.append(f"\tPayload size median value: {data_df['Payload Size (bytes)'].median()}")


        # 2
        case 'Response Code':

             # Type of plot
            sns.countplot(data=data_df, x='Code', color="lightgreen")

            # Plot Labels
            plt.xlabel('Response Code')
            plt.ylabel("# Responses")

            # Plot Title
            plt.title('[PAYLOAD] Valid CoAP Responses')

            stat_rows.append(f"Response Codes:\n {data_df['Code'].value_counts()}")
    

    '''PLOTTING'''
    plt.tight_layout()
    plt.show()

    '''STATS'''
    print("Additional statistics:\n")
    if len(stat_rows) > 0:
        for stat in stat_rows:
            print(stat)
    else:
        print("\tNo stats available")

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