import pandas as pd
import plotly.express as px
from collections import Counter

from utils import workflow_handling

########################################à

CHUNK_SIZE = 1000

########################################

def analysis(data_paths):

    zmap_res = Counter()
            
    for path in data_paths:
        
        with pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['saddr', 'data', 'classification', 'icmp_unreach_str']) as csv_reader:

            for _, chunk in enumerate(csv_reader):

                # ----------- remove-duplicates -----------
                # clean the chunk by removing eventual ICMP duplicates -> 'probes' + 'output-filter' options
                chunk = workflow_handling.remove_duplicates(chunk)
                
                # ----------- remove-invalid-ips -----------
                # remove rows with empty 'saddr' field
                chunk.dropna(subset=['saddr'], inplace=True)
                        
                for _, row in chunk.iterrows():

                    # detect correct messages
                    if row['classification'] == 'udp':
                        key = 'udp'
                    # detect icmp kind of messages
                    elif row['classification'] == 'icmp':
                        key = f'{row['icmp_unreach_str']}'
                    else:
                        print(f"Not classified case: {row['data']}")
                        continue
                                    
                    zmap_res.update([key])
    
    # logging
    print("ZMap results: ")
    for classification, count in zmap_res.items():
        print(f"\t+ {classification} _ {count}")
    
    
    to_plot = [        
        {'classification': pck_type, 'count': count}
        for pck_type, count in zmap_res.items()
    ]

    ##############################

    # DICTIONARY TO DATAFRAME
    # list -> pd DataFrame
    df_plot = pd.DataFrame(to_plot, columns=['classification', 'count'])
    df_plot = df_plot.sort_values(['count', 'classification'], ascending=[False, True]).reset_index(drop=True)

    ##############################

    # PLOTTING
    fig = px.bar(
        df_plot,
        x='classification',
        y='count',
        barmode='group',
        title="ZMap Results"
    )
    fig.show()