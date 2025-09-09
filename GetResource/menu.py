import asyncio
import datetime

from utils import files_handling
from GetResource import coap

def getresource_menu():

    '''1) Partition selection'''
    # Levels:
    #   0. dataset (ex. 01_output.csv)
    #   1. experiment (ex. exp_0)
    #   2. date (ex. 2025-09-05)
    #   3. partition (ex. 3_5.csv)

    levels = ['dataset', 'experiment', 'date', 'partition']
    path = {'phase': 'Discovery', 'folder': 'csv'}

    for level in levels:

        if level == 'date':
            path.update({level: datetime.date.today()})

        else:
            choice = files_handling.level_selection(level, path)
            if choice == None:
                return
            else: 
                path.update({level: choice})


    dataset = path['dataset']
    experiment = path['experiment']
    date = path['date']
    partition = path['partition'][:1]


    if files_handling.is_part_done(dataset, experiment, date, partition)[0] == '[Done]':
        '''2) Perform GET requests'''
        asyncio.run(coap.get_requests(path))
    else:
        print("Requested Partition has not been processed yet at stage [Discovery] check")
        print("-" * 100)