import asyncio

from Discovery import coap
from utils import files_handling

        
def discovery_menu():

    '''1) Partition selection'''
    # Levels:
    #   0. dataset (ex. 01_output.csv)
    #   1. experiment (ex. exp_0)
    #   2. partition (ex. 3_5.csv)

    levels = ['dataset', 'experiment', 'partition']
    path = {'phase': 'Partitioning', 'folder': 'csv'}

    for level in levels:
        choice = files_handling.level_selection(level, path)

        if choice == None:
            return
        else: 
            path.update({level: choice})


    # 2) Find CoAP server
    '''
    EVENT LOOP: 
    - the core that manages and distributes tasks
    - like a central hub with tasks circling around it waiting for their turn to be executed
    -- each task takes its turn in the center where it's either
    --- EXECUTED immediately
    --- PAUSED if it's waiting for something
    ---- when a tasks awaits it steps aside making room for another task to run ensuring the loop is always efficiently utilized
    ---- once the awaited operation is complete the task will resume ensuring a smooth and responsive program flow
    '''

    asyncio.run(coap.findCoapServers(path))

    return None