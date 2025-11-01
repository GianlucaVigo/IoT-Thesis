import os

#############################

def path_dict_to_str(dict_path):

    string_path = ""

    for value in dict_path.values():
        string_path += f"{value}/"

    return string_path[:-1]

#############################

def read_file_system(base_path):

    portions = ['0', '1', '2', '3', '4', '5', '6']
    
    date_paths = []

    for portion in portions:

        path = base_path + f"/{portion}"
        
        dates = os.listdir(path)
        dates.sort()
        
        if not dates:
            continue 
        
        for date in dates:
            date_paths.append(path + f"/{date}")

    return date_paths