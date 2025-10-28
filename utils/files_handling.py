import csv
import os


def path_dict_to_str(dict_path):

    string_path = ""

    for value in dict_path.values():
        string_path += f"{value}/"

    return string_path[:-1]



def store_data(data, file):

    with open(file, "a", newline = '') as csv_file:
        write = csv.writer(csv_file)
        write.writerow(data)



def read_file_system(base_path):

    file_system = []

    items = os.listdir(base_path)
    items.sort()

    if not items:
        return []

    for item in items:

        to_test = base_path + f"/{item}"

        if os.path.isfile(to_test):
            file_system.append(to_test)
        else:
            file_system.extend(read_file_system(to_test))

    return file_system
