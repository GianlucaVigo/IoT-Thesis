import itertools
import math

''''''
def uri_list_of(payload):

    uri_list = []

    for resource in str(payload).split(','):
        uri = resource[resource.find("<"):resource.find(">")+1]
        uri_list.append(uri)

    return uri_list


''''''
def resource_list_of(payload):

    resource_list = []

    for resource in str(payload).split(','):
        resource_list.append(resource)

    return resource_list


''''''
def n_levels_of(uri):

    return len(uri.split('/'))


''''''
def resource_metadata_names_of(payload):

    attributes_list = []

    res_attributes = str(payload).split(';')

    if len(res_attributes) == 1: # no attributes present -> skip
        return None
    
    for attr in res_attributes[1:]: # skip resource name

        # getting metadata name
        attr_name = attr.split('=')[0]

        attributes_list.append(attr_name)

    return attributes_list


''''''
def get_metadata_value_of(payload, metadata_name):

    res_attributes = str(payload).split(';')

    if len(res_attributes) == 1: # no attributes present -> skip
        return None
    
    for attr in res_attributes[1:]: # skip resource name

        # [metadata_name, metadata_value]
        attribute_info = attr.split('=') 

        if attribute_info[0] == metadata_name:
            match metadata_name:
                case 'ct':
                    return [int(attribute_info[1])]

    return None