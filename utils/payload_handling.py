import itertools

''''''
def URI_list_of(payload):

    URI_list = []

    for resource in str(payload).split(','):
        URI_list.append(resource[resource.find("<"):resource.find(">")+1])

    return URI_list


''''''
def resource_list_of(payload):

    resource_list = []

    for resource in str(payload)[2:-1].split(','):
        resource_list.append(resource)

    return resource_list


''''''
def n_levels_of(single_resource_payload):

    return len(single_resource_payload.split('/'))


''''''
def resource_attributes(payload):

    attributes_list = []

    res_attributes = payload.split(';')

    if len(res_attributes) == 1: # no attributes present -> skip
        return None
    
    for attr in itertools.islice(res_attributes, 1, None): # skip resource name

        # getting metadata name
        attr_name = attr.split('=')[0]

        if attr_name != None:
            attributes_list.append(attr_name)

    return attributes_list