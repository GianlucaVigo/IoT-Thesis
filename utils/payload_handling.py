import json
from dateutil.parser import parse

''''''
def detect_format(payload):
    
    # empty payload/data field -> None
    if not payload:
        return None
        
    # 1. Number first
    try:
        int(payload)
        return "int"
    except ValueError:
        try:
            float(payload)
            return "float"
        except ValueError:
            try:
                complex(payload)
                return "complex"
            except ValueError:
                pass

    # 2. Boolean
    if payload.lower() in {"true", "false"}:
        return "boolean"

    # 3. Datetime
    try:
        parse(payload)
        return "datetime"
    except Exception:
        pass

    # 4. JSON
    try:
        json.loads(payload)
        return "json"
    except (json.JSONDecodeError, TypeError):
        pass

    # 5. Fallback: just a string
    return "string"

''''''
def uri_list_of(payload):

    uri_list = []

    for resource in str(payload).split(','):
        # start label
        start = resource.find("<")
        if start == -1:
            continue

        # end label
        end = resource.find(">")
        if end == -1:
            uri = resource[start:] # allowing also truncated resources
        else:
            end += 1
            uri = resource[start:end]

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

    return len(uri.split('/'))-1 # ex. /gw/help -> not 3 but 2 levels


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
                    try:
                        return [int(attribute_info[1])]
                    except:
                        #.strip() removes whitespace
                        #.strip('"') removes double quotes
                        #.strip("'") removes single quotes if present
                        value = attribute_info[1].strip().strip('"').strip("'")
                        return [int(value)]
                case 'obs':
                    return True

    return None


def get_version(message):

    try:
        version = message.version
    except Exception:
        print("[ERROR] Version extraction")
        return None

    return version


def get_mtype(message):

    try:
        mtype = message.mtype
    except Exception:
        print("[ERROR] Message Type extraction")
        return None

    return mtype


def get_token_length(message):

    try:
        token_length = len(message.token)
    except Exception:
        print("[ERROR] Token Length extraction")
        return None

    return token_length


def get_code(message):

    try:
        code= str(message.code)
    except Exception:
        print("[ERROR] Code extraction")
        return None

    return code


def get_mid(message):

    try:
        mid= message.mid
    except Exception:
        print("[ERROR] MessageID extraction")
        return None

    return mid


def get_token(message):

    try:
        token= message.token.hex()
    except Exception:
        print("[ERROR] Token extraction")
        return None

    return token


def get_options(message):

    try:

        options = None

        # if options are defined
        if (len(message.opt._options.keys()) > 0):
            options = {}

            options_key = []
            for option in message.opt._options:
                options_key.append(str(option))

            options_value = []
            for option in message.opt.option_list():
                options_value.append(str(option))

            for i in range(len(options_key)):
                options.update({options_key[i]: options_value[i]})
    
    except Exception:
        print("[ERROR] Options extraction")
        return None
    
    return options


def get_payload(message):

    if isinstance(message.payload, bytes):
        try:
            payload = message.payload.decode("utf-8")
        except UnicodeDecodeError as e:
            print(f"\t\t\tFailed to decode payload: {e}")
            payload = message.payload  # fallback to raw bytes
    else:
        payload = message.payload  # already a str

    return payload


def get_payload_format(message):

    if isinstance(message.payload, bytes):
        try:
            payload = message.payload.decode("utf-8")
        except UnicodeDecodeError as e:
            print(f"\t\t\tFailed to decode payload: {e}")
            payload = message.payload  # fallback to raw bytes
    else:
        payload = message.payload  # already a str
    
    payload_format = detect_format(payload)

    return payload_format


def get_payload_length(message):

    try:
        payload_length = len(message.payload)
    except Exception:
        print("[ERROR] Payload Length extraction")
        return None

    return payload_length


def get_observe(message, uri):

    try:
        observe = message.opt.observe
    except Exception:
        print("[ERROR] Observe Option extraction")
        return None
    
    if observe is not None:
        print(f"\t\t\tOBS: {uri}")
        return True
    else:
        return False
    

def options_to_json(discovery_df):

    discovery_df["options"] = discovery_df["options"].apply(
        lambda x: json.dumps(x) if isinstance(x, dict) else x
    )

    return discovery_df


def detect_truncated_response(udp_pkt_size, raw_coap_message, decoded_msg):

    # Handle Block2
    # if options is not empty/None + 'BLOCK2' is part of the option list
    #   -> ZMap got a truncated result
    if decoded_msg['options'] and 'BLOCK2' in decoded_msg['options'].keys():
        print(f"\nBlock2 detected!")
        return True


    # Check UDP Pkt Size
    # UDP Packet
    # = UDP Header [8 B - fixed] + UDP Payload
    #
    # UDP Payload
    # = CoAP Header [(#hex chars until 'ff' / 2 Bytes) - variable] + CoAP Payload
    #
    # I have the UDP Pkt Size: 
    # if by subtracting the UDP Header and the CoAP Header, 
    # I don't get the payload length this means that is truncated
    if decoded_msg['data_length'] > 0:

        # getting Coap Header size
        raw_coap_delimeter = raw_coap_message.find('ff')
        raw_coap_header_size = (raw_coap_delimeter + 2) / 2 # in Bytes
        # getting raw Coap Payload size
        raw_coap_payload_size = (len(raw_coap_message) / 2) - raw_coap_header_size # in Bytes

        udp_header_size = 8 # fixed

        if (udp_pkt_size - udp_header_size - raw_coap_header_size != raw_coap_payload_size):
            print("\nTruncated payload: a new request must be performed!")
            return True


    return False