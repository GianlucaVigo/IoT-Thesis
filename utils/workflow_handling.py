import maxminddb
import pandas as pd
import os
import datetime

from utils import payload_handling

from collections import Counter
from aiocoap import *


################################################################################################

def create_file(path, cidr_id):
    
    
    os.makedirs(path, exist_ok=True)
    
    
    if cidr_id == None:
        
        # get current date
        current_date = str(datetime.date.today())

        path = os.path.join(path, f"{current_date}.csv")
        
    else:
        
        path = os.path.join(path, f"{cidr_id}.csv")
        

    with open(path, "w"):
        pass

    return path
        
        
################################################################################################


def avoid_get(row):

    # When can GET requests be avoided?

    # ----- SUCCESS check -----
    if row['success'] == 0: # success = 0/1
        print(f"\t\tNot successful response to resource discovery: \n\t\tskip {row['saddr']}")
        return True

    # ----- CODE check -----
    if row['code'] != '2.05 Content': # not successful discovery (= 2.05 Content)
        print(f"\t\tNot '2.05 Content' discovery: \n\t\tskip {row['saddr']}")
        return True

    # ----- OPTION check -----
    # Assumption: /.well-known/core resource use the CONTENT FORMAT option equal to LINK FORMAT
    # NB: I filter out everything that is not in LINKFORMAT style (TEXT, ...)
    if row['options'] != None:

        options_dict = eval(row['options'])

        if 'CONTENT_FORMAT' in options_dict.keys():
            if options_dict['CONTENT_FORMAT'] != 'LINKFORMAT':
                return True

    # ----- EMPTY PAYLOAD check -----
    if len(row['data']) == 0: # empty payload string
        print(f"\t\tEmpty payload was returned from resource discovery: \n\t\tskip {row['saddr']}")
        return True

    # ----- STILL BINARY PAYLOAD check -----
    if isinstance(row['data'], bytes):
        print(f"\t\tCan't decode its binary payload: \n\t\tskip {row['saddr']}")
        return True

    return False

################################################################################################

# remove_duplicates()
#   it is able to identify duplicates in the ZMap scan csv files: keep the first, discard the others
#   duplicate = both IP address and data fields equal
def remove_duplicates(df_zmap):

    # [subset=['saddr']] Remove duplicates considering 'saddr' field
    # [keep='first'] Keep the first instance (not delete all duplicates)
    # [inplace=True] Whether to modify the DataFrame rather than creating a new one.
    df_zmap.drop_duplicates(subset=['saddr'], keep='first', inplace=True)

    # [inplace=True] Whether to modify the DataFrame rather than creating a new one.
    # [drop=True] Do not try to insert index into dataframe columns. This resets the index to the default integer index.
    df_zmap.reset_index(inplace=True, drop=True)

    return df_zmap

################################################################################################

# extract_ip_info()
#   it is able to retrieve IP related information as country, continent, ...
#   it returns a flat dataframe that will be then stored safely
def extract_ip_info(ip_list_df):

    with maxminddb.open_database('utils/ipinfo/ipinfo_lite.mmdb') as reader:
        # apply -> returns a dictionary of IP related fields
        ip_info = ip_list_df['saddr'].apply(reader.get)
        
        reader.close()

    # normalize/flatten the dictionary
    ip_info_df = pd.json_normalize(ip_info)
        
    # concatenate IP + IP info
    ip_info_df = pd.concat([ip_list_df.reset_index(drop=True), ip_info_df], axis=1)

    return ip_info_df

################################################################################################

# perform the resource discovery operation -> GET (<IP> + <URI>)
async def get(ip_address, truncated_decoded_msg, context):

    print(f"\t\t\tPerforming new GET request to {truncated_decoded_msg['uri']} ...")

    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{truncated_decoded_msg['uri']}"
    # build the request message
    request = Message(code=GET, uri=uri_to_check)

    try:
        # send the request and obtained the response
        response = await context.request(request).response

    except Exception as e:
        print(f"\t{e}")
        print("\t[aiocoap] Unsuccessful response: storing truncated message ...")
        return truncated_decoded_msg

    else:

        full_decoded_msg = {
            'version': payload_handling.get_version(response),
            'mtype': payload_handling.get_mtype(response),
            'token': payload_handling.get_token(response),
            'token_length': payload_handling.get_token_length(response),
            'code': payload_handling.get_code(response),
            'mid': payload_handling.get_mid(response),
            'options': payload_handling.get_options(response),
            'observable': payload_handling.get_observe(response, truncated_decoded_msg['uri']),
            'data': payload_handling.get_payload(response),
            'data_format': payload_handling.get_payload_format(response),
            'data_length': payload_handling.get_payload_length(response),
            'uri': truncated_decoded_msg['uri']
        }

        return full_decoded_msg # success

################################################################################################
# decode_data()
#   I: binary payload (in hex)
#   O: structured object with multiple fields
def decode_data(binary_data, uri):

    # default message to be returned (if any error occurs during the decoding process)
    decoded_msg = {
        'version': None,
        'mtype': None,
        'token': None,
        'token_length': None,
        'code': None,
        'mid': None,
        'options': None,
        'observable': None,
        # maintain the raw payload as is
        'data': binary_data,
        'data_format': None,     
        'data_length': len(binary_data),
        'uri': uri
    }

    # ----- decoding ZMap binary message -----

    try:

        msg = Message.decode(bytes.fromhex(binary_data))

    # unable to decode the message
    except Exception as e:
        print(f'Decoding Error - {e}')
        return decoded_msg
    
    # ----------------------------------------

    # update/populate the starting decoded message structure with all retrieved info/data
    decoded_msg.update({
        'version': payload_handling.get_version(msg),
        'mtype': payload_handling.get_mtype(msg),
        'token': payload_handling.get_token(msg),
        'token_length': payload_handling.get_token_length(msg),
        'code': payload_handling.get_code(msg),
        'mid': payload_handling.get_mid(msg),
        'options': payload_handling.get_options(msg),
        'observable': payload_handling.get_observe(msg, uri),
        'data': payload_handling.get_payload(msg),
        'data_format': payload_handling.get_payload_format(msg),
        'data_length': payload_handling.get_payload_length(msg)
    })

    return decoded_msg
        
################################################################################################

# decode()
#   it takes as input the raw/binary ZMap data field and it returns a structured object representing a CoAP response message
#   O: version, message type (mtype), token length, code (response code), mid (message id), tokenn, options, data (payload)
async def decode(df_zmap, uri):

    # it contains all the decoded messages
    new_data_list = []
    # undecodable messages
    undecodable_msgs = []
    # it contains a summary of the decode process (success, unsuccess/<REASON>)
    decode_results = Counter()

    # client context creation for eventual GETs
    context = await Context.create_client_context()

    # iterate over rows
    for _, row in df_zmap.iterrows():

        # default decoded message (it will be returned if any error occurs)
        decoded_msg = {
            'version': None,
            'mtype': None,
            'token': None,
            'token_length': None,
            'code': None,
            'mid': None,
            'options': None,
            'observable': None,
            'data': None,
            'data_format': None,
            'data_length': None,
            'uri': None
        }

        # if success field is equal to 1 (all UDP kind of result)
        if row['success'] == 1:

            # decoding binary data and get a dictionary as result
            decoded_msg = decode_data(row['data'], uri)

            # if decodede message code field is equal to None -> something went wrong during the decoding process
            #   ex. undecodable message, ...
            if decoded_msg['code'] is None:

                # update summary
                decode_results.update([f"undecodable_msg"])
                
                undecodable_msgs.append([row['saddr'], decoded_msg['data'], decoded_msg['data_length'], decoded_msg['uri']])

            # otherwise it is a success
            else:

                # update summary
                decode_results.update(['success'])

                # detect if ZMap retrieved payload is truncated
                if payload_handling.detect_truncated_response(row['udp_pkt_size'], row['data'], decoded_msg):
                    
                    print('£' * 50)
                    
                    # print decoded message data field
                    print(f"ZMap Payload decoded: {decoded_msg['data']}")
                    
                    # -> if so, perform a new discovery through aiocoap library
                    decoded_msg = await get(row['saddr'], decoded_msg, context)

                    # print the complete/not truncated data/payload field
                    print(f"ZMap Complete Payload decoded: {decoded_msg['data']}")


        # if success field is equal to 0 (all icmp, ... kind of result)
        else:
            decode_results.update([f"unsuccess/{row['icmp_unreach_str']}"])

        # each case (un/success) -> append and store it
        new_data_list.append(decoded_msg)
        
    
    # close context/UDP socket
    await context.shutdown()
    
    # Build dataframe from list
    columns = ['version', 'mtype', 'token', 'token_length', 'code', 'mid', 'options', 'data', 'data_format', 'data_length', 'observable', 'uri']
    for col in columns:
        df_zmap[col] = [d[col] for d in new_data_list]
        
    # define result to be returned
    result = [df_zmap, decode_results]
    
    # ---------- undecodable-msgs management ----------
    
    # not empty list
    if undecodable_msgs:
        
        try: 
            # convert list of lists into Pandas dataframe
            undecodable_df = pd.DataFrame(undecodable_msgs, columns=['saddr', 'data', 'data_length', 'uri'])
            
            result.append(undecodable_df)
            
        except Exception as e:
            print(e)
            
    else:
        print("\tNo undecodable messages")
        result.append(None)
            
    # -------------------------------------------------

    # return datafram structure + summary
    return result