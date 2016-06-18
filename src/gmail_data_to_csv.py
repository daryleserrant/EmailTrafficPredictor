import pandas as pd
import numpy as np
import cPickle as pickle

def threads_to_csv(threads):
    '''
    Converts the list of threads obtained from the GMAIL API into a CSV file.
    
    Arguments:
        threads - a list of threads retrieved from the GMAIL API
    '''
    history_id = []
    thread_id = []
    snippet = []
    
    for t in threads:
        history_id.append(t['historyId'])
        thread_id.append(t['id'])
        snippet.append(t['snippet'])
    
    df_threads = pd.DataFrame({'id':thread_id, 'snippet':snippet, 'history_id':history_id})
    
    df_threads.to_csv('gmail_threads.csv',encoding='utf-8')

def messages_to_csv(messages):
    '''
    Converts the list of messages obtained from the GMAIL API into a CSV file.
    
    Arguments:
        threads - a list of messages retrieved from the GMAIL API
    '''
    history_id = []
    msg_id = []
    internal_date = []
    label_ids = []
    payload_body = []
    payload_filename = []
    payload_headers = []
    payload_parts = []
    payload_part_id = []
    payload_mime_type = []
    size_estimate = []
    snippet = []
    thread_id = []
    
    for m in messages:
        # Check for NoneTypes and integers. The GMAIL API will return None for any message it could not
        # find in the user's inbox. 
        if  type(m) == dict:
            history_id.append(m['historyId'])
            msg_id.append(m['id'])
            
            internal_date.append(m['internalDate'])
            
            if 'labelIds' in m:
                label_ids.append(m['labelIds'])
            else:
                label_ids.append(None)
                
            payload_body.append(m['payload']['body'])
            
            if m['payload']['filename'] == '':
                payload_filename.append(None)
            else:
                payload_filename.append(m['payload']['filename'])
            
            if 'headers' in m['payload']:
                payload_headers.append(m['payload']['headers'])
            else:
                payload_headers.append(None)

            if 'parts' in m['payload']:
                payload_parts.append(m['payload']['parts'])
            else:
                payload_parts.append(None)

            if 'partId'in m['payload']:
                if m['payload']['partId'] == '':
                    payload_part_id.append(None)
                else:
                    payload_part_id.append(m['payload']['partId'])
            else:
                payload_part_id.append(None)

            payload_mime_type.append(m['payload']['mimeType'])
            size_estimate.append(m['sizeEstimate'])
            snippet.append(m['snippet'])
            thread_id.append(m['threadId'])
    
    df_emails = pd.DataFrame(
    {
        'history_id': history_id,
        'msg_id': msg_id,
        'internal_date': internal_date,
        'label_ids' : label_ids,
        'payload_body' : payload_body,
        'payload_filename' : payload_filename,
        'payload_headers' : payload_headers,
        'payload_parts' : payload_parts,
        'payload_part_id' : payload_part_id,
        'payload_mime_type' : payload_mime_type,
        'size_estimate' : size_estimate,
        'snippet' : snippet,
        'thread_id' : thread_id
    })
    
    df_emails.to_csv('gmail_messages.csv',encoding='utf-8')

if __name__ == '__main__':

    with open('threads.pkl', 'r') as f:
        threads = pickle.load(f)
        threads_to_csv(threads)
    
    with open('emails.pkl', 'r') as f:
        messages = pickle.load(f)
        messages_to_csv(messages)