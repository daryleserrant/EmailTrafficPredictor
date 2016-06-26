from apiclient.discovery import build
from apiclient.http import BatchHttpRequest
from httplib2 import Http
from oauth2client import file, client, tools
import sys
import cPickle as pickle

MAX_CALLS_PER_REQUEST = 1000  # The Google API limits us to 1000 calls in a single batch request!
                              # This variable should never exceed 1000. 

emails = []

def add_email_message(request_id, response, exception):
    '''
    Callback function for message batch requests performed in request_message
    function below. Store the request response in a list
    
    Arguments:
       request_id - The id of the request
       response - The result obtained from the API call request
       exception - The exception thrown from the API, if any occurred
    '''
    emails.append(response)
     
def create_service():
    '''
    Creates a GMAIL API service object using stored user credentials. If nothing has been
    stored or the stored credentials are invalid, obtain new credentials from the user.
    
    Returns:
        The GMAIL API service object
    '''
    SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
    CLIENT_SECRET = 'client_secret.json'
    
    store = file.Storage('storage.json')
    creds = store.get()
    
    if creds is None or creds.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET, SCOPES)
        creds = tools.run(flow,store)
    
    GMAIL = build('gmail','v1', http=creds.authorize(Http()))
    return GMAIL

def request_threads(service):
    '''
    Retrieves all message threads from the user's GMAIL account.
    
    Arguments:
        service - A GMAIL API Service object
    Returns:
        A list of message threads (defined as dictionaries)
    '''
    response = service.users().threads().list(userId='me').execute()
    threads = []
    
    print "Requesting message threads..."
    
    if 'threads' in response:
        threads.extend(response['threads'])

    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = service.users().threads().list(userId='me',pageToken=page_token).execute()
        threads.extend(response['threads'])
    
    return threads

def request_message_ids(service, date_range = None):
    '''
    Retrieves all message ids from the user's GMAIL account.
    
    Arguments:
        service - A GMAIL API Service object
        date_range - If specified, returns emails that arrived within a date range
    Returns:
        A list of dictionaries that contain the id of a single message
    '''
    query = None
    
    if date_range:
        before = date_range[0].strftime('%Y/%m/%d')
        after = date_range[1].strftime('%Y/%m/%d')
        query = 'before:{} and after:{}'.format(before,after)
    
    response = service.users().messages().list(userId='me',q=query).execute()
    messages = []

    print "Requesting message ids..."
    
    if 'messages' in response:
        messages.extend(response['messages'])
    
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = service.users().messages().list(userId='me',q=query, pageToken=page_token).execute()
        messages.extend(response['messages'])
            
    return messages

def request_messages(service, messages):
    '''
    Executes one or more batch requests in order to retrieve every message in the 
    user's GMAIL account.
    
    Arguments:
        service  - A GMAIL API Service object
        messages - A list of dictionaries that contain the id of a single message 
    Returns:
        A list of messages (defined as dictionaries)
    '''    
    curr = -1
    batch_requests = []
    global emails
    emails.append(1)
    
    print "Creating message batch requests..."
    for i, msg in enumerate(messages):
        if i % MAX_CALLS_PER_REQUEST == 0:
            batch_requests.append(service.new_batch_http_request())
            curr += 1
        else:
            batch_requests[curr].add(service.users().messages().get(userId='me', id = msg['id']),
                    callback=add_email_message)
    
    batch_size = len(batch_requests)
    for i, batch in enumerate(batch_requests):
        print "Executing batch request {} of {}...".format(i+1, batch_size)
        batch.execute()

def collect_messages(date_range):
    '''
    Collects all the messages in the user's inbox
    
    Arguments:
        date_range - only pulls messages that arrive within a date range. Specified as a tuple:
                     (before_date, after_date)
    
    Returns:
        A list of messages (as dicts) from the user's mailbox.
    '''
    service = create_service()
    
    message_ids = request_message_ids(service, date_range)
    
    # Empty list
    global emails
    emails = []
    
    request_messages(service, message_ids)
    
    return emails[:]
    
        
if __name__ == '__main__':
    service = create_service()
    
    threads = request_threads(service)
    
    print "Saving threads.pkl"
    with open('threads.pkl' 'w') as f:
        pickle.dump(threads,f)
    
    message_ids = request_message_ids(service)
    
    print "Saving message_ids.pkl"
    with open('message_ids.pkl' 'w') as f:
        pickle.dump(threads,f)    
    
    request_messages(service, message_ids)
    
    print "Saving emails.pkl"
    with open('emails.pkl', 'w') as f:
        pickle.dump(emails,f)
    