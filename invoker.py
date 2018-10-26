import copy
import os
import json
import xmltodict
import boto3
from base64 import b64decode

from botocore.vendored import requests

record_template = {
    "study": {
        "dbgap_id": "phs001247"
    }
}

BATCH_SIZE = 10
SLACK_TOKEN = os.environ.get('SLACK_TOKEN', None)
SLACK_CHANNELS = os.environ.get('SLACK_CHANNEL', '').split(',')
SLACK_CHANNELS = [c.replace('#', '').replace('@', '') for c in SLACK_CHANNELS]

if SLACK_TOKEN:
    kms = boto3.client('kms', region_name='us-east-1')
    SLACK_TOKEN = kms.decrypt(CiphertextBlob=b64decode(
        SLACK_TOKEN)).get('Plaintext', None).decode('utf-8')


class DbGapException(Exception):
    pass


def dict_or_list(key, dictionary):
    """
    Flattens dictionary or list to list
    """
    if type(dictionary) != 'str':
        for k, v in dictionary.items():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for result in dict_or_list(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    if isinstance(d, dict):
                        for result in dict_or_list(key, d):
                            yield result


def handler(event, context):
    """
    Reads dbgap xml and invokes the consent code lambda for every
    sample found in batches of 10 records for the dbgap study.
    If dbgap study_id is not provided then gets all the studies
    from the dataservice and re-invokes lambda for each study.

    Will recieve an event of the form:
    ```
    {
        "study": "phs001247"
    }
    ```
    """
    DATASERVICE = os.environ.get('DATASERVICE', None)

    if DATASERVICE is None:
        return 'no dataservice url set'

    # The consent code lambda ARN
    consentcode_func = os.environ.get('FUNCTION', None)

    # User must give a function that will process individual entities
    if consentcode_func is None:
        return 'no lambda specified'

    lam = boto3.client('lambda')

    study = event.get('study', None)
    # If there is no study in the event, we should re-call this function for
    # each event in the dataservice
    if study is None:
        map_to_studies(lam, context.function_name, DATASERVICE)
    # Call functions for each sample in the study
    elif study and consentcode:
        status = map_one_study(study, lam, consentcode, context, DATASERVICE)
        if status:
            attachments = [
                {"fallback": "Failed to invoke update for "
                 "study `{}`, message:{}".format(study, status),
                 "text": "Failed to invoke update for "
                 "study `{}`, message:{}".format(study, status),
                 "color": "danger"
                 }
            ]
            send_slack(attachments=attachments)


def map_one_study(study, lam, consentcode, context, DATASERVICE):
    """
    Attempt to load a dbGaP xml for a study and call a function for each
    sample to update 
    """
    # Get dbgap released version from dataservice
    resp = requests.get(DATASERVICE + '/studies?external_id='+study)
    if resp.status_code != 200 and len(resp.json()['results']) != 1:
        return 'No unique study found with external id'

    version = resp.json()['results'][0]['version']
    if not version:
        return 'No version found for study'
    dbgap_codes = read_dbgap_xml(study+'.'+version)
    invoked = 0
    events = []
    for row in dbgap_codes:
        events.append(event_generator(study, row))

        # Flush events
        if len(events)+1 % BATCH_SIZE == 0:
            invoked += 1
            invoke(lam, consentcode, events)
            events = []

    if len(events) > 0:
        invoked += 1
        invoke(lam, consentcode, events)


def read_dbgap_xml(accession):
    """
    Reads db_gap xml file and fetches consent code and external sample id
    for a given study
    :returns: A list of tuples (consent_code, sample_id, consent_name)
        for each sample in the study.
    """
    url = (f'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/' +
           f'GetSampleStatus.cgi?study_id={accession}&rettype=xml')
    data = requests.get(url)
    if data.status_code != 200:
        raise DbGapException(f'Request for study {accession} returned non-200 '
                             f'status code: {data.status_code}')

    data = xmltodict.parse(data.content)
    study_status = list(dict_or_list('@registration_status', data))

    if study_status[0] in ['released']:
        dbgap_codes = zip(dict_or_list('@consent_code', data),
                          dict_or_list('@submitted_sample_id', data),
                          dict_or_list('@consent_short_name', data))
        return dbgap_codes
    else:
        raise DbGapException(f'study {accession} is not released by dbgap. '
                             f'registration_status: {study_status[0]}')


def invoke(lam, consentcode, records):
    """
    Invokes the lambda for given records
    """
    payload = {'Records': records}
    response = lam.invoke(
        FunctionName=consentcode,
        InvocationType='Event',
        Payload=str.encode(json.dumps(payload)),
    )


def event_generator(study, row):
    """
    Generates events for each sample in dbgap
    """
    ev = copy.deepcopy(record_template)
    ev["study"]["dbgap_id"] = study
    ev["study"]["sample_id"] = row[1]
    ev["study"]["consent_code"] = row[0]
    ev["study"]["consent_short_name"] = row[2]
    return ev


def map_to_studies(DATASERVICE, lam, invoker_func):
    """
    Gets all studies in the dataservice and re-calls this lambda for each
    providing the study_id as a parameter in the event.
    """
    resp = requests.get(DATASERVICE + '/studies?limit=100')

    if resp.status_code == 200 and len(resp.json()['results']) > 0:
        for r in resp.json()['results']:
            payload = {'study': r['external_id']}
            response = lam.invoke(
                FunctionName=invoker_func,
                InvocationType='Event',
                Payload=str.encode(json.dumps(payload)),
            )

    total = len(resp.json()['results'])
    attachments = [
        {"fallback": "I'm about to update consent codes"
         " for `{}` studies,' hold tight...".format(total),
         "text": "I'm about to update consent codes"
         " for `{}` studies,' hold tight...".format(total),
         "color": "#005e99"
         }
    ]
    send_slack(attachments=attachments)


def send_slack(msg=None, attachments=None):
    """
    Sends a slack notification
    """
    if SLACK_TOKEN is not None:
        for channel in SLACK_CHANNELS:
            message = {
                'username': 'Consent Updater',
                'icon_emoji': ':file_folder:',
                'channel': channel
            }
            if msg:
                message['text'] = msg
            if attachments:
                message['attachments'] = attachments

            resp = requests.post('https://slack.com/api/chat.postMessage',
                                 headers={
                                     'Authorization': 'Bearer '+SLACK_TOKEN},
                                 json=message)
