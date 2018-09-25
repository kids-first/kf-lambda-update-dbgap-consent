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

    study = event.get('study', None)
    lam = boto3.client('lambda')
    # # The consent code lambda ARN
    consentcode = os.environ.get('FUNCTION', None)
    if consentcode is None:
        return 'no lambda specified'
    if study is None:
        invoke_invidual_study_lamba(
            DATASERVICE, lam, context.function_name)
    elif study and consentcode:
        print('Try calling study with study id ', study)
        status = invoke_individual_study(
            study, lam, consentcode, context, DATASERVICE)
        print(status)
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


def invoke_individual_study(study, lam, consentcode,
                            context, DATASERVICE):
    """
    invokes lambda for specific study
    """
    # Get dbgap released version from dataservice
    resp = requests.get(
        DATASERVICE + '/studies?external_id='+study)
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
    returns dataframe with consent code and external_sample_id
    """
    url = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id=" + \
        accession+"&rettype=xml"
    data = requests.get(url)
    if data.status_code != 200:
        raise DbGapException('Study with version doesnt exist in '
                             'dbgap or bad request')
    data = xmltodict.parse(data.content)
    study_status = list(dict_or_list('@registration_status', data))
    if study_status[0] in ['released']:
        dbgap_codes = zip(dict_or_list('@consent_code', data),
                          dict_or_list('@submitted_sample_id', data),
                          dict_or_list('@consent_short_name', data))
        return dbgap_codes
    else:
        raise DbGapException('study is not released by dbgap')


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
    ev["study"]["consent_short_name"] = row[0]
    return ev


def invoke_invidual_study_lamba(DATASERVICE, lam, invoker_func):
    """
    invokes lambda for individual study
    """
    resp = requests.get(DATASERVICE + '/studies?limit=100')
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
    if resp.status_code == 200 and len(resp.json()['results']) > 0:
        for r in resp.json()['results']:
            payload = {'study': r['external_id']}
            response = lam.invoke(
                FunctionName=invoker_func,
                InvocationType='Event',
                Payload=str.encode(json.dumps(payload)),
            )


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
