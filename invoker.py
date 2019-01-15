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

SLACK_TOKEN = os.environ.get('SLACK_TOKEN', None)
SLACK_CHANNELS = os.environ.get('SLACK_CHANNEL', '').split(',')
SLACK_CHANNELS = [c.replace('#', '').replace('@', '') for c in SLACK_CHANNELS]

if SLACK_TOKEN:
    kms = boto3.client('kms', region_name='us-east-1')
    SLACK_TOKEN = kms.decrypt(CiphertextBlob=b64decode(
        SLACK_TOKEN)).get('Plaintext', None).decode('utf-8')


class DbGapException(Exception):
    pass


class DataserviceException(Exception):
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
    Reads dbgap xml and invokes the consent code lambda for the dbgap study.
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
    elif study and consentcode_func:
        try:
            map_one_study(study, lam, consentcode_func, DATASERVICE)
        except (DataserviceException, DbGapException) as err:
            # There was a problem trying to process the study, notify slack
            msg = f'Problem invoking for `{study}`: {err}'
            attachments = [{
                'fallback': msg,
                'text': msg,
                'color': 'danger'
            }]
            send_slack(attachments=attachments)


def map_one_study(study, lam, consentcode, dataservice_api):
    """
    Attempt to load a dbGaP xml for a study and call a function for each
    sample to update

    :param study: The dbGaP study_id
    :param lam: A boto lambda client used to invoke lamda functions
    :param consentcode: The name of the function that will be called for each
        sample to update it inside the dataservice
    :param dataservice_api: The url of the dataservice api
    """
    # Get dbgap released version from dataservice
    url = f'{dataservice_api}/studies?external_id={study}'
    resp = requests.get(url)

    # Problem with the request
    if resp.status_code != 200:
        raise DataserviceException(f'Problem requesting dataservice: '
                                   f'{url}, {resp.content}')
    # There was more than one study returned for this accession code
    if len(resp.json()['results']) > 1:
        raise DataserviceException(f'More than one study found for {study}')
    # There was no study found in the dataservice with the accession code
    if len(resp.json()['results']) == 0:
        raise DataserviceException(f'Could not find a study for {study}')

    version = resp.json()['results'][0]['version']
    # The study has no version registered in the dataservice
    if not version:
        raise DataserviceException(f'{study} has no version in dataservice')

    # Need to now invoke new functions in batches to process each sample
    dbgap_codes = read_dbgap_xml(study+'.'+version)
    events = []
    for row in dbgap_codes:
        events.append(event_generator(study, row))
    if len(events) > 0:
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


def map_to_studies(lam, invoker_func, dataservice_api):
    """
    Gets all studies in the dataservice and re-calls this lambda for each
    providing the study_id as a parameter in the event.

    :param lam: A boto lambda client used to invoke lamda functions
    :param invoker_func: The name of the current function to call again to
        process a given study
    :param dataservice_api: The url of the dataservice api
    """
    url = f'{dataservice_api}/studies?limit=100'
    resp = requests.get(url)

    if resp.status_code != 200:
        raise DataserviceException(f'Problem requesting dataservice: '
                                   f'{url}, {resp.content}')
    if 'total' not in resp.json() or resp.json()['total'] == 0:
        raise DataserviceException(f'Dataservice has no studies')

    for r in resp.json()['results']:
        payload = {'study': r['external_id']}
        response = lam.invoke(
            FunctionName=invoker_func,
            InvocationType='Event',
            Payload=str.encode(json.dumps(payload)),
        )

    total = resp.json()['total']
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
