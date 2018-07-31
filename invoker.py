import copy
import os
import json
import xmltodict
import boto3

from botocore.vendored import requests

record_template = {
    "study": {
        "dbgap_id": "phs001247"
    }
}

BATCH_SIZE = 10


def dict_or_list(key, dictionary):
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
    sample found in batches of 10 records.

    Will recieve an event of the form:
    ```
    "study_id": {
        "dbgap_id": "phs001247"
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
    studies = []
    if study is None:
        resp = requests.get(
            DATASERVICE + '/studies')
        if resp.status_code == 200 and len(resp.json()['results']) > 0:
            for r in resp.json()['results']:
                studies.append(study_generator(r['external_id']))

                # Flush events
                if len(studies) % BATCH_SIZE == 0:
                    invoke(lam, consentcode, studies)
                    studies = []

            if len(studies) > 0:
                invoke(lam, consentcode, studies)

    if study is None or consentcode is None:
        return 'no study or lambda specified'

    # Get dbgap released version from dataservice
    resp = requests.get(
        DATASERVICE + '/studies?external_id='+study)
    if resp.status_code == 200 and len(resp.json()['results']) > 0:
        version = resp.json()['results'][0]['version']

    dbgap_codes = read_dbgap_xml(study['dbgap_id']+version)

    records = 0
    invoked = 0
    events = []
    for row in dbgap_codes:
        if context.get_remaining_time_in_millis()/1000 < 1:
            break
        records += 1
        events.append(event_generator(study.dbgap_id, row))

        # Flush events
        if len(events) % BATCH_SIZE == 0:
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
    data = requests.get(url).content
    data = xmltodict.parse(data)
    study_status = list(dict_or_list('@registration_status', data))
    accession = list(dict_or_list('@accession', data))[0]
    if study_status[0] in ['released']:
        dbgap_codes = zip(dict_or_list('@consent_code', data),
                          dict_or_list('@submitted_sample_id', data))
        return dbgap_codes


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
    ev = copy.deepcopy(record_template)
    ev["study"]["dbgap_id"] = study
    ev["study"]["sample_id"] = row["external_id"]
    ev["study"]["consent_code"] = row["consent_code"]
    return ev


def study_generator(study):
    ev = copy.deepcopy(record_template)
    ev["study"]["dbgap_id"] = study
