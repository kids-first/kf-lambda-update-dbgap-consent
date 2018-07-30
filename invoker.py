import copy
import os
import json
import xmltodict
import boto3
import pandas as pd

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
    # # The consent code lambda ARN
    consentcode = os.environ.get('CONSENTCODE', None)
    if study is None or consentcode is None:
        return 'no study or lambda specified'

    # Get dbgap relaesed version from dataservice
    resp = requests.get(
        DATASERVICE + '/studies?external_id='+study)
    if resp.status_code == 200 and 'results' in resp.json():
        version = resp.json()['results'][0]['version']

    bio_df = read_dbgap_xml(study['dbgap_id']+version)

    lam = boto3.client('lambda')

    records = 0
    invoked = 0
    events = []
    for index, row in bio_df.iterrows():
        if context.get_remaining_time_in_millis()/1000 < 1:
            break
        records += 1
        events.append(event_generator(study.dbgap_id, row))

        # Flush events
        if len(events) >= BATCH_SIZE:
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
        bio_df = pd.DataFrame()
        bio_df['consent_code'] = list(
            dict_or_list('@consent_code', data))
        bio_df['external_id'] = list(
            dict_or_list('@submitted_sample_id', data))
        return bio_df


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
