from botocore.vendored import requests
import os
import boto3
import json


class ImportException(Exception):
    pass


class DataServiceException(Exception):
    pass


class DbGapException(Exception):
    pass


def handler(event, context):
    """
    Update dbgap_consent_code in biospecimen and acl's in genomic file
    from a list of s3 events. If all events are not processed before
    the lambda runs out of time, the remaining will be submitted to
    a new function
    """
    DATASERVICE = os.environ.get('DATASERVICE', None)

    if DATASERVICE is None:
        return 'no dataservice url set'

    updater = AclUpdater(DATASERVICE)
    res = {}
    for i, record in enumerate(event['Records']):

        # If we're running out of time, stop processing and re-invoke
        # NB: We check that i > 0 to ensure that *some* progress has been made
        # to avoid infinite call chains.
        if (hasattr(context, 'invoked_function_arn') and
            context.get_remaining_time_in_millis() < 5000 and
                i > 0):
            records = event['Records'][i:]
            print('not able to complete {} records, '
                  're-invoking the function'.format(len(records)))
            remaining = {'Records': records}
            lam = boto3.client('lambda')
            context.invoked_function_arn
            # Invoke the lambda again with remaining records
            response = lam.invoke(
                FunctionName=context.invoked_function_arn,
                InvocationType='Event',
                Payload=str.encode(json.dumps(remaining))
            )
            # Stop processing and exit
            break

        study = record['study']['dbgap_id']
        external_id = record["study"]["sample_id"]
        consent_code = record["study"]["consent_code"]
        kf_id, version = updater.get_study_kf_id(study_id=study)
        bs_id = updater.get_biospecimen_kf_id(
            external_sample_id=external_id,
            study_id=kf_id)
        print('got biospecimen'+bs_id)
        consent_code = study+'.c' + consent_code
        updater.update_dbgap_consent_code(biospecimen_id=bs_id,
                                          consent_code=consent_code,
                                          study_id=kf_id)
        print('updated consent code'+bs_id)
        updater.update_acl_genomic_file(kf_id=kf_id, study_id=study,
                                        biospecimen_id=bs_id,
                                        consent_code=consent_code)
        print('updated acl'+bs_id)
    else:
        print('processed all records')
    return res


class AclUpdater:

    def __init__(self, api):
        self.api = api
        self.external_ids = {}

    def get_study_kf_id(self, study_id):
        if study_id is None:
            return
        if study_id in self.external_ids:
            return self.external_ids[study_id]
        resp = requests.get(self.api+'/studies?external_id='+study_id)
        print(self.api+'/studies?external_id='+study_id)
        print(resp)
        print(self.api)
        if resp.status_code == 200 and 'results' in resp.json():
            print(resp.json()['results'])
            self.external_ids[study_id] = resp.json()['results'][0]['kf_id']
            version = resp.json()['results'][0]['version']
            return self.external_ids[study_id], version

    def get_biospecimen_kf_id(self, external_sample_id, study_id):
        resp = requests.get(
            self.api+'/biospecimens?study_id='+study_id +
            '&external_sample_id='+external_sample_id)
        if resp.status_code == 200 and 'results' in resp.json():
            bs_id = resp.json()['results'][0]['kf_id']
            return bs_id

    def update_dbgap_consent_code(self, biospecimen_id,
                                  consent_code,
                                  study_id):
        bs = {
            "dbgap_consent_code": consent_code
        }
        resp = requests.patch(
            self.api+'/biospecimens/'+biospecimen_id,
            json=bs)
        if resp.status_code == 200:
            print('Updated consent code for biospecimen')
        return

    def update_acl_genomic_file(self, kf_id, biospecimen_id,
                                consent_code,
                                study_id):
        gf = {"acl": []}
        gf['acl'].extend((consent_code, study_id, kf_id))
        resp = requests.get(
            self.api+'/biospecimens/'+biospecimen_id)
        if resp.status_code == 200 and 'results' in resp.json():
            # ds_code = resp.json()['results'][0]['dbgap_consent_code']
            row = resp.json()['results'][0]
            """
            Get the links of genomic files for that biospecimen
            """
            print(row)
            resp = requests.get(
                self.api+row['_links']['biospecimen_genomic_files'])
            if resp.status_code == 200 and 'results' in resp.json():
                response = resp.json()
                for r in response['results']:
                    gf['acl'] = list(
                        set(r['acl']).union(set(gf['acl'])))
                    gf_link = r['_links']['genomic_file']
                    resp = requests.patch(
                        self.api+gf_link,
                        json=gf)
        return
