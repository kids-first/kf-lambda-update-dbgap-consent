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
    from a list of lambda events. If all events are not processed before
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
        else:
            # update consent code and acl
            updater.update_acl(record)
            res['genomic_file'] = 'processed all records'
    return res


class AclUpdater:

    def __init__(self, api):
        self.api = api
        self.external_ids = {}
        self.version = {}

    def update_acl(self, record):
        """
        Gets the external sample id and consent code from dbgap and
        updates dbgap consent code of biospecimen and acl's of genomic files
        in dataservice
        """
        study = record['study']['dbgap_id']
        external_id = record["study"]["sample_id"]
        consent_code = record["study"]["consent_code"]
        kf_id, version = self.get_study_kf_id(study_id=study)
        bs_id = self.get_biospecimen_kf_id(
            external_sample_id=external_id,
            study_id=kf_id)
        # if matching biospecimen is found updates the consent code
        if bs_id:
            consent_code = study+'.c' + consent_code
            self.update_dbgap_consent_code(biospecimen_id=bs_id,
                                           consent_code=consent_code,
                                           study_id=kf_id)
            self.update_acl_genomic_file(kf_id=kf_id, study_id=study,
                                         biospecimen_id=bs_id,
                                         consent_code=consent_code)
        else:
            print('Biospecimen doesnot exist')

    def get_study_kf_id(self, study_id):
        """
        Gets and stores the study's kf_id and version based
        on external study id
        """
        if study_id is None:
            return
        if study_id in self.external_ids:
            return self.external_ids[study_id], self.version[study_id]
        resp = requests.get(self.api+'/studies?external_id='+study_id)
        if resp.status_code == 200 and len(resp.json()['results']) == 1:
            self.external_ids[study_id] = resp.json()['results'][0]['kf_id']
            self.version[study_id] = resp.json()['results'][0]['version']
            return self.external_ids[study_id], self.version[study_id]

    def get_biospecimen_kf_id(self, external_sample_id, study_id):
        """
        Gets biospecimen kf_id based on external sample id and study kf_id
        """
        resp = requests.get(
            self.api+'/biospecimens?study_id='+study_id +
            '&external_sample_id='+external_sample_id)
        if resp.status_code == 200 and len(resp.json()['results']) == 1:
            bs_id = resp.json()['results'][0]['kf_id']
            return bs_id

    def update_dbgap_consent_code(self, biospecimen_id,
                                  consent_code,
                                  study_id):
        """
        Updates dbgap consent code for biospecimen id
        """
        bs = {
            "dbgap_consent_code": consent_code
        }
        resp = requests.patch(
            self.api+'/biospecimens/'+biospecimen_id,
            json=bs)
        if resp.status_code == 200:
            print('Updated consent code for biospecimen')
        return

    def get_gfs_from_biospecimen(self, biospecimen_id):
        """
        Returns the links of biospecimen
        """
        resp = requests.get(
            self.api+'/biospecimens/'+biospecimen_id)
        if resp.status_code == 200 and len(resp.json()['results']) > 0:
            row = resp.json()
            return row

    def update_acl_genomic_file(self, kf_id, biospecimen_id,
                                consent_code,
                                study_id):
        """
        Updates acl's of genomic files taht are associated with biospecimen
        """
        gf = {"acl": []}
        gf['acl'].extend((consent_code, study_id, kf_id))
        row = self.get_gfs_from_biospecimen(biospecimen_id)
        if row:
            """
            Get the links of genomic files for that biospecimen
            """
            resp = requests.get(
                self.api +
                row['_links']['biospecimen_genomic_files']+'&limit=100')
            if resp.status_code == 200 and len(resp.json()['results']) > 0:
                response = resp.json()
                for r in response['results']:
                    gf_link = r['_links']['genomic_file']
                    resp = requests.get(
                        self.api+gf_link)
                    if (resp.status_code == 200 and
                            len(resp.json()['results']) > 0):
                        response = resp.json()
                        gf['acl'] = list(
                            set(response['results']['acl'])
                            .union(set(gf['acl'])))
                        resp = requests.patch(
                            self.api+gf_link,
                            json=gf)
                        if resp.status_code == 200 and len(
                                resp.json()['results']) > 0:
                            print('Updated acl for genomic file')
            else:
                return 'No associated genomic-files found'
        return
