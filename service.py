from botocore.vendored import requests
import os
import boto3
import json


class DataserviceException(Exception):
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
        cons_short_name = record["study"]["consent_short_name"]
        kf_id, version = self.get_study_kf_id(study_id=study)
        (bs_id, dbgap_cons_code,
         consent_type, visible) = self.get_biospecimen_kf_id(
            external_sample_id=external_id,
            study_id=kf_id)
        # if matching biospecimen is found updates the consent code
        if not bs_id:
            return 'Biospecimen does not exist'

        gf = {"acl": []}
        if not visible:
            consent_code = None
        else:
            consent_code = study+'.c' + consent_code
            gf['acl'].extend((consent_code, study, kf_id))

        # Do not update biospecimen if consent code is not changed
        if dbgap_cons_code != consent_code or consent_type != cons_short_name:
            self.update_dbgap_consent_code(biospecimen_id=bs_id,
                                           consent_code=consent_code,
                                           consent_short_name=cons_short_name)
        self.update_acl_genomic_file(biospecimen_id=bs_id, gf=gf)

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
            dbgap_cons_code = resp.json()['results'][0]['dbgap_consent_code']
            consent_type = resp.json()['results'][0]['consent_type']
            visible = resp.json()['results'][0]['visible']
            return bs_id, dbgap_cons_code, consent_type, visible

    def update_dbgap_consent_code(self, biospecimen_id,
                                  consent_code, consent_short_name):
        """
        Updates dbgap consent code for biospecimen id
        """
        bs = {
            "dbgap_consent_code": consent_code,
            "consent_type": consent_short_name}
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
            self.api+'/genomic-files?biospecimen_id='+biospecimen_id +
            '&limit=100')
        if resp.status_code != 200 and len(resp.json()['results']) <= 0:
            raise DataserviceException(
                f'No associated genomic-files found for '
                f'biospecimen {biospecimen_id}')
        return resp.json()

    def update_acl_genomic_file(self, gf, biospecimen_id):
        """
        Updates acl's of genomic files that are associated with biospecimen
        """
        # Get the links of genomic files for that biospecimen
        response = self.get_gfs_from_biospecimen(biospecimen_id)
        for r in response['results']:
            acl = gf
            if not r['visible']:
                acl = {"acl": []}
            # Do not update if acl's are as expected
            if r['acl'] != acl['acl']:
                resp = requests.patch(
                    self.api+'/genomic-files/'+r['kf_id'], json=acl)
                if resp.status_code == 200 and len(
                        resp.json()['results']) == 1:
                    print('Updated acl for genomic file')
        return
