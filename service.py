from botocore.vendored import requests
import os
import boto3
import json


class DataserviceException(Exception):
    pass


class TimeoutException(Exception):
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
    updater = AclUpdater(DATASERVICE, context)
    res = {}
    while len(event['Records']) > 0:
        if (hasattr(context, 'invoked_function_arn') and
                context.get_remaining_time_in_millis() < 15000):
            print('not able to complete {} records, '
                  're-invoking the function'.format(len(event['Records'])))
            lam = boto3.client('lambda')
            # Invoke the lambda again with remaining records
            response = lam.invoke(
                FunctionName=context.invoked_function_arn,
                InvocationType='Event',
                Payload=str.encode(json.dumps(event))
            )
            # Stop processing and exit
            break
        else:
            try:
                record = event['Records'].pop()
                updater.update_acl(record)
                res["genomic_file"] = 'processed all records'
            except DataserviceException:
                pass
            except Exception:
                event['Records'].append(record)
    return res


class AclUpdater:

    def __init__(self, api, context):
        self.api = api
        self.context = context
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
            status = self.update_dbgap_consent_code(
                biospecimen_id=bs_id,
                consent_code=consent_code,
                consent_short_name=cons_short_name)
            if not status:
                return False
        status = self.update_acl_genomic_file(biospecimen_id=bs_id, gf=gf)
        if not status:
            return False
        return True

    def get_study_kf_id(self, study_id):
        """
        Gets and stores the study's kf_id and version based
        on external study id
        """
        retry_count = 3
        if study_id is None:
            return
        if study_id in self.external_ids:
            return self.external_ids[study_id], self.version[study_id]
        while retry_count > 1:
            resp = requests.get(
                self.api+'/studies?external_id='+study_id,
                timeout=self.context.get_remaining_time_in_millis()-14000)
            if resp.status_code != 500:
                break
            else:
                retry_count = retry_count - 1
        if resp.status_code != 200:
            raise TimeoutException
        if len(resp.json()['results']) == 1:
            self.external_ids[study_id] = resp.json()['results'][0]['kf_id']
            self.version[study_id] = resp.json()['results'][0]['version']
            return self.external_ids[study_id], self.version[study_id]

    def get_biospecimen_kf_id(self, external_sample_id, study_id):
        """
        Gets biospecimen kf_id based on external sample id and study kf_id
        """
        retry_count = 3
        while retry_count > 1:
            resp = requests.get(
                self.api+'/biospecimens?study_id='+study_id +
                '&external_sample_id='+external_sample_id,
                timeout=self.context.get_remaining_time_in_millis()-13000)
            if resp.status_code != 500:
                break
            else:
                retry_count = retry_count - 1
        if resp.status_code != 200:
            raise TimeoutException
        elif len(resp.json()['results']) == 1:
            bs_id = resp.json()['results'][0]['kf_id']
            dbgap_cons_code = resp.json()['results'][0]['dbgap_consent_code']
            consent_type = resp.json()['results'][0]['consent_type']
            visible = resp.json()['results'][0]['visible']
            return bs_id, dbgap_cons_code, consent_type, visible
        else:
            raise DataserviceException(f'No biospecimen found for '
            f'external sample id {external_sample_id}')

    def update_dbgap_consent_code(self, biospecimen_id,
                                  consent_code, consent_short_name):
        """
        Updates dbgap consent code for biospecimen id
        """
        retry_count = 3
        bs = {
            "dbgap_consent_code": consent_code,
            "consent_type": consent_short_name}
        while retry_count > 1:
            resp = requests.patch(
                self.api+'/biospecimens/'+biospecimen_id,
                json=bs,
                timeout=self.context.get_remaining_time_in_millis()-12000)
            if resp.status_code != 500:
                break
            else:
                retry_count = retry_count - 1
        if resp.status_code != 200:
            raise TimeoutException
        return True

    def get_gfs_from_biospecimen(self, biospecimen_id):
        """
        Returns the links of biospecimen
        """
        retry_count = 3
        while retry_count > 1:
            resp = requests.get(
                self.api+'/genomic-files?biospecimen_id='+biospecimen_id +
                '&limit=100',
                timeout=self.context.get_remaining_time_in_millis()-11000)
            if resp.status_code != 500:
                break
            else:
                retry_count = retry_count - 1
        if resp.status_code != 200:
            raise TimeoutException
        elif resp.status_code == 200 and len(resp.json()['results']) <= 0:
            raise DataserviceException(
                f'No associated genomic-files found for '
                f'biospecimen {biospecimen_id}')
        else:
            return resp.json()

    def update_acl_genomic_file(self, gf, biospecimen_id):
        """
        Updates acl's of genomic files that are associated with biospecimen
        """
        # Get the links of genomic files for that biospecimen
        retry_count = 3
        response = self.get_gfs_from_biospecimen(biospecimen_id)
        for r in response['results']:
            acl = gf
            if not r['visible']:
                acl = {"acl": []}
            # Do not update if acl's are as expected
            if r['acl'] != acl['acl']:
                while retry_count > 1:
                    resp = requests.patch(
                        self.api+'/genomic-files/'+r['kf_id'], json=acl,
                        timeout=self.context
                        .get_remaining_time_in_millis()-8000)
                    if resp.status_code != 500:
                        break
                    else:
                        retry_count = retry_count - 1
                if resp.status_code != 200:
                    raise TimeoutException
        return
