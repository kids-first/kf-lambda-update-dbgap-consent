from botocore.vendored import requests
import os
import boto3
import json
import xmltodict
import copy

class DataserviceException(Exception):
    pass


class TimeoutException(Exception):
    pass

record_template = {
    "study": {
        "dbgap_id": "phs001247"
    }
}

bs_record_template = {
    "biospecimen": {
        "kf_id": "SD_9PYZAHHE"
    }
}

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

def map_one_study(study, dataservice_api):
    """
    Attempt to load a dbGaP xml for a study and call a function for each
    sample to update
    :param study: The dbGaP study_id
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
    payload = {'Records': events, 'Biospecimens': [], 'GenomicFiles': []}
    if len(payload['Records']) > 0:
        handler(payload, DATASERVICE=dataservice_api)

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

def biospecimen_event_generator(row):
    """
    Generates events for each sample in dbgap
    """
    ev = copy.deepcopy(bs_record_template)
    ev['biospecimen']["study_id"] = row[0]
    ev['biospecimen']["external_sample_id"]= row[1]
    ev['biospecimen']["kf_id"] = row[2]
    ev['biospecimen']["dbgap_consent_code"] = row[3]
    ev['biospecimen']["consent_code"] = row[4]
    ev['biospecimen']["consent_type"] = row[5]
    ev['biospecimen']["cons_short_name"] = row[6]
    ev['biospecimen']["visible"] = row[7]
    ev['biospecimen']['phs_id'] = row[8]
    return ev

def gf_event_generator(row):
    """
    Generates events for each genomic_file
    """
    ev = {}
    ev[row[0]]={"acl": row[1],
                "visible": row[2],
                "consent_code": [row[3]],
                "biospecimen_id": [row[4]],
                 "study_id": row[5],
                 "phs_id": row[6]
                }
    return ev

def handler(event, DATASERVICE):
    """
    Update dbgap_consent_code in biospecimen and acl's in genomic file
    from a list of dbgap samples.
    """
    # DATASERVICE = os.environ.get('DATASERVICE', None)

    if DATASERVICE is None:
        return 'no dataservice url set'
    updater = AclUpdater(DATASERVICE)
    res = {}
    print('dbgap', len(event['Records']))
    while len(event['Records']) > 0:
            try:
                record = event['Records'].pop()
                bs_values = updater.update_biospecimens(record, event)
                event['Biospecimens'].append(biospecimen_event_generator(bs_values))
            except DataserviceException:
                pass
            except (TimeoutException, ValueError):
                event['Records'].append(record)
    print('bs', len(event['Biospecimens']))
    while len(event['Biospecimens']) > 0:
        try:
            bio_record = event['Biospecimens'].pop()
            # Do not update biospecimen if consent code is not changed
            if ((bio_record['biospecimen']['dbgap_consent_code']
            != bio_record['biospecimen']['consent_code']) or
            (bio_record['biospecimen']['consent_type'] !=
            bio_record['biospecimen']['cons_short_name'])):
                updater.update_dbgap_consent_code(
                    bio_record['biospecimen']['kf_id'],
                    bio_record['biospecimen']['consent_code'],
                    bio_record['biospecimen']['consent_type'])
            event = updater.update_genomic_files(bio_record, event)
        except DataserviceException:
            pass
        except TimeoutException:
            event['Biospecimens'].append(bio_record)
    print('gf', len(event['GenomicFiles']))
    while len(event['GenomicFiles']) > 0:
        try:
            gf_record = event['GenomicFiles'].pop()
            updater.update_acl_genomic_file(gf_record)
            print('gf', len(event['GenomicFiles']))
        except DataserviceException:
            pass
        except TimeoutException:
            event['GenomicFiles'].append(gf_record)
    return res


class AclUpdater:

    def __init__(self, api):
        self.api = api
        self.external_ids = {}
        self.version = {}

    def update_biospecimens(self, record, event):
        """
        Gets the external sample id and consent code from dbgap and
        updates dbgap consent code of biospecimen and acl's of genomic files
        in dataservice
        """

        study = record['study']['dbgap_id']
        external_id = record["study"]["sample_id"]
        consent_code = record["study"]["consent_code"]
        cons_short_name = record["study"]["consent_short_name"]
        study_id, version = self.get_study_kf_id(study_id=study)
        (bs_id, dbgap_cons_code,
            consent_type, visible) = self.get_biospecimen_kf_id(
            external_sample_id=external_id,
            study_id=study_id)

        if not visible:
            consent_code = None
        else:
            consent_code = study+'.c' + consent_code
        bs_values = [study_id, external_id, bs_id, dbgap_cons_code, consent_code,
         consent_type, cons_short_name, visible, study]
        return bs_values

    def update_genomic_files(self, record, events):
        bs_id = record['biospecimen']['kf_id']
        study_id = record['biospecimen']["study_id"]
        phs_id = record['biospecimen']["phs_id"]
        consent_code = record['biospecimen']["consent_code"]
        response = self.get_gfs_from_biospecimen(bs_id)
        for r in response['results']:
            gf_values = [r['kf_id'], r['acl'], r['visible'], consent_code,
            bs_id, study_id, phs_id]
            gf_dict = gf_event_generator(gf_values)
            if list(dict_or_list(r['kf_id'], events)):
                for gf in events['GenomicFiles']:
                    for key, val in gf.items():
                        if r['kf_id'] == key:
                            val['consent_code'].append(consent_code)
                            val['biospecimen_id'].append(bs_id)
            else:
                events['GenomicFiles'].append(gf_dict)
        return events

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
                self.api+'/studies?external_id='+study_id)
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
                '&external_sample_id='+external_sample_id)
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
            raise DataserviceException

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
                json=bs)
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
                '&limit=100')
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

    def update_acl_genomic_file(self, gf_record):
        """
        Updates acl's of genomic files
        """
        record = list(gf_record.values())[0]
        acl = {"acl": [record['phs_id'], record['study_id']]}
        if not record['visible']:
            acl = {"acl": [record['phs_id'], record['study_id']]}
        else:
            if ((len(set(record['consent_code']))>1) or
            (set(record['consent_code']) is None)):
                acl = {"acl": [record['phs_id'], record['study_id']]}
            else:
                acl['acl'].extend((record['consent_code'][0]))
        retry_count = 3
        kf_id = list(gf_record.keys())[0]
        # Do not update if acl's are as expected
        if set(record['acl']) != set(acl['acl']):
           print(gf_record)
           while retry_count > 1:
                resp = requests.patch(
                        self.api+'/genomic-files/'+kf_id, json=acl)
                print(kf_id, resp)
                if resp.status_code != 500:
                    break
                else:
                    retry_count = retry_count - 1
                if resp.status_code != 200:
                    raise TimeoutException
        return

if __name__ == '__main__':
    study = 'phs001410'
    dataservice_api = 'https://kf-api-dataservice.kids-first.io'
    map_one_study(study, dataservice_api)
