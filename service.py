from botocore.vendored import requests
import os
import xmltodict
import pandas as pd


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


class ImportException(Exception):
    pass


class DataServiceException(Exception):
    pass


class DbGapException(Exception):
    pass


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


def get_biospecimen_dataservice(study_id, DATASERVICE):
    """
    get biospecimens for the study from dataservice
    """
    resp = requests.get(
        DATASERVICE + '/studies?external_id='+study_id)
    if resp.status_code == 200 and 'results' in resp.json():
        kf_id = resp.json()['results'][0]['kf_id']
    b_df = pd.DataFrame()
    resp = requests.get(
        DATASERVICE + '/biospecimens?limit=100&study_id='+kf_id)
    response = resp.json()
    if resp.status_code == 200 and 'results' in resp.json():
        if 'next' not in response['_links']:
            biospecimen = response['results']
            b_df = b_df.append(pd.DataFrame(biospecimen),
                               ignore_index=True)
        else:
            while 'next' in response['_links']:
                biospecimen = response['results']
                b_df = b_df.append(pd.DataFrame(biospecimen),
                                   ignore_index=True)
                next_page = DATASERVICE + response['_links']['next']
                response = requests.get(next_page)
                response = response.json()
            else:
                biospecimen = response['results']
                b_df = b_df.append(pd.DataFrame(biospecimen),
                                   ignore_index=True)
    return b_df, kf_id


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
    # DATASERVICE = 'http://localhost:5000'
    study_id = 'phs001247'
    bio_df = read_dbgap_xml(study_id+'.v1.p1')
    print("Extracted {0} dbgap consent code/s for the study {1}".format(
        len(bio_df), study_id))

    updater = AclUpdater(DATASERVICE)
    print(study_id)
    kf_id, version = updater.get_study_kf_id(study_id=study_id)
    for index, row in bio_df.iterrows():
        bs_id = updater.get_biospecimen_kf_id(
            external_sample_id=row['external_id'],
            study_id=kf_id)
        print('got biospecimen'+bs_id)
        consent_code = study_id+'.c' + row['consent_code']
        updater.update_dbgap_consent_code(biospecimen_id=bs_id,
                                          consent_code=consent_code,
                                          study_id=kf_id)
        print('updated consent code'+bs_id)
        updater.update_acl_genomic_file(kf_id=kf_id, study_id=study_id,
                                        biospecimen_id=bs_id,
                                        consent_code=consent_code)
        print('updated acl'+bs_id)


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
        if resp.status_code == 200 and 'results' in resp.json():
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
            ds_code = resp.json()['results']['dbgap_consent_code']
            row = resp.json()
            """
            Get the links of genomic files for that biospecimen
            """
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
