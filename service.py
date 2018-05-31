from botocore.vendored import requests
import os
import xmltodict
import pandas as pd


def dict_or_list(key, dictionary):
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in dict_or_list(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in dict_or_list(key, d):
                    yield result


def read_dbgap_xml(study_id):
    """
    Reads db_gap xml file and fetches consent code and external sample id
    for a given study
    returns dataframe with consent code and external_sample_id
    """
    url = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id=" + \
        study_id+"&rettype=xml"
    data = requests.get(url).content
    data = xmltodict.parse(data)
    study_status = list(dict_or_list('@registration_status', data))
    if 'released' in study_status:
        bio_df = pd.DataFrame()
        bio_df['consent_code'] = list(
            dict_or_list('@consent_code', data))
        bio_df['external_id'] = list(
            dict_or_list('@submitted_sample_id', data))
        bio_df['consent_code'] = study_id+'.c' + \
            bio_df['consent_code'].astype(str)
    return bio_df


def get_biospecimen_dataservice(study_id, DATASERVICE):
    """
    get biospecimens for the study from dataservice
    """
    resp = requests.get(DATASERVICE + '/biospecimens?'+study_id)
    response = resp.json()
    b_df = pd.DataFrame()
    if response['_status']['code'] == 200:
        while 'next' in response['_links']:
            biospecimen = response['results']
            b_df = b_df.append(pd.DataFrame(biospecimen),
                               ignore_index=True)
            next_page = DATASERVICE + response['_links']['next']
            response = requests.get(next_page)
            response = response.json()
    return b_df


def handler(event, context):
    """
    Entry point to the lambda function
    """
    # DATASERVICE = os.environ.get('DATASERVICE', None)
    #
    # if DATASERVICE is None:
    #     return 'no dataservice url set'
    DATASERVICE = 'http://localhost:1080'
    study_id = 'phs001168'
    bio_df = read_dbgap_xml(study_id)

    print("Extracted {0} dbgap consent code/s for the study {1}".format(
        len(bio_df), study_id))

    if bio_df is not None:
        b_df = get_biospecimen_dataservice(study_id, DATASERVICE)
        print("Extracted {0} biospecimen/s from dataservice for the study {1}".
              format(len(b_df), study_id))

        if b_df is not None:
            merged_inner = pd.merge(
                left=b_df, right=bio_df, left_on='external_sample_id',
                right_on='external_id')
            print("Updating {0} dbgap consent code/s of "
                  "biospecimens for the study {1}".
                  format(len(merged_inner), study_id))

        """
        Update dbgap_consent_code for biospecimens with the
        matched external sample id's
        """
        if merged_inner is not None:
            bs_count = 0
            gf_count = 0
            for index, row in merged_inner.iterrows():
                bs = {
                    "dbgap_consent_code": row['consent_code']
                }
                resp = requests.patch(
                    DATASERVICE+'/biospecimens/'+row['kf_id'],
                    json=bs)
                if resp.status_code == 502:
                    resp = requests.patch(
                        DATASERVICE+'/biospecimens/'+row['kf_id'],
                        json=bs)
                # r = resp.json()
                bs_count = bs_count+1

                """
                 Get the links of genomic files for that biospecimen
                """
                resp = requests.get(
                    DATASERVICE+row['_links']['genomic_files'])
                """
                Update acl in genomic_files

                """
                gf = {"acl": [row['consent_code']]}
                if resp.status_code == 200:
                    response = resp.json()
                    for r in response['results']:
                        resp = requests.patch(
                            DATASERVICE+'/genomic-files/' +
                            r['kf_id'],
                            json=gf)
                elif resp.status_code == 502:
                    resp = requests.get(
                        DATASERVICE+row['_links']['genomic_files'])

                    if resp.status_code == 200:
                        response = resp.json()
                        for r in response['results']:
                            resp = requests.patch(
                                DATASERVICE+'/genomic-files/' +
                                r['kf_id'],
                                json=gf)
                gf_count = gf_count + 1
            print("Updated {0} dbgap consent code/s for "
                  "biospecimens in the study {1}".
                  format(bs_count, study_id))
            print("Updated {0} genomic_file/s acl's "
                  "for the study {1}".
                  format(gf_count, study_id))
