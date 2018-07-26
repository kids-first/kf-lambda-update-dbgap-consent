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
    print(type(data))
    study_status = list(dict_or_list('@registration_status', data))
    accession = list(dict_or_list('@accession', data))[0]
    if 'released' in study_status:
        bio_df = pd.DataFrame()
        bio_df['consent_code'] = list(
            dict_or_list('@consent_code', data))
        bio_df['external_id'] = list(
            dict_or_list('@submitted_sample_id', data))
        bio_df['consent_code'] = study_id+'.c' + \
            bio_df['consent_code'].astype(str)
        return bio_df
    else:
        version = accession.split('.')
        version[1] = 'v'+str(int(version[1][1])-1)
        accession = ".".join(version)
        print(accession)
        url = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id=" + \
            accession+"&rettype=xml"
        data = requests.get(url).content
        data = xmltodict.parse(data)
        study_status = list(dict_or_list('@registration_status', data))
        accession = list(dict_or_list('@accession', data))
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
    b_df = pd.DataFrame()
    resp = requests.get(
        DATASERVICE + '/biospecimens?limit=100&study_id='+study_id)
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
    study_id = 'SD_BHJXBDQK'
    consent_code = study_id+'.c1'
    b_df = get_biospecimen_dataservice(study_id, DATASERVICE)
    bs = {
        "dbgap_consent_code": consent_code
    }
    print(bs)
    bs_count = 0
    if b_df is not None:
        gf_count = 0
        for index, row in b_df.iterrows():
            resp = requests.patch(
                DATASERVICE+'/biospecimens/'+row['kf_id'],
                json=bs)
            if resp.status_code == 502:
                resp = requests.patch(
                    DATASERVICE+'/biospecimens/'+row['kf_id'],
                    json=bs)
            bs_count = bs_count+1
            """
             Get the links of genomic files for that biospecimen
            """
            resp = requests.get(
                DATASERVICE+row['_links']['genomic_files'])
            """
            Update acl in genomic_files

            """
            gf = {"acl": []}
            gf['acl'].extend((consent_code, study_id))
            # gf = {"acl": [study_id]}
            if resp.status_code == 200:
                response = resp.json()
                for r in response['results']:
                    print(r['acl'])
                    gf['acl'] = list(set(r['acl']).union(set(gf['acl'])))
                    print(gf)
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
                        gf['acl'] = list(
                            set(r['acl']).union(set(gf['acl'])))
                        print(gf)
                        resp = requests.patch(
                            DATASERVICE+'/genomic-files/' +
                            r['kf_id'],
                            json=gf)
            gf_count = gf_count + 1

        print("Updated {0} genomic_file/s acl's "
              "for the study {1}".
              format(gf_count, study_id))
