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
    study_status = list(dict_or_list('@registration_status', data))
    accession = list(dict_or_list('@accession', data))[0]
    if study_status[0] in ['released', 'completed_by_gpa']:
        # if study_status[0] in ['released']:
        bio_df = pd.DataFrame()
        bio_df['consent_code'] = list(
            dict_or_list('@consent_code', data))
        bio_df['external_id'] = list(
            dict_or_list('@submitted_sample_id', data))
        bio_df['consent_short_name'] = list(
            dict_or_list('@consent_short_name', data))
        bio_df['submitted_subject_id'] = list(
            dict_or_list('@submitted_subject_id', data))
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
            bio_df['submitted_subject_id'] = list(
                dict_or_list('@submitted_subject_id', data))
            bio_df['consent_short_name'] = list(
                dict_or_list('@consent_short_name', data))
            bio_df['consent_code'] = study_id+'.c' + \
                bio_df['consent_code'].astype(str)
            return bio_df


def get_resource_dataservice(study_id, DATASERVICE, resource):
    """
    get biospecimens for the study from dataservice
    """
    resp = requests.get(
        DATASERVICE + '/studies?external_id='+study_id)
    if resp.status_code == 200 and 'results' in resp.json():
        kf_id = resp.json()['results'][0]['kf_id']
    b_df = pd.DataFrame()
    resp = requests.get(
        DATASERVICE + resource+'?limit=100&study_id='+kf_id)
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
    Entry point to the lambda function
    """
    # DATASERVICE = os.environ.get('DATASERVICE', None)
    #
    # if DATASERVICE is None:
    #     return 'no dataservice url set'
    # DATASERVICE = 'http://kf-api-dataservice-dev.kids-first.io/'
    DATASERVICE = 'http://localhost:1080/'
    study_id = 'phs001228'
    bio_df = read_dbgap_xml(study_id)
    print("Extracted {0} dbgap consent code/s for the study {1}".format(
        len(bio_df), study_id))
    if bio_df is not None:
        b_df, kf_id = get_resource_dataservice(
            study_id, DATASERVICE, '/biospecimens')
        print("Extracted {0} biospecimen/s from dataservice for the study {1}".
              format(len(b_df), study_id))
        p_df, kf_id = get_resource_dataservice(
            study_id, DATASERVICE, '/participants')
        if b_df is not None:
            # print(b_df.head(5))
            print(bio_df.head(5))
            # print(p_df.head(5))
            merged_inner = pd.merge(
                left=b_df, right=bio_df, left_on='external_sample_id',
                right_on='external_id')
            print(merged_inner.head(5))
            merged_inner = pd.merge(
                left=p_df, right=merged_inner, left_on='external_id',
                right_on='submitted_subject_id')
            print(merged_inner.columns)
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
                # bs = {
                #     "dbgap_consent_code": row['consent_code']
                # }
                # resp = requests.patch(
                #     DATASERVICE+'/biospecimens/'+row['kf_id'],
                #     json=bs)
                # if resp.status_code == 502:
                #     resp = requests.patch(
                #         DATASERVICE+'/biospecimens/'+row['kf_id'],
                #         json=bs)
                pt = {
                    "consent_type": row['consent_short_name']
                }
                resp = requests.patch(
                    DATASERVICE+'/participants/'+row['kf_id_x'],
                    json=pt)
                if resp.status_code == 502:
                    resp = requests.patch(
                        DATASERVICE+'/participants/'+row['kf_id_x'],
                        json=pt)
                bs_count = bs_count+1

                # """
                #  Get the links of genomic files for that biospecimen
                # """
                # resp = requests.get(
                #     DATASERVICE+row['_links']['biospecimen_genomic_files'])
                # """
                # Update acl in genomic_files
                #
                # """
                # gf = {"acl": []}
                # gf['acl'].extend((row['consent_code'], study_id, kf_id))
                # if resp.status_code == 200:
                #     response = resp.json()
                #     for r in response['results']:
                #         gf_link = r['_links']['genomic_file']
                #         resp = requests.get(
                #             DATASERVICE+gf_link)
                #         if (resp.status_code == 200 and
                #                 len(resp.json()['results']) > 0):
                #             response = resp.json()
                #             print(response['results']['acl'])
                #             resp = requests.patch(
                #                 DATASERVICE+gf_link,
                #                 json=gf)
                #             print(resp, gf, gf_link)
                #             gf_count = gf_count + 1
                # elif resp.status_code == 502:
                #     resp = requests.get(
                #         DATASERVICE+row['_links']['biospecimen_genomic_files'])
                #
                #     if resp.status_code == 200:
                #         response = resp.json()
                #         for r in response['results']:
                #             response = resp.json()
                #             gf['acl'] = list(
                #                 set(response['results']['acl'])
                #                 .union(set(gf['acl'])))
                #             resp = requests.patch(
                #                 DATASERVICE+gf_link,
                #                 json=gf)
                #             print('fail', resp, gf, gf_link)
                #             gf_count = gf_count + 1
            print("Updated {0} dbgap consent code/s for "
                  "biospecimens in the study {1}".
                  format(bs_count, study_id))
            print("Updated {0} genomic_file/s acl's "
                  "for the study {1}".
                  format(gf_count, study_id))
