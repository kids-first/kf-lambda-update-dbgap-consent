import service
import os
import pytest
from moto import mock_s3
from mock import patch, MagicMock

STUDY = None


@pytest.fixture(scope='function')
def event():
    """ Returns a test event """
    data = {'Records': [{"study": {
            "dbgap_id": "phs001168",
            "sample_id": "PA2645",
            "consent_code": "1",
            "consent_short_name": "IRB"
            }}]}
    return data


@mock_s3
def test_create(event):
    """ Test that the lamba calls the dataservice """
    os.environ['DATASERVICE'] = 'http://api.com/'
    mock = patch('service.requests')
    req = mock.start()

    def mock_get(url, *args, **kwargs):
        if '/genomic-files/' in url:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                'results': {
                    'acl': [],
                    'visible': None,
                    'kf_id': 'GF_00000000'}
            }
            return resp
        elif '/genomic-files' in url:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                'results': [{'acl': [],
                             'visible': None,
                             'kf_id': 'GF_00000000'}]}
            return resp
        elif '/biospecimens/' in url:
            resp = MagicMock()
            resp.json.return_value = {
                '_links':
                {'biospecimen_genomic_files':
                 '/biospecimen-genomic-files'
                 '?biospecimen_id = BS_HFY3Y3XM',
                 'genomic_files': '/genomic-files?biospecimen_id=BS_HFY3Y3XM'
                 },
                'results': {'kf_id': url[:-11],
                            'dbgap_consent_code': [],
                            "consent_short_name": None,
                            'consent_type': None,
                            'visible': None
                            }}
            resp.status_code = 200
            return resp
        elif '/biospecimens' in url:
            resp = MagicMock()
            resp.json.return_value = {
                'results': [{'kf_id': url[:-11],
                             'dbgap_consent_code': [],
                             'consent_type': None,
                             'visible': None,
                             '_links':
                             {'biospecimen_genomic_files':
                              '/biospecimen-genomic-files'
                              '?biospecimen_id = BS_HFY3Y3XM'
                              }}]}
            resp.status_code = 200
            return resp
        elif '/studies' in url:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {'results': [{'external_id': 'phs001168',
                                                   'version': 'v1.p1',
                                                   'kf_id': 'SD_9PYZAHHE'}]}
            return resp
        elif '/biospecimen-genomic-files' in url:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                'results':
                [{"_links": {
                    "biospecimen": "/biospecimens/BS_HFY3Y3XM",
                    "genomic_file": "/genomic-files/GF_00000000"
                },
                    'biospecimen_id': 'BS_HFY3Y3XM',
                    'genomic_file_id': 'GF_00000000'}]}
            return resp

    req.get.side_effect = mock_get

    mock_resp = MagicMock()
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000000'}}
    mock_resp.status_code = 201
    req.post.return_value = mock_resp

    res = service.handler(event, {})
    assert len(res) == 1
