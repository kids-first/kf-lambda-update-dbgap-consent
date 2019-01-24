import service
import os
import pytest
from moto import mock_s3
from mock import patch, MagicMock
import json

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

    class Context:
        def get_remaining_time_in_millis(self):
            return 1600

    def mock_get(url, *args, **kwargs):
        if '/genomic-files/' in url:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                'results': {
                    'acl': [],
                    'visible': True,
                    'kf_id': 'GF_00000000'}
            }
            return resp
        elif '/genomic-files' in url:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                'results': [{'acl': [],
                             'visible': True,
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
                'results': {'kf_id': 'BS_HFY3Y3XM',
                            'dbgap_consent_code': 'phs001168.c1',
                            "consent_short_name": None,
                            'consent_type': None,
                            'visible': True
                            }}
            resp.status_code = 200
            return resp
        elif '/biospecimens' in url:
            resp = MagicMock()
            resp.json.return_value = {
                'results': [{'kf_id': 'BS_HFY3Y3XM',
                             'dbgap_consent_code': 'phs001168.c1',
                             'consent_type': None,
                             'visible': True,
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
    req.patch.side_effect = mock_get

    mock_resp = MagicMock()
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000000'}}
    mock_resp.status_code = 201
    req.post.return_value = mock_resp
    req.patch.return_value = mock_resp
    res = service.handler(event, Context())

    assert len(res) == 1

@mock_s3
def test_out_of_time(event):
    """ Test that a function is re-invoked when records remain """
    os.environ['DATASERVICE_API'] = 'http://api.com/'
    mock_r = patch('service.requests')
    req = mock_r.start()

    class Context:
        def __init__(self):
            self.invoked_function_arn = 'arn:aws:lambda:::function:kf-lambda'

        def get_remaining_time_in_millis(self):
            return 300

    # Add a second record
    event['Records'].append(event['Records'][0])

    with patch('service.boto3.client') as mock:
        service.handler(event, Context())
        assert mock().invoke.call_count == 1

        _, args = mock().invoke.call_args_list[0]
        assert args['FunctionName'] == Context().invoked_function_arn
        assert args['InvocationType'] == 'Event'
        payload = json.loads(args['Payload'].decode('utf-8'))
        assert  payload == {'Records': event['Records']}

    mock_r.stop()

@mock_s3
def test_get_biospecimen_id(event):
    """ Test that the consent code is upadted for biospecimen """
    os.environ['DATASERVICE_API'] = 'http://api.com/'

    mock = patch('service.requests')
    req = mock.start()

    class Context:
        def __init__(self):
            self.invoked_function_arn = 'arn:aws:lambda:::function:kf-lambda'

        def get_remaining_time_in_millis(self):
            return 300

    def mock_get(url, *args, **kwargs):
        if '/biospecimens' in url:
            resp = MagicMock()
            resp.json.return_value = {
                'results': [{'kf_id': 'BS_HFY3Y3XM',
                             'dbgap_consent_code': 'phs001168.c1',
                             'consent_type': None,
                             'visible': True,
                             '_links':
                             {'biospecimen_genomic_files':
                              '/biospecimen-genomic-files'
                              '?biospecimen_id = BS_HFY3Y3XM'
                              }}]}
            resp.status_code = 200
            return resp

    req.get.side_effect = mock_get

    mock_resp = MagicMock()
    mock_resp.status_code = 201
    req.post.return_value = mock_resp

    updater = service.AclUpdater('http://api.com/', Context())
    updater.get_biospecimen_kf_id(external_sample_id='PA2645',
    study_id='SD_9PYZAHHE')
    assert req.get.call_count == 1
    mock.stop()


@mock_s3
def test_update_consent_code(event):
    """ Test that the consent code is upadted for biospecimen """
    os.environ['DATASERVICE_API'] = 'http://api.com/'

    mock = patch('service.requests')
    req = mock.start()

    class Context:
        def __init__(self):
            self.invoked_function_arn = 'arn:aws:lambda:::function:kf-lambda'

        def get_remaining_time_in_millis(self):
            return 300

    def mock_get(url, *args, **kwargs):
        if '/biospecimens/' in url:
            resp = MagicMock()
            resp.json.return_value = {
                '_links':
                {'biospecimen_genomic_files':
                 '/biospecimen-genomic-files'
                 '?biospecimen_id = BS_HFY3Y3XM',
                 'genomic_files': '/genomic-files?biospecimen_id=BS_HFY3Y3XM'
                 },
                'results': {'kf_id': 'BS_HFY3Y3XM',
                            'dbgap_consent_code': 'phs001168.c1',
                            "consent_short_name": None,
                            'consent_type': None,
                            'visible': True
                            }}
            resp.status_code = 200
            return resp

    req.get.side_effect = mock_get
    req.patch.side_effect = mock_get

    mock_resp = MagicMock()
    mock_resp.status_code = 201
    req.post.return_value = mock_resp

    updater = service.AclUpdater('http://api.com/', Context())
    updater.update_dbgap_consent_code(biospecimen_id='BS_HFY3Y3XM',
                                  consent_code='phs001168.c1',
                                  consent_short_name='')
    # Should patch biospecimen once
    assert req.patch.call_count == 1
    mock.stop()
