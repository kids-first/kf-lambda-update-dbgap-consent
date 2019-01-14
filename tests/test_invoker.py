import os
import json
import pytest
from mock import patch, MagicMock
import invoker

STUDY = None


def test_read_dbgap_xml(mock_dbgap):
    """ Test that an xml is fetched from dbGaP correctly """
    mock = patch('invoker.requests')
    req = mock.start()
    req.get.return_value = mock_dbgap()

    resp = invoker.read_dbgap_xml('phs001228')

    assert req.get.call_count == 1
    assert len(list(resp)) == 1113


def test_read_dbgap_xml_bad_resp(mock_dbgap):
    """ Test that error is thrown if there is trouble with requesting dbgap """
    mock = patch('invoker.requests')
    req = mock.start()
    req.get.return_value = mock_dbgap(status_code=404)

    with pytest.raises(invoker.DbGapException) as err:
        resp = invoker.read_dbgap_xml('phs001228')
        assert req.get.call_count == 1

        assert 'study phs001228 returned non-200 status code: 404' in err


def test_read_dbgap_xml_not_released(mock_dbgap):
    """ Test that error is thrown if study has not been released """
    mock = patch('invoker.requests')
    req = mock.start()
    req.get.return_value = mock_dbgap(released=False)

    with pytest.raises(invoker.DbGapException) as err:
        resp = invoker.read_dbgap_xml('phs001228')
        assert req.get.call_count == 1

        assert 'study phs001228 not released' in err
        assert 'registration_status: completed_by_gpa' in err


def test_map_one_study(mock_dbgap, mock_dataservice):
    """ Test that functions are called for each sample in the dbGaP xml """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    invoker.map_one_study('phs001228', lam, 'consent_func', 'http://ds')

    # Calling entire study in one batch
    assert lam.invoke.call_count == 1
    # Check one of the calls to lambda were made with the right payload
    call = lam.invoke.call_args_list[0]
    assert call[1]['FunctionName'] == 'consent_func'
    assert call[1]['InvocationType'] == 'Event'
    payload = json.loads(call[1]['Payload'])
    assert 'Records' in payload
    assert len(payload['Records']) == 1113
    assert 'study' in payload['Records'][0]
    assert payload['Records'][0]['study']['consent_code'] == '1'
    assert payload['Records'][0]['study']['consent_short_name'] == 'GRU'
    assert payload['Records'][0]['study']['dbgap_id'] == 'phs001228'
    assert 'sample_id' in payload['Records'][0]['study']


def test_map_one_study_bad_ds_resp(mock_dbgap, mock_dataservice):
    """ Test behavior when the dataservice responds with non-200 """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, status_code=500, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    with pytest.raises(invoker.DataserviceException) as err:
        invoker.map_one_study('phs001228', lam, 'consent_func', 'http://ds')
    assert 'Problem requesting dataservice: ' in str(err.value)


def test_map_one_study_non_unique(mock_dbgap, mock_dataservice):
    """ Test behavior when given phs id returns multiple studies """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, many=True, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    with pytest.raises(invoker.DataserviceException) as err:
        r = invoker.map_one_study('phs001228', lam, 'consent_func', 'http://ds')

    assert 'More than one study found for phs001228' in str(err.value)


def test_map_one_study_no_results(mock_dbgap, mock_dataservice):
    """ Test behavior when no study is returned for given phs is """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    with pytest.raises(invoker.DataserviceException) as err:
        r = invoker.map_one_study('NOTFOUND', lam, 'consent_func', 'http://ds')

    assert 'Could not find a study for NOTFOUND' in str(err.value)


def test_map_one_study_no_version(mock_dbgap, mock_dataservice):
    """ Test behavior when a study with no version is returned """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, no_version=True, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    with pytest.raises(invoker.DataserviceException) as err:
        r = invoker.map_one_study('phs001228', lam, 'consent_func', 'http://ds')

    assert 'phs001228 has no version in dataservice' in str(err.value)


def test_map_to_studies(mock_dbgap, mock_dataservice):
    """ Test that the invoker is called for each study """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    invoker.map_to_studies(lam, 'invoker_func', 'http://ds')
    assert req.get.call_count == 1
    assert lam.invoke.call_count == 1


def test_map_to_studies_bad_response(mock_dbgap, mock_dataservice):
    """ Test behavior when dataservice returns a bad response """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, status_code=500, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    with pytest.raises(invoker.DataserviceException) as err:
        invoker.map_to_studies(lam, 'invoker_func', 'http://ds')
    assert req.get.call_count == 1
    assert 'Problem requesting dataservice' in str(err.value)
    assert lam.invoke.call_count == 0


def test_map_to_studies_no_results(mock_dbgap, mock_dataservice):
    """ Test behavior when dataservice returns no studies """
    mock_req = patch('invoker.requests')
    req = mock_req.start()

    def router(r, *args, **kwargs):
        if r.startswith('http://ds'):
            return mock_dataservice(r, no_results=True, *args, **kwargs)
        else:
            return mock_dbgap()

    req.get.side_effect = router

    lam = MagicMock()
    with pytest.raises(invoker.DataserviceException) as err:
        invoker.map_to_studies(lam, 'invoker_func', 'http://ds')
    assert req.get.call_count == 1
    assert 'Dataservice has no studies' in str(err.value)
    assert lam.invoke.call_count == 0
