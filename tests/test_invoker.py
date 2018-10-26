import os
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
