import os
import pytest
import xml.etree.ElementTree as ET


@pytest.fixture
def mock_dbgap():
    """
    Returns a class that mocks a dbGaP xml response
    """

    class MockdbGaP():

        def __init__(self, status_code=200, released=True):
            self.status_code = status_code
            self.released = released

        @property
        def content(self):
            """
            Returns an xml from dbGaP for a study that has been released:
            https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id=phs001228&rettype=xml
            """
            with open('tests/test_study.xml') as f:
                study = f.read()

            # Patch the registration_status to completed_by_gpa
            if not self.released:
                tree = ET.fromstring(study)
                st = tree.find('Study')
                st.set('registration_status', 'completed_by_gpa')
                study = ET.tostring(tree)
            return study

    return MockdbGaP


@pytest.fixture
def mock_dataservice():
    """
    Returns a class that mocks the dataservice api
    """

    class MockDataservice():

        def __init__(self, r, status_code=200, many=False, no_version=False,
                     no_results=False):
            self.request = r
            self.status_code = status_code
            self.many = many
            self.no_version = no_version
            self.no_results = no_results

        @property
        def content(self):
            return self.json()

        def json(self):
            """
            Returns study if external_id=phs001228 is passed:
                Returns empty version in response if self.no_version
                Returns two studies if self.many

            Returns no results if an external id other than phs001228 is given
            """
            if '/studies?external_id=' in self.request:
                if self.request.endswith('phs001228'):
                    res = [{
                        'kf_id': 'SD_00000000',
                        'version': 'v1.p1'
                    } for i in range(self.many+1)]
                    if self.no_version:
                        res[0]['version'] = None

                    return {
                        'results': res
                    }
                else:
                    return {
                        'results': []
                    }
            elif self.request.endswith('/studies?limit=100'):
                if self.no_results:
                    return {'results': [], 'total': 0}

                return {
                    'results': [
                        {'kf_id': 'SD_00000000', 'external_id': 'phs001228'}
                    ],
                    'total': 1
                }
                

    return MockDataservice
