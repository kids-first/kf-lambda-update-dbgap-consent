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
