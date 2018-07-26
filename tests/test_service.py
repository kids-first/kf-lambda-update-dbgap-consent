import pytest
import service
# import cbttc_service


def test_handler():
    """
    Test the service handler
    """
    service.handler({}, {})
    # cbttc_service.handler({}, {})
