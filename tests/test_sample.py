# For a pytest overview: 
# https://www.guru99.com/pytest-tutorial.html
#
# cd inside the project top folder and run:
# py.test tests
#
# to run one single test file:
# py.test tests/test_<name>.py 
# 
# to run tests in parallel
# py.test -n <integer number> tests


import pytest
import os

from sdaab.utils.hello import hello


@pytest.mark.parametrize("input",[("Goku"), ("Vegeta")])
def test_hello(input):
    output = hello(input)
    output_expected = "hi " +  input + "!"
    assert output == output_expected, \
        "Failed, expected output '%s', but received '%s'" \
        % (output_expected, output)
    

def test_env():
    assert os.environ["ENV_RUN"] == "TESTING"
