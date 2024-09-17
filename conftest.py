import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
   "create spark session"
   # return get_spark_session("LOCAL")  -- this will keep the session active and resources will be occupied
   # Below logic will release resources
   spark_session = get_spark_session("LOCAL")
   yield spark_session
   spark_session.stop()

   @pytest.fixture
   def expected_results(spark):
      "gives the expected result"
      results_schema = "state string, count integer"
      return spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("data/test_result/state_aggregate.csv")







'''
import pytest
from lib.Utils import get_spark_session
from lib import ConfigReader

@pytest.fixture
def spark():
    """Creates and manages the Spark session.""" 
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    # Stop the Spark session to release resources after the test
    spark_session.stop()

@pytest.fixture
def expected_results(spark, env):
    """ Gives the expected result by loading the CSV file into a DataFrame. """
    results_schema = "state string, count integer"
    conf = ConfigReader.get_app_config(env)
    expected_results_file_path =conf["expected_results.file.path"]
    return spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("expected_results_file_path")
'''