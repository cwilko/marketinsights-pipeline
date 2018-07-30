import unittest
import pandas
from quantutils.api.auth import CredentialsFileStore
from quantutils.api.marketinsights import MarketInsights

class PipelineTest(unittest.TestCase):

	def setUp(self):
		self.mi = MarketInsights(CredentialsFileStore('~/.marketinsights'))

	def testDatasetGeneration(self):

		DATASET_ID = "265e2f7f3e06af1c6fc9e74434514c86"

		ds,_ = self.mi.get_dataset_by_id(DATASET_ID)
		ds.index = ds.index.tz_localize(None)

		expectedDS = pandas.read_csv("testDataset.csv", index_col=0, parse_dates=True, float_precision='round_trip')
		expectedDS.columns = ds.columns

		self.assertTrue(expectedDS.equals(ds))

if __name__ == '__main__':
    unittest.main()