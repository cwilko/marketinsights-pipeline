import unittest
import pandas
import json
from quantutils.api.auth import CredentialsStore
from quantutils.api.marketinsights import MarketInsights, Dataset

class PipelineTest(unittest.TestCase):

    def setUp(self):
        self.mi = MarketInsights(CredentialsStore())

    def testDatasetGeneration(self):

        #DATASET_ID = "265e2f7f3e06af1c6fc9e74434514c86"
        DATASET_ID="4234f0f1b6fcc17f6458696a6cdf5101"
        dataset_desc_file = "../../../marketinsights-data/datasets/WallSt-FinalTradingHour.json"
        with open(dataset_desc_file) as data_file:    
            dataset_desc = json.load(data_file)["dataset_desc"]

        # Test id generation 
        self.assertEqual(Dataset.generateId(dataset_desc, "DOW"), DATASET_ID)

        ds,_ = self.mi.get_dataset_by_id(DATASET_ID)

        expectedDS = pandas.read_csv("testDataset_DOW.csv", index_col=0, parse_dates=True, float_precision='round_trip')
        expectedDS.index = expectedDS.index.tz_localize("UTC").tz_convert("US/Eastern")
        expectedDS.columns = ds.columns

        # Test correct dataset output
        self.assertTrue(expectedDS.equals(ds))

if __name__ == '__main__':
    unittest.main()