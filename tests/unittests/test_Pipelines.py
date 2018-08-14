import unittest
import pandas
import json
import os
from quantutils.api.auth import CredentialsStore
from quantutils.api.marketinsights import MarketInsights, Dataset

root_dir = os.path.dirname(os.path.abspath(__file__)) + "/"

class PipelineTest(unittest.TestCase):

    def setUp(self):
        self.mi = MarketInsights(CredentialsStore())

    def testDatasetGeneration(self):

        DATASET_ID="4234f0f1b6fcc17f6458696a6cdf5101"

        dataset_desc_file = root_dir + "../../../marketinsights-data/datasets/WallSt-FinalTradingHour.json"
        with open(dataset_desc_file) as data_file:    
            dataset_desc = json.load(data_file)["dataset_desc"]

        # Test id generation 
        self.assertEqual(Dataset.generateId(dataset_desc, "DOW"), DATASET_ID)

        ds,_ = self.mi.get_dataset_by_id(DATASET_ID)
        ds = ds["2013-01-01":"2017-05-18"]

        expectedDS = pandas.read_csv(root_dir + "testDataset_DOW.csv", index_col=0, parse_dates=True, float_precision='round_trip')
        expectedDS.index = expectedDS.index.tz_localize("UTC").tz_convert("US/Eastern")
        expectedDS.columns = ds.columns

        # Test correct dataset output
        self.assertTrue(expectedDS.equals(ds))

if __name__ == '__main__':
    unittest.main()