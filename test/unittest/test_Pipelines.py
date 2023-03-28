import unittest
import pandas
import json
import os
import numpy as np
import pandas as pd
import hashlib
import quantutils.dataset.pipeline as ppl
from marketinsights.utils.auth import CredentialsStore
from marketinsights.remote.ibmcloud import CloudFunctions
from marketinsights.remote.ml import MIAssembly, Dataset

dir = os.path.dirname(os.path.abspath(__file__))


class PipelineTest(unittest.TestCase):

    def setUp(self):
        self.mi = MIAssembly(secret="marketinsights-k8s-cred")
        self.fun = CloudFunctions()

    def testDatasetGeneration(self):

        DATASET_ID = "4234f0f1b6fcc17f6458696a6cdf5101"

        dataset_desc_file = dir + "/data/WallSt-FinalTradingHour.json"
        with open(dataset_desc_file) as data_file:
            dataset_desc = json.load(data_file)["dataset_desc"]

        # Test id generation
        self.assertEqual(MIAssembly.generateMarketId(dataset_desc, "DOW"), DATASET_ID)

        ds, _ = self.mi.get_dataset_by_id(DATASET_ID)
        ds = ds["2013-01-01":"2017-05-18"]

        expectedDS = pandas.read_csv(dir + "/data/testDataset_DOW.csv", index_col=0, date_parser=lambda col: pd.to_datetime(col, utc=True), parse_dates=["Date_Time"], float_precision='round_trip')
        expectedDS.index = expectedDS.index.tz_convert("US/Eastern")
        expectedDS.columns = ds.columns

        print(expectedDS.compare(ds))
        # Test correct dataset output
        self.assertTrue(expectedDS.equals(ds))

    def testPipelineAPI(self):

        dataset_desc_file = dir + "/data/WallSt-FinalTradingHour.json"
        with open(dataset_desc_file) as data_file:
            dataset_desc = json.load(data_file)["dataset_desc"]

        ppl_desc = dataset_desc["pipeline"]["pipeline_desc"]

        with open(dir + "/data/testRawData.json") as data_file:
            testRawData = json.load(data_file)

        data = Dataset.jsontocsv(testRawData)
        data.columns = ["Open", "High", "Low", "Close"]

        csvData = {"data": json.loads(data.to_json(orient='split', date_format="iso")), "dataset": ppl_desc}
        ds = self.fun.call_function("marketdirection", csvData, debug=False)
        ds = pd.read_json(json.dumps(ds), orient='split', dtype=False)

        # Sniff test correct pipeline output
        self.assertTrue(np.allclose(ds[0][0], 0.568182))

        print(ds)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(ds).values.flatten()).hexdigest()
        assert ds.shape == (8, 10)
        assert dataHash == "31e3bf8b3a3e1cc29b70bd1c6529c658"
