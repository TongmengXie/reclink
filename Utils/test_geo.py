import unittest
from geo import GeoTable
import pandas as pd
import dask.dataframe as dd
import recordlinkage as rl
from recordlinkage.base import BaseCompareFeature

class TestGeoTable(unittest.TestCase):
    def setUp(self):
        # multiplication the multiplication of (0, 1, 2) and (0, 1, 2)
        self.index = pd.MultiIndex.from_product([[0, 1, 2], [0, 1, 2]])
        # print(self.index)
        # Create a GeoTable instance with some sample data
        self.dfA = pd.DataFrame({'given_name': ['John', 'Jane', 'Michael'],
                                 'surname': ['Doe', 'Smith', 'Johnson'],
                                 'parID': [111530, 99999999, 111530]})

        self.dfB = pd.DataFrame({'given_name': ['John', 'Jane', 'Michael'],
                                 'surname': ['Doe', 'Smith', 'Johnson'],
                                 'parID': [99999999, 0, 111490]})
        
        buffer_10k = pd.read_csv('../Census_samples/Buffer_10k.csv')
        self.pairs_within_10km = {pair['conparid_1']:pair['conparid_2'] for pair in buffer_10k.to_dict(orient = 'records')}
        
    def test_compute_pandas(self):
        comparer = rl.Compare()
        # comparer.add(CompareZipCodes('postcode', 'postcode', partial_sim_value=0.5, label='postcode'))
        comparer.add(GeoTable('parID', 'parID', pairs_within_10km=self.pairs_within_10km, label = 'par_within_10km'))

        result = comparer.compute(self.index, self.dfA, self.dfB)
        # print(f"\n{result}\n")
        self.assertIsInstance(result, pd.DataFrame)
        # self.assertIn('y_postcode', result.columns)

    def test_compute_dask(self):
        dfA = dd.from_pandas(self.dfA, npartitions=2)
        dfB = dd.from_pandas(self.dfB, npartitions=2)
        comparer = rl.Compare()
        comparer.add(GeoTable('parID', 'parID', pairs_within_10km=self.pairs_within_10km, label = 'par_within_10km'))
        result = comparer.compute(self.index, self.dfA, self.dfB)
        print(f"\n{result}\n")
        self.assertIsInstance(result, pd.DataFrame)

if __name__ == "__main__":
    # brah = pd.read_parquet("../Output/temp/61-51_compare_corrected/partition_13000000_14000000.parquet")
    # print(brah)

    # buffer_10k = pd.read_csv('../Census_samples/Buffer_10k.csv')
    # pairs_within_10km = {pair['conparid_1']:pair['conparid_2'] for pair in buffer_10k.to_dict(orient = 'records')}
    # print(pairs_within_10km)

    unittest.main()