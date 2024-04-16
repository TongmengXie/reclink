import unittest
import pandas as pd
import numpy as np
from dask import dataframe as dd
import recordlinkage as rl

from sex_1_2_9 import sex_1_2_9

class TestSex129(unittest.TestCase):
    def setUp(self):
        self.comparator = sex_1_2_9('s1', 's2')

    def test_equal_2(self):
        s1 = pd.Series([2, 2, 2])
        s2 = pd.Series([2, 2, 2])
        expected_output = np.array([1, 1, 1])
        result = self.comparator._compute_vectorized(s1, s2)
        np.testing.assert_array_equal(result, expected_output)

    def test_equal_1(self):
        s1 = pd.Series([1, 1, 1])
        s2 = pd.Series([1, 1, 1])
        expected_output = np.array([1, 1, 1])
        result = self.comparator._compute_vectorized(s1, s2)
        np.testing.assert_array_equal(result, expected_output)

    def test_equal_9(self):
        s1 = pd.Series([9, 2, 1])
        s2 = pd.Series([2, 9, 1])
        expected_output = np.array([0.5, 0.5, 1])
        result = self.comparator._compute_vectorized(s1, s2)
        np.testing.assert_array_equal(result, expected_output)

    def test_other_cases(self):
        s1 = pd.Series([2, 1, 9])
        s2 = pd.Series([1, 2, 9])
        expected_output = np.array([0, 0, 0.5])
        result = self.comparator._compute_vectorized(s1, s2)
        np.testing.assert_array_equal(result, expected_output)

    def test_pd_dataframe(self):
        s1 = pd.Series([2, 1, 9])
        s2 = pd.Series([1, 2, 9])
        expected_output = np.array([0, 0, 0.5])
        df = pd.DataFrame({'s1': s1, 's2': s2})
        result = self.comparator._compute_vectorized(df['s1'], df['s2'])
        np.testing.assert_array_equal(result, expected_output)

    # def test_two_dask_dataframe(self):
    #     import dask.dataframe as dd
    #     # create dd
    #     seires1 = pd.Series([2, 1, 9])
    #     seires2 = pd.Series([1, 2, 9])
        
    #     df1 = pd.DataFrame({'s': seires1})
    #     # df1 = dd.from_pandas(pd.DataFrame({'s': seires1}), npartitions=2)
    #     ###########
    #     # df1 = df1.compute_chunk_sizes()
    #     # df1 = df1.to_dask_array(lengths=True)
    #     ##############
    #     df2 = pd.DataFrame({'s': seires2})
    #     # df2 = dd.from_pandas(pd.DataFrame({'s': seires2}), npartitions=2)
    #     # df2 = df2.compute_chunk_sizes()\
    #     # df2 = df2.to_dask_array(lengths=True)
    #     # create indexer
        
    #     # from recordlinkage import SortedNeighbourhoodIndex
    #     from recordlinkage.index import SortedNeighbourhood
    #     indexer = SortedNeighbourhood(
    #     's', window=3
    #         )
    #     indexer.index(df1, df2)
    #     candidates = indexer.index(df1, df2)
    #     print("Initializing indexer...")
        
    #     # create comparer        
    #     self.comparer = rl.Compare()
    #     self.comparer.add(sex_1_2_9('s', 's', label = 's'))
    #     # self.comparer.numeric('s', 's', label = 's')

    #     result = self.comparer.compute(candidates, df1, df2)
    #     print(result)
    #     expected_output = np.array([0, 0, 0.5])
    #     np.testing.assert_array_equal(result, expected_output)

        

    
if __name__ == '__main__':
    unittest.main()