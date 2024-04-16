import pandas as pd
import dask.dataframe as dd
import unittest
import recordlinkage as rl
from recordlinkage.base import BaseCompareFeature


class CompareZipCodes(BaseCompareFeature):

    def __init__(self, left_on, right_on, partial_sim_value, *args, **kwargs):
        super(CompareZipCodes, self).__init__(left_on, right_on, *args, **kwargs)

        self.partial_sim_value = partial_sim_value

    def _compute_vectorized(self, s1, s2):
        """Compare zipcodes.

        If the zipcodes in both records are identical, the similarity
        is 0. If the first two values agree and the last two don't, then
        the similarity is 0.5. Otherwise, the similarity is 0.
        """

        # check if the zipcode are identical (return 1 or 0)
        # print(type(s1)) #<class 'pandas.core.series.Series'>
        sim = (s1 == s2).astype(float)

        # check the first 2 numbers of the distinct comparisons
        sim[(sim == 0) & (s1.str[0:2] == s2.str[0:2])] = self.partial_sim_value

        return sim

class TestCompareZipCodes(unittest.TestCase):

    def setUp(self):
        self.index = pd.MultiIndex.from_tuples([(0, 0), (0, 1), (1, 0), (1, 1)])
        # print(self.index)
        self.dfA = pd.DataFrame({'given_name': ['John', 'Jane', 'Michael'],
                                 'surname': ['Doe', 'Smith', 'Johnson'],
                                 'postcode': ['12345', '67890', '54321']})
        self.dfB = pd.DataFrame({'given_name': ['John', 'Jane', 'Michael'],
                                 'surname': ['Doe', 'Smith', 'Johnson'],
                                 'postcode': ['12345', '67890', '54321']})

    def test_compute_pandas(self):
        comparer = rl.Compare()
        # comparer.add(CompareZipCodes('postcode', 'postcode', partial_sim_value=0.5, label='postcode'))
        comparer.add(CompareZipCodes('postcode', 'postcode', label = 'postcode', partial_sim_value=0.5))

        result = comparer.compute(self.index, self.dfA, self.dfB)
        
        self.assertIsInstance(result, pd.DataFrame)
        # self.assertIn('y_postcode', result.columns)

    def test_compute_dask(self):
        dfA = dd.from_pandas(self.dfA, npartitions=2)
        dfB = dd.from_pandas(self.dfB, npartitions=2)
        comparer = rl.Compare()
        comparer.add(CompareZipCodes('postcode', 'postcode', label='postcode', partial_sim_value=0.5))
        result = comparer.compute(self.index, self.dfA, self.dfB)
        print(f"\n{result}\n")
        self.assertIsInstance(result, pd.DataFrame)
        # self.assertIn('y_postcode', result.columns)

if __name__ == '__main__':
    unittest.main()
