

import numpy as np
import recordlinkage as rl
from recordlinkage.base import BaseCompareFeature
import pandas as pd

class GeoTable(BaseCompareFeature):
    '''
    GeoTable is a class that inherits from BaseCompareFeature. It is used to compare two records based on their
    '''
    def __init__(self, left_on, right_on, pairs_within_10km, *args, **kwargs):
        super(GeoTable, self).__init__(left_on, right_on, *args, **kwargs)

        self.pairs_within_10km = pairs_within_10km
    
    def _compute_vectorized(self, parID_1, parID_2):
        print(type(parID_1))
        # if parID_1 == parID_2: return True
        # if parID_1 in self.pairs_within_10km.keys():
        #     return self.pairs_within_10km[parID_1] == parID_2
        # else: return False
        return parID_1.eq(parID_2) | (parID_1.isin(self.pairs_within_10km.keys()) & parID_2.eq(parID_1.map(self.pairs_within_10km)))

        
    
# use
# # read
# import pickle
# # name = ""
# name = "61-51"
# filename = f'../../Output/{name}_pool_idx.sav'
# # pickle.dump(candidates, open(filename, 'wb'))
# candidates = pickle.load(open(filename, 'rb'))[0:100]

# df1 = pd.read_parquet("../../Census_samples/Whole_proc/Whole_1861_mom_pop_sp")[candidates.get_level_values(0).unique()].reset_index()
# df2 = pd.read_parquet("../../Census_samples/Whole_proc/Whole_1851_mom_pop_sp")[candidates.get_level_values(0).unique()].reset_index()

# # buffer_10k = pd.read_csv('/notebooks/data/Buffer_10k.csv')
# buffer_10k = pd.read_csv('../../Census_samples/Buffer_10k.csv')
# # par_coordinates = pd.read_csv('../Census_samples/Conparid_coordinates.csv')

# pairs_within_10km = {pair['conparid_1']:pair['conparid_2'] for pair in buffer_10k.to_dict(orient = 'records')}

# # from recordlinkage.index import SortedNeighbourhood
#     #     indexer = SortedNeighbourhood(
#     #     's', window=3
#     #         )

# index = rl.Index()
# index.full()
# candidates = index.index(df1, df2)

# comparer = rl.Compare(GeoTable('parID', 'parID', pairs_within_10km, label = 'par_witin_10km'))
# comparer = rl.Compare(GeoTable('birth_parID', 'birth_parID', pairs_within_10km, label = 'birth_par_witin_10km'))

# compared = comparer.compute(candidates, df1, df2)


# result = result.drop(columns = ['parID_1', 'parID_2'])
# print(f"done with par_witin_10km")

# result = result.drop(columns = ['birth_parID_1', 'birth_parID_2'])
# result = result[result.birth_par_witin_10km == True]
# print(f"done with birth_par_witin_10km")
# result.to_parquet('../Output/61-51_pool_geo_big.parquet')