import numpy as np
from recordlinkage.base import BaseCompareFeature

class sex_1_2_9(BaseCompareFeature):
    '''
    Designed for sex spectrum 1, 2, 9: 9 for unknown
    
    This class implements a comparison function for sex spectrum 1, 2, 9. It computes a vectorized output based on the input series s1 and s2.
    The conditions for the comparison are as follows:
    - If both s1 and s2 are equal to 2, the output is 1.
    - If both s1 and s2 are equal to 1, the output is 1.
    - If either s1 or s2 is equal to 9, the output is 0.5.
    - For all other cases, the output is 0.
    '''
    # def __init__(self, s1, s2):
    #     super(sex_1_2_9, self).__init__()
    
    def _compute_vectorized(self, s1, s2, *args):
        '''s1, s2 are float'''
        if s1.dtype != np.float64:
            s1 = s1.astype(np.float64)
        if s2.dtype != np.float64:
            s2 = s2.astype(np.float64)
        
        condition1 = (s1 == 2) & (s2 == 2)
        condition2 = (s1 == 1) & (s2 == 1)
        condition3 = (s1 == 9) | (s2 == 9)

        # Use numpy.select to vectorize the conditional logic
        return np.select([condition1 | condition2, condition3], [1, 0.5], default=0)
