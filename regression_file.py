import citations
import pickle
import pandas as pd
# import plotly.io as pio
# pio.renderers.default = "browser"
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf
import numpy as np

path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"

import timeit
start_time = timeit.default_timer()
print("The start time is :", start_time)
a = citations.reading_data(1999)
print("The time difference is :", timeit.default_timer() - start_time, "seconds")


def formula_from_cols(df, y):
    return 'np.log(' + y + ')' + ' ~ ' + ' + '.join([col for col in df.columns if not col==y and col.startswith('path')])

smf.ols(formula_from_cols(a, 'type_2'), a).fit().summary()


formula = formula_from_cols(a,'type_2')


smf.ols(formula, data=a).fit().summary()

fam = sm.families.Gaussian()
ind = sm.cov_struct.Exchangeable()

mod = smf.gee(formula_from_cols(a, 'type_2'),'CitedAffiliatoinId' , a,
               cov_struct=ind, family=fam).fit().summary()

print(mod)

a['group'] = 1
md2 = smf.mixedlm(formula_from_cols(a, 'type_3'),
                  vc_formula = {"CitingAffiliatoinId":"0 + CitingAffiliatoinId",
                                "CitedAffiliatoinId":"0 + CitedAffiliatoinId"},
                  groups = a['group'], data = a).fit().summary()