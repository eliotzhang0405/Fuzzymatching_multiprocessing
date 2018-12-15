# -*- coding: utf-8 -*-
"""
Created on Sat Nov 10 10:53:20 2018

@author: Zhang Yiming
"""
import pandas as pd
import time
from fuzzywuzzy import fuzz
from multiprocessing import Pool

acq = pd.read_excel(r'D:\AFPD\Projects\My code\6. Fuzzy and multiprocessing\data\acquirers.xlsx')
bank = pd.read_csv(r'D:\AFPD\Projects\My code\6. Fuzzy and multiprocessing\data\bank_names.csv')
bank_names = list(bank['bank_names'])
df_final_result = pd.DataFrame({'name':acq['Acquirer Name'],
                                '0':['']*len(acq),
                                '1':['']*len(acq),
                                '2':['']*len(acq),
                                '3':['']*len(acq),
                                '4':['']*len(acq)})

#This part define the function#    
def fuzzy_match(index):
    ratio_record= []
    name = acq['Acquirer Name'][index].lower()
    for i in bank_names:
        name_to_match = i.lower()
        ratio_record.append(
                fuzz.ratio(name,name_to_match)
                + fuzz.partial_ratio(name,name_to_match)
                + fuzz.token_sort_ratio(name,name_to_match)
                + fuzz.token_set_ratio(name,name_to_match)
                ) #The rank is based on the sum of 4 ratios#
    df_ratio = pd.DataFrame([bank_names,ratio_record])
    df_ratio.sort_values(axis = 1, by = 1, ascending = False, inplace = True)
    return df_ratio.iloc[0][0:5]

#Using Poor function to do the multiprocessing#
if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    start_time = time.time() 
    p = Pool(processes=7)
    list_rank = p.map(fuzzy_match, range(len(acq)))
    for i in range(len(list_rank)):
        df_final_result.loc[i][1:6] = list_rank[i][0:5]
    df_final_result.to_csv(r'D:\AFPD\Projects\My code\6. Fuzzy and multiprocessing\data\final.csv')
    print("--- %s seconds ---" % (time.time() - start_time))