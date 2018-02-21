
import matplotlib as mpl
mpl.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

FinalCsvFile = 'company_info_NY_10000 EXP_FINAL.csv'

df = pd.read_csv(FinalCsvFile)

#total amount of emp
print("total amount of tested emp:",sum(df['total_#_of_emp']))

print("total amount of emp from USA with country and state: ", sum(df['#_of_emp_from_USA_with_State']))

print("total emp with no location: ", sum(df['#_of_emp_with_no_loc']))

print("total emp not from US: ", sum(df['#_of_emp_not_from_USA']))

print("total emp from US with no state: ", sum(df['#_of_emp_with_no_state']))

print("employees found in states where there is a company branch: ",sum(df['emp_num_with_comp_loc']))

'''
print("emp %_of_no_loc info:  ",df['%_of_no_loc'].describe())
fig = plt.hist(df['%_of_no_loc'][~np.isnan(df['%_of_no_loc'])],bins=100)
plt.title('companies % of employees with no loc')
plt.xlabel("%_of_no_loc")
plt.ylabel("number of companies")
plt.savefig("%_of_no_loc.png")
plt.show()
'''

print("%US_emp_with_comp_loc info:  ",df['%US_emp_with_comp_loc'].describe())
fig = plt.hist(df['%US_emp_with_comp_loc'].dropna(),bins=100)
plt.title('% of USA employees found in location of company branch')
plt.xlabel("%US_emp_with_comp_loc")
plt.ylabel("number of companies")
plt.savefig("%US_emp_with_comp_loc.png")
plt.show()