
import csv
import pandas as pd
import matplotlib as mpl
mpl.use('TkAgg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

import ast

fieldnames = ['companyID', 'company_emp_amount','company_verified_emp','company_loc_in_USA', 'company_num_of_loc_in_USA', 'company_loc_not_in_USA','company_num_of_loc_not_in_USA','company_in_USA_with_no_state','emp_locations_in_USA','#_of_emp_from_USA_with_State','#_of_emp_with_no_loc','#_of_emp_not_from_USA','#_of_emp_with_no_country','#_of_emp_with_no_state','total_#_of_emp']
OrgCsvFile = 'company_info_NY_10000 EXP.csv'
newCsvFile = 'company_info_NY_10000 EXP_result.csv'
FinalCsvFile = 'company_info_NY_10000 EXP_FINAL.csv'
ZeroEmpFile = 'EXP_comp_total_emp_zero.txt'


def add_fieldNames_to_csv(fieldsnames,orgFile,newCsvFile):
    # adding header to the csv file - result.csv - csv file with headers
    with open(orgFile) as csvfile, open(newCsvFile,"w",newline='') as result:
        rdr = csv.DictReader(csvfile, fieldnames=fieldnames)
        wtr = csv.DictWriter(result, fieldnames)
        wtr.writeheader()
        for line in rdr:
            wtr.writerow(line)


def dataFrameCalc(df,FinalCsvFile):
    #Stat calc - printed into final csv file
    #check if no employees in DB - write to comp_total_emp_zero.txt
    for index, row in df.iterrows():
        if row['total_#_of_emp'] == 0:
            with open(ZeroEmpFile, 'a+') as f:
                f.write("%s\n" % str(int(row["companyID"])))

    df['%_of_no_loc'] = round(df['#_of_emp_with_no_loc']/df['total_#_of_emp']*100,2)
    df['%_of_no_country'] = round(df['#_of_emp_with_no_country']/df['total_#_of_emp']*100,2)
    df['%_of_no_state'] = round(df['#_of_emp_with_no_state']/df['total_#_of_emp']*100,2)
    df['%_of_not_from_USA'] = round(df['#_of_emp_not_from_USA']/df['total_#_of_emp']*100,2)


    #checking if emp location in comp location and calculating avg

    emp_loc = df['emp_locations_in_USA']
    comp_loc = df['company_loc_in_USA']

    empNoComp = []
    empComp = []
    avgEmpLoc = []
    empNumWithCompLoc = []

    for index,locations in enumerate(emp_loc):
        emp_loc_no_comp = {}
        emp_loc_with_comp = {}
        num_of_emp_with_comp_loc = 0
        num_of_avg_emp_in_loc = {}
        emp_locations = ast.literal_eval(locations)
        comp_locations = ast.literal_eval(comp_loc[index])
        for loc in emp_locations.keys():
            if loc in comp_locations.keys():
                num_of_emp_with_comp_loc+=emp_locations[loc]
                emp_loc_with_comp[loc] = comp_locations[loc]
                num_of_avg_emp_in_loc[loc]=emp_locations[loc]/comp_locations[loc]
            else:
                emp_loc_no_comp[loc]=emp_locations[loc]

        empNoComp.append(emp_loc_no_comp)
        empComp.append(emp_loc_with_comp)
        avgEmpLoc.append(num_of_avg_emp_in_loc)
        empNumWithCompLoc.append(num_of_emp_with_comp_loc)

    df['emp_loc_with_comp']= empComp
    df['emp_num_with_comp_loc'] = empNumWithCompLoc
    df['%US_emp_with_comp_loc'] =  round(df['emp_num_with_comp_loc']/df['#_of_emp_from_USA_with_State']*100,2)
    df['num_of_avg_emp_in_loc'] = avgEmpLoc
    df['emp_loc_no_comp'] = empNoComp

    print(df.head())

    df.to_csv(FinalCsvFile)

'''
plt.hist(df['%_of_no_loc'][~np.isnan(df['%_of_no_loc'])],bins=100)
plt.xlabel("no location %")
plt.ylabel("# of companies")
plt.title('Emp with no location %')

plt.show()
'''
'''perc_of_no_loc = round(df['#_of_emp_with_no_loc']/df['total_#_of_emp']*100,2);

#using values that are not zero
print(perc_of_no_loc[perc_of_no_loc!=0])
#plt.plot
sns.set()
perc_clean = perc_of_no_loc[perc_of_no_loc!=0].sort_values()
plt.hist(perc_clean,bins=100)
plt.xlabel("no location %")
plt.ylabel("# of companies")
plt.title('Emp with no location %')
plt.show()'''


#running program

add_fieldNames_to_csv(fieldnames,OrgCsvFile,newCsvFile)
df = pd.read_csv('company_info_NY_10000 EXP_result.csv')
dataFrameCalc(df,FinalCsvFile)
