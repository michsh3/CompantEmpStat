import psycopg2
import psycopg2.extras
import csv

HOST="unomy-newbeta-pg10.cq3osgn0otff.us-east-1.rds.amazonaws.com"
DATABASE="unomy_data"
USER="stats2018"
PASSWORD="stats2018"

CsvFile = 'company_info_NY_10000.csv'

conn = None

#set of company states for pretty printing
company_lst_of_states = set()


def printToCSV(companys_data):
    with open(CsvFile, 'a+') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(companys_data)


def connect():
    """ Connect to the PostgreSQL database server """
    try:
        # Define our connection string
        conn_string = "host="+HOST+" dbname="+DATABASE+" user="+USER+" password="+PASSWORD

        # print the connection string we will use to connect
        print("Connecting to database\n	->%s" % (conn_string))

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        print("Connected!\n")

        return conn


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    '''finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')'''

def createCompanyDataDictionary(connection):
    #returns info of company and emp locations and numbers

    # conn.cursor will return a cursor object, you can use this query to perform queries
    # note that in this example we pass a cursor_factory argument that will
    # dictionary cursor so COLUMNS will be returned as a dictionary so we
    # can access columns by their name instead of index.
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # execute Query  - return company id and source_data for comp_id<specific value, and where main location = USA and comp # of emp (verified)>10,000

    companyID = 0
    companyPersonLocations = {}

    cursor.execute("select cd.company_id,cd.source_data,json_extract_path_text(cd.source_data::json,'employees_amount')::int, json_extract_path_text(cd.source_data::json,'people_stats', 'verified')::int from unomy_data_companydata as cd where (json_extract_path_text(cd.source_data::json,'main_location', 'country')::text = 'USA' AND json_extract_path_text(cd.source_data::json,'main_location', 'state')::text  = 'New York' AND json_extract_path_text(cd.source_data::json,'employees_amount')::int >10000);")
    cursor_rows = cursor.fetchall()
    #cursor.execute("select cd.company_id,cd.source_data from unomy_data_companydata as cd where (company_id < %d and json_extract_path_text(cd.source_data::json,'main_location', 'country')::text = 'USA' AND json_extract_path_text(cd.source_data::json,'people_stats', 'verified')::int >10000);"%10000)
    print(len(cursor_rows),cursor_rows)

    if len(cursor_rows)!=0:
        for rec in cursor_rows:

            companyID = rec[0]
            compNoState = 0
            compNoUSALoc={}
            companyUSALocations={}
            companyEmployeeAmount = rec[2]
            companyVerifiedEmployee = rec[3]

            for location in rec[1]['locations']:
                #check all comp locations - counts loc not in USA, loc with no state, loc in USA with state
                if location['country']!='USA':
                    compNoUSALoc[location['country']]=compNoUSALoc.get(location['country'], 0) + 1
                elif location['state']=='':
                    compNoState+=1
                else:
                    companyUSALocations[location['state']]=companyUSALocations.get(location['state'], 0) + 1
                    company_lst_of_states.add(location['state'])

            #companyLocations = {location['state']:location.get(location['state'], 0) + 1 for location in rec[1]['locations']}

            companyPersonLocations['companyID'] = companyID
            companyPersonLocations['company_emp_amount'] = companyEmployeeAmount
            companyPersonLocations['company_verified_emp'] = companyVerifiedEmployee
            companyPersonLocations['company_loc_in_USA']=companyUSALocations
            companyPersonLocations['company_num_of_loc_in_USA']=sum(companyUSALocations.values())
            companyPersonLocations['company_loc_not_in_USA'] = compNoUSALoc
            companyPersonLocations['company_num_of_loc_not_in_USA'] = sum(compNoUSALoc.values())
            companyPersonLocations['company_in_USA_with_no_state'] = compNoState

            #testing
            print(companyPersonLocations)

            #emp_data
            noLoc = 0
            noUSAloc = 0
            noCountry = 0
            noState = 0

            companyPersonLocations['emp_locations_in_USA']={}

            #query returns emp for the companie_IDs returned from prev query
            cursor.execute("select p.company_id,pd.source_data from unomy_data_persondata as pd  join unomy_data_person as p on pd.person_id=p.id where p.company_id =%s;" %companyID )
            for rec in cursor:
                #verify if emp location has value, check if has country and if country is not USA and locations in USA
                try:
                    location = rec[1]['location']
                    if location == None or location=={}:
                        noLoc+=1
                    else:
                        country = rec[1]['location']['country']
                        if country=='':
                            noCountry+=1
                        elif country!='USA':
                            noUSAloc+=1
                        else:
                            state = rec[1]['location']['state']
                            if state=='':
                                noState+=1
                            else:
                                companyPersonLocations['emp_locations_in_USA'][state]=companyPersonLocations['emp_locations_in_USA'].get(state, 0) + 1
                except Exception as err:
                    print(err)
                    print(rec[1])
                    continue

            num_of_emp_with_state_and_country = sum(companyPersonLocations['emp_locations_in_USA'].values())

            companyPersonLocations['#_of_emp_from_USA_with_State'] = num_of_emp_with_state_and_country
            companyPersonLocations['#_of_emp_with_no_loc'] = noLoc
            companyPersonLocations['#_of_emp_not_from_USA'] = noUSAloc
            companyPersonLocations['#_of_emp_with_no_country'] = noCountry
            companyPersonLocations['#_of_emp_with_no_state'] = noState
            companyPersonLocations['total_#_of_emp'] = num_of_emp_with_state_and_country+noLoc+noUSAloc+noCountry+noState

            printToCSV(companyPersonLocations.values())

            companyPersonLocations = {}


def readFromCsv():
    with open('dict.csv', 'r') as csv_file:
        reader = csv.reader(csv_file)
        mydict = dict(reader)


print(sorted(company_lst_of_states))

conn = connect()
createCompanyDataDictionary(conn)


# testing
'''for k, v in companys_data.items():
    print(k)
    print(v)'''
#change



