"""
A minimal reproducible example to demonstrate how to insert records into a local SQL Server db

1. Restore db to local
2. Test the connection between python and your local db
3. Create dataframes of dummy data
4. Prep dummy dataframes to match db
5. Load dataframes to local db
    5.1 Alternative way to load db
5. Validate load/referential integrity

"""
import pandas as pd
import numpy as np
import sqlalchemy as sa
import assets
import src.create_dataframes as cd
import src.target as t

##############################################
# 1. Restore a local SQL Server db
##############################################
"""
1.1 Put the ARMI db .bak file into your \BACKUPS folder
1.2 Restore the db to your local SQL Server
"""

##############################################
# 2. Test the connection between python and your local db
##############################################
"""
We'll focus on two tables for this reprex
[dbo.SurveyEvent, dbo.SpeciesInfo]
"""
con = sa.create_engine(assets.SACXN_STR)
with open(f'src/qry/select_SurveyEvent.sql', 'r') as query:
        ref_surveys = pd.read_sql_query(query.read(),con)

##############################################
# 3. Create dataframes of dummy data
##############################################
"""
We'll focus on one table reprex
[dbo].[SurveyEvent]
"""
survey_events = cd.create_survey_events() # we'll load this to dbo.SurveyEvent

##############################################
# 4. Prep dummy dataframes to match db
##############################################
"""
Prep the dataframe of records to match the db schema

1. column-order must match
2. column-names must match
3. data types must match; this takes trial-and-error
    - dates and times tend to be fiddly
        - SQL Server requires date/time/datetime INSERTS to be python str-type and the format of the string to match SQL Server's expectation
    - int should be python int-type
    - decimal should be python float-type
    - char/varchar should be python str-type
4. this db requires us to assign primary keys for INSERTs
    - some dbs prohibit INSERTS from including values in the primary key field and assign keys upon INSERT instead
5. foreign keys must be present in the related table(s) in SQL Server
"""
# Make a data structure to hold all of the relevant information
targets = {
    'dbo.SurveyEvent':[ # the name of a table in the "target" db
        'insert_SurveyEvent' # the name of the INSERT query
        ,survey_events # a dataframe of dummy surveys we want to add to the db
        ,ref_surveys # a dataframe of real surveys present in the db
        ,'SurveyRecID' # the name of the primary key field from the db
        ,{ # a lookup table of column names {<a column in `survey_events`>:<a column in `ref_surveys`>}
            'visit_date':'SDate'
            ,'location_id':'SiteRecID'
        }
        ]
}

# crosswalk column names from the source dataframe to the reference dataframe
for k,v in targets.items():
    misnamed_cols = [x for x in v[1].columns if x not in v[2].columns and x!='id'] # 'id' is the primary key in each of the dummy dataframes
    v[1].rename(
        columns=v[4]
        ,inplace=True
    )
    # confirm that the lookup for column-names is complete
    misnamed_cols = [x for x in v[1].columns if x not in v[2].columns and x!='id']
    if len(misnamed_cols) > 0:
        print(f"The column-name lookup is incomplete for {k}; {len(misnamed_cols)} are missing!")
        print(misnamed_cols)

# add columns to the source df that are present in the reference but absent from the source
# correct the column-order
# for k,v in targets.items():
#     # remove primary keys from source dataframes; SQL Server assigns these and you cannot keep your old primary keys
#     try:
#         del v[1]['id']
#     except:
#         pass
#     # figure out which columns are present in the reference dataframe but absent from the source dataframe; and add all of the missing columns
#     missingcols = [x for x in v[2].columns if x not in v[1].columns]
#     for col in missingcols:
#         v[1][col] = None
#     # make the source dataframe's column-order match the reference dataframe's column order
#     v[1] = v[1][v[2].columns]
#     # try:
#     #     del v[1][v[3]] # remove the primary key field
#     # except:
#     #     print('Warning, primary key {v[3]} still present in {k}')

# update data types to match SQL Server
for k,v in targets.items():
    for col in v[1].columns:
        # handle NAs (SQL Server requires NAs to be passed as 'NULL' strings)
        mask = (v[1][col].isna())
        v[1][col] = np.where(mask, 'NULL', v[1][col])
        # handle dates (SQL Server requires 8-character strings, no separator)
        if 'date' in col.lower():
            mask = (v[1][col].isna()==False)
            v[1][col] = np.where(mask, v[1][col].astype(str).str.replace('-',''), v[1][col])

# ARMILocal requires that we provide each new records's primary key
# this is odd, but ok
for k,v in targets.items():
    pkey = targets[k][3] # the primary key field in the dataframe
    maxval = max(targets[k][2][pkey].values) # what's the most-recent primary key in the dataframe (integer)
    adds = len(targets[k][1]) # how many rows are we adding?
    targets[k][1][pkey] = np.append(np.arange(maxval+1, maxval+adds, step = 1) ,maxval+adds)

##############################################
# 5. Make SQL
##############################################
for k,v in targets.items(): # loop over each dataframe we need to insert into db
    sql_texts = [] # a list where each item in the list is one line of SQL
    sql_texts.append('SET IDENTITY_INSERT dbo.SurveyEvent ON') # required syntax for this particular db because the db was created with this setting/safety-barrier
    for index, row in v[1].iterrows(): # loop over the rows in the dataframe
        line = 'INSERT INTO '+k+' ('+ str(', '.join(v[1].columns))+ ') VALUES '+ str(tuple(row.values)) # translate the df rows to SQL INSERT statements
        sql_texts.append(line) # add each INSERT the list
    sql_texts.append('SET IDENTITY_INSERT dbo.SurveyEvent OFF') # required syntax for this particular db because the db was created with this setting/safety-barrier
    sqltext = '\n'.join(sql_texts) # concat each INSERT into one text string
    sql_file = open(f"src/qry/{v[0]}.sql", "w") # write the text to file
    sql_file.write(sqltext)
    sql_file.close()
    v.append(sql_texts)
    print(f"Wrote SQL to 'src/qry/{v[0]}.sql'")

##############################################
# 6. Load dataframes to local db
# OPTION 6.1: open Data Studio or SSMS and run the queries
# OPTION 6.2: use pd.to_sql() as commented-out below
##############################################
# if con is None: # create a connection if you don't already have one
#     con = sa.create_engine(assets.SACXN_STR)
# for k,v in targets.items():
#     try:
#         # option 1: load the dataframes directly
#         v[1].to_sql(k, con, index=False,if_exists="append") # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
#     except:
#         print(f'Failed to append rows to {k}.')
##############################################
# 6. Validate load
##############################################

with open(f'src/qry/select_SurveyEvent.sql', 'r') as query:
        test_surveys = pd.read_sql_query(query.read(),con)
len(ref_surveys) == len(test_surveys)
# Are the pk-fk relationships still valid?

