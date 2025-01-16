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
import datetime as dt

##############################################
# 1. Restore a local SQL Server db
##############################################
"""
1.1 Put the ARMI db .bak file into your \BACKUPS folder
1.2 Restore the db to your local SQL Server
"""

##############################################
# 2. SELECT the entire table from db into dataframe
##############################################
"""
We'll focus on one table for this reprex
[dbo].[SurveyEvent]

We need the entire table so that we can validate column names and generate primary keys for the INSERTs
"""
con = sa.create_engine(assets.SACXN_STR)
ref_surveys = pd.read_sql_table('SurveyEvent',con) # a shortcut for SELECT * FROM...
# write and execute custom queries:
# with open(f'src/qry/select_SurveyEvent.sql', 'r') as query:
#     ref_surveys = pd.read_sql_query(query.read(),con)

##############################################
# 3. Create dataframes of dummy data
##############################################
"""
We need to know exactly what SQL Server "expects" INSERTs to look like.
Those "expectations" are coded into the table's CREATE TABLE query.
Specifically, we care:
    1. which columns are non-nullable
    2. what the data type is for each column

Generate a table's CREATE TABLE routine by right-clicking the table name in Data Studio
and then clicking "Script as Create". SSMS will have a similar feature.

CREATE TABLE [dbo].[SurveyEvent](
	[SurveyRecID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[SiteRecID] [int] NOT NULL,
	[PIID] [char](3) NOT NULL,
	[EditDate] [datetime] NOT NULL,
	[UnitID] [int] NULL,
	[HabitatTypeID] [int] NULL,
	[SurveyID] [int] NULL,
	[ProjectCode] [varchar](10) NOT NULL,
	[DetEstID] [char](2) NOT NULL,
	[ObsDetTypeID] [char](1) NOT NULL,
	[SMID] [char](2) NOT NULL,
	[SDate] [datetime] NOT NULL,
	[TBegin] [char](5) NOT NULL,
	[TEnd] [char](5) NOT NULL,
	[Noise] [char](1) NULL,
	[Visit] [int] NULL,
	[ObsName1] [varchar](30) NOT NULL,
	[ObsName2] [varchar](30) NULL,
	[ObsName3] [varchar](30) NULL,
	[ObsName4] [varchar](30) NULL,
	[ObsName5] [varchar](30) NULL,
	[TransectDir] [varchar](1) NULL,
	[UnitEffortID] [char](2) NULL,
	[EffectValue1] [int] NULL,
	[EffectValue2] [int] NULL,
	[EffectValue3] [int] NULL,
	[EffectValue4] [int] NULL,
	[EffectValue5] [int] NULL,
	[SurveyNotes] [varchar](2000) NULL,
	[UserID] [char](5) NOT NULL,
	[Checked] [char](1) NOT NULL,
	[ExportDate] [datetime] NULL,
	[PlotTransect] [varchar](50) NULL
    )
"""
# Here, we're creating one survey
# for our example, I'm only adding the NOT NULL fields
# you'll need to adjust this to include whatever fields you're moving from csvs to SQL Server
survey_inserts = pd.DataFrame({
    'SurveyRecID':[np.nan] # we assign primary keys later
    ,'SiteRecID':[46] # FK: SiteConstants.SiteRecID
    ,'PIID':['NE1'] # DEFAULT ('NE1')
    ,'EditDate':[str(dt.datetime.now())]
    ,'ProjectCode':['Streams'] # FK: Project.ProjectCode
    ,'DetEstID':['05']
    ,'ObsDetTypeID':['2']
    ,'SMID':['19']
    ,'SDate':['2024-06-23 00:00:00.000']
    ,'TBegin':['1801']
    ,'TEnd':['1802']
    ,'ObsName1':['Brander, Susanne'] # FK: LocalObserver.ObsName
    ,'UserID':['testx']
    ,'Checked':['2']
})

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
        ,survey_inserts # a dataframe of dummy surveys we want to add to the db
        ,ref_surveys # a dataframe of real surveys present in the db
        ,'SurveyRecID' # the name of the primary key field from the db
        ]
}

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
# OPTION 6.1: open Data Studio or SSMS and run the queries from step 5
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

test_surveys = pd.read_sql_table('SurveyEvent',con)
len(ref_surveys) == len(test_surveys)
