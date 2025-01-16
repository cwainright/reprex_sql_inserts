"""
A scaleable process to insert records into a local SQL Server db

1. Restore db to local
2. Process db-source CREATE TABLE queries into requirements
3. Process csvs into dataframes that match db requirements
4. Generate and execute SQL
5. Validate load/referential integrity

"""
import pandas as pd
import numpy as np
import sqlalchemy as sa
import assets
import src.target as t

# in class Target, user provides a CREATE TABLE query and the program parses it
# then the user tells gives the program the df of rows to add and a current copy of the table to which it needs to add rows
surveys = t.Target(r'src\qry\create_SurveyEvent.sql')
# surveys.set_df(df=survey_events)
# surveys.set_ref(ref=ref_surveys)
surveys_xwalk ={ # a lookup table of column names {<a column in `survey_events`>:<a column in `ref_surveys`>}
            'visit_date':'SDate'
            ,'location_id':'SiteRecID'
        }
surveys.set_col_xwalk(surveys_xwalk)
surveys.show()

surveys.get_create_qry()
surveys.get_insert_qry()
surveys.get_df()
surveys.get_ref()
surveys.get_target_tablename()
surveys.get_reqs()
surveys.get_col_xwalk()

