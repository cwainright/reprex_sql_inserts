
import re
import pandas as pd
import numpy as np

class Target():

    def __init__(self, create_qry:str):
        """A Target is a table in the SQL Server db

        Each attribute is needed to both a) load records to the db and b) validate that the rows were added.

        Args:
            create_qry (str, required): Relative or absolute filepath to a CREATE TABLE sql query for the `Target` table. Required.

        Examples:
            import src.target as t
            surveys = t.Target(r'src\qry\create_SurveyEvent.sql')
        """

        self.create_qry = create_qry
        self.insert_qry = 'no query set; use Target.set_insert_qry()'
        self.df = pd.DataFrame()
        self.ref = pd.DataFrame()
        self.col_xwalk = {'no_columns specified':'use Target.set_col_xwalk()'}
        self.attrs = {
            'create_qry':"User-provided; str. Relative or absolute filepath to a .sql file. This is the filepath where your table's CREATE TABLE .sql file lives."
            ,'insert_qry':"User-provided; str. Relative or absolute filepath to a .sql file. This is the filepath to which you will write the insert SQL output for the table."
            ,'df':"User-provided; pd.DataFrame. Short for 'dataframe'. A pd.DataFrame() containing rows of data to be added to a table the SQL Server db"
            ,'ref':"User-provided; pd.DataFrame. Short for 'reference'. A pd.DataFrame() contining rows of data from the table to which you want to add rows SQL Server db"
            ,'col_xwalk': "User-provided; dict. Short for column name crosswalk. A dictionary where the keys are columns present in `df` and values are columns present in `ref`."
            ,'target_tablename': "Program generated; str. The name of the SQL Server table to which the `create_qry` corresponds."
            ,'reqs': "Program generated; pd.DataFrame. A dataframe of fieldnames, data types, field constraints that were extracted from the query at `create_qry`."
        }
        if self.create_qry:
            print(f'Create query set to {self.create_qry}')
            self._set_target_tablename()
            print(f'\nTarget tablename updated to match {self.create_qry}:')
            print(self.get_target_tablename())
            self._set_reqs()
            print(f'\nRequirements parsed from {self.create_qry}:')
            print(self.get_reqs())
        else:
            self.reqs = pd.DataFrame()
            self.target_tablename = 'no target tablename; use Target.set_insert_qry()'
    
    def show(self):
        """
        Print a description of what each attribute in a `Target` object is
        """
        for k,v in self.attrs.items():
            print(f"Attribute name: {k}")
            print(f"    Attribute description: {v}")

    def get_col_xwalk(self):
        for k,v in self.col_xwalk.items():
            print(f"{k} -> {v}")

    def set_col_xwalk(self, xwalk:dict):
        """_summary_

        Args:
            xwalk (dict): A dictionary where the keys are columns present in `df` and values are columns present in `ref`.

        Examples:
            import src.target as t
            import src.create_dataframes as cd
            surveys = t.Target('src/qry/create_SurveyEvent.sql')
            surveys_xwalk ={ # a lookup table of column names {<a column in `df`>:<a column in `ref`>}
                'visit_date':'SDate'
                ,'location_id':'SiteRecID'
            }
            surveys.set_col_xwalk(xwalk=survey_events)
        """
        self.col_xwalk = xwalk

    
    def get_target_tablename(self):
        """
        Get the value stored as `target_tablename` attribute of a Target

        Returns:
            str: The name of the SQL Server db table to which you want to add rows

        Examples:
            import src.target as t
            surveys = t.Target(r'src\qry\create_SurveyEvent.sql')
            surveys.get_target_tablename()
        """

        return self.target_tablename

    def _set_target_tablename(self):
        """
        Read SQL file, break into lines, clean the lines, parse the lines to determine table name from CREATE TABLE.
        
        Not intended to be called directly; executes automatically when user calls set_create_qry().

        Args:
            self (src.target.Target): An object of type Target. Required.

        Examples:
            import src.target as t
            mytarget = t.Target('src/qry/create_a_table.sql')
            # reads the sql file and assigns the `tablename`
        """

        fname = self.create_qry
        f = open(fname, "r")
        # f = open(r'src\qry\create_SpeciesInfo.sql', "r")
        lines:str = f.read()
        f.close()

        s2 = lines.replace('\n',' ').replace('\t',' ')
        tablename = re.search('CREATE TABLE(?:(?!CONSTRAINT).)*', s2)
        tablename = tablename.group(0).split('(',1)[0].replace('CREATE TABLE','').strip()

        self.target_tablename = tablename

    def get_df(self):
        """
        Get the value stored as `df` attribute of a Target

        Returns:
            pd.DataFrame: Dataframe of records to be added to the SQL Server table (Target.target_tablename).

        Examples:
            import src.target as t
            surveys = t.Target(r'src\qry\create_SurveyEvent.sql')
            surveys.get_df()
        """

        return self.df

    def set_df(self, df:pd.DataFrame):
        """
        Set the `df` attribute of a Target

        Args:
            df (pd.DataFrame): Dataframe of records to be added to the SQL Server table (Target.target_tablename)

        Examples:
            import src.target as t
            import src.create_dataframes as cd
            surveys = t.Target('src/qry/create_SurveyEvent.sql')
            survey_events = cd.create_survey_events()
            surveys.set_df(df=survey_events)
        """

        self.df = df

    def check_df(self) -> str:
        """
        Checks a `Target` object's `df` attribute against rules specified in its `reqs` attribute

        Returns
        """

        outcome = 'FAIL: Self.df failed validation'

        return outcome



    def get_ref(self):

        return self.ref

    def set_ref(self, ref:pd.DataFrame):

        self.ref = ref
    
    def get_insert_qry(self):
        """
        Get the value stored as `insert_qry` attribute of a Target

        Returns:
            str: relative or absolute filepath to a .sql file. This is the filepath to which you will write the insert SQL output for the table.

        Examples:
            import src.target as t
            surveys = t.Target(r'src\qry\create_SurveyEvent.sql')
            surveys.get_insert_qry()
        """
        return self.insert_qry

    def set_insert_qry(self, insert_qry:str):
        """
        Set the `insert_qry` attribute of a Target

        Args:
            insert_qry (str): relative or absolute filepath to a .sql file. This is the filepath to which you will write the insert SQL output for the table.

        Examples:
            import src.target as t
            surveys = t.Target('src/qry/create_SurveyEvent.sql')
            surveys.set_insert_qry('src/qry/insert_SurveyEvents.sql')
        """
        assert insert_qry.endswith('.sql'), print('`insert_qry` must end in ".sql". You provided {insert_qry}')
        self.insert_qry = insert_qry
    
    def get_reqs(self):
        """
        Get the value stored as `reqs` attribute of a Target

        Returns:
            pd.DataFrame: a dataframe of fieldnames and their specifications for the Target table

        Examples:
            import src.target as t
            surveys = t.Target(r'src\qry\create_SurveyEvent.sql')
            surveys.get_reqs()
        """
        return self.reqs

    def _set_reqs(self):
        """
        Read SQL file, break into lines, clean the lines, parse the lines to determine fieldnames, data types, field constraints.
        
        Not intended to be called directly; executes automatically when user calls set_create_qry().

        Args:
            self (src.target.Target): An object of type Target. Required.

        Examples:
            import src.target as t
            mytarget = t.Target('src/qry/create_a_table.sql')
            mytarget.set_create_qry('src/qry/create_another_table.sql)
        """
        assert self.create_qry is not None

        f = open(self.create_qry, "r")
        lines:str = f.read()
        f.close()
        
        s2 = lines.replace('\n',' ').replace('\t',' ')
        fieldstr = re.search('CREATE TABLE(?:(?!CONSTRAINT).)*', s2)
        fieldstr = fieldstr.group(0).replace('CREATE TABLE','').strip().split('(',1)[1].strip()
        fieldlist = fieldstr.split(', ')
        fieldlist = [x.strip() for x in fieldlist]

        constraints = {
            'original':[]
            ,'fieldnames':[]
            ,'fieldtypes':[]
            ,'maxlens':[]
            ,'can_be_null':[]
        }

        for field in fieldlist:
            constraints['original'].append(field)
            pat = r"\[(.*?)\]"
            found = re.findall(pat,field)
            if len(found) == 2:
                fieldname = found[0]
                fieldtype = found[1]
                if fieldname:
                    constraints['fieldnames'].append(fieldname)
                else:
                    constraints['fieldnames'].append(None)
                if fieldtype:
                    constraints['fieldtypes'].append(fieldtype)
                else:
                    constraints['fieldtypes'].append(None)
                # print(f'Found: {fieldname=} of type {fieldtype=}')
                if fieldtype == 'char':
                    pat = r"\((.*?)\)"
                    maxlen = int(re.search(pat, field).group(1))
                    constraints['maxlens'].append(maxlen)
                else:
                    constraints['maxlens'].append(np.nan)
                    # print(f'Found {fieldname} of {fieldtype} len {maxlen=}')
                if 'not null' in field.lower():
                    constraints['can_be_null'].append('NOT NULL')
                else:
                    constraints['can_be_null'].append('NULLABLE')

            else:
                print(f'WARNING: line {field} in {self.create_sql} did not include fieldname and/or fieldtype')

        constraints = pd.DataFrame(constraints)
        self.reqs = constraints

        return self
    
    def set_create_qry(self, create_qry:str):
        """Set the `create_qry` attribute of a Target

        Use-case: a Target object exists and its `create_qry` needs to be updated.

        Args:
            create_qry (str): relative or absolute filepath to a .sql file. This is the filepath where your table's CREATE TABLE .sql file lives.

        Examples:
            mytarget = t.Target('src/qry/create_a_table.sql')
            mytarget.set_create_qry('src/qry/create_another_table.sql)
        """
        self.create_qry = create_qry
        print(f'Create query set to {self.create_qry}')
        self._set_target_tablename()
        print(f'\nTarget tablename updated to match {self.create_qry}:')
        print(self.get_target_tablename())
        self._set_reqs()
        print(f'\nRequirements updated to match {self.create_qry}:')
        print(self.get_reqs())
    
    def get_create_qry(self):
        return print(self.create_qry)
