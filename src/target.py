
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
        self.col_xwalk = dict()
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
    
    def get_target_tablename(self):

        return self.target_tablename

    def _set_target_tablename(self):

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

        return self.df

    def set_df(self, df:pd.DataFrame):

        self.df = df

    def get_ref(self):

        return self.ref

    def set_df(self, ref:pd.DataFrame):

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
        """_summary_

        Args:
            create_qry (str): _description_
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