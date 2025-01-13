"""
Create dummy dataframes
"""
import pandas as pd
import datetime as dt

def create_survey_events() -> pd.DataFrame:
    """Create dummy survey_events dataframe

    Returns:
        pd.DataFrame: Dataframe of survey_events species_info
    
    Examples:
        import pandas as pd
        import src.py.create_dataframes as cd
        survey_events = cd.create_survey_events()
    """

    survey_events = pd.DataFrame({
        'id':[
            1
            ,2
        ]
        ,'visit_date':[
            dt.date(2023,6,1)
            ,dt.date(2023,6,2)
        ]
        ,'location_id':[
            1
            ,2
        ]
    })

    return survey_events

def create_species_info() -> pd.DataFrame:
    """Create dummy species_info dataframe

    Returns:
        pd.DataFrame: Dataframe of dummy species_info

    Examples:
        import pandas as pd
        import src.py.create_dataframes as cd
        species_info = cd.create_species_info()
    """
    species_info = pd.DataFrame({
        'id':[
            1
            ,2
            ,3
        ]
        ,'species_id':[
            1
            ,2
            ,1
        ]
        ,'visit_id':[
            1
            ,1
            ,2
        ]
    })

    return species_info

def create_locations() -> pd.DataFrame:
    """Create dummy locations lookup dataframe

    Returns:
        pd.DataFrame: Dataframe of locations lookup species_info
    
    Examples:
        import pandas as pd
        import src.py.create_dataframes as cd
        locations_lookup = cd.create_locations()
    """

    locations_lookup = pd.DataFrame({
        'id':[
            1
            ,2
        ]
        ,'location_name':[
            'big pond'
            ,'small pond'
        ]
    })

    return locations_lookup

def create_species() -> pd.DataFrame:
    """Create dummy species lookup dataframe

    Returns:
        pd.DataFrame: Dataframe of species lookup species_info
    
    Examples:
        import pandas as pd
        import src.py.create_dataframes as cd
        species_lookup = cd.create_species()
    """
    species_lookup = pd.DataFrame({
        'id':[
            1
            ,2
        ]
        ,'species_code':[
            'BLFG'
            ,'GRFG'
        ]
        ,'common_name':[
            'blue frog'
            ,'green frog'
        ]
        ,'n':[
            1
            ,3
        ]
    })

    return species_lookup
