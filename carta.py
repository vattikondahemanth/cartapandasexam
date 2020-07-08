import pandas as pd
import numpy as np
from sqlalchemy import create_engine
all_data = pd.DataFrame()


columns = [
    "MRN",
    "EncounterID",
    "FirstName",
    "LastName",
    "BirthDate",
    "AdmissionDate",
    "DischargeDate",
    "UpdateDate"
    
]

from carta_interview import Datasets, get_data_file

patient_extract1 = get_data_file(Datasets.PATIENT_EXTRACT1)
patient_extract2 = get_data_file(Datasets.PATIENT_EXTRACT2)

pe1 = pd.read_excel(patient_extract1)
pe2 = pd.read_excel(patient_extract2)

# print(pe1)
# print(pe2)

all_data = all_data.append(pe1)
all_data = all_data.append(pe2)



all_data.groupby(['Encounter ID', 'First Name', 'Last Name']).apply(lambda x: x[x['Update D/T'] == x['Update D/T'].max()])
all_data.drop_duplicates(subset ="First Name", 
                     keep = 'first', inplace = True)

# print(all_data)

engine = create_engine('postgresql://carta:password@localhost:5432/carta')


all_data.to_sql(
    'carta', 
    engine,
    if_exists='replace',
    index=False # Not copying over the index
)


# print(all_data)

person_details = pd.read_sql_query('''SELECT "First Name","Last Name" FROM carta;''', engine)
# print(person_details)



encounter_details = pd.read_sql_query('''SELECT "MRN","Encounter ID", "Admission D/T", "Discharge D/T" FROM carta;''', engine)
# print(encounter_details)


print(person_details.to_json())
print('\n')
print(encounter_details.to_json())

