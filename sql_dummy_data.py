import dbm
import os
import random
import uuid

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from faker import Faker  # https://faker.readthedocs.io/en/master/
from sqlalchemy import create_engine

load_dotenv()

MYSQL_HOSTNAME = os.getenv("MYSQL_HOSTNAME_AZURE")
MYSQL_USER = os.getenv("MYSQL_USER_AZURE")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD_AZURE")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE_AZURE")

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
connection_string

db_azure = create_engine(connection_string)

### show tables from databases
tableNames_azure = db_azure.table_names()
### show databases
print(db_azure.table_names())
#### fake stuff 
fake = Faker()

fake_patients = [
    {
    #keep just the first 8 characters of the uuid
    'mrn': str(uuid.uuid4())[:8], 
    'first_name':fake.first_name(), 
    'last_name':fake.last_name(),
    'zip_code':fake.zipcode(),
    'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
    'gender': fake.random_element(elements=('M', 'F')),
    'contact_mobile':fake.phone_number(),
    'contact_home':fake.phone_number()
    } for x in range(50)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])
df_fake_patients


#### ICD-10 CODES ####
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates from icd10codesShort_1k
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')

#### NDC CODES ####
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

#### CPT CODES ####
cpt_codes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
# drop duplicates from cpt_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(
    subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')

#### LOINC CODES ####
loinccodes = pd.read_csv('cloud-managed-db-azure/data/Loinc.csv')
list(loinccodes.columns)
loinccodesShort = loinccodes[['LOINC_NUM', 'COMPONENT']]
loinccodesShort_1k = loinccodesShort.sample(n=1000)
loinccodesShort_1k = loinccodesShort_1k.drop_duplicates(subset=['LOINC_NUM'], keep='first')

########## INSERTING FAKE PTS ##########
#df_fake_patients.to_sql('patients', con=db_azure, if_exists='append', index=False)
#db_azure = pd.read_sql_query("SELECT * FROM patients", db_azure)
insertQuery = "INSERT INTO patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
for index, row in df_fake_patients.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index)
# # query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM patients", db_azure)

########## INSERTING IN FAKE CONDITIONS ##########
insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"
startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    # db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break
# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM conditions", db_azure)


##### CREATING FAKE PT CONDITIONS WITH ICD 10 ####
##query conditions and patients to get the ids
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)
# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)
print(df_patient_conditions.head(20))

##### ADD RANDOM CONDITION TO EACH PT ####
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"
for index, row in df_patient_conditions.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)


########## INSERTING IN FAKE MEDS ##########
insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"
medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 75:
        break
# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM medications", db_azure)

##### CREATE FAKE PT MEDS ####
# first, lets query medications and patients to get the ids
df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db_azure) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])
for index, row in df_patients.iterrows():
    numMedications = random.randint(1, 5)
    df_medications_sample = df_medications.sample(n=numMedications)
    df_medications_sample['mrn'] = row['mrn']
    df_patient_medications = df_patient_medications.append(df_medications_sample)
print(df_patient_medications.head(10))

#INSERT MEDS
insertQuery = "INSERT INTO patient_medications (mrn, med_ndc) VALUES (%s, %s)"
for index, row in df_patient_medications.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)

# TX AND PROC
insertQuery = "INSERT INTO treatment_procedures (cpt, description) VALUES (%s, %s)"
medRowCount = 0 
for index, row in cpt_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row: ", index)
    if medRowCount == 100:
        break
df_azure = pd.read_sql_query("SELECT * FROM treatment_procedures", db_azure)

## PT TX AND PROCS
df_treatment_procedures = pd.read_sql_query("SELECT cpt FROM treatment_procedures", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

df_patient_treatments_procedures = pd.DataFrame(columns=['mrn', 'cpt'])
for index, row in df_patients.iterrows():
    df_treatment_procedures_sample = df_treatment_procedures.sample(n=random.randint(1, 5))
    df_treatment_procedures_sample['mrn'] = row['mrn']
    df_patient_treatments_procedures = df_patient_treatments_procedures.append(df_treatment_procedures_sample)

print(df_patient_treatments_procedures.head(10))

insertQuery = "INSERT INTO patient_treatments_procedures (mrn, cpt) VALUES (%s, %s)"

for index, row in df_patient_treatments_procedures.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['cpt']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patient_treatments_procedures", db_azure)

#### CREATING FAKE SOCIAL DETERMINANTS ####
insertQuery = "INSERT INTO social_determinants (loinc, description) VALUES (%s, %s)"
startingRow = 0
for index, row in loinccodesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_azure.execute(insertQuery, (row['LOINC_NUM'], row['COMPONENT']))
    print("inserted row db: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break
df_azure = pd.read_sql_query("SELECT * FROM social_determinants", db_azure)

#INSERT SOCIAL DETERMINANTS
df_social_determinants = pd.read_sql_query("SELECT loinc FROM social_determinants", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

df_patient_social_determinants = pd.DataFrame(columns=['mrn', 'loinc'])
for index, row in df_patients.iterrows():
    df_social_determinants_sample = df_social_determinants.sample(n=random.randint(1, 5))
    df_social_determinants_sample['mrn'] = row['mrn']
    df_patient_social_determinants = df_patient_social_determinants.append(df_social_determinants_sample)

print(df_patient_social_determinants.head(10))

insertQuery = "INSERT INTO patient_social_determinants (mrn, loinc) VALUES (%s, %s)"

for index, row in df_patient_social_determinants.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['loinc']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patient_social_determinants", db_azure)
