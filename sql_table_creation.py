#import packages
from sqlalchemy import create_engine
import sqlalchemy
from dotenv import load_dotenv
import os
load_dotenv()
### drop the old tables that do not start with production_
def droppingFunction_limited(dbList, db_source):
    for table in dbList:
        if table.startswith('production_') == False:
            db_source.execute(f'drop table {table}')
            print(f'dropped table {table}')
        else:
            print(f'kept table {table}')

def droppingFunction_all(dbList, db_source):
    for table in dbList:
        db_source.execute(f'drop table {table}')
        print(f'dropped table {table} succesfully!')
    else:
        print(f'kept table {table}')

MYSQL_HOSTNAME = os.getenv("MYSQL_HOSTNAME_AZURE")
MYSQL_USER = os.getenv("MYSQL_USER_AZURE")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD_AZURE")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE_AZURE")

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
connection_string

db_azure = create_engine(connection_string)


### show tables from databases
tableNames_azure = db_azure.table_names()


# reoder tables: production_patient_conditions, production_patient_medications, production_medications, production_patients, production_conditions
tableNames_azure = ['patients','patient_conditions','patient_medications','treatments_procedures','conditions','social_determinants','conditions','medications']
# ### delete everything 
#droppingFunction_all(tableNames_azure, db_azure)

### show tables from databases
tableNames_azure = db_azure.table_names()
print(tableNames_azure)


table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_patient_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""

table_treatments_procedures = """
create table if not exists treatment_procedures (
    id int auto_increment,
    cpt varchar(255) null unique,
    description varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""

table_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    loinc varchar(255) null unique,
    description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
db_azure.execute(table_patients)
db_azure.execute(table_medications)
db_azure.execute(table_conditions)
db_azure.execute(table_patient_medications)
db_azure.execute(table_patient_conditions)
db_azure.execute(table_treatments_procedures)
db_azure.execute(table_conditions)
db_azure.execute(table_social_determinants)


# get tables from db_azure
azure_tables = db_azure.table_names()
print (azure_tables)