#import packages
from sqlalchemy import create_engine
import sqlalchemy
from dotenv import load_dotenv
import os
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
print(tableNames_azure)
# reoder tables: production_patient_conditions, production_patient_medications, production_medications, production_patients, production_conditions
tableNames_azure = ['production_patient_conditions', 'production_patient_medications', 'production_medications', 'production_patients', 'production_conditions']

table_patients = """
create table if not exists production_patients (
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
create table if not exists production_medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_conditions = """
create table if not exists production_conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""




table_patient_medications = """
create table if not exists production_patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES production_patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES production_medications(med_ndc) ON DELETE CASCADE
); 
"""


table_patient_conditions = """
create table if not exists production_patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES production_patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES production_conditions(icd10_code) ON DELETE CASCADE
); 
"""


db_azure.execute(table_patients)
db_azure.execute(table_medications)
db_azure.execute(table_conditions)
db_azure.execute(table_patient_medications)
db_azure.execute(table_patient_conditions)


# get tables from db_azure
azure_tables = db_azure.table_names()