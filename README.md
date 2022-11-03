# cloud-managed-db-azure
Assignment 6, HHA 504, AHI, SBU with azure vm


Assignment 6 - Cloud Managed SQL + ERDs + Dummy Data


1. Create a cloud-managed MySQL DB on either Azure or GCP

2. Create a new database inside of that mysql instance called patient_portal  

3. Create a python script called (sql_table_creation.py) that creates the following tables inside of patient_portal: patients, medications, treatments_procedures, conditions, and social determinants. Be sure to use a .env file to hide your login credentials 

4. Create a python script called (sql_dummy_data.py) using python and send some dummy data into each of the tables. Please see notes for ideas related to dummy data. 

5. Create an ERD for your DB design using MySQL Work Bench. You must have at least two foreignKeys representing a relationship between at least 2 tables. 

6. Github docs to include: 
- a python script that contains the SQL code to create db (sql_table_creation.py) 
- a python script that contains code to insert in some dummy data (sql_dummy_data.py) 
- a readme file that describes a) where you setup the mySQL db, b) any issues you ran into 
- a images folder that contains: 
    - screen shot of a ERD of your proposed setup (use either popSQL or mysql work bench) 
    - screen shots of you connected to the sql server, performing the following queries: 
        - Query1: show databases (e.g., show databases;) 
        - Query2: all of the tables from your database (e.g., show tables;)  
        - Query3: select * from patient_portal.medications 
        - Query4: select * from patient_portal.treatment_procedures
        - Query5: select * from patient_portal.conditions

Be CREATE with your dummy data and find examples that are from real-world codexes: 

    Medications: NDC codes
    Treatments/Procedures: CPT 
    Conditions: ICD10 codes
    Social_Determinants: LOINC codes 


Resources to pull some test data: 
NDC: https://dailymed.nlm.nih.gov/dailymed/index.cfm 
CPT: https://www.aapc.com/codes/cpt-codes-range/
ICD: https://icdcodelookup.com/icd-10/codes
LOINC: https://www.findacode.com/loinc/LG41762-2--socialdeterminantsofhealth.html

REAL CPT Values that are older: https://gist.github.com/lieldulev/439793dc3c5a6613b661c33d71fdd185


## Setting up MySQL

###### Step 1: Update OS
    sudo apt-get update

###### Step 2: Install MySQL
    sudo apt install mysql-server mysql-client

###### Step 3: Log into MySQL
    sudo mysql
    
###### Step 4: Check all available databases installed
    show databases;
   

## Creating a new user in the database
###### Step 1: Change username and password to what you want
    CREATE USER 'username'@'%' IDENTIFIED BY 'password';

###### Step 2: Check to cofirm user exists
    select user from mysql.user;

###### Step 3: Gran privileges to the user you created
    GRANT ALL PRIVILEGES ON *.* TO 'username'@'%' WITH GRANT OPTION;

###### Step 4: Check to confirm user granted privelege
    show grants for username;
    
###### Step 5: Quit out of MySQL
    \q
    
###### Step 6: Test local user connection from linux terminal
    mysql -u username -p
###### You will be prompted for password from step 1, type that into terminal

## Creating a Data base
###### Step 1: Create database
    create database databasename;

###### Step 2: Check to see if databases were created
    show databases;

## Getting Connection Refused while trying to connect to VM instance

###### Step 1: Open Terminal in VM instance and open .conf file for mysql
    sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf

###### Step 2: Look for the line bind-address and replace ip with:
    0.0.0.0

###### Step 3: Save and Exit the file
    
    
###### Step 4: Restart mysql from linux terminal
    /etc/init.d/mysql restart
