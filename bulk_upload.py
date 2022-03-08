import json
from jsonschema import validate, exceptions
import pandas as pd
import re, os, uuid
from collections import defaultdict

# Define important variables
SUPPORTED_INPUT_FORMATS = {'csv': 'csv', 'excel': 'xlsx'} # Key - pair = File type - extension name
AGE_LIMIT = {'upper limit': 15, 'lower limit': 5}
FILE_PATH = "./data.csv"
SCHEMA_9JA_KIDS = {
    "title": "Record of children",
    "description": "This document records the details of a child",
    "type": "object",
    "properties": {
        "First Name": {"type": "string"},
        "Last Name": {"type": "string"},
        "Age": {"type": "integer"},
        "Gender": {"type": "string"},
        "State": {"type": "string"},
        "parent email": {"type": "string"},
        "Church Name": {"type": "string"}
    },
    "required": [
        "First Name", "Last Name", "Age", "Gender",
        "State", "parent email", "Church Name"
    ]
}

# Pseudo database & table (For testing purposes)
class Database:
    def __init__(self, db_name):
        self.tbs = []
        self.tables = {}
        self.db_name = db_name
        print(f'Successfully created database: {db_name}')

    # Creates tables
    def create_table(self, tb_name, schema):
        if self.tbs == []: 
            self.tables[tb_name] = Table(tb_name, schema)
        elif tb_name not in self.tbs:
            self.tables[tb_name] = Table(tb_name, schema)
        else:
            print(f'A table with the same name already exists in {self.db_name}')

    # Deletes tables
    def delete_table(self, tb_name):
        try:
            del self.tables[tb_name]
            print(f'deleted {tb_name} from {self.db_name}')
            
        except KeyError:
            print(f'No table named {tb_name} in {self.db_name}')
    
    # Allows us to use square bracket notations to access tables
    def __getitem__(self, tb_name):
        try:
            return self.tables[tb_name]

        except KeyError:
            print(f'No table named {tb_name} in {self.db_name}')

    # Prints a list of tables
    def list(self):
        print(list(self.tables))

class Table:
    def __init__(self, name, schema):
        print(f"Initializing {name} database...")
        self.name = name 
        self.schema = schema
        self.storage = []
        print('Table successfully created')

    # Allows us to search through the table for a particular child's details
    def read(self, attr, value):
        print(f'Searching for {value} in {attr}s')
        if self.storage == []:
            response = {
                'error': True,
                'message': 'Table is empty',
                'data': None
            }
            return response

        try:
            storage = pd.DataFrame.from_records(self.storage, index='id')
            response = {
                'error': False,
                'message': None,
                'data': storage[storage[attr] == value]
            }

        except KeyError:
            properties = ', '.join(self.schema['properties'].keys())
            response = {
                'error': True,
                'message': f'The attr {attr} does not exist, the available properties are: {properties}',
                'data': None
            }

        return response
            
    # Adds a child's data to table
    def update(self, child_data):
        print('----------------------------------------------------------------')
        print(f'Updating {self.name} table')
        id = str(uuid.uuid4()).split('-')[0]
        while id in self.storage:
            id = str(uuid.uuid4()).split('-')[0]
        child_data['id'] = id
        self.storage.append(child_data)

        print(f'Update complete added: \'{child_data["First Name"]} {child_data["Last Name"]}\' to {self.name}')
        response = {
            'error': False,
            'message': None,
        }

        return response
    
    # Allows us to print the table
    def list(self):
        if self.storage != []:
            return pd.DataFrame.from_records(self.storage, index='id')
        else:
            return []

# -------------------------- Auxiliary functions ---------------------------------
# Detects the supplied input format and parses it
# parameters: uploaded file path in any of the supported formats, sheet name if the uploaded file is excel
# returns: a list of dictionaries, with each dictionary representing a child's details
def form_parser(file_path, sheet_name='Sheet1'):
    print("Reading file...")
    file_extension = (os.path.split(file_path)[1]).split('.')[1]

    try: 
        if file_extension not in SUPPORTED_INPUT_FORMATS.values():
            supported_formats = ", ".join([format for format in SUPPORTED_INPUT_FORMATS])
            response =  {
                'error': True,
                'message': f'File format not supported, supported file formats are: {supported_formats}',
                'data': None
            }
            return response

        elif file_extension == 'csv':
            print("csv file detected")
            response = {
                'error': False,
                'message': None,
                'data': pd.read_csv(file_path).to_dict('records')
            }
            return response
        
        elif file_extension == "xlsx":
            print("Excel file detected")
            response = {
                'error': False,
                'message': None,
                'data': pd.read_excel(file_path, sheet_name).to_dict('records')
            }
            return response

        else:
            response = {
                'error': True,
                'message': "Method for this format not defined",
                "data": None
            }
            return response
    
    except:
        response =  {
            'error': True,
            'message':'Something went wrong',
            'data': None
        }
        return response

# Filters out data that doesn't conform to given schema (Ensures all fields are complete)
# parameters: the given schema, the schema supplied by the admin
# returns: boolean: True if the supplied schema matches the given schema and vice versa
def schema_checker(supplied_data, given_schema):
    try:
        validate(instance=supplied_data, schema=given_schema)
        response = {
            'error': False,
            'message': None
        }

    except exceptions.ValidationError as err:
        response = {
            'error': True,
            'message': f"{err.message}\nPlease check this entry: {err.instance}"
        }
    
    except exceptions.SchemaError as err:
        response = {
            'error': True,
            'message': f"Incorrect schema definition"
        }
    
    return response

# Verifies that email is in the right format
# parameters = str: email
# returns = bool: True if email is in the correct format and vice versa
def verify_email_format(email):
    regex = re.compile('[a-zA-Z0-9]{3,}@[a-z]+\.[a-z]{,3}')
    return re.findall(regex, email) != []

# Verifies that age is within the given age limit
# parameters = int: age
# returns = bool: True if age is within age limit and vice versa
def is_within_ageLimit(age):
    return age >= AGE_LIMIT["lower limit"] and age <= AGE_LIMIT["upper limit"]

# Checks for duplicates using parent's email and child's name
# parameters = a child's data, all registered children data
# returns = bool: True if the child's data has a registered duplicate and vice versa
def has_duplicate(child_data, table):
    check1 = child_data['parent email'] in table.list()['parent email'].to_list()
    check2 = child_data['First Name'] in table.list()['First Name'].to_list()
    check3 = child_data['Last Name'] in table.list()['Last Name'].to_list()

    return check1 and check2 and check3

# Initialize a database instance and create a table
db = Database('9ija')
db.create_table('9ija kids', SCHEMA_9JA_KIDS)


# -------------------------- Main function ---------------------------------------
def bulk_uploader(path_to_file, tb):
    # Info
    num_duplicates = 0
    print('done')
    # Extract data
    f_response = form_parser(path_to_file)
    if not f_response['error']:
        children_data = f_response['data']
    else:
        print('\nError !!')
        print(f_response['message'])
        return f_response

    # Load data
    for child_data in children_data:

        # Check for duplicates
        if tb.storage != []:
            if has_duplicate(child_data, tb):
                print('\nError !!')
                print(f"{child_data} has more than one entry")
                num_duplicates += 1
                continue
            
        # Check if all fields are supplied
        s_response = schema_checker(child_data, tb.schema)
        if s_response['error']:
            print('\nError !!')
            print(s_response['message'])
            continue

        # Age limit check
        if not is_within_ageLimit(child_data['Age']):
            print('\nError !!')
            print(f"{child_data} is not within the age limit")
            continue
        
        # Verify email format
        if not verify_email_format(child_data['parent email']):
            print('\nError !!')
            print(f"{child_data} contains an invalid email")
            continue

        # Save to database
        tb.update(child_data)

    print(f'Bulk registration complete, {num_duplicates} duplicates were found')

if __name__ == "__main__":
    bulk_uploader(FILE_PATH, db['9ija kids'])
    db['9ija kids'].list()