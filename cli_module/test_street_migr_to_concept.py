## Import libraries
import os 
import sys
import tracemalloc
import click
import simplejson as json
from datetime import time, datetime
from tqdm import tqdm
from uuid import uuid4
import pandas as pd
import re
import rdflib
from rdflib import Graph, URIRef, Literal, Namespace, RDF, BNode, XSD
import logging
from rapidfuzz import fuzz
from pathlib import Path
cwd = os.getcwd()  # Get the current working directory (cwd)
files = os.listdir(cwd)  # Get all the files in that directory
print("Files in %r: %s" % (cwd, files))
#WORK_REPO = Path(r"C:\Users\swart053\Documents\VSC\saa-nexus-scripts") # Adjust base path based on location
HOME_REPO = Path("/opt/lampp/htdocs/test/cli_module")
WORK_REPO = Path("/opt/lampp/htdocs/saa-nexus-scripts")
sys.path.append(str(WORK_REPO))
from modules import memorix
from modules import saa

'''
   CLI script for exporting data from Memorix through various channels with help of
   various other scripts, one external data_document and with use of the Memorix API.
   Scripts used are: 
   streets.py
   conceptlist_turtle_to_excel.ipynb
      
   * External document used in this script can be altered.
   * Scripts can be altered for various purposes.
   * Click commands can be reduced or expanded 
   
   #### !!!! Alteration options mentioned like this  

   This script does in order:

   1) Retrieve deed turtle for definition from Memorix
   2) Retrieve total concept vocabulaire turtle from Memorix 
   3) Retrieve UUID's from Memorix based on turtle from step 1
   4) Alter concept turtle from step 2 to an excel sheet
   5) Read external document and define columns
   6) Iterate through uuid's from step 3 and retrieve info from Memorix
   7) 
   Export UUID's from Memorix using APi. Use these to get information from Memorix. 
   Alter this information. Upload this information back to Memorix.
   
   Call script with CLI and the environment option:

   'python this_script.py pipeline --env [specify tst /acc /prod] data_to_be_used.ext'
'''

# Declare global variables
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
# ctx.obj["current_datetime"] = current_datetime
logfile = f'../logs/get concept streetnames {str(current_datetime)}.log'
errors = []
#PREFIX = 'stadsarchiefamsterdam'

# define Namespaces
SAA = Namespace("https://data.archief.amsterdam/ontology#")
RICO = Namespace("https://www.ica.org/standards/RiC/ontology#")
MEMORIX = Namespace("http://memorix.io/ontology#")
#DEED = Namespace(f"{PREFIX}/resources/recordtypes/Deed#")
SCHEMA = Namespace(f"http://schema.org/")
SKOS = Namespace(f"http://www.w3.org/2004/02/skos/core#")

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(logfile, mode='w')]
)
log = logging.getLogger()

# -----------------------------------
# GLOBAL FUNCTIONS USED IN CLI
# -----------------------------------

# Environment setup
def setup_environment(env):
    if env == 'acc':
        global PREFIX 
        PREFIX = 'https://ams-migrate.memorix.io'
        settings_file = Path(WORK_REPO, 'settings.json') 
        #cwd = os.getcwd()  # Get the current working directory (cwd)
        #files = os.listdir(cwd)  # Get all the files in that directory
        #print("Files in %r: %s" % (cwd, files))
    elif env == 'prod':
        PREFIX = 'https://stadsarchiefamsterdam.memorix.io'
        settings_file = Path(WORK_REPO, 'settings.prod.json') 
    elif env == 'tst':
        settings_file = print(f'test output')
    else:
        raise ValueError("Environment must be 'acc' or 'prod'")
    
    settings = saa.readJsonFile(settings_file) 
    api = memorix.ApiClient(settings)

    return api

################## LOG FUNCTION! DO YOU NEED IT IN CLICK???  #########
'''# Log file setup
def log_setup(logfile):
    for handler in log.root.handlers[:]:
        log.root.removeHandler(handler)

    log.basicConfig(
        level=log.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[log.FileHandler(logfile, mode='w')]
    )
    log = log.getLogger()

    return log
'''
################## IF NOT DELETE ABOVE FUNCTION ####################

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DELETE DELETE DELETE DELETE DELETE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

## Step 1 function
#def retrieve_record_turtle_from_memorix(api, turtle_description, record_turtle):
#    
#
#    record_turtle_deleted_message = ""
#    if record_turtle:
#        os.remove(record_turtle)
#        record_turtle_deleted_message = f'''There was already a record_turtle present
#        in the 'data' folder at {record_turtle}. The records are appended in this file,
#        so to prevent working with double data, the old file has been deleted'''
#       # Get turtle for record to update 
#    try:
#        response = api.get_record_type( turtle_description)
#        print(response.text,  file=open(record_turtle, 'w', encoding='utf-8'))
#        return record_turtle, record_turtle_deleted_message
#    except:
#        log.error(f'Could not get record_type {turtle_description} from api {api} with response {response}')
#        errors.append({'fn: retrieve_record_turtle_from_memorix': [turtle_description, record_turtle, response]})

# Step 1 function
def retrieve_concept_turtle_from_memorix(env, api, vocabulair, concept_turtle):
    
    # Get turtle of concept vocabulaire
    try:
        response = api.list_concepts( vocabulair)
        print(response.text,  file=open(concept_turtle, 'w', encoding='utf-8'))
        return concept_turtle
    except:
        if concept_turtle:
            log.error(f'''There was an issue while creating the vocabulair: 
        {vocabulair} from entrypoint: {env} with api: {api}.
        It appears a concept_turtle was created. You should check the file for accuracy''')
        else: 
            log.error(f'''The concept_turtle : {concept_turtle} had issues being created at
            accespoint {env} with the api: {api}.''')
            errors.append({'fn: retrieve_concept_turtle_from_memorix': [vocabulair, concept_turtle, response]})

# Step 2 function
def retrieve_uuid_from_memorix(env, record_uuids):
    
    #### !!!! ALTER PATH BASED ON PLACEMENT OF MODULE 
    sys.path.append(str(HOME_REPO))
    import get_uuid  
    try: 
        # Get uuids based on turtle 
        response = get_uuid.main(env, record_uuids)

        df = pd.read_csv(record_uuids,

            sep=";",             
            dtype={ "uuid": str
               })

        df_record_uuids = pd.DataFrame(df)

        return df_record_uuids
    except:
        log.error(f'''The uuid's : {record_uuids} had an issue at 
        accespoint {env}.''')
        errors.append({'fn: retrieve_uuid_from_memorix': [env,record_uuids, response, df, df_record_uuids, HOME_REPO]})


# Step 3 function
def concept_turtle_to_list( concept_turtle):
    
    # Load RDF/concept_turtle 
    g = rdflib.Graph()
    g.parse(concept_turtle, format="ttl")

    # Namespace
    SKOS = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")

    concept_list = []
    try: 
        for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
            s_str = str(s)

            match = re.search(r'/vocabularies/concepts/([^/>]+)', s_str)
            uuid = match.group(1) if match else ""

            prefLabel = next((str(lab) for lab in g.objects(s, SKOS.prefLabel)), "")
            exactMatch = next((str(em) for em in g.objects(s, SKOS.exactMatch)), "") # <-- fout: want exactMatch kan nu meer dan 1 waarde hebben
            scopeNote = next((str(sn) for sn in g.objects(s, SKOS.scopeNote)), "")

            concept_list.append({
                "uuid": uuid,
                "prefLabel": prefLabel,
                "exactMatch": exactMatch,
                "scopeNote": scopeNote
            })

        return concept_list
    except:
        log.error(f'''There was an issue reading the concept turtle : {concept_turtle}
                  and creating the concept_list {concept_list}.''')
        errors.append({'fn: concept_turtle_to_list': [concept_turtle, concept_list]})

# Step 4 function
def read_external_data_with_panda(data):
    
    df = pd.read_csv(data, 

    sep=";",             
    dtype={ "straat-label-altlabel": str
           })
    
    df_external_data = pd.DataFrame(df)
    
    return df_external_data

# Step 5 function
def process_data(env, api, df_record_uuids, records):

    # Check if file already exists and delete based on env
    records_deleted_message = ''
    if records and env == 'prod':
        os.remove(records)
        records_deleted_message = f'''There was already a records file
        present in the 'data' folder at {records}. You are working in production, so for safety
        purposes this old file has now been removed to prevent working with older data.
        A new one is created in this function'''
    elif records and env == 'acc':
        records_deleted_message = f'''There is already a records file
        present in the 'data' folder. You are working in acceptence environment, so nothing
        has been done with this file. If output is different than expected, consider removing 
        this file at {records}'''
    #IMAGE = Namespace(f"{PREFIX}/resources/records/Image#")
    #DEED = Namespace (f"{PREFIX}/resources/recordtypes/Deed#")
    #RT = Namespace(f"{PREFIX}/resources/recordtypes")
    #print(f'I get in the MOTHERFOCKING FUNCTION with {df_record_uuids}')
    # loop through data with progress meter
    #for index, row in tqdm(data.iterrows(), total=data.shape[0]):
    test_numbers = 5
    try:
        for uuid in df_record_uuids["uuid"].iloc[:test_numbers]:
            response = api.get_record(uuid)
            print(response.text, file=open(records, 'a', encoding='utf-8')) 
            #record_block = URIRef(f"{PREFIX}/resources/records/{uuid}")
            #print (f'This is the record block: {record_block}')
            if response.status_code != 200:
                log.info("...Try to read again...")
                time.sleep(3)
                response = api.get_record(uuid)
                if response.status_code != 200:
                    log.error(f"Reading failed for {uuid}")
                    # Working with the records
            #g = Graph()
            #g.parse(data=response.text, format='turtle')                    
            ## check if migration address is filled
            
            #for s in set(g.subjects()):
            #    print(f'This is my s: {s}')

       
        return records, records_deleted_message
                
    except:
        log.error(f"FAILED TO GET RECORD FOR UUID: {uuid}")
    
    #for index, row in df_record_uuids.iterrows():
    #    print(type(row))
    #    print(row.shape)
    #    print(row.head())
    #    break
        
    #    log.info(f"START {row.iloc[0]}")
    #    try:
    #        response = api.get_record(row.iloc[0])
    #        print(response.text)
    #        if response.status_code != 200:
    #            log.info("...Try to read again...")
    #            time.sleep(3)
    #            response = api.get_record(row.iloc[0])
    #            if response.status_code != 200:
    #                log.error(f"Reading failed for {row.iloc[0]}")
    #                continue
    #
    #
    #        '''# load the graph
    #        g = Graph()
    #        g.parse(data=response.text, format='turtle')
    #        adresid_added = False

    #        record_block = URIRef(f"https://{PREFIX}.memorix.io/resources/records/{row.auuid}")
    #        # check if adamlink uri is empty
    #        if any(g.triples((record_block, SAA['hasOrHadSubjectLocation'], None))):
    #            log.info(f"Adamlink already filled in for {row.auuid}")
    #        else:
    #            g.add((record_block, SAA['hasOrHadSubjectLocation'], URIRef(f"https://adamlink.nl/geo/address/{row.adresid}")))
    #            coord_bnode = BNode()
    #            if row.wkt==row.wkt:
    #                g.add((record_block, SAA['hasOrHadSubjectCoordinates'], coord_bnode))
    #                g.add((coord_bnode, RDF.type, MEMORIX.GeoCoordinates))
    #                g.add((coord_bnode, SCHEMA.latitude, Literal(row.latitude, datatype=XSD.decimal)))         
    #                g.add((coord_bnode, SCHEMA.longitude, Literal(row.longitude, datatype=XSD.decimal)))       
    #                g.add((coord_bnode, SCHEMA.name, Literal(row.adresid)))  
    #            adresid_added = True

    #        if adresid_added:
    #            log.info("Correct")
    #            turtle = g.serialize(format="turtle")
    #            # put the updated record in memorix
    #            response = api.update_record(row.auuid, turtle)
    #            if response.status_code == 200:            
    #                log.info(f"SUCCEED {row.auuid}")        
    #            else:            
    #                log.error(f"FAIL {row.auuid}")
    #                log.error(response.text)'''
    #    except:
    #        log.error(f"FAILED TRANSFORMATION {row.iloc[0]}")


# Helper function Step 6 
def get_value_from_records(record, inst, g):
    
    houseNumber = str(g.value(subject=inst, predicate=SAA['houseNumber']))
    numberAddition = str(g.value(subject=inst, predicate=SAA['houseNumberAddition']))
    ################### Check for ADD if not there ADD ADD
    streetTextualValue = str(g.value(subject=inst, predicate=SAA['streetTextualValue']))
    uuid = str(record).split('/')[-1]  # Does not work
    
    return uuid, houseNumber, numberAddition, streetTextualValue


# Step 6 function
def working_in_the_turtle(records, concept_list, alternatives, outliers):

    # Define Namespaces
    DEED = Namespace (f"{PREFIX}/resources/recordtypes/Deed#")
    RT = Namespace(f"{PREFIX}/resources/recordtypes")

    try:
        # Working with the records
        g = Graph()
        g.parse(records, format='turtle')
        
        print(concept_list)

        concept_df = pd.DataFrame(concept_list, 
                                  
            
            dtype={'uuid': str,
                  'preflabel': str,
                  'scopeNote': str,
                  'exactMatch': str,
                  })
        
        for index, row in concept_df.iterrows():
            row.uuid 

        for record in g.subjects(RDF.type,MEMORIX.Record):
            
            for inst in g.objects(subject=record, predicate=SAA['isAssociatedWithModernAddress'] ):
                uuid, houseNumber, numberAddition, streetTextualValue = get_value_from_records(record, inst, g)
            #if any(g.triples((deed, SAA['isAssociatedWithModernAddress'], None))):
            #    print(f'This is inside the triples') # This is my saa: textualvalue: {SAA['streetTextualValue']} for the deed: {deed}')  ##### WE ARE HERE !!!!!!!!!##########
            return records, alternatives, outliers
        
    except:
        log.error(f'Something went wrong with reading the records {records}')

# Step 7 function
def alter_shit(g,records, houseNumber, numberAddition, migrate_street, record, uuid, df_external_data, concept_list):

    # loop through data with progress meter
    try:
        for inst in g.objects(subject=record, predicate=SAA['isAssociatedWithModernAddress']):
            print(f'This is my street: {migrate_street}')
            print(f'This is my house number: {houseNumber}')
            print(f'This is my house number addition: {numberAddition}')
            print(f'This is my record: {record}')
            print(f'This is my uuid: {uuid}')
        #    record_block = URIRef(f"https://{PREFIX}.memorix.io/resources/records/{row.auuid}")
        #    # check if adamlink uri is empty
        #    if any(g.triples((record_block, SAA['streetTextualValue'], None))):
        #        log.info(f"Checking if value is present in {row.auuid}")  ##### WE ARE HERE !!!!!!!!!##########
        #    else:
        #        g.add((record_block, SAA['hasOrHadSubjectLocation'], URIRef(f"https://adamlink.nl/geo/address/{row.adresid}")))
        #        coord_bnode = BNode()
        #        if row.wkt==row.wkt:
        #            g.add((record_block, SAA['hasOrHadSubjectCoordinates'], coord_bnode))
        #            g.add((coord_bnode, RDF.type, MEMORIX.GeoCoordinates))
        #            g.add((coord_bnode, SCHEMA.latitude, Literal(row.latitude, datatype=XSD.decimal)))         
        #            g.add((coord_bnode, SCHEMA.longitude, Literal(row.longitude, datatype=XSD.decimal)))       
        #            g.add((coord_bnode, SCHEMA.name, Literal(row.adresid)))  
        #        adresid_added = True
#
        #    if adresid_added:
        #        log.info("Correct")
        #        turtle = g.serialize(format="turtle")
        #        # put the updated record in memorix
        #        response = api.update_record(row.auuid, turtle)
        #        if response.status_code == 200:            
        #            log.info(f"SUCCEED {row.auuid}")        
        #        else:            
        #            log.error(f"FAIL {row.auuid}")
        #            log.error(response.text)
    except:
        log.error(f"FAILED TRANSFORMATION {records}")

# Step 9 function
def alter_some_more_shit(g, houseNu4mber, numberAddition, migrate_street, record, uuid):

        pass



# -----------------------------------
# MAIN CLI GROUP
# -----------------------------------

# Start command line input 
@click.group()
# Options determine which environment
@click.option(
    '--env',
    '-e',
    type=click.Choice(['acc', 'prod', 'tst']),
    default='acc',
    help='Which environment to use: “acc”, “prod” or "tst".'
)
@click.pass_context
def cli(ctx, env):

    # Create shared storage
    ctx.obj = {}

    # Store environment
    ctx.obj["env"] = env

    # Determine environment
    ctx.obj["api"]= setup_environment(env)

    # Declare user variables
    ctx.obj['vocabulair'] = 'a4863c0c-d9e5-3902-831a-d0960e381a41'  #### !!!! uuid of vocabul air            
    ctx.obj['Deed'] = 'Deed'                                        #### !!!! Recordname for turtle
    # ctx.obj["record_turtle"] = "data/record_turtle.ttl"                 #### !!!! Location of deed turtle
    ctx.obj['concept_turtle'] = "data/concept_turtle.ttl"                #### !!!! Location of street turtle
    ctx.obj["record_uuids"] = "data/record_uuids.csv"            #### !!!! Location of deed turtle
    ctx.obj["records"] = 'data/records.ttl'      
    ctx.obj["alternatives"] = "data/alternatives.csv"            #### !!!! Location of deed turtle
    ctx.obj["outliers"] = "data/outliers.csv"            #### !!!! Location of deed turtle

    ######### SOWIESO WEG ctx.obj["excel_sheet"] = "../data/concept_streets.xlsx"         #### !!!! Location of concepts
    #ctx.obj["data"] = '../data/alternatieve_straatnamen.csv'    #### !!!! Location of adamlinks /alternative names
                    #### !!!! Location of adamlinks /alternative names
    #ctx.obj["log"] = log_setup(logfile)
  
# -----------------------------------
# EXPORT 
# -----------------------------------

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DELETE DELETE DELETE DELETE DELETE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


# @cli.command()
# # Step 1 cli command
# def download_record_turtle(ctx):
# 
#     retrieve_record_turtle_from_memorix(
#         ctx.obj["api"], 
#         ctx.obj["Deed"], 
#         ctx.obj['record_turtle']
#         )

# Step 1 cli command
@cli.command()
def download_concept_vocabulaire(ctx):

    retrieve_concept_turtle_from_memorix(
        ctx.obj["env"], 
        ctx.obj["api"], 
        ctx.obj['vocabulair'], 
        ctx.obj['concept_turtle']
        )

# Step 2 cli command
@cli.command()
def download_record_uuid(ctx):

    retrieve_uuid_from_memorix( ctx.obj["env"], ctx.obj['record_uuids'])
    
# Step 3 cli command
@cli.command()
def turtle_to_list(ctx):
    
    concept_turtle_to_list( ctx.obj["concept_turtle"])

# Step 4 cli command
@cli.command()
def read_added_datasheet(ctx):
    
    read_external_data_with_panda( ctx.obj["data"])

# Step 5 cli command
@cli.command()
def concept_turtle_to_excel(ctx):
    
    process_data(
        ctx.obj["env"], 
        ctx.obj['api'], 
        ctx.obj['df_record_uuids'], 
        ctx.obj['records'], 
        ctx.obj['df_external_data'], 
        ctx.obj['concept_list']
        )

# Step 6 cli command
@cli.command()
def testing_working_with_data(ctx):
    
    working_in_the_turtle(
        ctx.obj['records'], 
        ctx.obj['concept_list'],
        ctx.obj['alternatives'],
        ctx.obj['outliers'],   
        )
    
# Step 7 cli command
@cli.command()
def change_the_turtle(ctx):

    alter_shit(
        ctx.obj['graph'],
        ctx.obj['records'],
        ctx.obj['houseNumber'],
        ctx.obj['numberAddition'],
        ctx.obj['migrate_street'],
        ctx.obj['record'],
        ctx.obj['uuid'],
        ctx.obj['df_external_data'], 
        ctx.obj['concept_list']
        )
    
# Step 8 cli command
@cli.command()
def alternative_list_creation(ctx):

    alter_some_more_shit(
        ctx.obj['graph'],
        ctx.obj['houseNumber'],
        ctx.obj['numberAddition'],
        ctx.obj['migrate_street'],
        ctx.obj['record'],
        ctx.obj['uuid'],
        ctx.obj['df_external_data'], 
        ctx.obj['concept_list']
        )
       
    #click.echo(f"Environment: {ctx.obj['env']}")
#
    #click.confirm(
    #    "Continue to next step?",
    #    abort=True
    #) 

# CLI Pipeline
@cli.command()
@click.argument("data")
@click.pass_context
def pipeline(ctx, data):

    # Store provided data globally
    ctx.obj["data"] = data
  
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DELETE DELETE DELETE DELETE DELETE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


    # click.echo("STEP 1: DOWNLOAD TURTLE RECORD")
# 
    # record_turtle, record_turtle_deleted_message = retrieve_record_turtle_from_memorix(
    #     ctx.obj["api"], 
    #     ctx.obj['Deed'], 
    #     ctx.obj['record_turtle']
    #     )
    # 
    #     # check for a deleted file and print message
    # if record_turtle_deleted_message:
    #     print(record_turtle_deleted_message)
    #    
    # ctx.obj['record_turtle'] = record_turtle

    # click.echo(f"Environment: {ctx.obj['env']}")
# 
    # click.confirm(
    #     "Continue to next step?",
    #     abort=True
    # )

    click.echo("STEP 1: DOWNLOADING A TURTLE FROM CONCEPT VOCABULAIR")


    click.echo(f"We are in the environment: {ctx.obj['env']}")

    # Retrieve a turtle with all concepts based on vocabulair    
    concept_turtle = retrieve_concept_turtle_from_memorix(
        ctx.obj["env"], 
        ctx.obj["api"], 
        ctx.obj['vocabulair'], 
        ctx.obj['concept_turtle']
        ) 
    
    ctx.obj['concept_turtle'] = concept_turtle

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tDownloading concept turtle is complete. \n
        \tDo you want to continue retrieving the record uuids from memorix?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    )
    
    click.echo("STEP 2: DOWNLOADING RECORD UUIDS")
    
    # Retrieve record UUIDS from memorix and put in dataframe 
    df_record_uuids = retrieve_uuid_from_memorix(
        ctx.obj["env"], 
        ctx.obj['record_uuids']
        )
    
    ctx.obj['df_record_uuids'] = df_record_uuids

    # click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tWe have the record uuids. \n
        \tDo you want to continue reading the concept turtle\n
        \tand create a list of the data?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    )

    click.echo("STEP 3: READ TURTLE AND PUT IN LIST")
    
    # Read the concept turtle retrieved in step 1 and append data to list variable
    concept_list = concept_turtle_to_list( ctx.obj["concept_turtle"])
    print('I am here for the concept_list')
    ctx.obj['concept_list'] = concept_list

    # click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tThe concept turtle is appended to a list. Do you want to continue\n
        \treading the datasheet you provided?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    )

    click.echo("STEP 4: READ EXTERNAL DATASHEET WITH PANDA")
    
    # Add the provided external data to a dataframe
    df_external_data = read_external_data_with_panda( ctx.obj["data"])
    ctx.obj['df_external_data'] = df_external_data
    
    # click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tWe have a dataframe of the datasheet you provided. \n
        \tDo you want to continue retrieving the records based on uuids?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    )

    click.echo("STEP 5: HERE WE START WITH GETTING A RECORD TURTLE")
    
    # Get all records in a turtle, based on UUIDS retrieved in step 2
    records, records_deleted_message = process_data(
        ctx.obj["env"], 
        ctx.obj['api'], 
        ctx.obj['df_record_uuids'], 
        ctx.obj['records']
        )
    
    # Check for a deleted file and print message accordingly
    if records_deleted_message:
        print(records_deleted_message)
    ctx.obj['records'] = records

    # click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tAll record uuids now have a full record. \n
        \tDo you want to continue altering data?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    )   

    click.echo("STEP 6: WE ARE NOW ALTERING THE RECORDS")

    records, alternatives, outliers = working_in_the_turtle(
        ctx.obj['records'],
        ctx.obj['concept_list'], 
        ctx.obj['alternatives'],
        ctx.obj['outliers'] 
        )
    
    # ctx.obj['graph'] = g
    # ctx.obj['houseNumber'] = houseNumber
    # ctx.obj['numberAddition'] = numberAddition
    # ctx.obj['migrate_street'] = streetTextualValue
    # ctx.obj['record'] = record
    # ctx.obj['uuid'] = uuid 
    ctx.obj['alternatives'] = alternatives
    ctx.obj['outliers'] = outliers
    
#    click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tThe records are altered. \n
        \tDo you want to continue with ... YESS??? ?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    ) 

    click.echo("STEP 7: TRYING TO ALTER SHIT WITH WHAT WE GOT")

    # THIS IS NEXT STEP FOR ALTERNATIVE DATA OR OUTLIERS. MAKE ALL YOUR SHIT IN THE TURTLE HAPPEN IN 7!!

    # g, record, uuid, houseNumber, numberAddition, streetTextualValue = alter_shit(
    #     ctx.obj['graph'],
    #     ctx.obj['records'],
    #     ctx.obj['houseNumber'],
    #     ctx.obj['numberAddition'],
    #     ctx.obj['migrate_street'],
    #     ctx.obj['record'],
    #     ctx.obj['uuid'],
    #     ctx.obj['df_external_data'], 
    #     ctx.obj['concept_list']
    #     )
       
    # click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tThe bla bla bla. \n
        \tDo you want to continue with ... YESS??? ?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    ) 

    click.echo("STEP 8: NOT HERE YET")

    #g, record, uuid, houseNumber, numberAddition, streetTextualValue = alter_shit(
    #    ctx.obj['graph'],
    #    ctx.obj['houseNumber'],
    #    ctx.obj['numberAddition'],
    #    ctx.obj['migrate_street'],
    #    ctx.obj['record'],
    #    ctx.obj['uuid'],
    #    ctx.obj['df_external_data'], 
    #    ctx.obj['concept_list']
    #    )
    #   
    # click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        '''\n
        ----------------------------------------------------------------------------\n
        \tThe bla bla bla. \n
        \tDo you want to continue with ... YESS??? ?\n
        ----------------------------------------------------------------------------\n
        \t''',
        abort=True
    ) 

    click.echo('Done!')





'''
# FULL COMMAND LINE INPUT TRY
tracemalloc.start()
PYTHONTRACEMALLOC = 1
# Script for creating brand new files to a fonds and importing it
@click.command()
@click.argument('script')
@click.option('--env', '-e', type=click.Choice(['acc', 'prod']), default='acc',
              help='Which environment to use: “acc” or “prod”.')
@click.argument('records')
#@click.option('--output', '-o', default='output.csv',
#              help='Path to CSV logfile (will be appended).')
#@click.option('--workers', '-w', default=10, show_default=True,
#              help='Number of concurrent worker threads.')
#@click.option('--max-calls', '-m', default=0, type=int, show_default=True,
#              help='Maximum API calls per second (0 = unlimited).')
def main(script, env, records): 
    pass





for index, row in tqdm(data.iterrows(), total=data.shape[0]):
    log.info(f"START {row.uuid}")
    try:
        response = api.get_record(row.uuid)
        if response.status_code != 200:
            log.error(f"Reading failed for {row.uuid}")
        else:
            # load the graph
            g = Graph()
            g.parse(data=response.text, format='turtle')
            persons = []

            # iterate over every person
            for inst in g.objects(subject=None, predicate=RICO['hasOrHadSubject']):  
                person_rol = g.value(subject=inst, predicate=SAA['relatedPersonObservationRole'])
                # if person == 'geregistreerde':
                if person_rol==URIRef(f'https://{PREFIX}.memorix.io/resources/vocabularies/concepts/e8a92b13-f000-4b2e-e053-b784100a3466'): 
                    person_uri = g.value(subject=inst, predicate=SAA['relatedPersonObservation'])
                    person_uuid = person_uri.split('/')[-1]
                    persons.append(person_uuid)
                else:
                    log.info(f"{row.uuid}, Geen geregistreerde: {person_uuid}")

            data.loc[index, "geregistreerden"] = str(persons)
            log.info(f"SUCCEED {row.uuid}")
    except:
        log.error(f"FAILED TRANSFORMATION {row.uuid}")

### changes ###
'''

if __name__ == '__main__':
    cli(obj={})