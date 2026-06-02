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

# Step 1 function
def retrieve_record_turtle_from_memorix(api, turtle_description, record_turtle):
    
    # Get turtle for record to update 
    try:
        response = api.get_record_type( turtle_description)
        print(response.text,  file=open(record_turtle, 'w', encoding='utf-8'))
        return record_turtle
    except:
        log.error(f'Could not get record_type {turtle_description} from api {api} with response {response}')
        errors.append({'fn: retrieve_record_turtle_from_memorix': [turtle_description, record_turtle, response]})

# Step 2 function
def retrieve_concept_turtle_from_memorix(api, vocabulair, concept_turtle):
    
    # Get turtle of concept vocabulaire
    response = api.list_concepts( vocabulair)
    print(response.text,  file=open(concept_turtle, 'w', encoding='utf-8'))

    return concept_turtle

# Step 3 function
def retrieve_uuid_from_memorix(env, record_uuids):
    
    #### !!!! ALTER IMPORT AND PATH BASED ON PLACEMENT OF MODULE 
    sys.path.append(str(HOME_REPO))
    import get_uuid  

    # Get uuids based on turtle 
    response = get_uuid.main(env, record_uuids)

    df = pd.read_csv(record_uuids,
                                  
        sep=";",             
        dtype={ "uuid": str
           })
    
    df_record_uuids = pd.DataFrame(df)

    return df_record_uuids

# Step 4 function
def concept_turtle_to_list( concept_turtle):
    
    print(f'Here with the {concept_turtle}')
    # Load RDF/concept_turtle 
    g = rdflib.Graph()
    g.parse(concept_turtle, format="ttl")

    # Namespace
    SKOS = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")

    concept_list = []
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

# Step 5 function
def read_external_data_with_panda(data):
    
    df = pd.read_csv(data, 

    sep=";",             
    dtype={ "straat-label-altlabel": str
           })
    
    df_external_data = pd.DataFrame(df)
    
    return df_external_data

# Step 6 function
def process_data(api, df_record_uuids, records):

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

       
        return records
                
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


'''# Step 6 function
def create_df_from_excel_sheet(excel):
    
    df = pd.read_excel(excel)

    excel_df =  pd.DataFrame({
        'concept_uuid' : df['uuid'],
        'concept_street' : df['prefLabel'],
        'concept_adamlink' : df['exactMatch'],
        'concept_scopeNote' : df['scopeNote']
    })    

    return excel_df'''
# Step 7 function
def working_in_the_turtle(records, df_external_data, concept_list):

    IMAGE = Namespace(f"{PREFIX}/resources/recordtypes/Image#")
    DEED = Namespace (f"{PREFIX}/resources/recordtypes/Deed#")
    RT = Namespace(f"{PREFIX}/resources/recordtypes")

    try:
        # Working with the records
        g = Graph()
        g.parse(records, format='turtle')
        
        for deed in g.subjects(RDF.type,MEMORIX.Record):
            
            result = []
            for inst in g.objects(subject=deed, predicate=SAA['isAssociatedWithModernAddress'] ):
                houseNumber = g.value(subject=inst, predicate=SAA['houseNumber'])
                streetTextualValue = g.value(subject=inst, predicate=SAA['streetTextualValue'])
                uuid = g.value(subject=inst, predicate=RT['Deed'])
                print(f'This is my housenumber: {houseNumber}')
                result.append({inst :{'houseNumber' : houseNumber, 'streetTextualValue' : streetTextualValue }})

            if any(g.triples((deed, SAA['isAssociatedWithModernAddress'], None))):
                print(f' This is my saa: textualvalue: {SAA['streetTextualValue']} for the deed: {deed}')  ##### WE ARE HERE !!!!!!!!!##########


        for inst in g.objects(subject=None, predicate=IMAGE['isAssociatedWithModernAddress']):  
            print(g.value(subject=inst, predicate=IMAGE['street']))
        def obtain_values(inst, g):
            # get street and house number begin and end
            street_concept = g.value(subject=inst, predicate=IMAGE['street'])
            street_name_memorix = str(g.value(subject=street_concept, predicate=SKOS['prefLabel']))
            street_alternatief_manual = concept_list['pref_label'][concept_list['uuid']==street_concept.split('/')[-1]].item()
            if street_alternatief_manual!=street_alternatief_manual:
                street_alternatief_manual=""
            streets_alternatief_adamlink = df_external_data['altlabel'][df_external_data['label']==street_name_memorix].to_list()
            streets_alternatief_adamlink = list(map(str.lower,streets_alternatief_adamlink))
            housenr_begin = g.value(subject=inst, predicate=IMAGE['houseNumberBegin'])
            housenr_end = g.value(subject=inst, predicate=IMAGE['houseNumberEnd'])
            #logging.info(f"\t\t{print_streets(street_name_memorix,street_alternatief_manual,streets_alternatief_adamlink)}, {housenr_begin}, {housenr_end}")
            logging.info(f"\t\t{street_name_memorix}, {housenr_begin}, {housenr_end}")
            return street_name_memorix,street_alternatief_manual,streets_alternatief_adamlink,housenr_begin,housenr_end
        for inst in g.objects(subject=None, predicate=IMAGE['streetTextualValue']):  
            print('I at lleast get in this motherfocking for loop')
            street_name_memorix,street_alternatief_manual,streets_alternatief_adamlink,housenr_begin,housenr_end = obtain_values(inst, g)
            print(street_name_memorix,street_alternatief_manual,streets_alternatief_adamlink,housenr_begin,housenr_end)
    
        return result
    except:
        log.error('It is just a mentioning. not a logging perse')

# Step 8 function
def process_some_other_data(api, records):


    # loop through data with progress meter
    for index, row in tqdm(records.iterrows(), total=records.shape[0]):
        log.info(f"START {row.uuid}")
        try:
            response = api.get_record(row.auuid)
            if response.status_code != 200:
                log.info("...Try to read again...")
                time.sleep(3)
                response = api.get_record(row.auuid)
                if response.status_code != 200:
                    log.error(f"Reading failed for {row.auuid}")
                    continue

            # load the graph
            g = Graph()
            g.parse(data=response.text, format='turtle')
            adresid_added = False

            record_block = URIRef(f"https://{PREFIX}.memorix.io/resources/records/{row.auuid}")
            # check if adamlink uri is empty
            if any(g.triples((record_block, SAA['streetTextualValue'], None))):
                log.info(f"Checking if value is present in {row.auuid}")  ##### WE ARE HERE !!!!!!!!!##########
            else:
                g.add((record_block, SAA['hasOrHadSubjectLocation'], URIRef(f"https://adamlink.nl/geo/address/{row.adresid}")))
                coord_bnode = BNode()
                if row.wkt==row.wkt:
                    g.add((record_block, SAA['hasOrHadSubjectCoordinates'], coord_bnode))
                    g.add((coord_bnode, RDF.type, MEMORIX.GeoCoordinates))
                    g.add((coord_bnode, SCHEMA.latitude, Literal(row.latitude, datatype=XSD.decimal)))         
                    g.add((coord_bnode, SCHEMA.longitude, Literal(row.longitude, datatype=XSD.decimal)))       
                    g.add((coord_bnode, SCHEMA.name, Literal(row.adresid)))  
                adresid_added = True

            if adresid_added:
                log.info("Correct")
                turtle = g.serialize(format="turtle")
                # put the updated record in memorix
                response = api.update_record(row.auuid, turtle)
                if response.status_code == 200:            
                    log.info(f"SUCCEED {row.auuid}")        
                else:            
                    log.error(f"FAIL {row.auuid}")
                    log.error(response.text)
        except:
            log.error(f"FAILED TRANSFORMATION {row.auuid}")

# Step 8 function
def new_logic_not_working_yet(api, turtle):
    pass

    # Turn turtle to working excel sheet 
    #response = api.get_record_type( 'Deed')
    #print(response.text,  file=open(turtle, 'w', encoding='utf-8'))
    ##print(f"Using file: {data}")
    #return response



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
    ctx.obj["record_turtle"] = "data/record_turtle.ttl"                 #### !!!! Location of deed turtle
    ctx.obj['concept_turtle'] = "data/concept_turtle.ttl"                #### !!!! Location of street turtle
    ctx.obj["record_uuids"] = "data/record_uuids.csv"            #### !!!! Location of deed turtle
    ######### SOWIESO WEG ctx.obj["excel_sheet"] = "../data/concept_streets.xlsx"         #### !!!! Location of concepts
    #ctx.obj["data"] = '../data/alternatieve_straatnamen.csv'    #### !!!! Location of adamlinks /alternative names
    ctx.obj["records"] = 'data/records.ttl'                      #### !!!! Location of adamlinks /alternative names
    #ctx.obj["log"] = log_setup(logfile)
  
# -----------------------------------
# EXPORT 
# -----------------------------------

@cli.command()
# Step 1 cli command
def download_record_turtle(ctx):

    retrieve_record_turtle_from_memorix( ctx.obj["api"], ctx.obj["Deed"], ctx.obj['record_turtle'])

# Step 2 cli command
@cli.command()
def download_concept_vocabulaire(ctx):

    retrieve_concept_turtle_from_memorix( ctx.obj["api"], ctx.obj['vocabulair'], ctx.obj['concept_turtle'])

# Step 3 cli command
@cli.command()
def download_record_uuid(ctx):

    retrieve_uuid_from_memorix( ctx.obj["env"], ctx.obj['record_uuids'])
    
# Step 4 cli command
@cli.command()
def turtle_to_list(ctx):
    
    concept_turtle_to_list( ctx.obj["concept_turtle"])

# Step 5 cli command
@cli.command()
def read_added_datasheet(ctx):
    
    read_external_data_with_panda( ctx.obj["data"])

# Step 6 cli command
@cli.command()
def concept_turtle_to_excel(ctx):
    
    process_data(ctx.obj['api'], ctx.obj['df_record_uuids'], ctx.obj['records'], ctx.obj['df_external_data'], ctx.obj['concept_list'])

# Step 7 cli command
@cli.command()
def testing_working_with_data(ctx):
    
    working_in_the_turtle(ctx.obj['records'], ctx.obj['df_external_data'], ctx.obj['concept_list'])
    print('Oke')

# CLI Pipeline
@cli.command()
@click.argument("data")
@click.pass_context
def pipeline(ctx, data):

    # Store data globally
    ctx.obj["data"] = data
  
    click.echo("STEP 1: DOWNLOAD TURTLE RECORD")

    #retrieve_record_turtle_from_memorix( ctx.obj["api"], ctx.obj["Deed"], ctx.obj['record_turtle'])

    record_turtle = retrieve_record_turtle_from_memorix(ctx.obj["api"], ctx.obj['Deed'], ctx.obj['record_turtle'])
    ctx.obj['record_turtle'] = record_turtle

    click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        "Continue to next step?",
        abort=True
    )

    click.echo("STEP 2: DOWNLOAD TURTLE CONCEPT")

    #retrieve_concept_turtle_from_memorix( ctx.obj["api"], ctx.obj['vocabulair'], ctx.obj['concept_turtle'])
#   
    concept_turtle = retrieve_concept_turtle_from_memorix(ctx.obj["api"], ctx.obj['vocabulair'], ctx.obj['concept_turtle']) 
    ctx.obj['concept_turtle'] = concept_turtle

    click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        "Continue to next step?",
        abort=True
    )
    
    click.echo("STEP 3: DOWNLOAD UUIDS RECORD")
    
    #retrieve_uuid_from_memorix( ctx.obj["env"], ctx.obj['record_uuids'])

    df_record_uuids = retrieve_uuid_from_memorix(ctx.obj["env"], ctx.obj['record_uuids'])
    ctx.obj['df_record_uuids'] = df_record_uuids

    click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        "Continue to next step?",
        abort=True
    )

    click.echo("STEP 4: READ TURTLE AND PUT IN LIST")
    
    #concept_turtle_to_list( ctx.obj["concept_turtle"])

    concept_list = concept_turtle_to_list( ctx.obj["concept_turtle"])
    print('I am here for the concept_list')
    ctx.obj['concept_list'] = concept_list

    click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        "Continue to next step?",
        abort=True
    )

    click.echo("STEP 5: READ EXTERNAL DATASHEET WITH PANDA")
    
    #read_external_data_with_panda( ctx.obj["data"])

    df_external_data = read_external_data_with_panda( ctx.obj["data"])
    ctx.obj['df_external_data'] = df_external_data
    
    click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        "Continue to next step?",
        abort=True
    )

    click.echo("STEP 6: HERE WE START WITH GETTING A RECORD TURTLE")
    
    #process_data(ctx.obj['api'], ctx.obj['df_record_uuids'], ctx.obj['records'] )

    records = process_data(ctx.obj['api'], ctx.obj['df_record_uuids'], ctx.obj['records'])
    #ctx.obj['raw_turtle_data'] = raw_turtle_data
    ctx.obj['records'] = records
    print(f' I this my fucking none?: {records}')

    click.echo(f"Environment: {ctx.obj['env']}")

    click.confirm(
        "Continue to next step?",
        abort=True
    )   

    click.echo("STEP 7: HERE WE GO")

    result= working_in_the_turtle(ctx.obj['records'], ctx.obj['df_external_data'], ctx.obj['concept_list'])
    print(result)
    click.confirm(f"I am connected to: {ctx.obj['env']} Continue?", abort=True)


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