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
import math, numpy as np
#WORK_REPO = Path(r"C:\\Users\\swart053\\Documents\\VSC\\saa-nexus-scripts") # Adjust base path based on location
#HOME_REPO = Path(r"C:\\Users\\swart053\\Documents\\VSC\\test\\cli_module") # Adjust base path based on location
HOME_REPO = Path("/opt/lampp/htdocs/test/cli_module")
WORK_REPO = Path("/opt/lampp/htdocs/saa-nexus-scripts")
sys.path.append(str(WORK_REPO))
from modules import memorix
from modules import saa
from modules import saa_rdf as nrdf
PREFIX = 'stadsarchief'

'''
   Script for exporting data from Memorix through various channels with help of
   various other scripts, one external data_document and with use of the Memorix API.
   Scripts used are: 
   streets.py
   conceptlist_turtle_to_excel.ipynb
      
   * External document used in this script can be altered.
   * Scripts can be altered for various purposes.
   * Click commands can be reduced or expanded 
   
   #### !!!! Alteration options mentioned like this  

   This script does in order:  \\# ADJUST THESE WHEN ALL FUNCTIONS ARE DONE CREATING!!!!!!!!! 

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

# Script variabelen
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
logfile = f'logs/street_migr_to_concept {str(current_datetime)}.log'
errors = []

# Log handler 
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(logfile, mode='w')]
)
log = logging.getLogger()

kwrgs = {
        # Declare script variables
        'response' : {},
        'concept_list' : [],
        'predicates' : [],
        'extracted' : [],
        'total_concept_uuids' : [],
        'total_record_uuids' : [],
        'total_predicates' : 0, 
        'test_amount': 5,
        'records_deleted_message' : (f'\tThere was already a records file present in the \'data\' folder.\n' +
            '\tTo prevent double data, this file has been deleted\n' +
            '\tA new one has been created in this very function'),
        # User variables
        'vocabulair' : 'a4863c0c-d9e5-3902-831a-d0960e381a41',  #### !!!! uuid of vocabul air            
        'concept_turtle' : "data/concept_turtle.ttl",               #### !!!! Location of street turtle
        'record_uuids' : "data/record_uuids.csv",            #### !!!! Location of deed turtle
        'records' : 'data/records.ttl',      
        'recordia' : 'data/recordia.ttl',      
        'alternatives' : "data/alternatives.csv",            #### !!!! Location of deed turtle
        'outliers' : "data/outliers.csv",            #### !!!! Location of deed turtle
        'pattern' : r'^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$'
}

# CLI 
env = sys.argv[1]
data = sys.argv[2]

# -----------------------------------
# DECLARATIONS
# -----------------------------------

# Omgeving setup
if env == 'acc':
    PREFIX = 'https://ams-migrate.memorix.io'
    settings_file = Path(WORK_REPO, 'settings.json') 
elif env == 'prod':
    PREFIX = 'https://stadsarchiefamsterdam.memorix.io'
    settings_file = Path(WORK_REPO, 'settings.prod.json') 
elif env == 'tst':
    settings_file = print(f'test output')
else:
    raise ValueError("Environment must be 'acc' or 'prod'")

settings = saa.readJsonFile(settings_file) 
api = memorix.ApiClient(settings)

# Namespace bepalingen
SAA = Namespace("https://data.archief.amsterdam/ontology#")
RICO = Namespace("https://www.ica.org/standards/RiC/ontology#")
MEMORIX = Namespace("http://memorix.io/ontology#")
DEED = Namespace(f"{PREFIX}/resources/recordtypes/Deed#")
SCHEMA = Namespace(f"http://schema.org/")
SKOS = Namespace(f"http://www.w3.org/2004/02/skos/core#")
DEED = Namespace (f"{PREFIX}/resources/recordtypes/Deed#")
RT = Namespace(f"{PREFIX}/resources/recordtypes")
IMAGE = Namespace(f"https://{PREFIX}.memorix.io/resources/recordtypes/Image#")
SKOS = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")

# -----------------------------------
# FUNCTIONS
# -----------------------------------

# Step 1 fucntie
def retrieve_concept_turtle_from_memorix(s, s_str, g, **kwrgs):
    
    print("\n\tSTEP 1: DOWNLOADEN VAN DE DE TURTLE CONCEPT VOCABULAIR")

    # Concept vocabulaire turtle uit memorix halen
    '''kwrgs['response'] = api.list_concepts( vocabulair)
    print(response.text,  file=open(concept_turtle, 'w', encoding='utf-8'))'''

    match = re.search(r'/vocabularies/concepts/([^/>]+)', s_str)
    uuid = match.group(1) if match else ""

    prefLabel = next((str(lab) for lab in g.objects(s, SKOS.prefLabel)), "")
    exactMatch = next((str(em) for em in g.objects(s, SKOS.exactMatch)), "") # <-- fout: want exactMatch kan nu meer dan 1 waarde hebben
    scopeNote = next((str(sn) for sn in g.objects(s, SKOS.scopeNote)), "")

    kwrgs['concept_list'].append({
        'concept_uuid' : uuid,
        'streetTextualValue' : prefLabel,
        'adamlink' : exactMatch,
        'scope' : scopeNote
    }) 

    kwrgs['total_concept_uuids'].append(uuid)   
        
    return kwrgs['concept_list'], kwrgs['total_concept_uuids'], kwrgs['response']
    

# Step 2 function
def retrieve_uuid_from_memorix(**kwrgs):
    
    print("\n\tSTEP 2: DOWNLOADING RECORD UUIDS")

    # Path based on location of script and datafolder
    sys.path.append(str(HOME_REPO))
    import get_uuid  
    # Get uuids with query 
    #response = get_uuid.main(env, record_uuids)

    kwrgs['df_record_uuids'] = pd.read_csv(kwrgs['record_uuids'], #response

        sep=";",             
        dtype={ "uuid": str
           })

    kwrgs['df_record_uuids'] = pd.DataFrame(kwrgs['df_record_uuids'])
    print(kwrgs['df_record_uuids'])
    return kwrgs['df_record_uuids']


# Step 3 function
def read_external_data_with_panda(data, **kwrgs):
    
    print("\n\tSTEP 3: EXTERNE DATA UITLEZEN EN DATA NAAR DATAFRAME VERPLAATSEN")

    df = pd.read_csv(data, 

    sep=";",             
    dtype={ "straat-label-altlabel": str
        })

    kwrgs['df_external_data'] = pd.DataFrame(df)
  
    return kwrgs['df_external_data'] 


# Step 4 function
def get_turtle_for_record_with_uuid(index, row, **kwrgs):

    print("\n\tSTEP 4: RECORD TURTLE UITLEZEN EN DATA NAAR DATAFRAME VERPLAATSEN")

    # Check if file already exists and delete based on env
    if os.path.exists(kwrgs['records']):     
        #os.remove(records)
        print(kwrgs['records_deleted_message']) 

        kwrgs['test_amount'] = 5

        ##### UNCOMMENT WHEN LIVE ###########
        '''    #record_block = URIRef(f"{PREFIX}/resources/records/{uuid}")
            #print (f'This is the record block: {record_block}')
            if response.status_code != 200:
                log.info("...Try to read again...")
                time.sleep(3)
                response = api.get_record(uuid)
                if response.status_code != 200:
                    log.error(f"Reading failed for {uuid}")
        else:'''
    
    # load the graph
    g = Graph()
    g.parse(kwrgs['records'] , format='turtle')

    for record in g.subjects(RDF.type, MEMORIX.Record):

        record_uuid = str(record).rsplit('/', 1)[-1]

        address = next(g.objects(
            record,
            SAA.isAssociatedWithModernAddress
        ))

        kwrgs['predicates'].append({
            'uuid': record_uuid,
            'streetTextualValue': str(g.value(address, SAA.streetTextualValue)),
            'house_number': str(g.value(address, SAA.houseNumber)),
            'number_add': str(g.value(address, SAA.houseNumberAddition)),
        })

        kwrgs['total_predicates'] += 1

    return kwrgs['records'], kwrgs['predicates'], kwrgs['total_predicates'], kwrgs['test_amount']
    

# Step 5 function 
def match_data(
               **kwrgs
               ):

    print("\n\tSTEP 5 DATA MATCHEN EN TOEVOEGEN AAN DATAFRAME")

    '''Adamlink meenemen. Niet alleen om te kunnen vullen in csv voor manual check, maar ook omdat deze in combinatie met 
    het migratie adresveld al op 7.703.851 locaties_met_adam records is gevuld [ aldus Memorix ] dus deze wil je 
    filteren en niet meenemen in je wijziging'''
    
    # - De turtle predicates omzetten in een dataframe
    kwrgs['predicates_df'] = pd.DataFrame(kwrgs['predicates'])

    # - De migratie street-string opsplitsen in street nummer, nummertoevoeging
    extract_pattern = kwrgs['predicates_df']['streetTextualValue'].str.extract(kwrgs['pattern'])

    # - De string onderdelen toevoegen aan het dataframe
    kwrgs['predicates_df']['streetTextualValue'] = extract_pattern['street'].str.strip()
    kwrgs['predicates_df']['extracted_number'] = extract_pattern['number'].str.strip()
    kwrgs['predicates_df']['extracted_number_add'] = extract_pattern['add'].str.strip()
        
    # Lege velden normaliseren en string 'None' vervangen met NaN
    kwrgs['predicates_df'].fillna("",inplace=True)
    kwrgs['predicates_df']['house_number'] = kwrgs['predicates_df']['house_number'].replace('None', np.nan)
    kwrgs['predicates_df']['extracted_number'] = kwrgs['predicates_df']['extracted_number'].replace('None', np.nan)
    kwrgs['predicates_df']['number_add'] = kwrgs['predicates_df']['number_add'].replace('None', np.nan)
    kwrgs['predicates_df']['extracted_number_add'] = kwrgs['predicates_df']['extracted_number_add'].replace('None', np.nan)
        
    # Outliers naar dataframe omzetten obv index van predicates
    outliers_df = pd.DataFrame(index=kwrgs['predicates_df'].index)

    # Uuid overnemen van predicates
    outliers_df['uuid'] = kwrgs['predicates_df']['uuid']

    # huisnummers en toevoegingen vullen waar leeg en naar df schrijven indien afwijkend
    street_map = {
                  'house_number': 'extracted_number',
                  'number_add': 'extracted_number_add',
                  'uuid' : 'uuid'
    }
        
    for target, source in street_map.items():

        # masker voor vullen predicatenlijst indien leeg 
        mask_fill = (
            kwrgs['predicates_df'][target].isna() &
            kwrgs['predicates_df'][source].notna()
        )
        kwrgs['predicates_df'].loc[mask_fill, target] = kwrgs['predicates_df'].loc[mask_fill, source]

        # Wegschrijven naar outliers indien data reeds bestaat en afwijkt
        mask_to_csv = (
            kwrgs['predicates_df'][target].notna() &
            kwrgs['predicates_df'][source].notna() &
            (kwrgs['predicates_df'][target] != kwrgs['predicates_df'][source])
        )
        outliers_df.loc[mask_to_csv, target] = kwrgs['predicates_df'].loc[mask_to_csv, source]
        
    # Dataframe maken van concepten
    concept_df = pd.DataFrame(kwrgs['concept_list'], index=range(len(kwrgs['concept_list'])))
        
    # concept uuid en adamlink toevoegen aan predicates dataframe obv 'straat' met behulp van een merge         
    merge_concepts = kwrgs['predicates_df'].merge(concept_df[['streetTextualValue', 'concept_uuid', 'adamlink']], on = 'streetTextualValue', how='left' )
    kwrgs['predicates_df'] = merge_concepts

    # nummer van adamlink afhalen van alternatieve lijst en toevoegen aan kolom altlabel in twee dataframes separaat
    kwrgs['predicates_df']['altlabel'] = kwrgs['predicates_df']['adamlink'].str.extract(r'(\d+)')
    kwrgs['df_external_data']['number_altlabel'] = kwrgs['df_external_data']['straat-label-altlabel'].str.extract(r'(\d+)')

    # Nummer altlabel tussen dataframes vergelijken en toevoegen aan een lijst
    def find_alternatives(kwrgs):    
  
        if pd.notna(kwrgs['predicates_df']['altlabel']):
            # Find all rows in the alternatives dataframe with the same number
            number_to_match = kwrgs['predicates_df']['altlabel']
            #print(f"Looking for alternatives for number: {number_to_match}")
            alternatives = kwrgs['df_external_data'][kwrgs['df_external_data']['number_altlabel'] == number_to_match]
            return alternatives['straat-label-altlabel'].tolist()  # Return all matching alternatives
        return []

    # Lijst alternative schrijfwijzen toevoegen aan predicates dataframe
    kwrgs['predicates_df']['alternative_names'] = kwrgs['predicates_df'].apply(find_alternatives, axis=1, kwrgs=['df_external_data'])

    # List alternatieve schrijfwijzen overnemen in outliers obv 'uuid'
    merge_concepts = outliers_df.merge(kwrgs['predicates_df'][['uuid', 'alternative_names']], on = 'uuid', how='left' )
    outliers_df = merge_concepts

    print(outliers_df)
               
    return kwrgs['predicates_df'], kwrgs['outliers_df']




# Step 6 function
def main(**kwrgs):

    print('----------------------------------------------------------------------------\n\n' +
    f"\t\t\"WE ZITTEN IN DE VOLGENDE OMGEVING:\n\n\t\t\t'{env}'\"\n" +
    '\t\"We gaan nu staren met het ophalen van de concept vocabulaire.\" \n')

    user = input('\t\"Wil je starten met het ophalen van deze data?\": (Y/N) \n' +
    '\n----------------------------------------------------------------------------\n\n')
    if user == 'Y':
        pass

    try:
        # STEP 1
        # Retrieve a turtle with all concepts based on vocabulair    
        g = rdflib.Graph()
        g.parse(kwrgs['concept_turtle'], format="ttl")
        
        for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
            s_str = str(s)
            print('I\'m here first')
            kwrgs['concept_list'], kwrgs['total_concept_uuids'], kwrgs['response'] = retrieve_concept_turtle_from_memorix(
            s, s_str, g, **kwrgs
        ) 
            
        if kwrgs['concept_list']:
            log.info(f'Concept turtle downloaded from Memorix and put in list at : {kwrgs['total_concept_uuids']} with a total of {kwrgs['total_concept_uuids']} concepts')
            print('\n----------------------------------------------------------------------------\n\n' +
              f"\tGereed. Er zijn {len(kwrgs['total_concept_uuids'])} UUIDs naar de concept turtle geschreven op locatie: {kwrgs['concept_turtle']}" +
              f'\tDit is de lengte van de uitgelezen turtle van de concepten vocabulaire: {len(kwrgs['concept_list'])}\n' +
              f'\tEn dit was de lengte van het aantal concepten na downloaden uit memorix: \'{len(kwrgs['total_concept_uuids'])}\'\n' +
              f'\tEr zijn {len(kwrgs['concept_list']) - len(kwrgs['total_concept_uuids'])} concepten verloren gegaan bij het uitlezen van de data' +
              '\t\"Wil je doorgaan met het ophalen van de record uuids uit memorix?\"\n' +
              '\n----------------------------------------------------------------------------\n\n', 
            )
    except:
        if kwrgs['concept_turtle']:
            log.error(f'There was an issue while creating the vocabulair:{kwrgs['vocabulair']}' +
                      f'from entrypoint: {env}.' +
                      f'It appears a concept_turtle was created. You should check the file for accuracy.')
        else: 
            log.error(f'The concept_turtle : {kwrgs['concept_turtle']} had issues being created at accespoint {env},')
            errors.append({'fn: retrieve_concept_turtle_from_memorix': [kwrgs['vocabulair'], kwrgs['concept_turtle'], kwrgs['response']]})


    try:
        # SYTEP 2
        # Retrieve record UUIDS from memorix and put in dataframe 
        kwrgs['df_record_uuids'] = retrieve_uuid_from_memorix(**kwrgs)

        if kwrgs['df_record_uuids']:
            log.info(f'Recorded uuid\'s downloaded from Memorix at {kwrgs['df_record_uuids']} and put in dataframet with a total of {len(kwrgs['df_record_uuids'])} records')
            print( 
            '\n----------------------------------------------------------------------------\n\n' +
            f"\tGereed. Er zijn {len(kwrgs['df_record_uuids']['uuid'])} record UUIDs opgehaald uit Memorix.\n" + 
            f"\tDeze zijn opgeslagen op locatie: {kwrgs['record_uuids']}.\n" +
            '\n----------------------------------------------------------------------------\n\n',   
            )
 
    except:
        log.error(f'''The uuid's : {kwrgs['record_uuids']} had an issue at accespoint {env}.''')
        errors.append({'fn: retrieve_uuid_from_memorix': [env, kwrgs['record_uuids'], kwrgs['df_record_uuids'], HOME_REPO]})
      

    try:    
        # STEP 3
        # Add the provided external data to a dataframe
        kwrgs['df_external_data'] = read_external_data_with_panda(data, **kwrgs)
        
        if kwrgs['df_external_data']:
            log.info(f'The external data is transferred to a dataframe at {kwrgs['df_external_data']}')
            print(
            '\n----------------------------------------------------------------------------\n\n' +
            f"\tGereed. Er zijn {len(kwrgs['df_external_data']['street-label-altlabel'])} rijen opgehaald uit de externe datasheet {data}.\n" +
            '\n----------------------------------------------------------------------------\n\n',
            )
    except: 
        log.error(f'There was an issue reading the externally added data : {data},\n' +
                  f'and creating the dataframe with it.')
        errors.append({'fn: read_external_data_with_panda': [data]})

    try:   
        # STEP 4
        # Get all records in a turtle, based on UUIDS retrieved in step 2

        for index, row in kwrgs['df_record_uuids'].head(kwrgs['test_amount']).iterrows():
            log.info(f"START {row.uuid}")

            kwrgs['records'], kwrgs['predicates'], kwrgs['total_predicates'], kwrgs['test_amount'] = get_turtle_for_record_with_uuid( 
                index, row, **kwrgs
                )
        
        if kwrgs['predicates']:
            log.info(f"\t\t,  {kwrgs['predicates'], kwrgs['df_record_uuids'], kwrgs['records']}")
            print('\n----------------------------------------------------------------------------\n\n' +
                  f"\tGereed. Er zijn {len(kwrgs['predicates'])} records uit de turtle gehaald" +
                  f'\tDit was het aantal uuids uit Memorix gehaald: \'{len(kwrgs['df_record_uuids'])}\'\n' +
                  f'\tEr wordt getest met {kwrgs['test_amount']} uuids\n' +
                  f'\tEr zijn {(kwrgs['test_amount'] if kwrgs['test_amount'] >= 0 else len(kwrgs['df_record_uuids'])) - len(kwrgs['predicates'])} rec ords verloren gegaan bij het uitlezen van de data.',
                  '\t\"Do you want to continue retrieving the predicates from these records?\"\n' +
                  '\n----------------------------------------------------------------------------\n\n',
            )          
    
    except:
        log.error(f'There was an issue retrieving the turtle for id: {kwrgs['df_record_uuids'](row.uuid)},\n')               
        errors.append({'fn: get_turtle_for_record_with_uuid': [kwrgs['records'], kwrgs['df_record_uuids'], kwrgs['predicates']]})
        
        
    try:
        # STEP 5 
        kwrgs['predicates_df'], kwrgs['outliers_df'] = match_data(
               kwrgs
        )
       
        print('\n----------------------------------------------------------------------------\n\n' +
              f"\tGereed. Er zijn {len(kwrgs['predicates_df'])} rijen verwerkt in het predicates dataframe" +
              f'\tEr wordt gewerkt met {(kwrgs['test_amount'] if kwrgs['test_amount'] >= 0 else len(kwrgs['total_predicates']) - len())} records\'\n' +
              f'\tEr wordt getest met {kwrgs['test_amount']} uuids\n' +
              f'\tEr zijn {(kwrgs['test_amount'] if kwrgs['test_amount'] >= 0 else len(kwrgs['df_record_uuids'])) - len(kwrgs['predicates'])} records verloren gegaan bij het uitlezen van de data.'
              '\t\"Do you want to continue matching the extracted pattern with the concepts?\"\n' +
              '\n----------------------------------------------------------------------------\n\n',
        )

    except:
        log.error(f'There was an issue extracting the pattern from the predicates : {kwrgs['predicates']},\n')               
        errors.append({'fn: match_data': [kwrgs['predicates']]})
        
    try:
        # STEP 6
        print("\n\tSTEP 6: DATA TERUGZETTEN IN TURTLE EN UPLOADEN")
        g = Graph()
        g.parse(kwrgs['records'], format='turtle') 
        concept_added = False
        adamlink_added = False

        for index, row in kwrgs['predicates_df'].iterrows():
            print(kwrgs['predicates_df']["uuid"].tolist())    
            log.info(f"START {row.uuid}")
            log.info(f"Fill concept for street {row.streetTextualValue} and uuid {row.uuid} with concept uuid {row.concept_uuid}")
            
            # Record en Concept URI bepalen 
            record_uri = URIRef(f"{PREFIX}/resources/records/{row.uuid}")
            concept_uri = URIRef(f"{PREFIX}/resources/vocabularies/concepts/{row.concept_uuid}")
            adamlink = URIRef(row.adamlink)

            print(record_uri)
            print(record_uri in set(g.subjects(RDF.type, MEMORIX.Record)))
            print(row.adamlink)
            
            # Adresblock bepalen
            address = next(
                g.objects(
                record_uri,
                SAA.isAssociatedWithModernAddress
            ))

            # Data wanneer leeg, toevoegen aan turtle
            if ((record_uri, SAA.hasOrHadSubjectLocation,None) not in g):
                g.add((address, SAA.street, concept_uri))
                concept_added = True
            else: 
                log.error(f'Concept already filled for uuid: {record_uri}')    
            if ((record_uri, SAA.hasOrHadSubjectLocation,None) not in g):
                g.add((record_uri, SAA.hasOrHadSubjectLocation, adamlink))
                adamlink_added = True
            else: 
                log.info(f'Adamlink already filled for uuid: {record_uri}')  
            if (address, SAA.houseNumber, None) not in g:
                g.add((address, SAA.houseNumber, Literal(row.house_number)))
            else: 
                log.info(f'Housenumber not changed for uuid: {record_uri}')
            if (address, SAA.houseNumberAddition, None) not in g:
                g.add((address, SAA.houseNumberAddition, Literal(row.number_add)))
            else: 
                log.info(f'Housenumber Addition not changed for uuid: {record_uri}')

        
            for t in g.triples((address, None, None)):
                print(t)
            for t in g.triples((record_uri, SAA.hasOrHadSubjectLocation, None)):
                print(t)

 
            for index, row in kwrgs['predicates_df'].iterrows():
                if concept_added:
                        logging.info("Correct")
                        turtle = g.serialize(format="turtle")
                        response = api.update_record(row.uuid, turtle)
                        if response.status_code == 200:            
                            logging.info(f"SUCCEED {row.uuid}")        
                        else:            
                            logging.error(f"FAIL {row.uuid}")
                            logging.error(response.text)

            print(        
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"We hebben de data gevuld en geupload naar Memorix.\" \n' +
        '\t\"De job is nu afgerond.\"\n' +
        '\n----------------------------------------------------------------------------\n\n'
        )  
        print("\n\tALL STEPS DONE!")

        return kwrgs

    except Exception as e:
        print(f"FAILED {row.uuid}: {e}")
        log.error(f'Something went wrong with matching the data { kwrgs['records'], kwrgs['predicates_df']}')
        errors.append({'fn: fill_data': [kwrgs['records'], kwrgs['predicates_df']]})


   
  
# -----------------------------------
# EXPORT 
# -----------------------------------



    

    


    

    

    
    
    


    

    

    


'''    kwrgs['records'] = fill_data(
        kwrgs
        )'''
    






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
    main(**kwrgs)