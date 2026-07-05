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
        'result' : False,
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
        'vocabulair' : 'a4863c0c-d9e5-3902-831a-d0960e381a41',  #### !!!! uuid of vocabulair            
        'concept_turtle' : "data/concept_turtle.ttl",           #### !!!! Location of street turtle
        'record_uuids' : "data/record_uuids.csv",               #### !!!! Location of uuid from memorix
        'records' : 'data/records.ttl',                         #### !!!! Location of records turtle
        'alternatives' : "data/alternatives.csv",               #### !!!! Location of external csv
        'outliers' : "data/outliers.csv",                       #### !!!! Location of output outliers
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
def retrieve_concept_turtle_from_memorix(**kwrgs):

    kwrgs['result'] = False
    # Concept vocabulaire turtle uit memorix halen
    response = api.list_concepts( kwrgs['vocabulair'])
    
    print(response.text,  file=open(kwrgs['concept_turtle'], 'w', encoding='utf-8'))
    # response = 200  # TEMP
    
    g = rdflib.Graph()
    g.parse(kwrgs['concept_turtle'], format="ttl")

    for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
        s_str = str(s)
    
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

    kwrgs['result'] = True
        
    return kwrgs['concept_list'], kwrgs['total_concept_uuids'], kwrgs['result']
    

# Step 2 function
def retrieve_uuid_from_memorix(**kwrgs):
    
    kwrgs['result'] = False
    # Path based on location of script and datafolder
    sys.path.append(str(HOME_REPO))
    import get_uuid  

    # Get uuids with query and give storage location for csv
    response = get_uuid.main(env, kwrgs['record_uuids']) #Give storage location
    #response = 200  # TEMP

    #Read csv from storage location
    kwrgs['df_record_uuids'] = pd.read_csv(kwrgs['record_uuids'], #response

        sep=";",             
        dtype={ "uuid": str
           })

    kwrgs['df_record_uuids'] = pd.DataFrame(kwrgs['df_record_uuids'])

    kwrgs['result'] = True

    return kwrgs['df_record_uuids'], kwrgs['result'] 


# Step 3 function
def read_external_data_with_panda(data, **kwrgs):
    
    kwrgs['result'] = False

    df = pd.read_csv(data, 

    sep=";",             
    dtype={ "straat-label-altlabel": str
        })

    kwrgs['df_external_data'] = pd.DataFrame(df)
    
    kwrgs['result'] = True
   
    return kwrgs['df_external_data'], kwrgs['result'] 


# Step 4 function
def get_turtle_for_record_with_uuid(**kwrgs):

    kwrgs['result'] = False

    # Check if file already exists and delete based on env
    if os.path.exists(kwrgs['records']):     
        # os.remove(records)
        # print(kwrgs['records_deleted_message']) 
        pass
        
    for index, row in kwrgs['df_record_uuids'].head(kwrgs['test_amount']).iterrows():

        log.info(f"START {row.uuid}")
        uuid =row.uuid
        response = api.get_record(uuid)

        if response.status_code != 200:
            log.info("...Try to read again...")
            time.sleep(3)
            response = api.get_record(uuid)
            #print(response.text,  file=open(kwrgs['records'], 'w', encoding='utf-8'))

            if response.status_code != 200:
                log.error(f"Reading failed for {uuid}")
        else:
            #response = 200  # TEMP        
            # load the graph
            g = Graph()
            g.parse(kwrgs['records'] , format='turtle')

            record_uri = URIRef(f"{PREFIX}/resources/records/{uuid}")

            address = next(g.objects(
                record_uri,
                SAA.isAssociatedWithModernAddress
            ))
            #print(address.values)
            kwrgs['predicates'].append({
                  'uuid': uuid,
                  'streetTextualValue': str(g.value(address, SAA.streetTextualValue)),
                  'house_number': str(g.value(address, SAA.houseNumber)),
                  'number_add': str(g.value(address, SAA.houseNumberAddition)),
            })
            kwrgs['total_predicates'] += 1

            kwrgs['result'] = True

    return kwrgs['predicates'], kwrgs['total_predicates'], kwrgs['result']
    

# Step 5 function 
def match_data(**kwrgs):

    kwrgs['result'] = False    
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

        # masker voor vullen velden indien leeg 
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
    def find_alternatives(row, df_external_data):    
                
        if pd.notna(row['altlabel']):
            
            # Find all rows in the alternatives dataframe with the same number
            number_to_match = row['altlabel']
            
            alternatives = df_external_data[df_external_data['number_altlabel'] == number_to_match]
            return alternatives['straat-label-altlabel'].tolist()  # Return all matching alternatives
        return []

    # Lijst alternative schrijfwijzen toevoegen aan predicates dataframe
    df_external_data = kwrgs['df_external_data']
    predicates_df = kwrgs['predicates_df']
    predicates_df['alternative_names'] = predicates_df.apply(find_alternatives, axis=1, args=[df_external_data])
    
    # List alternatieve schrijfwijzen overnemen in outliers obv 'uuid'
    merge_concepts = outliers_df.merge(kwrgs['predicates_df'][['uuid', 'alternative_names']], on = 'uuid', how='left' )
    kwrgs['outliers_df'] = merge_concepts
    kwrgs['predicates_df'] = predicates_df
    kwrgs['result'] = True
               
    return kwrgs['predicates_df'], kwrgs['outliers_df'], kwrgs['result'] 


def upload_data(**kwrgs):

    kwrgs['result'] = False
    g = Graph()
    g.parse(kwrgs['records'], format='turtle') 
    turtle_changed = False
        
    for index, row in kwrgs['predicates_df'].iterrows():
        print(kwrgs['predicates_df']["uuid"].tolist())    
        log.info(f"START {row.uuid}")
        log.info(f"Fill concept for street {row.streetTextualValue} and uuid {row.uuid} with concept uuid {row.concept_uuid}")

        # Record en Concept URI bepalen 
        record_uri = URIRef(f"{PREFIX}/resources/records/{row.uuid}")
        concept_uri = URIRef(f"{PREFIX}/resources/vocabularies/concepts/{row.concept_uuid}")
        adamlink = URIRef(row.adamlink)

        '''print(record_uri)
        print(record_uri in set(g.subjects(RDF.type, MEMORIX.Record)))
        print(row.adamlink)'''          

        # Adresblock bepalen
        address = next(
            g.objects(
            record_uri,
            SAA.isAssociatedWithModernAddress
        ))

        # Data wanneer leeg, toevoegen aan turtle


        #if ((record_uri, SAA.hasOrHadSubjectLocation,None) not in g):
        #    g.add((record_uri, SAA.hasOrHadSubjectLocation, adamlink))
        #    turtle_changed = True
        #else: 
        #    log.info(f'Adamlink already filled for uuid: {record_uri}')  

        if ((record_uri, SAA.hasOrHadSubjectLocation,None) not in g):
            g.add((address, SAA.street, concept_uri))
            turtle_changed = True
        else: 
            log.error(f'Concept already filled for uuid: {record_uri}')    

        if (address, SAA.houseNumber, None) not in g:
            g.add((address, SAA.houseNumber, Literal(row.house_number)))
            turtle_changed = True
        else: 
            log.info(f'Housenumber not changed for uuid: {record_uri}')

        if (address, SAA.houseNumberAddition, None) not in g:
            g.add((address, SAA.houseNumberAddition, Literal(row.number_add)))
            turtle_changed = True
        else: 
            log.info(f'Housenumber Addition not changed for uuid: {record_uri}')


        for t in g.triples((address, None, None)):
            print(t)

        for t in g.triples((record_uri, SAA.hasOrHadSubjectLocation, None)):
            print(t)

        for index, row in kwrgs['records'].iterrows():

            if turtle_changed:
                logging.info("Correct")
                turtle = g.serialize(format="turtle")
                #response = api.update_record(row.uuid, turtle)
                print(turtle)
                #if response.status_code == 200:            
                #    logging.info(f"SUCCEED {row.uuid}")     
                #    kwrgs['result'] = True   
#
                #else:            
                #    logging.error(f"FAIL {row.uuid}")
                #    logging.error(response.text)

    return kwrgs['result']

# Step 6 function
def main(**kwrgs):

    print('----------------------------------------------------------------------------\n\n' +
    f"\t\t\"OMGEVING:\n\n\t\t\t'{env}'\"\n" )
    input('\t\"Starten met ophalen van de data?\": (Y/N) \n' +
    '\n----------------------------------------------------------------------------\n\n')

    try:
        print("\n\tSTEP 1: DOWNLOADEN VAN DE DE TURTLE CONCEPT VOCABULAIR")
        # Retrieve a turtle with all concepts based on vocabulair    

        kwrgs['concept_list'], kwrgs['total_concept_uuids'], kwrgs['result'] = retrieve_concept_turtle_from_memorix(**kwrgs) 
            
        if kwrgs['result']:  #response.status_code == 200:          
            print('\n----------------------------------------------------------------------------\n\n' +
              f"\tGereed. Er zijn {len(kwrgs['concept_list'])} concepten naar een lijst geschreven\'\'\n" +
              f'\tWe hebben \'{len(kwrgs['total_concept_uuids'])}\' concepten uit memorix gehaald. \'\'\n' +
              f'\tEr zijn {len(kwrgs['concept_list']) - len(kwrgs['total_concept_uuids'])} concepten verloren gegaan bij het uitlezen van de data\'\'\n' +
              '\n----------------------------------------------------------------------------\n\n', 
            )
            input('\t\"Verder met downloaden record UUIDS?\": (Y/N) \n' +
                  '\n----------------------------------------------------------------------------\n\n')
            log.info(f'Concept turtle downloaded from Memorix and put in list at : {kwrgs['total_concept_uuids']} with a total of {kwrgs['total_concept_uuids']} concepts')
        
        else:            
            log.error(f"FAIL {kwrgs['concept_list']['uuid']}")

    except:
        if kwrgs['concept_turtle']:
            log.error(f'There was an issue while creating the vocabulair:{kwrgs['vocabulair']}' +
                      f'from entrypoint: {env}.' +
                      f'It appears a concept_turtle was created. You should check the file for accuracy.')
        else: 
            log.error(f'The concept_turtle : {kwrgs['concept_turtle']} had issues being created at accespoint {env},')
            errors.append({'fn: retrieve_concept_turtle_from_memorix': [kwrgs['vocabulair'], kwrgs['concept_turtle'], kwrgs['response']]})


    try:
        print("\n\tSTEP 2: DOWNLOADING RECORD UUIDS")

        kwrgs['df_record_uuids'], kwrgs['result'] = retrieve_uuid_from_memorix(**kwrgs)

        if kwrgs['result']: 
            log.info(f'Recorded uuid\'s downloaded from Memorix at {kwrgs['df_record_uuids']} and put in dataframet with a total of {len(kwrgs['df_record_uuids'])} records')
            print( 
            '\n----------------------------------------------------------------------------\n\n' +
            f"\tGereed. Er zijn {len(kwrgs['df_record_uuids']['uuid'])} record UUIDs opgehaald uit Memorix.\n" + 
            f"\tDeze zijn opgeslagen op locatie: {kwrgs['record_uuids']}.\n" +
            '\n----------------------------------------------------------------------------\n\n',   
            )
            input('\t\"Verder met uitlezen externe csv file?\": (Y/N) \n' +
                  '\n----------------------------------------------------------------------------\n\n')
        else:            
            log.error(f"FAIL {kwrgs['df_record_uuids']['uuid']}")
 
    except:
        log.error(f'''The uuid's : {kwrgs['record_uuids']} had an issue at accespoint {env}.''')
        errors.append({'fn: retrieve_uuid_from_memorix': [env, kwrgs['record_uuids'], kwrgs['df_record_uuids'], HOME_REPO]})
      

    try:    
        print("\n\tSTEP 3: EXTERNE DATA UITLEZEN EN DATA NAAR DATAFRAME VERPLAATSEN")

        # Add the provided external data to a dataframe
        kwrgs['df_external_data'], kwrgs['result'] = read_external_data_with_panda(data, **kwrgs)

        print(kwrgs['df_external_data'])

        if kwrgs['result']:
            print('\n----------------------------------------------------------------------------\n\n' +
            f"\tGereed. Er zijn {len(kwrgs['df_external_data'])} rijen opgehaald uit de externe datasheet {data}.\n" +
            '\n----------------------------------------------------------------------------\n\n',
            )
            input('\t\"Verder met uitlezen turtle?\": (Y/N) \n' +
                  '\n----------------------------------------------------------------------------\n\n')
            log.info(f'The external data is transferred to a dataframe at {kwrgs['df_external_data']}')

    except: 
        log.error(f'There was an issue reading the externally added data : {data},\n' +
                  f'and creating the dataframe with it.')
        errors.append({'fn: read_external_data_with_panda': [data]})

    try:   
        print("\n\tSTEP 4: RECORD TURTLE UITLEZEN EN DATA NAAR DATAFRAME VERPLAATSEN")
        
        # Get all records in a turtle, based on UUIDS retrieved in step 2
        kwrgs['predicates'], kwrgs['total_predicates'], kwrgs['result'] = get_turtle_for_record_with_uuid( **kwrgs )
        
        if kwrgs['result']: 
            log.info(f"\t\t, Predicaten uitgelezen : \n\n {kwrgs['predicates']} \n\n {kwrgs['df_record_uuids']} \n\n {kwrgs['records']}")
            print('\n----------------------------------------------------------------------------\n\n' +
                  f"\tGereed. Er zijn '{len(kwrgs['predicates'])}' records uit de turtle gehaald," +
                  f'\ten er zijn \'{len(kwrgs['df_record_uuids'])} uuids uit Memorix gehaald\'\n' +
                  f'\tEr wordt getest met {kwrgs['test_amount']} uuids\n' +
                  f'\tEr zijn {(kwrgs['test_amount'] if kwrgs['test_amount'] >= 0 else len(kwrgs['df_record_uuids'])) - len(kwrgs['predicates'])} rec ords verloren gegaan bij het uitlezen van de data.',
                  '\n----------------------------------------------------------------------------\n\n',
            ) 
            input('\t\"Verder met matchen van de data?\": (Y/N) \n' +
                  '\n----------------------------------------------------------------------------\n\n')
                 
        else:            
            log.error(f"FAIL {kwrgs['df_record_uuids']['uuid']}")
    
    except:
        log.error(f'There was an issue retrieving the turtle for id: {kwrgs['df_record_uuids']['uuid']},\n')               
        errors.append({'fn: get_turtle_for_record_with_uuid': [kwrgs['records'], kwrgs['df_record_uuids'], kwrgs['predicates']]})
        
        
    try:
        print("\n\tSTEP 5 DATA MATCHEN EN TOEVOEGEN AAN DATAFRAME")

        '''Adamlink meenemen. Niet alleen om te kunnen vullen in csv voor manual check, maar ook omdat deze in combinatie met 
        het migratie adresveld al op 7.703.851 locaties_met_adam records is gevuld [ aldus Memorix ] dus deze wil je 
        filteren en niet meenemen in je wijziging'''

        kwrgs['predicates_df'], kwrgs['outliers_df'], kwrgs['result'] = match_data(**kwrgs)

        if kwrgs['result']:
            print(
              '\n----------------------------------------------------------------------------\n\n' +
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

        kwrgs['result'] = upload_data(**kwrgs)

        if kwrgs['result']:
            print(        
                '\n----------------------------------------------------------------------------\n\n' +
                '\t\"We hebben de data gevuld en geupload naar Memorix.\" \n' +
                '\t\"De job is nu afgerond.\"\n' +
                '\n----------------------------------------------------------------------------\n\n'
                )  
            print("\n\tALL STEPS DONE!")

        return kwrgs

    except:
        log.error(f'Something went wrong with matching the data { kwrgs['records'], kwrgs['predicates_df']}')
        errors.append({'fn: upload_data': [kwrgs['records'], kwrgs['predicates_df']]})


   
  
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