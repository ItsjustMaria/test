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

var = {
        # Declare script variables
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

def read_concept_turtle(s, g, s_str, **var):
    
    match = re.search(r'/vocabularies/concepts/([^/>]+)', s_str)
    uuid = match.group(1) if match else ""

    prefLabel = next((str(lab) for lab in g.objects(s, SKOS.prefLabel)), "")
    exactMatch = next((str(em) for em in g.objects(s, SKOS.exactMatch)), "") # <-- fout: want exactMatch kan nu meer dan 1 waarde hebben
    scopeNote = next((str(sn) for sn in g.objects(s, SKOS.scopeNote)), "")

    var['concept_list'].append({
        'concept_uuid' : uuid,
        'streetTextualValue' : prefLabel,
        'adamlink' : exactMatch,
        'scope' : scopeNote
    }) 

    var['total_concept_uuids'].append(uuid)   
    
    return var['concept_list'], var['total_concept_uuids']

# Step 2 function
def retrieve_uuid_from_memorix(**var):
     
    #Read csv from storage location


    var['df_record_uuids'] = pd.DataFrame(var['df_record_uuids'])

    var['result'] = True

    return var['df_record_uuids'], var['result'] 


# Step 3 function
def read_external_data_with_panda(data, **var):
    
    var['result'] = False

    df = pd.read_csv(data, 

    sep=";",             
    dtype={ "straat-label-altlabel": str
        })

    var['df_external_data'] = pd.DataFrame(df)
    
    var['result'] = True
   
    return var['df_external_data'], var['result'] 


# Step 4 function
def get_turtle_for_record_with_uuid(g, index, row, **var):
       
    log.info(f"START {row.uuid}")
    response = api.get_record(row.uuid)
    print(f' This is my uuid{row.uuid}')
    print(f'this is my response {response.status_code}')
    print(response.text)
    input('So far so good?')
    
    if response.status_code != 200:
        log.info("...Try to read again...")
        time.sleep(3)
        response = api.get_record(row.uuid)
        #print(response.text,  file=open(var['records'], 'w', encoding='utf-8'))
        input('I am trying again')
        if response.status_code != 200:
            log.error(f"Reading failed for {row.uuid}")
        else:
            g.parse(data=response.text , format='turtle')

            record_uri = URIRef(f"{PREFIX}/resources/records/{row.uuid}")

            address = g.objects(
                record_uri,
                SAA.isAssociatedWithModernAddress
            )
            #print(address.values)
            var['record'] = {
                  'uuid': row.uuid,
                  'streetTextualValue': str(g.value(address, SAA.streetTextualValue)),
                  'house_number': str(g.value(address, SAA.houseNumber)),
                  'number_add': str(g.value(address, SAA.houseNumberAddition)),
            }
            var['total_records'] += 1
            
            return var['record'], var['total_records']
    

# Step 5 function 
def match_data(**var):

    var['result'] = False    
    # - De turtle predicates omzetten in een dataframe
    var['predicates_df'] = pd.DataFrame(var['predicates'])

    # - De migratie street-string opsplitsen in street nummer, nummertoevoeging
    extract_pattern = var['predicates_df']['streetTextualValue'].str.extract(var['pattern'])

    # - De string onderdelen toevoegen aan het dataframe
    var['predicates_df']['streetTextualValue'] = extract_pattern['street'].str.strip()
    var['predicates_df']['extracted_number'] = extract_pattern['number'].str.strip()
    var['predicates_df']['extracted_number_add'] = extract_pattern['add'].str.strip()

    # Lege velden normaliseren en string 'None' vervangen met NaN
    var['predicates_df'].fillna("",inplace=True)
    var['predicates_df']['house_number'] = var['predicates_df']['house_number'].replace('None', np.nan)
    var['predicates_df']['extracted_number'] = var['predicates_df']['extracted_number'].replace('None', np.nan)
    var['predicates_df']['number_add'] = var['predicates_df']['number_add'].replace('None', np.nan)
    var['predicates_df']['extracted_number_add'] = var['predicates_df']['extracted_number_add'].replace('None', np.nan)
  
    # Outliers naar dataframe omzetten obv index van predicates
    outliers_df = pd.DataFrame(index=var['predicates_df'].index)

    # Uuid overnemen van predicates
    outliers_df['uuid'] = var['predicates_df']['uuid']

    # huisnummers en toevoegingen vullen waar leeg en naar df schrijven indien afwijkend
    street_map = {
                  'house_number': 'extracted_number',
                  'number_add': 'extracted_number_add',
                  'uuid' : 'uuid'
    }
        
    for target, source in street_map.items():

        # masker voor vullen velden indien leeg 
        mask_fill = (
            var['predicates_df'][target].isna() &
            var['predicates_df'][source].notna()
        )
        var['predicates_df'].loc[mask_fill, target] = var['predicates_df'].loc[mask_fill, source]

        # Wegschrijven naar outliers indien data reeds bestaat en afwijkt
        mask_to_csv = (
            var['predicates_df'][target].notna() &
            var['predicates_df'][source].notna() &
            (var['predicates_df'][target] != var['predicates_df'][source])
        )
        outliers_df.loc[mask_to_csv, target] = var['predicates_df'].loc[mask_to_csv, source]
 
    # Dataframe maken van concepten
    concept_df = pd.DataFrame(var['concept_list'], index=range(len(var['concept_list'])))
        
    # concept uuid en adamlink toevoegen aan predicates dataframe obv 'straat' met behulp van een merge         
    merge_concepts = var['predicates_df'].merge(concept_df[['streetTextualValue', 'concept_uuid', 'adamlink']], on = 'streetTextualValue', how='left' )
    var['predicates_df'] = merge_concepts

    # nummer van adamlink afhalen van alternatieve lijst en toevoegen aan kolom altlabel in twee dataframes separaat
    var['predicates_df']['altlabel'] = var['predicates_df']['adamlink'].str.extract(r'(\d+)')
    var['df_external_data']['number_altlabel'] = var['df_external_data']['straat-label-altlabel'].str.extract(r'(\d+)')
    
    # Nummer altlabel tussen dataframes vergelijken en toevoegen aan een lijst
    def find_alternatives(row, df_external_data):    
                
        if pd.notna(row['altlabel']):
            
            # Find all rows in the alternatives dataframe with the same number
            number_to_match = row['altlabel']
            
            alternatives = df_external_data[df_external_data['number_altlabel'] == number_to_match]
            return alternatives['straat-label-altlabel'].tolist()  # Return all matching alternatives
        return []

    # Lijst alternative schrijfwijzen toevoegen aan predicates dataframe
    df_external_data = var['df_external_data']
    predicates_df = var['predicates_df']
    predicates_df['alternative_names'] = predicates_df.apply(find_alternatives, axis=1, args=[df_external_data])
    
    # List alternatieve schrijfwijzen overnemen in outliers obv 'uuid'
    merge_concepts = outliers_df.merge(var['predicates_df'][['uuid', 'alternative_names']], on = 'uuid', how='left' )
    var['outliers_df'] = merge_concepts
    var['predicates_df'] = predicates_df
    var['result'] = True
               
    return var['predicates_df'], var['outliers_df'], var['result'] 

def upload_data():

    for index, row in var['predicates_df'].iterrows():
        print(var['predicates_df']["uuid"].tolist())    
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

        for index, row in var['records'].iterrows():

            if turtle_changed:
                logging.info("Correct")
                turtle = g.serialize(format="turtle")
                #response = api.update_record(row.uuid, turtle)
                print(turtle)
                #if response.status_code == 200:            
                #    logging.info(f"SUCCEED {row.uuid}")     
                #    var['result'] = True   
#
                #else:            
                #    logging.error(f"FAIL {row.uuid}")
                #    logging.error(response.text)

    return var['result']

def main(**var):

    g = Graph()

    try: 
        # Read turtle
        g.parse(var['concept_turtle'], format='turtle') 
        turtle_changed = False

        for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
            s_str = str(s)
    
            var['concept_list'], var['total_concept_uuids'] = read_concept_turtle(s, g, s_str, **var) 
        
        print('\n----------------------------------------------------------------------------\n\n' + f"\tGereed. Er zijn {len(var['concept_list'])} concepten naar een lijst geschreven\'\'\n" + f'\tWe hebben \'{len(var['total_concept_uuids'])}\' concepten uit memorix gehaald. \'\'\n' + f'\tEr zijn {len(var['concept_list']) - len(var['total_concept_uuids'])} concepten verloren gegaan bij het uitlezen van de data\'\'\n' )
        input('\t\"Verder met downloaden record UUIDS?\": (Y/N) \n' + '\n----------------------------------------------------------------------------\n\n')
        log.info(f'Concept turtle downloaded from Memorix and put in list at : {var['total_concept_uuids']} with a total of {var['total_concept_uuids']} concepts')   

    except Exception as e:
        log.info(f'Reading the concept turtle failed {e}')
        log.error(f'fn: read_concept_turtle{[var['concept_turtle'], var['concept_list'], var['total_concept_uuids'], e] }')
        
    try: 
        #Create dataframe from uuids
        var['df_record_uuids'] = pd.read_csv(var['record_uuids'], #response

        sep=";",             
        dtype={ "uuid": str
           })
        
        for index, row in var['df_record_uuids'].head(var['test_amount']).iterrows():

            log.info(f"START {row.uuid}")
            response = api.get_record(row.uuid)
            print(f' This is my uuid{row.uuid}')
            print(f'this is my response {response.status_code}')
            input('So far so good?')
            if response.status_code != 200:
                log.info("...Try to read again...")
                time.sleep(3)
                response = api.get_record(row.uuid)
                #print(response.text,  file=open(var['records'], 'w', encoding='utf-8'))
                input('I am trying again')
            if response.status_code != 200:
                log.error(f"Reading failed for {row.uuid}")
            else:
                g.parse(data=response.text , format='turtle')

                record_uri = URIRef(f"{PREFIX}/resources/records/{row.uuid}")

                address = g.objects(
                    record_uri,
                    SAA.isAssociatedWithModernAddress
                )
                #print(address.values)
                var['record'] = {
                      'uuid': row.uuid,
                      'streetTextualValue': str(g.value(address, SAA.streetTextualValue)),
                      'house_number': str(g.value(address, SAA.houseNumber)),
                      'number_add': str(g.value(address, SAA.houseNumberAddition)),
                }
                var['total_records'] += 1
    
                var['record'], var['total_records'] = get_turtle_for_record_with_uuid(g, index, row, **var )

            print('\n----------------------------------------------------------------------------\n\n' + f"\tGereed. Er zijn '{len(var['predicates'])}' records uit de turtle gehaald," + f'\ten er zijn \'{len(var['df_record_uuids'])} uuids uit Memorix gehaald\'\n' + f'\tEr wordt getest met {var['test_amount']} uuids\n' + f'\tEr zijn {(var['test_amount'] if var['test_amount'] >= 0 else len(var['df_record_uuids'])) - len(var['predicates'])} rec ords verloren gegaan bij het uitlezen van de data.') 
            input('\t\"Verder met matchen van de data?\": (Y/N) \n' + '\n----------------------------------------------------------------------------\n\n')
            log.info(f"\t\t, Predicaten uitgelezen : \n\n {var['predicates']} \n\n {var['df_record_uuids']} \n\n {var['records']}")     
    
    except:
        log.error(f'There was an issue retrieving the turtle for id: {var['df_record_uuids']['uuid']},\n')               
        errors.append({'fn: get_turtle_for_record_with_uuid': [var['records'], var['df_record_uuids'], var['predicates']]})
################################## 




       
        
    '''try:
        print("\n\tSTEP 5 DATA MATCHEN EN TOEVOEGEN AAN DATAFRAME")

        Adamlink meenemen. Niet alleen om te kunnen vullen in csv voor manual check, maar ook omdat deze in combinatie met 
        het migratie adresveld al op 7.703.851 locaties_met_adam records is gevuld [ aldus Memorix ] dus deze wil je 
        filteren en niet meenemen in je wijziging

        var['predicates_df'], var['outliers_df'], var['result'] = match_data(**var)

        if var['result']:
            print(
              '\n----------------------------------------------------------------------------\n\n' +
              f"\tGereed. Er zijn {len(var['predicates_df'])} rijen verwerkt in het predicates dataframe" +
              f'\tEr wordt gewerkt met {(var['test_amount'] if var['test_amount'] >= 0 else len(var['total_predicates']) - len())} records\'\n' +
              f'\tEr wordt getest met {var['test_amount']} uuids\n' +
              f'\tEr zijn {(var['test_amount'] if var['test_amount'] >= 0 else len(var['df_record_uuids'])) - len(var['predicates'])} records verloren gegaan bij het uitlezen van de data.'
              '\t\"Do you want to continue matching the extracted pattern with the concepts?\"\n' +
              '\n----------------------------------------------------------------------------\n\n',
        )

    except:
        log.error(f'There was an issue extracting the pattern from the predicates : {var['predicates']},\n')               
        errors.append({'fn: match_data': [var['predicates']]})
        
    try:
        # STEP 6
        print("\n\tSTEP 6: DATA TERUGZETTEN IN TURTLE EN UPLOADEN")

        var['result'] = upload_data(**var)

        if var['result']:
            print(        
                '\n----------------------------------------------------------------------------\n\n' +
                '\t\"We hebben de data gevuld en geupload naar Memorix.\" \n' +
                '\t\"De job is nu afgerond.\"\n' +
                '\n----------------------------------------------------------------------------\n\n'
                )  
            print("\n\tALL STEPS DONE!")

        return var

    except:
        log.error(f'Something went wrong with matching the data { var['records'], var['predicates_df']}')
        errors.append({'fn: upload_data': [var['records'], var['predicates_df']]})'''


   
  
# -----------------------------------
# EXPORT 
# -----------------------------------



    

    


    

    

    
    
    


    

    

    


'''    var['records'] = fill_data(
        var
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
    main(**var)