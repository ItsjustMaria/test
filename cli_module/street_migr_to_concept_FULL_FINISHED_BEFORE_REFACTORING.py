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
def retrieve_concept_turtle_from_memorix(vocabulair, concept_turtle, concept_list, total_concept_uuids):
    
    # Concept vocabulaire turtle uit memorix halen
    try:
        '''response = api.list_concepts( vocabulair)
        print(response.text,  file=open(concept_turtle, 'w', encoding='utf-8'))'''

        g = rdflib.Graph()
        g.parse(concept_turtle, format="ttl")

  
        for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
            s_str = str(s)

            match = re.search(r'/vocabularies/concepts/([^/>]+)', s_str)
            uuid = match.group(1) if match else ""

            prefLabel = next((str(lab) for lab in g.objects(s, SKOS.prefLabel)), "")
            exactMatch = next((str(em) for em in g.objects(s, SKOS.exactMatch)), "") # <-- fout: want exactMatch kan nu meer dan 1 waarde hebben
            scopeNote = next((str(sn) for sn in g.objects(s, SKOS.scopeNote)), "")

            concept_list.append({
                'concept_uuid' : uuid,
                'streetTextualValue' : prefLabel,
                'adamlink' : exactMatch,
                'scope' : scopeNote
            }) 

            total_concept_uuids.append(uuid)

        print('\n----------------------------------------------------------------------------\n\n' +
              f"\tGereed. Er zijn {len(total_concept_uuids)} UUIDs naar de concept turtle geschreven op locatie: {concept_turtle}" +
              f'\tDit is de lengte van de uitgelezen turtle van de concepten vocabulaire: {len(concept_list)}\n' +
              f'\tEn dit was de lengte van het aantal concepten na downloaden uit memorix: \'{len(total_concept_uuids)}\'\n' +
              f'\tEr zijn {len(concept_list) - len(total_concept_uuids)} concepten verloren gegaan bij het uitlezen van de data')
        
        return concept_list, total_concept_uuids

    except:
        if concept_turtle:
            log.error(f'There was an issue while creating the vocabulair:{vocabulair}' +
                      f'from entrypoint: {env}.' +
                      f'It appears a concept_turtle was created. You should check the file for accuracy.')
        else: 
            log.error(f'The concept_turtle : {concept_turtle} had issues being created at accespoint {env},')
            errors.append({'fn: retrieve_concept_turtle_from_memorix': [vocabulair, concept_turtle, response]})

# Step 2 function
def retrieve_uuid_from_memorix(env, record_uuids):
    
    # Path based on location of script and datafolder
    sys.path.append(str(HOME_REPO))
    import get_uuid  
    try: 
        # Get uuids with query 
        #response = get_uuid.main(env, record_uuids)

        df = pd.read_csv(record_uuids,

            sep=";",             
            dtype={ "uuid": str
               })

        df_record_uuids = pd.DataFrame(df)

        print('\n----------------------------------------------------------------------------\n\n' + 
              f"\tGereed. Er zijn {len(df_record_uuids['uuid'])} record UUIDs opgehaald uit Memorix.\n" + 
              f"\tDeze zijn opgeslagen op locatie: {record_uuids}.\n" +
              f"\tDe uuids zijn eveneens in een variabele opgeslagen als pandas DataFrame")

        return df_record_uuids
    
    except:
        log.error(f'''The uuid's : {record_uuids} had an issue at 
        accespoint {env}.''')
        errors.append({'fn: retrieve_uuid_from_memorix': [env,record_uuids, df, df_record_uuids, HOME_REPO]})


# Step 3 function
def read_external_data_with_panda(data):
    
    try: 
        df = pd.read_csv(data, 

        sep=";",             
        dtype={ "straat-label-altlabel": str
               })

        df_external_data = pd.DataFrame(df)

        print('\n----------------------------------------------------------------------------\n\n' + 
              f"\tGereed. Er zijn {len(df_external_data['street-label-altlabel'])} rijen opgehaald uit de externe datasheet {data}.\n")

    except: 
        log.error(f'There was an issue reading the externally added data : {data},\n' +
                  f'and creating the dataframe with it.')
        errors.append({'fn: read_external_data_with_panda': [data]})
    
    return df_external_data


# Step 4 function
def get_turtle_for_record_with_uuid(df_record_uuids, records, predicates, total_predicates):

    # Check if file already exists and delete based on env
    records_deleted_message = ''
    if os.path.exists(records):     
        #os.remove(records)
        records_deleted_message = (f'\tThere was already a records file present in the \'data\' folder at {records}.\n' +
        '\tTo prevent double data, this file has been deleted\n' +
        '\tA new one has been created in this very function')

    test_amount = 5
    try:
        ##### UNCOMMENT WHEN LIVE ###########
        for index, row in df_record_uuids.head(test_amount).iterrows():
            log.info(f"START {row.uuid}")
            print(row.uuid)

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
        g.parse(records , format='turtle')

        for record in g.subjects(RDF.type, MEMORIX.Record):

            record_uuid = str(record).rsplit('/', 1)[-1]

            address = next(g.objects(
                record,
                SAA.isAssociatedWithModernAddress
            ))

            predicates.append({
                'uuid': record_uuid,
                'streetTextualValue': str(g.value(address, SAA.streetTextualValue)),
                'house_number': str(g.value(address, SAA.houseNumber)),
                'number_add': str(g.value(address, SAA.houseNumberAddition)),
            })

            total_predicates += 1
            


        print('\n----------------------------------------------------------------------------\n\n' +
              f"\tGereed. Er zijn {len(predicates)} records uit de turtle gehaald" +
              f'\tDit was het aantal uuids uit Memorix gehaald: \'{len(df_record_uuids)}\'\n' +
              f'\tEr wordt getest met {test_amount} uuids\n' +
              f'\tEr zijn {(test_amount if test_amount >= 0 else len(df_record_uuids)) - len(predicates)} rec ords verloren gegaan bij het uitlezen van de data.')

        log.info(f"\t\t,  {predicates, df_record_uuids, records}")

        return records, predicates, total_predicates, test_amount
    
    except:
        log.error(f'There was an issue retrieving the turtle for id: {df_record_uuids(row.uuid)},\n')               
        errors.append({'fn: get_turtle_for_record_with_uuid': [records, df_record_uuids, predicates]})

# Step 5 function 
def match_data(pattern,
               predicates, 
               total_predicates, 
               df_external_data, 
               concept_list, 
               test_amount,
               df_record_uuids
               ):

    '''Adamlink meenemen. Niet alleen om te kunnen vullen in csv voor manual check, maar ook omdat deze in combinatie met 
    het migratie adresveld al op 7.703.851 locaties_met_adam records is gevuld [ aldus Memorix ] dus deze wil je 
    filteren en niet meenemen in je wijziging'''
    
    try:

        # - De turtle predicates omzetten in een dataframe
        predicates_df = pd.DataFrame(predicates)

        # - De migratie street-string opsplitsen in street nummer, nummertoevoeging
        extract_pattern = predicates_df['streetTextualValue'].str.extract(pattern)

        # - De string onderdelen toevoegen aan het dataframe
        predicates_df['streetTextualValue'] = extract_pattern['street'].str.strip()
        predicates_df['extracted_number'] = extract_pattern['number'].str.strip()
        predicates_df['extracted_number_add'] = extract_pattern['add'].str.strip()
        
        # Lege velden normaliseren en string 'None' vervangen met NaN
        predicates_df.fillna("",inplace=True)
        predicates_df['house_number'] = predicates_df['house_number'].replace('None', np.nan)
        predicates_df['extracted_number'] = predicates_df['extracted_number'].replace('None', np.nan)
        predicates_df['number_add'] = predicates_df['number_add'].replace('None', np.nan)
        predicates_df['extracted_number_add'] = predicates_df['extracted_number_add'].replace('None', np.nan)
        
        # Outliers naar dataframe omzetten obv index van predicates
        outliers_df = pd.DataFrame(index=predicates_df.index)

        # Uuid overnemen van predicates
        outliers_df['uuid'] = predicates_df['uuid']

        # huisnummers en toevoegingen vullen waar leeg en naar df schrijven indien afwijkend
        street_map = {
                      'house_number': 'extracted_number',
                      'number_add': 'extracted_number_add',
                      'uuid' : 'uuid'
        }
        
        for target, source in street_map.items():

            # masker voor vullen predicatenlijst indien leeg 
            mask_fill = (
                predicates_df[target].isna() &
                predicates_df[source].notna()
            )
            predicates_df.loc[mask_fill, target] = predicates_df.loc[mask_fill, source]

            # Wegschrijven naar outliers indien data reeds bestaat en afwijkt
            mask_to_csv = (
                predicates_df[target].notna() &
                predicates_df[source].notna() &
                (predicates_df[target] != predicates_df[source])
            )
            outliers_df.loc[mask_to_csv, target] = predicates_df.loc[mask_to_csv, source]
        
        # Dataframe maken van concepten
        concept_df = pd.DataFrame(concept_list, index=range(len(concept_list)))
        
        # concept uuid en adamlink toevoegen aan predicates dataframe obv 'straat' met behulp van een merge         
        merge_concepts = predicates_df.merge(concept_df[['streetTextualValue', 'concept_uuid', 'adamlink']], on = 'streetTextualValue', how='left' )
        predicates_df = merge_concepts

        # nummer van adamlink afhalen van alternatieve lijst en toevoegen aan kolom altlabel in twee dataframes separaat
        predicates_df['altlabel'] = predicates_df['adamlink'].str.extract(r'(\d+)')
        df_external_data['number_altlabel'] = df_external_data['straat-label-altlabel'].str.extract(r'(\d+)')

        # Nummer altlabel tussen dataframes vergelijken en toevoegen aan een lijst
        def find_alternatives(row, df_external_data):    
  
            if pd.notna(row['altlabel']):
                # Find all rows in the alternatives dataframe with the same number
                number_to_match = row['altlabel']
                #print(f"Looking for alternatives for number: {number_to_match}")
                alternatives = df_external_data[df_external_data['number_altlabel'] == number_to_match]
                return alternatives['straat-label-altlabel'].tolist()  # Return all matching alternatives
            return []

        # Lijst alternative schrijfwijzen toevoegen aan predicates dataframe
        predicates_df['alternative_names'] = predicates_df.apply(find_alternatives, axis=1, args=[df_external_data])

        # List alternatieve schrijfwijzen overnemen in outliers obv 'uuid'
        merge_concepts = outliers_df.merge(predicates_df[['uuid', 'alternative_names']], on = 'uuid', how='left' )
        outliers_df = merge_concepts

        print(outliers_df)
        
        print('\n----------------------------------------------------------------------------\n\n' +
             f"\tGereed. Er zijn {len(predicates_df)} rijen verwerkt in het predicates dataframe" +
             f'\tEr wordt gewerkt met {(test_amount if test_amount >= 0 else len(total_predicates) - len())} records\'\n' +
              f'\tEr wordt getest met {test_amount} uuids\n' +
              f'\tEr zijn {(test_amount if test_amount >= 0 else len(df_record_uuids)) - len(predicates)} records verloren gegaan bij het uitlezen van de data.')
        
        return predicates_df, outliers_df

    except:
        log.error(f'There was an issue extracting the pattern from the predicates : {predicates},\n')               
        errors.append({'fn: match_data': [predicates]})


# Step 6 function
def fill_data(records, predicates_df, outliers_df, concept_added, adamlink_added, outliers,
        house_number_added,
        house_number_addition_added):

    try:
        g = Graph()
        g.parse(records, format='turtle') 

        for index, row in predicates_df.iterrows():
            print(predicates_df["uuid"].tolist())    
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
                house_number_addition_added = True
            else: 
                log.info(f'Housenumber not changed for uuid: {record_uri}')
            if (address, SAA.houseNumberAddition, None) not in g:
                g.add((address, SAA.houseNumberAddition, Literal(row.number_add)))
                house_number_addition_added = True
            else: 
                log.info(f'Housenumber Addition not changed for uuid: {record_uri}')

            for t in g.triples((address, None, None)):
                print(t)
            for t in g.triples((record_uri, SAA.hasOrHadSubjectLocation, None)):
                print(t)

    except Exception as e:
        print(f"FAILED {row.uuid}: {e}")
        log.error(f'Something went wrong with matching the data { records, predicates_df}')
        errors.append({'fn: fill_data': [records, predicates_df]})

    try:

        g = Graph()
        g.parse(records, format='turtle') 
        for index, row in predicates_df.iterrows():
            if concept_added or adamlink_added or house_number_added or house_number_addition_added:
                    logging.info("Correct")
                    turtle = g.serialize(format="turtle")
                    response = api.update_record(row.uuid, turtle)
                    print('I match and am uploading')
                    if response.status_code == 200:            
                        logging.info(f"SUCCEED {row.uuid}")        
                    else:            
                        logging.error(f"FAIL {row.uuid}")
                        logging.error(response.text)


        outliers_df.to_csv(outliers, sep=';', 
    encoding= 'utf-8',
    index= False, header= True)        
        print(outliers.text)


    except:
        log.error(f"FAILED TRANSFORMATION {records}")



  
# -----------------------------------
# EXPORT 
# -----------------------------------

def main(): 

    # Declare script variables
    concept_list = []
    predicates = []
    extracted = []
    total_concept_uuids = []
    total_record_uuids = []
    total_predicates = 0 
    concept_added = False
    adamlink_added = False
    house_number_added = False
    house_number_addition_added = False

    # User variables
    vocabulair = 'a4863c0c-d9e5-3902-831a-d0960e381a41'  #### !!!! uuid of vocabul air            
    concept_turtle = "data/concept_turtle.ttl"                #### !!!! Location of street turtle
    record_uuids = "data/record_uuids.csv"            #### !!!! Location of deed turtle
    records = 'data/records.ttl'      
    recordia = 'data/recordia.ttl'      
    alternatives = "data/alternatives.csv"            #### !!!! Location of deed turtle
    outliers = "data/outliers.csv"            #### !!!! Location of deed turtle
    pattern = r'^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$'
    ######### SOWIESO WEG ctx.obj["excel_sheet"] = "../data/concept_streets.xlsx"         #### !!!! Location of concepts
    #ctx.obj["data"] = '../data/alternatieve_streetnamen.csv'    #### !!!! Location of adamlinks /alternative names
                    #### !!!! Location of adamlinks /alternative names
    #ctx.obj["log"] = log_setup(logfile)

    print('----------------------------------------------------------------------------\n\n' +
    f"\t\t\"WE ZITTEN IN DE VOLGENDE OMGEVING:\n\n\t\t\t'{env}'\"\n" +
    '\t\"We gaan nu staren met het ophalen van de concept vocabulaire.\" \n')

    user = input('\t\"Wil je starten met het ophalen van deze data?\": (Y/N) \n' +
    '\n----------------------------------------------------------------------------\n\n')
    if user == 'Y':
        pass
    

    print("\n\tSTEP 1: DOWNLOADEN VAN DE DE TURTLE CONCEPT VOCABULAIR")

    # Retrieve a turtle with all concepts based on vocabulair    
    concept_turtle, total_concept_uuids = retrieve_concept_turtle_from_memorix(
        vocabulair, 
        concept_turtle, 
        concept_list, 
        total_concept_uuids
        ) 
    
    print(
    '\n----------------------------------------------------------------------------\n\n' +
    '\t\"Downloaden van de concept turtle is compleet.\" \n' +
    '\t\"Wil je doorgaan met het ophalen van de record uuids uit memorix?\"\n' +
    '\n----------------------------------------------------------------------------\n\n', 
    )
    
    print("\n\tSTEP 2: DOWNLOADING RECORD UUIDS")
    
    # Retrieve record UUIDS from memorix and put in dataframe 
    df_record_uuids = retrieve_uuid_from_memorix(
        env,
        record_uuids
        )
    
    # print(f"Environment: {ctx.obj['env']}")

    print( 
    '\n----------------------------------------------------------------------------\n\n' +
    '\t\"We have the record uuids.\" \n' +
    '\t\"Do you want to continue reading the concept turtle,\n' +
    '\tand create a list of the data?\"\n' +
    '\n----------------------------------------------------------------------------\n\n',   
    )

    print("\n\tSTEP 3: READ EXTERNAL DATASHEET WITH PANDA")
    
    # Add the provided external data to a dataframe
    df_external_data = read_external_data_with_panda( 
        data)
    
    # print(f"Environment: {ctx.obj['env']}")

    print(
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"We have a dataframe of the datasheet you provided.\" \n' +
        '\t\"Do you want to continue retrieving the records based on uuids?\"\n' +
        '\n----------------------------------------------------------------------------\n\n',
    )

    print("\n\tSTEP 4: RECORD TURTLE UITLEZEN EN DATA NAAR DATAFRAME VERPLAATSEN")
    
    # Get all records in a turtle, based on UUIDS retrieved in step 2
    records, predicates, total_predicates, test_amount = get_turtle_for_record_with_uuid( 
        df_record_uuids, 
        records,
        predicates,
        total_predicates
        )
    
    # Check for a deleted file and print message accordingly
    #if records_deleted_message:
    #    print(f'\n----------------------------------------------------------------------------\n {records_deleted_message} \n'
    #    )

    print(        
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"All record uuids now have a full turtle record.\" \n' +
        '\t\"Do you want to continue retrieving the predicates from these records?\"\n' +
        '\n----------------------------------------------------------------------------\n\n',
    )   

    print("\n\tSTEP 5 DATA MATCHEN EN TOEVOEGEN AAN DATAFRAME")

    predicates_df, outliers_df = match_data(
               pattern,
               predicates, 
               total_predicates, 
               df_external_data, 
               concept_list, 
               test_amount,
               df_record_uuids
        )

    print(        
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"We have an extracted list.\"\n' +
        '\t\"Do you want to continue matching the extracted pattern with the concepts?\"\n' +
        '\n----------------------------------------------------------------------------\n\n',
    ) 

    print("\n\tSTEP 6: DATA TERUGZETTEN IN TURTLE")


    records = fill_data(
        records,
        predicates_df,
        outliers_df,
        concept_added,
        adamlink_added,
        outliers,
        house_number_added,
        house_number_addition_added
        )
    
    print(        
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"We hebben de data gevuld.\" \n' +
        '\t\"Wil je verder met uploaden naar Memorix?\"\n' +
        '\n----------------------------------------------------------------------------\n\n'
    )  
    
    print(
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"Upload naar Memorix is gereed.\" \n' +
        '\t\"De job is nu afgerond.\"\n' +
        '\n----------------------------------------------------------------------------\n\n',
    ) 

    print("\n\tSTEP 7: DONE!")





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
    main()