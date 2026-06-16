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
cwd = os.getcwd()  # Get the current working directory (cwd)
files = os.listdir(cwd)  # Get all the files in that directory
#print("Files in %r: %s" % (cwd, files))
#WORK_REPO = Path(r"C:\\Users\\swart053\\Documents\\VSC\\saa-nexus-scripts") # Adjust base path based on location
#HOME_REPO = Path(r"C:\\Users\\swart053\\Documents\\VSC\\test\\cli_module") # Adjust base path based on location
HOME_REPO = Path("/opt/lampp/htdocs/test/cli_module")
WORK_REPO = Path("/opt/lampp/htdocs/saa-nexus-scripts")
sys.path.append(str(WORK_REPO))
from modules import memorix
from modules import saa
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

# Declare global variables
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
# ctx.obj["current_datetime"] = current_datetime
logfile = f'logs/street_migr_to_concept {str(current_datetime)}.log'
errors = []
#PREFIX = 'stadsarchiefamsterdam'
#pattern = re.compile('^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$')

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

# Environment setup

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



# define Namespaces
SAA = Namespace("https://data.archief.amsterdam/ontology#")
RICO = Namespace("https://www.ica.org/standards/RiC/ontology#")
MEMORIX = Namespace("http://memorix.io/ontology#")
DEED = Namespace(f"{PREFIX}/resources/recordtypes/Deed#")
SCHEMA = Namespace(f"http://schema.org/")
SKOS = Namespace(f"http://www.w3.org/2004/02/skos/core#")
DEED = Namespace (f"{PREFIX}/resources/recordtypes/Deed#")
RT = Namespace(f"{PREFIX}/resources/recordtypes")
IMAGE = Namespace(f"https://{PREFIX}.memorix.io/resources/recordtypes/Image#")




'''args = [
    env,
    data,
    predicates,
    extracted,
    total_concept_uuids,
    total_record_uuids,
    total_predicates,
    vocabulair,
    deed,
    record_turtle,
    concept_turtle,
    record_uuids,
    records,
    alternatives,
    outliers,
    pattern
]'''


# -----------------------------------
# FUNCTIONS
# -----------------------------------

# Step 1 function
def retrieve_concept_turtle_from_memorix(vocabulair, concept_turtle, total_concept_uuids):
    

    # Get turtle of concept vocabulaire from Memorix
    try:
        response = api.list_concepts( vocabulair)
        print(response.text,  file=open(concept_turtle, 'w', encoding='utf-8'))

        g = rdflib.Graph()
        g.parse(concept_turtle, format="ttl")

        for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
            s_str = str(s)

            match = re.search(r'/vocabularies/concepts/([^/>]+)', s_str)
            uuid = match.group(1) if match else ""

            total_concept_uuids.append(uuid)
        print('\n----------------------------------------------------------------------------\n\n' + 
              f"\tGereed. Er zijn {len(total_concept_uuids)} UUIDs naar de concept turtle geschreven op locatie: {concept_turtle}")

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

        print('\n----------------------------------------------------------------------------\n\n' + 
              f"\tGereed. Er zijn {len(df_record_uuids['uuid'])} record UUIDs opgehaald uit Memorix.\n" + 
              f"\tDeze zijn opgeslagen op locatie: {record_uuids}.\n" +
              f"\tDe uuids zijn eveneens in een variabele opgeslagen als pandas DataFrame")

        return df_record_uuids
    
    except:
        log.error(f'''The uuid's : {record_uuids} had an issue at 
        accespoint {env}.''')
        errors.append({'fn: retrieve_uuid_from_memorix': [env,record_uuids, response, df, df_record_uuids, HOME_REPO]})


# Step 3 function
def concept_turtle_to_list( concept_turtle, concept_list, total_concept_uuids):
    
    # Load RDF/concept_turtle 
    g = rdflib.Graph()
    g.parse(concept_turtle, format="ttl")

    # Namespace
    SKOS = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")

    try: 
        for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
            s_str = str(s)

            match = re.search(r'/vocabularies/concepts/([^/>]+)', s_str)
            uuid = match.group(1) if match else ""

            prefLabel = next((str(lab) for lab in g.objects(s, SKOS.prefLabel)), "")
            exactMatch = next((str(em) for em in g.objects(s, SKOS.exactMatch)), "") # <-- fout: want exactMatch kan nu meer dan 1 waarde hebben
            scopeNote = next((str(sn) for sn in g.objects(s, SKOS.scopeNote)), "")

            concept_list.append({
                'uuid' : uuid,
                'concept_street' : prefLabel,
                'adamlink' : exactMatch,
                'scope' : scopeNote
            }) 

            # v = concept_list[0]['adamlink']
            # print(type(v), v, math.isnan(v) if isinstance(v, float) else None, v is None, v is np.nan)
        print('\n----------------------------------------------------------------------------\n\n' +
              f'\tDit is de lengte van de uitgelezen turtle van de concepten vocabulaire: {len(concept_list)}\n' +
              f'\tEn dit was de lengte van het aantal concepten na downloaden uit memorix: \'{len(total_concept_uuids)}\'\n' +
              f'\tEr zijn {len(concept_list) - len(total_concept_uuids)} concepten verloren gegaan bij het uitlezen van de data')
        #print(f'\n\"THIS IS THE CONCEPT LIST RETRIEVED FROM MEMORIX::\"\n\n{concept_list}')
        return concept_list
    
    except:
        log.error(f'There was an issue reading the concept turtle : {concept_turtle},\n'
                  f'and creating the concept_list.')
        errors.append({'fn: concept_turtle_to_list': [concept_turtle]})

# Step 4 function
def read_external_data_with_panda(data):
    
    try: 
        df = pd.read_csv(data, 

        sep=";",             
        dtype={ "straat-label-altlabel": str
               })

        df_external_data = pd.DataFrame(df)

        print('\n----------------------------------------------------------------------------\n\n' + 
              f"\tGereed. Er zijn {len(df_external_data['straat-label-altlabel'])} rijen opgehaald uit de externe datasheet {data}.\n")

    except: 
        log.error(f'There was an issue reading the externally added data : {data},\n' +
                  f'and creating the dataframe with it.')
        errors.append({'fn: read_external_data_with_panda': [data]})
    
    return df_external_data

# Step 5 function
def get_records_from_uuid_csv(df_record_uuids, records, predicates, total_predicates):

    # Check if file already exists and delete based on env
    '''
    records_deleted_message = ''
    if os.path.exists(records):
        pass
        
        os.remove(records)
        records_deleted_message = (f'\tThere was already a records file present in the \'data\' folder at {records}.\n' +
        '\tTo prevent double data, this file has been deleted\n' +
        '\tA new one has been created in this very function')
    else :

        for uuid in df_record_uuids["uuid"].iloc[:test_amount]:
            response = api.get_record(uuid)
            print(response.text, file=open(records, 'a', encoding='utf-8')) 
            #record_block = URIRef(f"{PREFIX}/resources/records/{uuid}")
            if response.status_code != 200:
                log.info("...Try to read again...")
                time.sleep(3)
                response = api.get_record(uuid)
                if response.status_code != 200:
                    log.error(f"Reading failed for {uuid}")
            
            yes = input(' Switch records data and we need a yes to go on. (Y/N) : ')
            if yes == 'Y':
                continue'''

    test_amount = 5
    try:
        g = Graph()
        g.parse(records, format='turtle')                    
        
        for record in g.subjects(RDF.type,MEMORIX.Record):
            record_uuid = str(record).split('/')[-1] 
            for inst in g.objects(record, SAA.isAssociatedWithModernAddress ):

                ## IF NOT AVAILABLE OR IF EMPTY?? 
                migrationAddress = next(g.objects(None, SAA['isAssociatedWithModernAddress']), None)       
                houseNumber = str(g.value(subject=inst, predicate=SAA['houseNumber']))
                numberAddition = str(g.value(subject=inst, predicate=SAA['houseNumberAddition']))
                street = str(g.value(subject=inst, predicate=SAA['street']))
                streetTextualValue = str(g.value(subject=inst, predicate=SAA['streetTextualValue']))
                adamlink = str(g.value(subject=inst, predicate=SAA['hasOrHadSubjectLocation']))
                predicates.append({
                    'uuid' : record_uuid,
                    'houseNumber' : houseNumber, 
                    'numberAddition' : numberAddition, 
                    'street' : street,
                    'streetTextualValue' : streetTextualValue,
                    'adamlink' : adamlink
                })
            
            total_predicates.append(record_uuid)  

        print(f'These are the predicates:\n\n {predicates}')

        print('\n----------------------------------------------------------------------------\n\n' +
          f'\tEr zijn: {len(total_predicates)} records opgehaald uit Memorix met behulp van de eerder verkregen uuids\n' +
          f'\tDe lengte van het originele bestand bedraagt: \'{len(df_record_uuids['uuid'])}\'\n' +
          f'\tEr wordt getest met {test_amount if env == 'acc' else 0} records\n' +
          f'\tEr zijn {(test_amount if env == 'acc' else len(df_record_uuids['uuid'])) - len(total_predicates)} records verloren gegaan,\n' +
          f'\tbij het ophalen van de uuids uit Memorix')  
        
        return records #, records_deleted_message
                
    except:
        log.error(f"FAILED TO GET RECORD FOR UUID: {df_record_uuids}")
    
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

### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! HELPER FUNCTIONS. USE THEM ELSEWHERE OR DELETE !!!!!!!!!!!!! 

# Helper function 2 Step 6 
def add_number_addition_to_address_block(inst, row, g, number_addition_added, error_list):
    if (inst, SAA['houseNumberAddition'], Literal(row.numberAddition)) not in g:
        g.add((inst, SAA['houseNumberAddition'], Literal(row.numberAddition)))
        logging.info("Number addition added")
        number_addition_added = True
        g, error_list = add_number_addition_to_address_block(inst, row, g, error_list)
    else:
        logging.info(f"Number addition already in block")
        number_addition_added = True
    return g, number_addition_added, error_list

# Helper function 3 Step 6 
def add_adamlink_to_address(inst, row, g, number_addition_added, error_list):
    if (inst, SAA['houseNumberAddition'], Literal(row.numberAddition)) not in g:
        g.add((inst, SAA['houseNumberAddition'], Literal(row.numberAddition)))
        logging.info("Number addition added")
        number_addition_added = True
        g, error_list = add_number_addition_to_address_block(inst, row, g, error_list)
    else:
        logging.info(f"Number addition already in block")
        number_addition_added = True
    return g, number_addition_added, error_list

# Helper function 3 Step 6 
def extract_street(inst, row, g, number_addition_added, error_list):
    if (inst, SAA['houseNumberAddition'], Literal(row.numberAddition)) not in g:
        g.add((inst, SAA['houseNumberAddition'], Literal(row.numberAddition)))
        logging.info("Number addition added")
        number_addition_added = True
        g, error_list = add_number_addition_to_address_block(inst, row, g, error_list)
    else:
        logging.info(f"Number addition already in block")
        number_addition_added = True
    return g, number_addition_added, error_list

### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! END HELPER FUNCTIONS.  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 


'''# Step 6 function
def get_predicates(records, predicates, total_record_uuids, total_predicates):
    
    Adamlink meenemen. Niet alleen om te kunnen vullen, maar ook omdat deze in combinatie met 
       het migratie adresveld al op 85.034 records is gevuld [ aldus Memorix ] dus deze wil je 
       filteren en niet meenemen in je wijziging

    input(' We are ate 6. CHeck the output or abort (Y/N): ')
    try:
        # Working with the records
        g = Graph()
        g.parse(records, format='turtle')
        
        for s, p, o in g:
            print(f"Subject = {s} \n Object  = {p}\n Predicate = {o}\n")

        for record in g.subjects(RDF.type, MEMORIX.Record):
            uuid = str(record).split('/')[-1] 
            print(f'I am printing the uuid here : {uuid}')

            for inst in g.objects(record, SAA.isAssociatedWithModernAddress ):
                
                print(inst)
                houseNumber = str(g.value(subject=inst, predicate=SAA['houseNumber']))
                numberAddition = str(g.value(subject=inst, predicate=SAA['houseNumberAddition']))
                street = str(g.value(subject=inst, predicate=SAA['street']))
                streetTextualValue = str(g.value(subject=inst, predicate=SAA['streetTextualValue']))
                adamlink = str(g.value(subject=inst, predicate=SAA['hasOrHadSubjectLocation']))

                predicates.append({
                    'uuid' : uuid,
                    'houseNumber' : houseNumber, 
                    'numberAddition' : numberAddition, 
                    'street' : street,
                    'streetTextualValue' : streetTextualValue,
                    'adamlink' : adamlink
                    })
                
                total_predicates.append(uuid)    

            print('\n----------------------------------------------------------------------------\n\n' +
              f'\tEr zijn nu: {len(total_predicates)} predicaten uitgelezen uit de records turtle\n' +
              f'\tHet aantal records waar in deze run mee wordt gewerkt bedraagt: {len(total_record_uuids)}\n' +
              f'\tEr zijn {(len(total_record_uuids)) - len(total_predicates)} records verloren gegaan,\n') 
        
            #print(f'\n\"THESE ARE THE PREDICATES RETRIEVED FROM THE TURTLE:\"\n\n{predicates}')

            return predicates
        
    except:
        log.error(f'There was an issue creating the predicate list for the records: {records},\n')               
        errors.append({'fn: get_predicates': [records]})'''

# Step 6 extract pattern
def extract_pattern(pattern, predicates, extracted):
    
    '''Adamlink meenemen. Niet alleen om te kunnen vullen, maar ook omdat deze in combinatie met 
    het migratie adresveld al op 85.034 records is gevuld [ aldus Memorix ] dus deze wil je 
    filteren en niet meenemen in je wijziging'''

    try:
        
        predicates_df = pd.DataFrame(predicates)
        extract_pattern = predicates_df['streetTextualValue'].str.extract(pattern)
        print(extract_pattern)

        predicates_df['extracted_street'] = extract_pattern['street'].str.strip()
        predicates_df['extracted_number'] = extract_pattern['number'].str.strip()
        predicates_df['extracted_number_add'] = extract_pattern['add'].str.strip()
        print(predicates_df['extracted_number'])
        extracted = predicates_df.to_dict(orient='records')
        #    record = {
        #    'uuid' : predicates_df['uuid'],
        #    'street' : predicates_df['street'],
        #    'houseNumber' : predicates_df['houseNumber'], 
        #    'numberAddition' : predicates_df['numberAddition'], 
        #    'streetTextualValue' : predicates_df['streetTextualValue'],
        #    'adamlink' : predicates_df['adamlink'],
#
        #
        #    extracted.append(record)
        #
        print(f'\n \"THIS IS THE LIST OF EXTRACTED STREETS:\"\n\n{extracted}')
        return extracted
    
    except:
        log.error(f'There was an issue extracting the pattern from the predicates : {predicates},\n')               
        errors.append({'fn: extract_pattern': [predicates]})

# Step 8 function
def match_data(concept_list, extracted):

    try:
        print ('I do get in the eight')
        concept_df = pd.DataFrame(concept_list, index=range(len(concept_list)))

        extracted_df = pd.DataFrame(extracted, index=range(len(extracted)))

        # Match if not empty and if not equal to already present data
        different_nr = extracted_df['extracted_number'].notna() & extracted_df['houseNumber'].notna() & (extracted_df['extracted_number'] != extracted_df['houseNumber'])
        different_add = extracted_df['extracted_number_add'].notna() & extracted_df['numberAddition'].notna() & (extracted_df['extracted_number_add'] != extracted_df['numberAddition'])
        
        # Match if not equal or if not empty string
        new_nr = (extracted_df['houseNumber'] != extracted_df['extr_nr']) | (extracted_df['houseNumber'] == '')
        new_add = (extracted_df['numberAddition'] != extracted_df['extracted_number_add']) | (extracted_df['numberAddition'] == '')

        print(different_add, different_nr, new_nr, new_add)

        matches = []
        match = ''
        '''# loop through data with progress meter
        for index, row in tqdm(concept_list.iterrows(), total=concept_list.shape[0]):
            logging.info(f"START {row.uuid}")
            logging.info(f"Find street and adamlink: ({concept_list['concept_street']}, {row.adamlink})")
            # concept_uuid = row.uuid
            # concept_street = row.prefLabel
            # adamlink = row.exactMatch
            # print(f'Printing rows of our dataframe. UUID : {row.uuid} street: {row.prefLabel} adamlink: {row.exactMatch}')'''

        '''        # loop through data with progress meter
        for index, row in tqdm(concept_df.iterrows(), total=concept_df.shape[0]):
            logging.info(f"START {row.uuid}")

            print('in the first loop')

            for index, row in tqdm(extracted_df.iterrows(), total=extracted_df.shape[0]):
                logging.info(f"Compare streetnames between concepts and migrated streets: ({concept_df['concept_street']}, {extracted['street']})")

                print('in the second loop')
                if extracted_street == concept_street:
                    match ='yes'
                    print('In th yes match')
                else: 
                    match = 'no'
                    print('in the no match')
                
                matches.append({{concept_df['uuid'],extracted_df['extracted_street']} : f'This is {match} match'})
                print(matches)'''
            # concept_uuid = row.uuid
            # concept_street = row.prefLabel
            # adamlink = row.exactMatch
            # print(f'Printing rows of our dataframe. UUID : {row.uuid} street: {row.prefLabel} adamlink: {row.exactMatch}')
           

                #uuid, houseNumber, numberAddition, streetTextualValue = get_value_from_records(record, inst, g)
                #print(streetTextualValue)

                #if numberAddition is None and houseNumber  
                #for s in streetTextualValue
                #extracted = streetTextualValue.str.extract(pattern)
                # street_name = extracted['street'].str.strip()
                # house_number  = extracted['number'].str.strip()
                # addition = extracted['add'].str.strip()
        
                #extracted = streetTextualValue.str.extract(pattern)
                #print(f'I am after the extracted {extracted}')
                #print(f'THIS IS THE EXTRACTED PATTERN: {extracted}')

            #if any(g.triples((deed, SAA['isAssociatedWithModernAddress'], None))):
            #    print(f'This is inside the triples') # This is my saa: textualvalue: {SAA['streetTextualValue']} for the deed: {deed}')  ##### WE ARE HERE !!!!!!!!!##########
            #    print(records)
        return concept_df, extracted_df
        
    except:
        log.error(f'Something went wrong with matching the data {concept_list, extracted}')
        errors.append({'fn: match_data': [concept_list, extracted]})


# Step 7 function
def alter_shit(records, houseNumber, numberAddition, migrate_street, record, uuid, alternatives, outliers, df_external_data, concept_list):

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
# EXPORT 
# -----------------------------------

def main(): 

    # Declare script variables
    concept_list = []
    predicates = []
    extracted = []
    total_concept_uuids = []
    total_record_uuids = []
    total_predicates = [] 

    # User variables
    vocabulair = 'a4863c0c-d9e5-3902-831a-d0960e381a41'  #### !!!! uuid of vocabul air            
    deed = 'Deed'                                        #### !!!! Recordname for turtle
    record_turtle = "data/record_turtle.ttl"                 #### !!!! Location of deed turtle
    concept_turtle = "data/concept_turtle.ttl"                #### !!!! Location of street turtle
    record_uuids = "data/record_uuids.csv"            #### !!!! Location of deed turtle
    records = 'data/records.ttl'      
    alternatives = "data/alternatives.csv"            #### !!!! Location of deed turtle
    outliers = "data/outliers.csv"            #### !!!! Location of deed turtle
    pattern = r'^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$'
    ######### SOWIESO WEG ctx.obj["excel_sheet"] = "../data/concept_streets.xlsx"         #### !!!! Location of concepts
    #ctx.obj["data"] = '../data/alternatieve_straatnamen.csv'    #### !!!! Location of adamlinks /alternative names
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
    concept_turtle = retrieve_concept_turtle_from_memorix(
        vocabulair, 
        concept_turtle,
        total_concept_uuids
        ) 
    
    print(
    '\n----------------------------------------------------------------------------\n\n' +
    '\t\"Downloaden van de concept turtle is compleet.\" \n' +
    '\t\"Wil je doorgaan met het ophalen van de record uuids uit memorix?\"\n' +
    '\n----------------------------------------------------------------------------\n\n', 
    )
    
    print("\n\tSTEP 3: READ TURTLE AND PUT IN LIST")
    
    # Read the concept turtle retrieved in step 1 and append data to list variable
    concept_list = concept_turtle_to_list( 
        concept_turtle,
        concept_list,
        total_concept_uuids
    )

    print(
    '\n----------------------------------------------------------------------------\n\n' +
    '\t\"The concept turtle is appended to a list.\"\n' +
    '\t\" Do you want to continue reading the datasheet you provided?\"\n' +
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

    print("\n\tSTEP 4: READ EXTERNAL DATASHEET WITH PANDA")
    
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

    print("\n\tSTEP 5: HERE WE START WITH GETTING A RECORD TURTLE")
    
    # Get all records in a turtle, based on UUIDS retrieved in step 2
    records = get_records_from_uuid_csv( 
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

    print("\n\tSTEP 6 WE ARE RETRIEVING THE PREDICATES FROM THE TURTLE")

    #predicates = get_predicates(
    #    records, 
    #    predicates,
    #    total_record_uuids,
    #    total_predicates
    #    )
    #
    #print(        
    #    '\n----------------------------------------------------------------------------\n\n' +
    #    '\t\"We have a predicate list.\" \n' +
    #    '\t\"Do you want to continue extracting the pattern from the given predicate?\"\n' +
    #    '\n----------------------------------------------------------------------------\n\n'
    #)   

    print("\n\tSTEP 6: WE ARE NOW EXTRACTING THE STREET FROM THE PREDICATE")

    extracted = extract_pattern(
        pattern,
        predicates,
        extracted
        )

    print(        
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"We have an extracted list.\"\n' +
        '\t\"Do you want to continue matching the extracted pattern with the concepts?\"\n' +
        '\n----------------------------------------------------------------------------\n\n',
    ) 

    print("\n\tSTEP 8: WE ARE NOW ALTERING THE RECORDS")

    records = match_data(
        concept_list, 
        extracted
        )
    
    # ctx.obj['graph'] = g
    # ctx.obj['houseNumber'] = houseNumber
    # ctx.obj['numberAddition'] = numberAddition
    # ctx.obj['migrate_street'] = streetTextualValue
    # ctx.obj['record'] = record
    # ctx.obj['uuid'] = uuid 
    # ctx.obj['alternatives'] = alternatives
    # ctx.obj['outliers'] = outliers
    #ctx.obj['pattern'] = pattern
    
#    print(f"Environment: {ctx.obj['env']}")

    print(
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"The records are altered.\" \n' +
        '\t\"Do you want to continue with ... YESS??? Here we do some more shit\"\n' +
        '\n----------------------------------------------------------------------------\n\n',
    ) 

    print("\n\tSTEP 7: TRYING TO ALTER SHIT WITH WHAT WE GOT")

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
    #    alternatives,
    #    outliers
    #     )
       
    # print(f"Environment: {ctx.obj['env']}")

    print(
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"The bla bla bla.\"\n' +
        '\t\"Do you want to continue with ... YESS??? ?\"\n' +
        '\n----------------------------------------------------------------------------\n\n'
    ) 

    print("\n\tSTEP 8: NOT HERE YET")

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
    # print(f"Environment: {ctx.obj['env']}")

    print(
        '\n----------------------------------------------------------------------------\n\n' +
        '\t\"The bla bla bla.\"\n' +
        '\t\"Do you want to continue with ... YESS??? ?\"\n' +
        '\n----------------------------------------------------------------------------\n\n'
    ) 

    print('Done!')





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