## Import libraries
import os 
import sys
import tracemalloc
import Levenshtein
import simplejson as json
from datetime import time, datetime
from tqdm import tqdm
from uuid import uuid4
import pandas as pd
import re
import rdflib
from rdflib import Graph, URIRef, Literal, Namespace, RDF, BNode, XSD
from collections import defaultdict
import logging
from rapidfuzz import fuzz
from pathlib import Path
import math, numpy as np
import time
#WORK_REPO = Path(r"C:\\Users\\swart053\\Documents\\VSC\\saa-nexus-scripts") # Adjust base path based on location
#HOME_REPO = Path(r"C:\\Users\\swart053\\Documents\\VSC\\test\\cli_module") # Adjust base path based on location
HOME_REPO = Path("/opt/lampp/htdocs/test/street_to_concept")
WORK_REPO = Path("/opt/lampp/htdocs/saa-nexus-scripts")
sys.path.append(str(WORK_REPO))
from modules import memorix
from modules import saa
from modules import saa_rdf as nrdf
PREFIX = 'stadsarchief'

'''
   Script for updating migration street fields [CAN BE ADJUSTED] to a concept URI
   various other data is needed; 
   * sys.argv[1] adamlink alternative streetnames [HEADER NEEDS TO BE ALTERED]
   * On file concept turtle
   * On file list of uuids

   Scripts used are: 
   * saa-memorix-nexus/scripts/generic/get_uuids_for_query_to_csv.py
   
   Modules used are:
   * def list_concepts(self, uuid):
   * def get_record(self, uuid, options = {}):
   * def update_record(self, uuid, turtle):

   
   #### !!!! Alteration options mentioned like this  

   This script does in order:  
   1) CONFIRM ENVIRONMENT
   2) Setup logging and create directories if needed
   3) Read concept turtle and store needed predicates
   4) Create fataframes from all files
   5) Normalize street from concept and add it to data
   6) Retrieve record from Memorix with a single uuid per iteration
   7) Read record turtle and store needed predicates and dismember the migrant street
   8) Normalise street to remove quotations and 1, 2, 3 for First, Second, Third etc.. 
   9) Look through file adamlink for alternative ways of writing and link any found adamlink to predicates df
   10) Find concept uuid by mapping all found alternative ways of writing  
   11) Add adamlink location number and add alternative names to outfile
   12) Merge dataframes based on adamlink location number for extra concept verification
   13) Write weird stuff to an out_csv
   14) Validate concept is not empty or None and fill concept uuid
   15) Validate housenumber is not empty and fill housenumber
   16) Validate housenumber addition is not empty and fill housenumber addition
   17) Upload altered turtle with a single uuid per iteration

   Output files are: 
   * logs/migr_street_to_concept_{current_date}.log
   * error/errors.csv
   * data/outliers.csv


   Export UUID's from Memorix using APi. Use these to get information from Memorix. 
   Alter this information. Upload this information back to Memorix.
   
   Call script with CLI and the environment option:

   'python this_script.py pipeline --env [specify tst /acc /prod] data_to_be_used.ext'
'''

# Script variables
errors = []
outliers = []
concept_list =  []
total_concept_uuids = []
test_amount = 5

# User variables
vocabulair = 'a4863c0c-d9e5-3902-831a-d0960e381a41'        #### !!!! uuid of vocabulair            
concept_turtle = r"data/concept_turtle.ttl"                #### !!!! Location of street turtle
record_uuids = r"data/uuids.csv"                           #### !!!! Location of uuid from memorixalternatives = r"data/alternatives.csv",               #### !!!! Location of external csv
outliers_csv = r"files/outliers.csv"                        #### !!!! Location of output outliers
pattern = r'^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$'

def setup_logging():
    
    log = logging.getLogger("organizer")
    log.setLevel(logging.DEBUG)

    if not os.path.exists("./files"):     
        os.makedirs("./files")
        log.info(f'Files directory did not exist. Logs directory created') 
    if not os.path.exists("./files/logs"):     
        os.makedirs("./files/logs")
        log.info(f'Error directory did not exist. Error directory created') 
    if not os.path.exists("./files/error"):     
        os.makedirs("./files/error")
        log.info(f'Error directory did not exist. Error directory created') 

    current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    logfile = f'files/logs/migr_street_to_concept {str(current_datetime)}.log'

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    log.addHandler(fh)
    log.addHandler(ch)

    return log



def error_logging(errors):
    
    for item in errors:

        df = pd.DataFrame(item)
        df.to_csv(
            f"files/error/errors.csv",
        mode="a",
        index=False
        )
        

    errors = []


#total_record_uuids = [] 

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

# Namespaces 
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

def read_concept_turtle(s, g, s_str):
    
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
    return concept_list, total_concept_uuids
    

def extract_street(inst, g, uuid, predicates, pattern ):
    
    predicates.append({
          'uuid': uuid,
          'streetTextualValue': str(g.value(inst, SAA.streetTextualValue)),
          'house_number': str(g.value(inst, SAA.houseNumber)),
          'number_add': str(g.value(inst, SAA.houseNumberAddition)),
          'street' : str(g.value(inst, SAA.street)),
          'adamlink' : str(g.value(None, SAA.hasOrHadSubjectLocation))
    })
    
    # Turn turtle predicates to dataframe
    predicates_df = pd.DataFrame(predicates)

    predicates_df = predicates_df.fillna('')
    predicates_df = predicates_df.replace('None', '')
    
    # Migration street extraction in street number, number addition
    extract_pattern = predicates_df['streetTextualValue'].str.extract(pattern)

    # Add string parts to dataframe
    predicates_df['extracted_street'] = extract_pattern['street'].str.strip()
    predicates_df['extracted_number'] = extract_pattern['number'].str.strip()
    predicates_df['extracted_add'] = extract_pattern['add'].str.strip()

    # Normalize empty fields and replace string 'None' with empty string
    predicates_df.fillna("",inplace=True)
    predicates_df['house_number'] = predicates_df['house_number'].replace('None', '')
    predicates_df['extracted_number'] = predicates_df['extracted_number'].replace('None', '')
    predicates_df['number_add'] = predicates_df['number_add'].replace('None', '')
    predicates_df['extracted_add'] = predicates_df['extracted_add'].replace('None', '')
    
    return predicates_df, predicates_df.streetTextualValue, predicates_df.street, predicates_df.house_number, predicates_df.number_add, predicates_df.adamlink

def normalize_street_name(street):
    
    replacement = {
        r'\b1\b|\b1e\b|\b1ste\b': "Eerste",
        r'\b2\b|\b2e\b|\b2de\b' : "Tweede",
        r'\b3\b|\b3e\b|\b3de\b': "Derde",
        r'\b4\b|\b4e\b|\b4de\b': "Vierde"
    }

    # Remove punctuation
    street = re.sub(r'[^\w\s]', '', street)

    # Normalize abbreviations for numbered streets 
    for pattern, replacement in replacement.items():
        street = re.sub(pattern, replacement, street) 

    # Extra whitespace removal
    street = ' '.join(street.split()).lower()

    return street

def fuzzy_match(query, candidates, threshold=0.95):

    best_match = None
    best_ratio = 0
    for candidate in candidates:
        if pd.notna(candidate) and isinstance(candidate, str):
            ratio = fuzz.ratio(query, candidate) / 100
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = candidate
    if best_ratio >= threshold:
        return best_match
    
    return None

def get_fuzzy_adamlink(street, adamlink_to_streets, threshold=0.85):
    for adamlink, candidates in adamlink_to_streets.items():
        match = fuzzy_match(street, candidates, threshold)
        if match is not None:
            return adamlink
    return None

def find_adamlink(predicates_df, external_df):

    # map all adamlinks to streetnames
    all_street_names = []
    for _, row in external_df.iterrows():
        adamlink = row['adamlink']
        if pd.notna(adamlink):
            conventional = row['streetTextualValue']
            alternative = row['altlabel']
            if pd.notna(conventional) and isinstance(conventional, str):
                all_street_names.append((conventional, adamlink))
            if pd.notna(alternative):
                if isinstance(alternative, str):
                    all_street_names.append((alternative, adamlink))
                elif isinstance(alternative, (list, tuple, set)):
                    for alt in alternative:
                        if pd.notna(alt) and isinstance(alt, str):
                            all_street_names.append((alt, adamlink))

    # Group all street names by their adamlink
    adamlink_to_streets = defaultdict(list)
    for street, adamlink in all_street_names:
        adamlink_to_streets[adamlink].append(street)

    # apply new column to predicates df with adamlink based on match in streetTextualValue  with fuzzymatch applied
    predicates_df['new_adamlink'] = predicates_df['streetTextualValue'].apply(
        lambda s: get_fuzzy_adamlink(s, adamlink_to_streets, threshold=0.95)
    )

    # apply new column to predicates df with adamlink based on match in normalized_street  with fuzzymatch applied
    second_column = 'normalized_street'
    mask = predicates_df['new_adamlink'].isna()
    predicates_df.loc[mask, 'new_adamlink'] = predicates_df.loc[mask, second_column].apply(
    lambda s: get_fuzzy_adamlink(s, adamlink_to_streets, threshold=0.95)
    )

    return predicates_df

def find_concept_uuid(concept_df, predicates_df):

    # map all concept_uuids to streetnames
    all_street_names = []
    for _, row in concept_df.iterrows():
        concept_uuid = str(row['concept_uuid'])
        #print(f'I\m the uuid: {concept_uuid}')
        if pd.notna(concept_uuid):
            conventional = row['streetTextualValue']
            alternative_column = row['normalized_street']
            if pd.notna(conventional) and isinstance(conventional, str):
                all_street_names.append((conventional, concept_uuid))
            if pd.notna(alternative_column):
                if isinstance(alternative_column, str):
                    all_street_names.append((alternative_column, concept_uuid))
                elif isinstance(alternative_column, (list, tuple, set)):
                    for alt in alternative_column:
                        if pd.notna(alt) and isinstance(alt, str):
                            all_street_names.append((alt, concept_uuid))

    # Group all street names by their concept_uuid
    concept_uuid_to_street = defaultdict(list)
    for street, concept_uuid in all_street_names:
        concept_uuid_to_street[concept_uuid].append(street)
            
    # apply new column to predicates df with concept_uuid based on match in streetTextualValue with fuzzymatch applied
    predicates_df['new_concept_uuid'] = predicates_df['streetTextualValue'].apply(
        lambda s: get_fuzzy_adamlink(s, concept_uuid_to_street, threshold=0.95)
    )
    
    # apply new column to predicates df with concept_uuid based on match in normalized_street  with fuzzymatch applied
    second_column = 'normalized_street'
    mask = predicates_df['new_concept_uuid'].isna()
    predicates_df.loc[mask, 'new_concept_uuid'] = predicates_df.loc[mask, second_column].apply(
    lambda s: get_fuzzy_adamlink(s, concept_uuid_to_street, threshold=0.95)
    )
    
    return concept_df, predicates_df, predicates_df['new_concept_uuid']

def add_adamlink_location_number(predicates_df, concept_df, external_df):

    # Remove number from adammlink and add to column in three dataframes 
    concept_df['alt_number'] = concept_df['adamlink'].str.extract(r'(\d+)') 
    external_df['alt_number'] = external_df['adamlink'].str.extract(r'(\d+)')
    predicates_df['alt_number'] = predicates_df['new_adamlink'].str.extract(r'(\d+)')

    # Compare number altlabel between dataframes and add to a list
    def find_alternatives(row, external_df):    
                
        if pd.notna(row['alt_number']):
            
            # Find all rows in the alternatives dataframe with the same number
            number_to_match = row['alt_number']
            
            alternatives = external_df[external_df['alt_number'] == number_to_match]
            altlabel = alternatives['altlabel'].tolist()
            label = alternatives['streetTextualValue'].tolist()
            merged_labels = altlabel + label
             
            return merged_labels  # Return all matching alternatives
        return []

    # Add list alternative writings to predicates dataframe
    concept_df['alternative_names'] = concept_df.apply(find_alternatives, axis=1, args=[external_df])
    
    return concept_df,  predicates_df

def merge_dataframes(concept_df, predicates_df):

    # merge dfs to accumulate all data  
    merge_dfs = predicates_df.merge(concept_df[['alt_number', 'concept_uuid', 'alternative_names']], on = 'alt_number', how='left')
    new_merge = merge_dfs.drop_duplicates(subset=['uuid'])

    # Assign working name to merged dataframe            
    predicates_df = new_merge

    return new_merge, predicates_df

def outliers_to_csv(new_merge, first):

    # Outliers to dataframe based on index predicates
    outliers_df = pd.DataFrame( index=new_merge.index)
    outliers_df['uuid'] = new_merge['uuid']

    # Fill numbers etc where deviant
    street_map = {
                  'house_number': 'extracted_number',
                  'number_add': 'extracted_add',
    }
        
    for target, source in street_map.items():

        # mask to fill empty fields 
        mask_fill = (
            new_merge[target].isna() &
            new_merge[source].notna()
        )
        outliers_df.loc[mask_fill, target] = new_merge.loc[mask_fill, source]
        
        # Write to outliers if data already exists
        mask_to_csv = (
            new_merge[target].notna() &
            new_merge[source].notna() &
            (new_merge[target] != new_merge[source])
        )
        outliers_df.loc[mask_to_csv, target] = new_merge.loc[mask_to_csv, source]

        merge_concepts = outliers_df.merge(new_merge[['uuid', 'alternative_names']], on = 'uuid', how='left' )
        outliers_df = merge_concepts

    outliers_df.to_csv(
        f"files/outliers.csv",
        mode="w" if first else "a",
        header=first,
        index=False
        
    )
    first = False


    return outliers_df, first


def main():

    log = setup_logging()
    log.info(f'ENVIRONMENT: {env}') 
    input('\t\"COLLECT AND ANALYZE DATA NOW?\": (Y/N)')

    count = 0
    g = Graph()
    first = True

    try: 
        # Read concept turtle and put in list
        g.parse(concept_turtle, format='turtle') 
        turtle_changed = False

        for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
            s_str = str(s)
    
            concept_list, total_concept_uuids = read_concept_turtle(s, g, s_str) 

        log.info(f'A total of : {len(concept_list)} are stored in a list. There are : {len(total_concept_uuids)} concepts retrieved from the turtle. There have been : {len(concept_list) - len(total_concept_uuids)} losses during data extraction.')
        log.info(f'Concept turtle put in list with a total of {len(total_concept_uuids)} concepts')   

    except Exception as e:
        log.info(f'Reading the concept turtle failed {e}')
        log.error(f'fn: read_concept_turtle{[concept_turtle, concept_list, total_concept_uuids, e] }')
        errors.append(('Error while reading concept turtle', e))
        error_logging(errors)
        
    try: 
        #Creating dataframes
        df_record_uuids = pd.read_csv(record_uuids, 
        sep=";",             
        dtype={ "uuid": str
           })


        df_data = pd.read_csv(data, 
            sep=",",             
            dtype={ "adamlink": str,
                   'streetTextualValue' : str,
                   'altlabel' : str,
                })

        external_df = pd.DataFrame(df_data)
        df_record_uuids = pd.DataFrame(df_record_uuids)
        concept_df = pd.DataFrame(concept_list, index=range(len(concept_list)))
        
        concept_df['normalized_street'] = concept_df["streetTextualValue"].apply(
            lambda street: normalize_street_name(street)
        )
   
        log.info(f'There are : {len(external_df)} rows in the datasheet added through commandline. \nA total of : {len(df_record_uuids)} record_uuids were added to a dataframe.\nThe list of concepts was also added to a dataframe')
        log.info(f'The streets in the concept dataframe are normalized')

    except Exception as e: 
        log.info(f'FAILED CREATING DATAFRAMES {df_record_uuids, data}')
        log.error(f'Error while creating dataframes {e}')    
        errors.append(('Error while creating dataframes' , e))
        error_logging(errors)
        
    for index, row in tqdm(df_record_uuids.head(test_amount).iterrows(), total=df_record_uuids.shape[0]):
        log.info(f"STARTING WITH UUID: {row.uuid}")
        uuid = row.uuid
        count += 1
        predicates = []

        try:
            # Get Record turtle 
            response = api.get_record(uuid)
            
            if response.status_code != 200:
                time.sleep(3)
                response = api.get_record(row.uuid)
                if response.status_code != 200:
                    log.error(f"Reading failed for {row.uuid}")
                    errors.append(("Record does not exist",row))
                    error_logging(errors)
                    continue

            # load the graph
            g = Graph()
            turtle = g.parse(data= response.text, format='turtle')       

            for inst in g.objects(None, SAA.isAssociatedWithModernAddress): 

                predicates_df, _, _, _, _, _ = extract_street(inst, g, uuid, predicates, pattern)
                
                # Extract SCALAR values from the (single-row) DataFrame
                migr_street = predicates_df['extracted_street'].iloc[0]
                street_val = predicates_df['street'].iloc[0]                
                house_number = predicates_df['house_number'].notna().iloc[0]
                number_add = predicates_df['number_add'].notna().iloc[0]
                adamlink = predicates_df['adamlink'].notna().iloc[0]

                log.info(f'predicates dataframe is created for row: {row.uuid}')

                try: 
                    # Normalize street by lowercasing, removing punctuation and letter prefixes
                    predicates_df['normalized_street'] = normalize_street_name(migr_street)
                    log.info(f'Street is normalized for row: {row.uuid}')
                except:
                    log.error(f'can not normalize street for row: {row.uuid}')

                try:
                    # Match street in predicates_df to all streets in external dataframe and collect the adamlink
                    predicates_df = find_adamlink(predicates_df, external_df)
                    log.info(f'Known adamlink is added for row: {row.uuid}')
                except:
                    log.error(f'can not find adamlink for row: {row.uuid}')

                try:
                    concept_df, predicates_df, _ = find_concept_uuid(concept_df, predicates_df)
                    log.info(f'Street is normalized for row: {row.uuid}')
                except:
                    log.error(f'can not find concept for row: {row.uuid}')

                # Extract SCALAR values from the (single-row) DataFrame
                concept_uuid = predicates_df['new_concept_uuid'].iloc[0]

                try:
                    # Get number from adamlink in all dataframes and add alternatives to list in concepts
                    concept_df, predicates_df = add_adamlink_location_number(predicates_df, concept_df, external_df)                   
                    log.info(f'Adamlink location number added for row: {row.uuid}')
                except:
                    log.error(f'can not find adamlink location number for row: {row.uuid}')

                try:
                    # Merge concepts on same street
                    new_merge, predicates_df = merge_dataframes(concept_df, predicates_df)
                    log.info(f'Dataframes merged for row: {row.uuid}')
                except:
                    log.error(f'merging dataframes failed for row: {row.uuid}')                               

                try:
                    # Create dictionairy and df for out_csv
                    outliers_df = outliers_to_csv(new_merge, first)
                    log.info(f'Outlier dataframe created for row: {row.uuid}')
                except:
                    log.error(f'Can not store outlier data in dataframe for row: {row.uuid} and dataframe : {outliers_df}')
                    errors.append(('Issues creating outliers dataframe', [row.uuid, outliers_df]))

                # Add concept URI to saa:street if empty
                if street_val == '' or street_val == 'None':
                    if pd.notna(concept_uuid) and concept_uuid != '':
                        concept_uri = URIRef(f"{PREFIX}/resources/vocabularies/concepts/{concept_uuid}")
                        g.add((inst, SAA.street, concept_uri))
                        turtle_changed = True
                        log.info(f'UUID: {uuid}') 
                        log.info(f'Changed migrant street: {predicates_df['streetTextualValue']} to normalized street : {predicates_df['normalized_street']}.')
                        log.info(f'Filled concept {concept_uuid} and concept street name: {predicates_df['streetTextualValue']}')                        
                    else:
                        log.warning(f'UUID: {uuid} No concept match found for street {predicates_df['extracted_street']}')
                        errors.append(('No concept match found ', row))
                        error_logging(errors)
                else:
                    log.info(f"Street already filled for uuid {uuid}")
                    log.error(f'Concept already filled for uuid: {concept_uuid}')  
                    errors.append(('Concept already filled', row))
                    error_logging(errors)
                # Change street to stripped version without additions  

                # 3. Fill houseNumber only if empty
                if house_number == '' or house_number == 'None':
                    extracted_number = predicates_df['extracted_number'].iloc[0]
                    if extracted_number and extracted_number != '':
                        g.add((inst, SAA.houseNumber, Literal(extracted_number)))
                        turtle_changed = True
                        log.info(f"Housenumber was empty and is now filled for uuid {uuid}")                        
                else:
                    log.info(f"HouseNumber: {predicates_df['house_number']} not changed for uuid {uuid}")    

                # 4. Fill houseNumberAddition only if empty
                if number_add == '' or number_add == 'None':
                    extracted_add = predicates_df['extracted_add'].iloc[0]
                    if extracted_add and extracted_add != '':
                        g.add((inst, SAA.houseNumberAddition, Literal(extracted_add)))
                        turtle_changed = True
                        log.info(f"Housenumber addition was empty and is now filled for uuid {uuid}")
                else:
                    log.info(f"HouseNumberAddition : {predicates_df['number_add']} not changed for uuid {uuid}")

            if turtle_changed:
                turtle = g.serialize(format="turtle")
                response = api.update_record(row.uuid, turtle)
                if response.status_code == 200:            
                    log.info(f"Turtle succesfully updated for row: {row.uuid}")     
                else:            
                    log.error(f"Turtle update failed for row:  {row.uuid}")
                    errors.append(('Update failed for uuid', [row.uuid, response.text]))
                    error_logging(errors)
                log.info(f"Turtle is changed in the script. Check loginfo for details on uuid: {uuid}")


        except Exception as e:
            log.error(f"Failure in the script for row: {row} with error : {e}")
            errors.append(("ERROR Main fn, failing tranformation or upload", [e, row]))
            error_logging(errors)
    
if __name__ == '__main__':
    main()
