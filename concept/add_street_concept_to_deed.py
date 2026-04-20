## Import libraries
import sys
import os 
from pathlib import Path
#dir_path = os.path.dirname(os.path.realpath(__file__))
import simplejson as json
from datetime import time, datetime
from tqdm import tqdm
# saa_nexus = Path(__file__).resolve().parents[]/ 'saa-nexus-scripts' 
# sys.path.append(str(saa_nexus)) #r'../saa-nexus-scripts/modules')
#root = '/opt/lampp/htdocs/saa-nexus-scripts'
sys.path.append('../../')
from modules import memorix
from modules import saa
# from modules import wrapper
import pandas as pd
import re
import logging
from rapidfuzz import fuzz

## Declare script variables
env = sys.argv[1]
memorix_export = sys.argv[2]
out_file = r'../data/output.csv' # name your file for output manual data verifcation = sys.argv[2]
upload = r'../data/upload.csv' # name your file for upload to memorix = sys.argv[3]
turtle = r'../templates/Deed.ttl' # name your turtle
concepts = r'../data/straten.xlsx' # name your file for concept export
alter = r'../data/alternatieve_straatnamen.csv'  # name your file for alternative street names
args = memorix_export, concepts, alter
errors = []
pattern = r'^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$'

######################## DECLARE EXPORT VARIABLES ########################
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
logfile = f'../logs/street_concepts to deed {str(current_datetime)}.log'
print (logfile)


############################# LOGGER SETUP ###############################
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
    
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(logfile, mode='w')]
)
my_log = logging.getLogger()


######################## DECLARE EXPORT VARIABLES ########################

## MEMORIX EXPORT
kwrgs = {
    'uuid' : 'id',
    'txt_value_lst' : 'Deed.saa:isAssociatedWithModernAddress.saa:streetTextualValue',
    'street_lst' : 'Deed.saa:isAssociatedWithModernAddress.saa:street',
    'h_nr_lst' : 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumber',
    'add_lst' : 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumberAddition',
    'adam_link' : 'Deed.saa:hasOrHadSubjectLocation',

    ## DEVIATES FOR MANUAL CHECK OUTPUT FILE
    'extr_str' : 'temp street',
    'extr_nr' : 'temp number',
    'extr_add' : 'temp add',
    'alt_nr' : 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumber',
    'alt_add' : 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumberAddition',

    # CONCEPT EXPORT
    "concept_uuid" : 'concept_uuid',
    'concept_street' : 'straat',
    'concept_adamlink' : 'adamlink',

    # ALTERNATVIES EXPORT
    'link' : 'straat-label-altlabel'
    }


# MN acc = acceptatieomgeving env = echie tst = dry-run from home
if env == 'acc':
    prefix = 'https://ams-migrate.memorix.io'
    settings_file = r'../../settings.json'
    #cwd = os.getcwd()  # Get the current working directory (cwd)
    #files = os.listdir(cwd)  # Get all the files in that directory
    #print("Files in %r: %s" % (cwd, files))
#elif env == 'prod':
#    prefix = 'https://stadsarchiefamsterdam.memorix.io'
#    settings_file = 'settings.prod.json'
elif env == 'tst':
    print(f'output: {out_file}')
else:
    raise ValueError("Environment must be 'acc' or 'prod'")



settings = saa.readJsonFile(f"{settings_file}")
api = memorix.ApiClient(settings)
# helper = wrapper.ApiBuildingBlocks(api)

# memorix.get_record_type(name)
def main():
    pass
    

########################     READ FILES    ########################    
df_streets = pd.read_csv(args[0],
    
    sep=";",             
    dtype={ "id": str,
           kwrgs['txt_value_lst']: str,
           kwrgs['street_lst']: str,
           kwrgs['h_nr_lst']: str,
           kwrgs['add_lst']: str,
           kwrgs['adam_link']: str
           }
    )

read_concepts = pd.read_excel(args[1]) 

df_alternatives = pd.read_csv(args[2],
    sep=";",              
    dtype={ kwrgs['link']: str,
           }
)

######################## CREATE DATAFRAMES ########################
df_concepts = pd.DataFrame({
    kwrgs['concept_uuid'] : read_concepts['concept'],
    kwrgs['concept_street'] : read_concepts['straat'],
    kwrgs['concept_adamlink'] : read_concepts['adamlink']
})
  
def match_data(pattern, df_streets, **kwrgs):

    try:
        #################### EXTRACT INFORMATION #########################
        extracted = df_streets[kwrgs['txt_value_lst']].str.extract(pattern)

        # Capture groups 
        df_streets[kwrgs['extr_str']] = extracted['street'].str.strip()
        df_streets[kwrgs['extr_nr']]  = extracted['number'].str.strip()
        df_streets[kwrgs['extr_add']] = extracted['add'].str.strip()

        ################### MATCH INFORMATION ############################
        different_nr = df_streets[kwrgs['extr_nr']].notna() & df_streets[kwrgs['h_nr_lst']].notna() & (df_streets[kwrgs['extr_nr']] != df_streets[kwrgs['h_nr_lst']])
        different_add = df_streets[kwrgs['extr_add']].notna() & df_streets[kwrgs['add_lst']].notna() & (df_streets[kwrgs['extr_add']] != df_streets[kwrgs['add_lst']])
        new_nr = (df_streets[kwrgs['h_nr_lst']] != df_streets[kwrgs['extr_nr']]) | (df_streets[kwrgs['h_nr_lst']] == '')
        new_add = (df_streets[kwrgs['add_lst']] != df_streets[kwrgs['extr_add']]) | (df_streets[kwrgs['add_lst']] == '')

        deviates = different_nr, different_add, new_nr, new_add,  
        return deviates

    except:
        my_log.error(f'Data extraction and match error with {df_streets}')
        errors.append({'fn: match_data': df_streets})

def write_to_files(deviates, df_streets, df_concepts, **kwrgs):

    try: 
        ################### write deviate data ##########################
        df_streets.loc[deviates[0], kwrgs['alt_nr']] = df_streets.loc[deviates[0], kwrgs['extr_nr']]
        df_streets.loc[deviates[1], kwrgs['alt_add']] = df_streets.loc[deviates[1], kwrgs['extr_add']]
        df_streets.loc[deviates[2], kwrgs['h_nr_lst']] = df_streets.loc[deviates[2], kwrgs['extr_nr']]
        df_streets.loc[deviates[3], kwrgs['add_lst']] = df_streets.loc[deviates[3], kwrgs['extr_add']]


        ################ RELOCATE TO BASE FILE ##########################
        # Determine index based on common denominator
        
        key_streets = 'temp street'
        key_concepts = 'straat'
  
        # Map column from merged to basefile
        update_mapping = {

            'Deed.saa:isAssociatedWithModernAddress.saa:street' : 'concept_uuid',
            'Deed.saa:hasOrHadSubjectLocation' : 'adamlink'
        }
        

        for streets_col, concepts_col in update_mapping.items():
            value_map = df_concepts.dropna(subset=[concepts_col]).set_index('straat')[concepts_col].to_dict()
            df_streets[streets_col] = df_streets[key_streets].map(lambda x: value_map.get(x, df_streets.loc[df_streets[key_streets] == x, streets_col].values[0]))
        
    except:
        my_log.error(f'Data writing error')
        errors.append({'fn: write_to_files': {df_streets, df_concepts}})

def merge_data(df_streets, df_concepts, df_alternatives, **kwrgs):
        
    try:
        #################### CREATE MERGE FILE ############################
        # Merge basefile with concepts
        merged = pd.merge(df_streets, df_concepts, left_on= kwrgs['extr_str'], right_on=kwrgs['concept_street'], how='left') ####

        ##################### MERGE ALTERNATIVES ##########################
        # Extract altlabel from adamlink 
        df_alternatives['number'] = df_alternatives[kwrgs['link']].str.extract(r'(\d+)')

        # Add extracted number [altlabel] to the merged file
        merged['number'] = merged['adamlink'].str.extract(r'(\d+)') 

        # Compare number in merged file with all numbers in df alternative names and store all alternative names in column_list
        def find_alternatives(row):    
            if pd.notna(row['number']):
                number_to_match = row['number']
                alternatives = df_alternatives[df_alternatives['number'] == number_to_match]
                return alternatives[kwrgs['link']].tolist()  # Return all matching alternatives
            return []

        merged['alternative_names'] = merged.apply(find_alternatives, axis=1)
        

        return merged

    except:
        my_log.error(f'Data merging error')
        errors.append({'fn: merge_data': [df_streets, df_concepts, df_alternatives]})

def output_to_file_and_db(merged, df_streets, deed, **kwrgs):
    ########################### OUTPUT #################################
    # Remove temp columns
    df_streets = df_streets.drop(columns = [kwrgs['extr_str'], kwrgs['extr_nr'], kwrgs['extr_add']])


    # Write updated record to csv
    df_streets.to_csv(upload, sep=';', 
    encoding= 'utf-8',
    index= False, header= True)

    df_out = merged

    df_out.to_csv(out_file, sep=';', 
    encoding= 'utf-8',
    index= False, header= True)

for index, row in tqdm(df_streets.iterrows()):
    uuid = row['id']
    my_log .info(f"Uploading record deed with uuid {uuid}")
    try:
        response = api.update_record(uuid, upload)
        if response.status_code != 200:
            my_log .error(f"Reading failed for {uuid}")
        elif response.status_code == 401:
            my_log.error('Token issue perhaps')
    except:
        my_log.error(f"FAILED TRANSFORMATION {uuid}")

if __name__ == '__main__':
        deviates = match_data(pattern, df_streets, **kwrgs)
        write_to_files(deviates, df_streets, df_concepts, **kwrgs)
        merged = merge_data(df_streets, df_concepts, df_alternatives, **kwrgs)
        output_to_file_and_db(merged, df_streets, **kwrgs)