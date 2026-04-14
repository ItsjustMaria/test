## Import libraries
import sys
import simplejson as json
from datetime import time
sys.path.append('../')
from modules import memorix
# from modules import saa
# from modules import wrapper
import pandas as pd
import re
import logging
from rapidfuzz import fuzz

## Declare script variables
env = sys.argv[1]
memorix_export = sys.argv[2]
out_file = r'./output.csv' # name your file for output manual data verifcation = sys.argv[2]
upload = r'./upload.csv' # name your file for upload to memorix = sys.argv[3]
deed = r'templates/deed.ttl' # name your turtle
concepts = r'./straten.xlsx' # name your file for concept export
alter = r'./alternatieve_straatnamen.csv'  # name your file for alternative street names
name = '' #  Early idea, Not in use? 
args = memorix_export, concepts, alter
errors = []
pattern = r'^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$'

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
    'extr_str' : 'temp extract for street',
    'extr_nr' : 'temp extract for number',
    'extr_add' : 'temp extract for add',
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
    settings_file = 'settings.json'
elif env == 'prod':
    prefix = 'https://stadsarchiefamsterdam.memorix.io'
    settings_file = 'settings.prod.json'
elif env == 'tst':
    print(f'output: {out_file}')
else:
    raise ValueError("Environment must be 'acc' or 'prod'")

# settings = saa.readJsonFile(f"{settings_file}")
# api = memorix.ApiClient(settings)
# helper = wrapper.ApiBuildingBlocks(api)


# memorix.get_record_type(name)
def main():
    pass
    # get_uuid_for_query_to_csv() ## --> los draaien?
    


# Read files, create dataframes and declare variables
def create_data_frames(args, **kwrgs):
    try: 
        ########################     READ FILES    ########################    
        print(args[0])
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
        
        print(df_streets)
        read_concepts = pd.read_excel(args[1]) 
        
        df_alternatives = pd.read_csv(args[2],
            sep=";",              
            dtype={ kwrgs['link']: str,
                   }
        )

        ######################## CREATE DATAFRAMES ########################
        df_concepts = pd.DataFrame({
            kwrgs['concept_uuid'] : read_concepts['concept'],
            kwrgs['concept_adamlink'] : read_concepts['adamlink'],
            kwrgs['concept_street'] : read_concepts['straat']
        })

        
        return df_streets, df_alternatives, df_concepts      

    except:
        for arg in args:
            logging.error(f'Dataframe creation error with {arg}')
            errors.append({'fn: create_df_frames': arg})
       
def match_data(pattern, dfs, **kwrgs):

    try:
        #################### EXTRACT INFORMATION #########################
        extracted = dfs[0][kwrgs['txt_value_lst']].str.extract(pattern)

        # Capture groups 
        dfs[0][kwrgs['extr_str']] = extracted['street'].str.strip()
        dfs[0][kwrgs['extr_nr']]  = extracted['number'].str.strip()
        dfs[0][kwrgs['extr_add']] = extracted['add'].str.strip()

        ################### MATCH INFORMATION ############################
        different_nr = dfs[0][kwrgs['extr_nr']].notna() & dfs[0][kwrgs['h_nr_lst']].notna() & (dfs[0][kwrgs['extr_nr']] != dfs[0][kwrgs['h_nr_lst']])
        different_add = dfs[0][kwrgs['extr_add']].notna() & dfs[0][kwrgs['add_lst']].notna() & (dfs[0][kwrgs['extr_add']] != dfs[0][kwrgs['add_lst']])
        new_street = (dfs[0][kwrgs['street_lst']] != dfs[0][kwrgs['extr_str']]) | (dfs[0][kwrgs['street_lst']] == '')
        new_nr = (dfs[0][kwrgs['h_nr_lst']] != dfs[0][kwrgs['extr_nr']]) | (dfs[0][kwrgs['h_nr_lst']] == '')
        new_add = (dfs[0][kwrgs['add_lst']] != dfs[0][kwrgs['extr_add']]) | (dfs[0][kwrgs['add_lst']] == '')

        ###############################LOGICA HIERBOVEN TOEPASSEN ##############################################################

        # ifferent_number = mask_nr & (df_streets[kwrgs['extr_nr']] != df_streets[kwrgs['h_nr_lst']])
        # ifferent_add = mask_add & (df_streets[kwrgs['extr_add']] != df_streets[kwrgs['add_lst']])
        # logging.error(f'Differentiating error')

        # Fill street fields ONLY if they are empty and ALL THIS WORK FOR JCK SHIT IT NEEDS TO BE A UUID
        # df_streets[kwrgs['street_lst']] = df_streets[kwrgs['street_lst']].replace('', pd.NA).combine_first(df_streets[kwrgs['extr_str']])


        # transfer_standard = df_streets[kwrgs['extr_str']] != different_street
        # print(f'This is the transfer standard {transfer_standard}')
        deviates = different_nr, different_add, new_nr, new_add, new_street, 
        return deviates, dfs

    except:
        for df in dfs:
            logging.error(f'Data extraction and match error with {df}')
            errors.append({'fn: match_data': df})
            print(f'We have an error in {df}') ##################   ------------->>> HIER WILLEN WE DE FILENAAM EXTRACTEN

def write_to_files(deviates, dfs, **kwrgs):

    try: 
        ################### write deviate data ##########################
        dfs[0].loc[deviates[0], kwrgs['alt_nr']] = dfs[0].loc[deviates[0], kwrgs['extr_nr']]
        dfs[0].loc[deviates[1], kwrgs['alt_add']] = dfs[0].loc[deviates[1], kwrgs['extr_add']]
        dfs[0].loc[deviates[2], kwrgs['h_nr_lst']] = dfs[0].loc[deviates[2], kwrgs['extr_nr']]
        dfs[0].loc[deviates[3], kwrgs['add_lst']] = dfs[0].loc[deviates[3], kwrgs['extr_add']]

        ################ RELOCATE TO BASE FILE ##########################
        # Determine index based on common denominator
        key_streets = kwrgs['txt_value_lst']
        key_concepts = kwrgs['concept_street']

        # Map column from merged to basefile
        update_mapping = {
            kwrgs['street_lst'] : kwrgs['concept_uuid'],
            kwrgs['adam_link'] : kwrgs['concept_adamlink']
        }

        # Replace data 'in place' based on map and index
        for streets_col, concepts_col in update_mapping.items():
            value_map = dfs[1].dropna(subset=[concepts_col]).set_index('straat')[concepts_col].to_dict()
            dfs[0][streets_col] = dfs[0][key_streets].map(lambda x: value_map.get(x, dfs[0].loc[dfs[0][key_streets] == x, streets_col].values[0]))

        return dfs

    except:
        for df in dfs:
            logging.error(f'Data writing error with {df}')
            errors.append({'fn: write_to_files': df})
            df.to_csv('error.csv', sep=';', 
                encoding= 'utf-8',
                index= False, header= True)
            print(f'We have an error in {df}') ##################   ------------->>> HIER WILLEN WE DE FILENAAM EXTRACTEN

        # mask_not_alt = df_streets[alt_str].notna()
        # 
        # if not mask_not_alt:
        #   df_streets[kwrgs['street_lst']] = df_streets[kwrgs['extr_str']] # Werkt niet. Wat als hij niet gevuld is? Daar is je masker voor doos. Nee doos! Je boolean is nu False, omdat er geen data in staat
        #print(f' This is same street {same_street}')

        # Write data from temp column to db column ONLY for data that is the same as already available data
        # df_streets.loc[same_street, kwrgs['street_lst']] = df_streets.loc[same_street, kwrgs['extr_str']]



        # Create mask for different 
        # mask_diff

        # Replace stripped street in temp column 'in place' on location [row, column] of existing df_streets spreadsheet
        # df_streets.loc[mask_str, kwrgs['street_lst']] = df_streets.loc[mask_str, kwrgs['extr_str']] if df_streets[]

def merge_data(dfs, **kwrgs):
    #################### CREATE MERGE FILE ############################
    # Merge BASE with df_concepts
    df_streets = dfs[0]
    df_concepts = dfs[1]
    merged = pd.merge(df_streets, df_concepts, left_on= kwrgs['extr_str'], right_on=kwrgs['concept_street'], how='left') ####

    ##################### MERGE ALTERNATIVES ##########################

    # Extract the number [altlabel] from the adamlink with a pattern and temp store this in column 
    dfs[2]['number'] = dfs[2][link].str.extract(r'(\d+)')

    # Add extracted number [altlabel] to the merged file
    merged['number'] = merged['adamlink'].str.extract(r'(\d+)') 

    #print(merged)

    # Compare number in merged file with all numbers in df alternative names and store all alternative names in column_list
    def find_alternatives(row):    
        if pd.notna(row['number']):
            # Find all rows in the alternatives dataframe with the same number
            number_to_match = row['number']
            print(f"Looking for alternatives for number: {number_to_match}")

            alternatives = dfs[2][dfs[2]['number'] == number_to_match]
            return alternatives[link].tolist()  # Return all matching alternatives
        return []

    merged['alternative_names'] = merged.apply(find_alternatives, axis=1)

    # Replace data in merged df_streets[columns] with data from merged df_concepts[columns]
    #merged[kwrgs['street_lst']] = merged[concept_adamlink] 
    # merged[df_streets[kwrgs['h_nr_lst']]] = merged[df_streets[kwrgs['h_nr_lst']]].replace('', pd.NA).combine_first(df_streets[kwrgs['extr_nr']])
    # merged[df_streets[kwrgs['add_lst']]] = merged[df_streets[kwrgs['add_lst']]].replace('', pd.NA).combine_first(df_streets[kwrgs['extr_add']])



    print(merged)

    ##################### AI BS ###############################
    # specify which columns of my base file I wish to update
    # cols_to_update = [kwrgs['street_lst'], kwrgs['h_nr_lst'], kwrgs['add_lst']]



    ################################ OUTPUT #########################################

    #### HERE YOU NEED THE UPDATE LINK ########

    df_streets = df_streets.drop(columns = [kwrgs['extr_str'], kwrgs['extr_nr'], kwrgs['extr_add']])

    memorix.update_record_type(df_streets, deed) 

    df_streets.to_csv(upload, sep=';', 
    encoding= 'utf-8',
    index= False, header= True)

    df_out = merged # df[[uuid, txt_value_lst, kwrgs['street_lst'], kwrgs['h_nr_lst'], kwrgs['add_lst'], alt_nr, alt_add]]
    # execptions_df = [[uuid,  alt_nr, alt_add]]
    
    # Output dfframe to a new spreadsheet 
    df_out.to_csv(out_file, sep=';', 
    encoding= 'utf-8',
    index= False, header= True)

if __name__ == '__main__':
        dfs = create_data_frames(args, **kwrgs)
        deviates, dfs = match_data(pattern, dfs, **kwrgs)
        stamped = write_to_files(deviates, dfs, **kwrgs)
        dfs = merge_data(dfs, **kwrgs)