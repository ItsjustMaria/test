## Import libraries
import sys
import simplejson as json
from datetime import time
sys.path.append('./SAA')
# from modules import memorix
# from modules import saa
# from modules import wrapper
import pandas as pd
import re
import logging
from rapidfuzz import fuzz


## Declare script variables
env = sys.argv[1]
memorix_export = sys.argv[2]
out_file = r'./output.csv'
name = ''
error_list = []


# MN acc = acceptatieomgeving env = echie tst = dry-run from home
if env == 'acc':
    prefix = 'https://ams-migrate.memorix.io'
    settings_file = 'settings.json'
elif env == 'prod':
    prefix = 'https://stadsarchiefamsterdam.memorix.io'
    settings_file = 'settings.prod.json'
elif env == 'tst':
    print(f'output: {'not yet a file'}')
else:
    raise ValueError("Environment must be 'acc' or 'prod'")

# settings = saa.readJsonFile(f"{settings_file}")
# api = memorix.ApiClient(settings)
# helper = wrapper.ApiBuildingBlocks(api)


# memorix.get_record_type(name)

############### READ FILES #########################

## Read Memorix deed file and specify columns of interest
df_streets = pd.read_csv(memorix_export,
    sep=";",              
    dtype={ "id": str,
           'Deed.saa:isAssociatedWithModernAddress.saa:streetTextualValue': str,
           'Deed.saa:isAssociatedWithModernAddress.saa:street': str,
           'Deed.saa:isAssociatedWithModernAddress.saa:houseNumber': str,
           'Deed.saa:isAssociatedWithModernAddress.saa:houseNumberAddition': str,
           'Deed.saa:hasOrHadSubjectLocation': str
           }
)

# Read concept export file
read_concepts = pd.read_excel('straten.xlsx') 

# Read alternative street-name file
df_alternatives = pd.read_csv('alternatieve_straatnamen.csv',
    sep=";",              
    dtype={ "straat-label-altlabel": str,
           }
)

############################### CREATE DATAFRAME ##########################

# Create dataframe from concept read spreadsheet
df_concepts = pd.DataFrame({
    'concept_uuid' : read_concepts['concept'],
    'straat' : read_concepts['straat'],
    'adamlink' : read_concepts['adamlink']
})

######################## DECLARE VARIABLES ########################

## Declare variables for columns of Memorix street df_streets file
uuid = 'id'
txt_value_lst = 'Deed.saa:isAssociatedWithModernAddress.saa:streetTextualValue'
street_lst = 'Deed.saa:isAssociatedWithModernAddress.saa:street'
h_nr_lst = 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumber'
add_lst = 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumberAddition'
adam_link = 'Deed.saa:hasOrHadSubjectLocation'

## Declare variables for columns of all deviates to write to out_file
extr_str = 'temp extract for street'
extr_nr = 'temp extract for number'
extr_add = 'temp extract for add'
alt_nr = 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumber'
alt_add = 'Deed.saa:isAssociatedWithModernAddress.saa:houseNumberAddition'

# Declare variables for columns of concept_file
concept_uuid = 'concept_uuid'
concept_street = 'straat'
concept_adamlink = 'adamlink'

# Declare variable for column in df_alternatives
link = 'straat-label-altlabel'



############################## EXTRACT INFORMATION #######################################

#  Pattern for filtering migration address for added housenumber and housenumber additions per group  
pattern = r'^(?P<street>.*?)(?:\s+(?P<number>\d+)(?P<add>.*))?$'

# Lay pattern over desired column of spreadsheet 
extracted = df_streets[txt_value_lst].str.extract(pattern)

# Capture groups from regex pattern and temp store them in a column on spreadsheet
df_streets[extr_str] = extracted['street'].str.strip()
df_streets[extr_nr]  = extracted['number'].str.strip()
df_streets[extr_add] = extracted['add'].str.strip()


################################### MATCH INFORMATION #######################################


# Check stripped groups for match with existing colunns and store deviates in variable
different_nr = df_streets[extr_nr].notna() & df_streets[h_nr_lst].notna() & (df_streets[extr_nr] != df_streets[h_nr_lst])
different_add = df_streets[extr_add].notna() & df_streets[add_lst].notna() & (df_streets[extr_add] != df_streets[add_lst])
new_street = (df_streets[street_lst] != df_streets[extr_str]) | (df_streets[street_lst] == '')
new_nr = (df_streets[h_nr_lst] != df_streets[extr_nr]) | (df_streets[h_nr_lst] == '')
new_add = (df_streets[add_lst] != df_streets[extr_add]) | (df_streets[add_lst] == '')
print(f'This is a new_street {new_street}')
###############################LOGICA HIERBOVEN TOEPASSEN ##############################################################

    # ifferent_number = mask_nr & (df_streets[extr_nr] != df_streets[h_nr_lst])
    # ifferent_add = mask_add & (df_streets[extr_add] != df_streets[add_lst])
    # logging.error(f'Differentiating error')

    # Fill street fields ONLY if they are empty and ALL THIS WORK FOR JCK SHIT IT NEEDS TO BE A UUID
    # df_streets[street_lst] = df_streets[street_lst].replace('', pd.NA).combine_first(df_streets[extr_str])


    # transfer_standard = df_streets[extr_str] != different_street
    # print(f'This is the transfer standard {transfer_standard}')

################################# RELOCATE INFORMATION FROM BASE FILE ####################################

# Create columns for any differentiating migration data
    # df_streets.loc[different_street, alt_str] = df_streets.loc[different_street, extr_str]
df_streets.loc[different_nr, alt_nr] = df_streets.loc[different_nr, extr_nr]
df_streets.loc[different_add, alt_add] = df_streets.loc[different_add, extr_add]
# df_streets.loc[new_street, street_lst] = df_concepts[new_street, concept_adamlink]
df_streets.loc[new_nr, h_nr_lst] = df_streets.loc[new_nr, extr_nr]
df_streets.loc[new_add, add_lst] = df_streets.loc[new_add, extr_add]

################################# RELOCATE INFORMATION BETWEEN FILES ####################################

# Determine index based on common denominator
key_streets = txt_value_lst
key_concepts = concept_street

# Map column from merged to basefile
update_mapping = {
    street_lst : concept_uuid,
    adam_link : concept_adamlink
}

for streets_col, concepts_col in update_mapping.items():
    value_map = df_concepts.dropna(subset=[concepts_col]).set_index('straat')[concepts_col].to_dict()
    df_streets[streets_col] = df_streets[key_streets].map(lambda x: value_map.get(x, df_streets.loc[df_streets[key_streets] == x, streets_col].values[0]))

    # mask_not_alt = df_streets[alt_str].notna()
    # 
    # if not mask_not_alt:
    #   df_streets[street_lst] = df_streets[extr_str] # Werkt niet. Wat als hij niet gevuld is? Daar is je masker voor doos. Nee doos! Je boolean is nu False, omdat er geen data in staat
    #print(f' This is same street {same_street}')

    # Write data from temp column to db column ONLY for data that is the same as already available data
    # df_streets.loc[same_street, street_lst] = df_streets.loc[same_street, extr_str]



    # Create mask for different 
    # mask_diff

    # Replace stripped street in temp column 'in place' on location [row, column] of existing df_streets spreadsheet
    # df_streets.loc[mask_str, street_lst] = df_streets.loc[mask_str, extr_str] if df_streets[]

################################## MERGE DATA ###########################

# Merge df with df_concepts
merged = pd.merge(df_streets, df_concepts, left_on= extr_str, right_on='straat', how='left') ####

################################# FIND ALTERNATIVES #################################

# Extract the number [altlabel] from the adamlink with a pattern and temp store this in column 
df_alternatives['number'] = df_alternatives[link].str.extract(r'(\d+)')

# Add extracted number [altlabel] to the merged file
merged['number'] = merged['adamlink'].str.extract(r'(\d+)') 

#print(merged)

# Compare number in merged file with all numbers in df alternative names and store all alternative names in column_list
def find_alternatives(row):    
    if pd.notna(row['number']):
        # Find all rows in the alternatives dataframe with the same number
        number_to_match = row['number']
        print(f"Looking for alternatives for number: {number_to_match}")

        alternatives = df_alternatives[df_alternatives['number'] == number_to_match]
        return alternatives[link].tolist()  # Return all matching alternatives
    return []

merged['alternative_names'] = merged.apply(find_alternatives, axis=1)

# Replace data in merged df_streets[columns] with data from merged df_concepts[columns]
#merged[street_lst] = merged[concept_adamlink] 
# merged[df_streets[h_nr_lst]] = merged[df_streets[h_nr_lst]].replace('', pd.NA).combine_first(df_streets[extr_nr])
# merged[df_streets[add_lst]] = merged[df_streets[add_lst]].replace('', pd.NA).combine_first(df_streets[extr_add])



print(merged)

##################### AI BS ###############################
# specify which columns of my base file I wish to update
# cols_to_update = [street_lst, h_nr_lst, add_lst]



################################ OUTPUT #########################################

#### HERE YOU NEED THE DATE LINK ########

df_streets = df_streets.drop(columns = [extr_str, extr_nr, extr_add])
                             
df_streets.to_csv('upload.csv', sep=';', 
encoding= 'utf-8',
index= False, header= True)

df_out = merged # df[[uuid, txt_value_lst, street_lst, h_nr_lst, add_lst, alt_nr, alt_add]]
# execptions_df = [[uuid,  alt_nr, alt_add]]

# Output dfframe to a new spreadsheet 
df_out.to_csv('output.csv', sep=';', 
encoding= 'utf-8',
index= False, header= True)

