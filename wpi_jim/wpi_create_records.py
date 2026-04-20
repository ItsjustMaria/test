## Import libraries
import sys
import os 
import simplejson as json
from datetime import time, datetime
from tqdm import tqdm
sys.path.append(r'../../')
from modules import memorix
from modules import saa
# from modules import wrapper
from rdflib import Graph, URIRef, Literal, Namespace, RDF, BNode, XSD
import pandas as pd
import re
import logging
from rapidfuzz import fuzz

'''Script for creating brand new files to a fonds and importing it'''

## Declare script variables
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
env = sys.argv[1]
file = sys.argv[2]

# Define a namespace
record = Namespace("/resources/records/")
rt = Namespace("/resources/recordtypes/")
rico = Namespace("https://www.ica.org/standards/RiC/ontology#")
mmx = Namespace("http://memorix.io/ontology#") 
saa_nm = Namespace("https://data.archief.amsterdam/ontology#")
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
# concept = Namespace("/resources/vocabularies/concepts/")
# vocabularies = Namespace("/resources/vocabularies/conceptschemes/")
dr = Namespace("/resources/recordtypes/DigitalRecord#")
dd = Namespace("/resources/recordtypes/DigitalDossier#")
skos = Namespace("http://www.w3.org/2004/02/skos/core#")


# Declare user case dependent variables 
turtle_file = "../template/file.ttl"       # Specify location and name of turtle
logfile = f'../logs/create_files {str(current_datetime)}.log' # Specify your own log name and location
print (logfile)
names = ['ADMNR','NADERE_TOELICHTING','NAAM','GEBOORTEDATUM','DOSSIERTYPE','DOSSIERTYPE_BESCHRIJVING','CREATIEDATUM_DOSSIER'] # Column names in one string
pattern = r'''
(\d+)                      # 1: admin_nr
([A-Z0-9]+)                # 2: admin_extra
([A-Za-z]s\.]+[A-Za-z]s\*)                # 3: name + initials
#([A-Za-z\s\.]*)            # 4: initials / extra
(\d{2}-\d{2}-\d{4})        # 4: date
([A-Za-z]{3})              # 5:type  
([A-Za-z]+)                # 6: description
(\d{2}-\d{2}-\d{4})        # 7: date
'''

# Log handling
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
    
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(logfile, mode='w')]
)
my_log = logging.getLogger()


# MN acc = acceptatieomgeving env = echie tst = dry-run from home
if env == 'acc':
    prefix = 'https://ams-migrate.memorix.io'
    settings_file = r'../../settings.json'
    cwd = os.getcwd()  # Get the current working directory (cwd)
    files = os.listdir(cwd)  # Get all the files in that directory
    print("Files in %r: %s" % (cwd, files))
elif env == 'prod':
    prefix = 'https://stadsarchiefamsterdam.memorix.io'
    settings_file = '../../settings.prod.json'
elif env == 'tst':
    settings_file = r'../../settings.json'
    print(f'test output')
else:
    raise ValueError("Environment must be 'acc' or 'prod'")

# Standard API client variables
settings = saa.readJsonFile(settings_file) # prod of acc
api = memorix.ApiClient(settings)


# read file and convert to set for fast searching

df = pd.read_csv(file, header=None, delimiter = ',')
# df_set = set(df.iloc[:, 0]) # DO I NEED A SET? I DON'T NEED TO SEARCH RIGHT?

#df[names] = df[0].str.extract(pattern, flags=re.VERBOSE)
df[0] = df[0].str.replace('""', '', regex=False)
df[0] = df[0].str.strip()
print (df[0])
df[names] = df[0].str.extract(pattern, flags=re.VERBOSE)

#print(f'this is the shap:{result.shape}')
#print(f'This is the head: {result.head()}')
#df_sep.notna('', inplace = True)

# Merge back columns that should not be separated

print(f'This is the length: {len(names)}')

df_sep = df[names]
# Add names to colunns
df_sep.columns.fillna = [names]


#df_sep[2] = df_sep[3] + ' ' + df_sep[2]
#df_sep.drop(columns = [3])
#print(f'This is names: {df_sep[2]}') 
#print(f'This is additives: {df_sep[3]}')
#names_list = list(names.split(','))
for index, name in enumerate(names):
    df_sep.rename(columns={index: name}, inplace = True)

#        df_sep = pd.concat([df, sep], axis=1).fillna
print(df_sep)
#print(df_sep)


#file_to_rows = []
#
#for index, row in df.iterrows():
#    admin_nr = ast.literal_eval(row["ADMNR"])
#    for locatie in locatie_list:
#        doc_id_list = ast.literal_eval(str(locatie["doc_id"]))
#        for doc_id in doc_id_list:
#            if doc_id is not None:
#                if "|" in doc_id:
#                    result = doc_id.split('|')
#                    for id in result:
#                        doc_id_rows.append({'uuid': row['uuid'], 'doc_id': id, 'end_date': row['end_date'], 'description': row['description']})
#                else:
#                    doc_id_rows.append({'uuid': row['uuid'], 'doc_id': doc_id, 'end_date': row['end_date'], 'description': row['description']})
#doc_id_df = pd.DataFrame(doc_id_rows)
#
#
#for index, row in df.iterrows():
#    g = Graph()
#
#    g.bind("record", record)
#    g.bind("rt", rt)
#    g.bind("rico", rico)
#    g.bind("memorix", mmx)
#    g.bind("saa", saa_nm)
#    # g.bind("concept", concept)
#    # g.bind("vocabularies", vocabularies)
#    # g.bind("dd", dd)
#    g.bind("skos", skos)
#
#ar_uri = BNode()
#g.add((ar_uri, RDF.type, mmx.AccessibilityAndRightsComponent))
#g.add((ar_uri, mmx.accessModeDisplay, mmx.DisplayAssets))
#g.add((ar_uri, mmx.accessModeDownload, Literal(True, datatype=XSD.boolean)))
#g.add((ar_uri, mmx.accessModeReservation, Literal(False, datatype=XSD.boolean)))
#g.add((ar_uri, mmx.accessModeScanningOnDemand, Literal(False, datatype=XSD.boolean)))
#g.add((ar_uri, mmx.attributionRequired, Literal(False, datatype=XSD.boolean)))
#g.add((ar_uri, mmx.audience, mmx.AudienceExternal))
#use_uri = URIRef(concept["e8a92b13-efaf-4b2e-e053-b784100a3466"])
#g.add((use_uri, RDF.type, skos.Concept))
#g.add((ar_uri, mmx.limitationOfUse, use_uri))
#g.add((ar_uri, mmx.physicallyAvailable, Literal(False, datatype=XSD.boolean)))
#raw_value = row.get("openbaarheid", None)
#openbaarheid_dict = ast.literal_eval(raw_value)
#if openbaarheid_dict.get("omschrijvingBeperkingen") == "Aanvraagformulier":
#    access_uri = URIRef(concept["56b6ffe6-f801-4715-872b-303db35f48f9"])
#    g.add((ar_uri, mmx.limitationOfAccess, access_uri))
#    g.add((access_uri, RDF.type, skos.Concept))
#    g.add((ar_uri, mmx.restrictionsExpire, Literal(openbaarheid_dict.get("datum"), datatype=XSD.date)))
#if openbaarheid_dict.get("omschrijvingBeperkingen") is None:
#    access_uri = URIRef(concept["b91d25b5-a1b4-4bc9-a15c-d48883a95d0b"])
#    g.add((ar_uri, mmx.limitationOfAccess, access_uri))
#    g.add((access_uri, RDF.type, skos.Concept))
#g.serialize(f"E:/wabo/bwt/{toegangsnr}/rechten/{row["uuid"]}.ttl", format="turtle", encoding='utf-8')
#print(f"Turtle gemaakt voor rechten {row["uuid"]}")
#