## Import libraries
import sys
from pathlib import Path
WORK_REPO = Path("/opt/lampp/htdocs/saa-nexus-scripts")
sys.path.append(str(WORK_REPO))
import os 
import tracemalloc
import click
import simplejson as json
from datetime import time, datetime
from tqdm import tqdm
from modules import memorix
from modules import saa
import pandas as pd
import re
import rdflib
from rdflib import Graph, URIRef, Literal, Namespace, RDF, BNode, XSD
import logging
from rapidfuzz import fuzz



## Declare script variables
#env = sys.argv[1]
'''
######################## DECLARE EXPORT VARIABLES ########################
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
logfile = f'../logs/get concept streetnames {str(current_datetime)}.log'
print (logfile)


############################# my_log SETUP ###############################
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
    settings_file = Path(WORK_REPO, 'settings.json') 
    cwd = os.getcwd()  # Get the current working directory (cwd)
    files = os.listdir(cwd)  # Get all the files in that directory
    print("Files in %r: %s" % (cwd, files))
elif env == 'prod':
    prefix = 'https://stadsarchiefamsterdam.memorix.io'
    settings_file = Path(WORK_REPO, 'settings.json') 
elif env == 'tst':
    print(f'test output')
else:
    raise ValueError("Environment must be 'acc' or 'prod'")

settings = saa.readJsonFile(settings_file) # prod of acc
api = memorix.ApiClient(settings)
'''




# Bestandspaden
turtle = "../template/deed.ttl"                      # <-- Locatie van de turtle
excel_file = "straten.xlsx"                          # <-- Locatie van de concepten
adamlink = '../data/alternatieve_straatnamen.csv'    # <-- Locatie van de adamlinks /alternatieve namen

'''# Turtle ophalen
response = api.get_record_type( 'Deed')
print(response.text,  file=open(turtle, 'w', encoding='utf-8'))'''



# FULL COMMAND LINE INPUT TRY
tracemalloc.start()
PYTHONTRACEMALLOC = 1
'''Script for creating brand new files to a fonds and importing it'''

# Start your click group command line inputs
@click.group()

def cli():
    pass

@cli.command()
#@click.argument('script')
@click.option('--env', '-e', type=click.Choice(['acc', 'prod', 'tst']), default='acc',
    help='Which environment to use: “acc”, “prod” or "tst".')
@click.argument('data')

def imp(env, data):
    print('This is the first function where you grab all UUID\'s')

    ### THIS WHOLE BLOCK BECOMES OBSOLETE, BECAUSE CLICK NOW HANDLES IT #######
    if env == 'acc':
        print("We are testing")
    elif env == 'prod':
        print("We are for reals")
    elif env == 'tst':
        print('We\'re having a dry run')
    else:
        raise ValueError("Environment must be 'acc' or 'prod'")
    
    #### BETTER USE ##############

    environments = {
        'acc': 'We are testing',
        'prod': 'We are for reals',
        'tst': 'We are having a dry run'
    }

    print(environments[env])

    print(f"Using file: {data}")

@cli.command()
def process():
    ## process_logic comes here
    pass

@cli.command()
def pipeline():

    imp()

    click.confirm("Continue?", abort=True)

    process()

    click.echo('Done!')

#@click.option('--output', '-o', default='output.csv',
#              help='Path to CSV logfile (will be appended).')
#@click.option('--workers', '-w', default=10, show_default=True,
#              help='Number of concurrent worker threads.')
#@click.option('--max-calls', '-m', default=0, type=int, show_default=True,
#              help='Maximum API calls per second (0 = unlimited).')

# def main(script, env, records): 
#     pass



#def next():
#    pass




#for index, row in tqdm(data.iterrows(), total=data.shape[0]):
#    logging.info(f"START {row.uuid}")
#    try:
#        response = api.get_record(row.uuid)
#        if response.status_code != 200:
#            logging.error(f"Reading failed for {row.uuid}")
#        else:
#            # load the graph
#            g = Graph()
#            g.parse(data=response.text, format='turtle')
#            persons = []
#
#            # iterate over every person
#            for inst in g.objects(subject=None, predicate=RICO['hasOrHadSubject']):  
#                person_rol = g.value(subject=inst, predicate=SAA['relatedPersonObservationRole'])
#                # if person == 'geregistreerde':
#                if person_rol==URIRef(f'https://{PREFIX}.memorix.io/resources/vocabularies/concepts/e8a92b13-f000-4b2e-e053-b784100a3466'): 
#                    person_uri = g.value(subject=inst, predicate=SAA['relatedPersonObservation'])
#                    person_uuid = person_uri.split('/')[-1]
#                    persons.append(person_uuid)
#                else:
#                    logging.info(f"{row.uuid}, Geen geregistreerde: {person_uuid}")
#
#            data.loc[index, "geregistreerden"] = str(persons)
#            logging.info(f"SUCCEED {row.uuid}")
#    except:
#        logging.error(f"FAILED TRANSFORMATION {row.uuid}")

### changes ###


if __name__ == '__main__':
    cli()