## Import libraries
import os 
import sys
from pathlib import Path
cwd = os.getcwd()  # Get the current working directory (cwd)
files = os.listdir(cwd)  # Get all the files in that directory
print("Files in %r: %s" % (cwd, files))
WORK_REPO = Path(r"C:\Users\swart053\Documents\VSC\saa-nexus-scripts") # Adjust base path based on location
sys.path.append(str(WORK_REPO))
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

'''Export UUID's from Memorix using APi. Use these to get information from Memorix. 
   Alter this information. Upload this information back to Memorix.
   Call script with command cli and the environment option:  
   'python my_script.py pipeline --env data_to_be_used'
'''

# Declare global variables
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
# ctx.obj["current_datetime"] = current_datetime
logfile = f'../logs/get concept streetnames {str(current_datetime)}.log'
errors = []

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(logfile, mode='w')]
)
log = logging.getLogger()

# -----------------------------------
# GLOBAL FUNCTIONS FOR SCRIPT
# -----------------------------------

# Environment setup
def setup_environment(env):
    if env == 'acc':
        prefix = 'https://ams-migrate.memorix.io'
        settings_file = Path(WORK_REPO, 'settings.json') 
        #cwd = os.getcwd()  # Get the current working directory (cwd)
        #files = os.listdir(cwd)  # Get all the files in that directory
        #print("Files in %r: %s" % (cwd, files))
    elif env == 'prod':
        prefix = 'https://stadsarchiefamsterdam.memorix.io'
        settings_file = Path(WORK_REPO, 'settings.prod.json') 
    elif env == 'tst':
        settings_file = print(f'test output')
    else:
        raise ValueError("Environment must be 'acc' or 'prod'")
    
    settings = saa.readJsonFile(settings_file) # prod of acc
    api = memorix.ApiClient(settings)

    return api

################## LOG FUNCTION! DO YOU NEED IT IN CLICK???  #########
'''# Log file setup
def log_setup(logfile):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.FileHandler(logfile, mode='w')]
    )
    log = logging.getLogger()

    return log
'''
################## IF NOT DELETE ABOVE FUNCTION ####################


def retrieve_deed_turtle_from_memorix(api, turtle, result):
    
    # Get turtle for record to update 
    try:
        response = api.get_record_type( turtle)
        print(response.text,  file=open(result, 'w', encoding='utf-8'))
        return response
    except:
        log.error(f'Could not get record_type {turtle} from api {api} with response {response}')
        errors.append({'fn: retrieve_deed_turtle_from_memorix': [turtle, result, response]})


def retrieve_concept_turtle_from_memorix(api, turtle, result):
    
    print('This is the first function where you grab turtle and all UUID\'s')

    # Get turtle 
    response = api.list_concepts( turtle)
    print(response.text,  file=open(result, 'w', encoding='utf-8'))
    print(turtle, result)
    return response

def retrieve_uuid_from_memorix(api, turtle):
    
    print('This is the first function where you grab the turtle and all UUID\'s')

    # Get uuids with street migration field 
    response = api.get_record_type( 'Deed')
    print(response.text,  file=open(turtle, 'w', encoding='utf-8'))
    #print(f"Using file: {data}")
    return response


def retrieve_concept_from_memorix(api, turtle):
    
    print('This is the first function where you grab turtle and all UUID\'s')

    # Get concept streets 
    response = api.get_record_type( 'Deed')
    print(response.text,  file=open(turtle, 'w', encoding='utf-8'))
    #print(f"Using file: {data}")
    return response

def concept_from_turtle_to_excel(api, turtle):
    
    # Turn turtle to working excel sheet 
    response = api.get_record_type( 'Deed')
    print(response.text,  file=open(turtle, 'w', encoding='utf-8'))
    #print(f"Using file: {data}")
    return response


# -----------------------------------
# MAIN CLI GROUP
# -----------------------------------

# Start command line input 
@click.group()
# Options determine which environment
@click.option(
    '--env',
    '-e',
    type=click.Choice(['acc', 'prod', 'tst']),
    default='acc',
    help='Which environment to use: “acc”, “prod” or "tst".'
)
@click.pass_context
def cli(ctx, env):

    # Create shared storage
    ctx.obj = {}

    # Store environment
    ctx.obj["env"] = env

    # Determine environment
    ctx.obj["api"]= setup_environment(env)

    # Declare user variables
    ctx.obj['vocabulair'] = 'a4863c0c-d9e5-3902-831a-d0960e381a41'
    ctx.obj['Deed'] = 'Deed'
    ctx.obj["deed_turtle"] = "../template/deed.ttl"               # <-- Location of deed turtle
    ctx.obj['concept_turtle'] = '../data/street.ttl'              # <-- Location of street turtle
    ctx.obj["streets"] = "straten.xlsx"                           # <-- Location of concepts
    ctx.obj["adamlink"] = '../data/alternatieve_straatnamen.csv'  # <-- Location of adamlinks /alternative names

    #ctx.obj["log"] = log_setup(logfile)
  
# -----------------------------------
# EXPORT 
# -----------------------------------

@cli.command()
@click.pass_context
def download_deed_turtle(ctx, turtle):

    retrieve_deed_turtle_from_memorix( ctx.obj["api"], ctx.obj["Deed"], ctx.obj['deed_turtle'])

    ctx.obj['deed_turtle'] = retrieve_deed_turtle_from_memorix(ctx.obj["api"], ctx.obj[turtle])

@cli.command()
def download_deed_street(ctx):

    retrieve_concept_turtle_from_memorix( ctx.obj["api"], ctx.obj['vocabulair'], ctx.obj['concept_turtle'])

    ctx.obj['concept_turtle'] = retrieve_concept_turtle_from_memorix(ctx.obj["api"])


@cli.command()
def download_uuid(ctx):

    retrieve_uuid_from_memorix( ctx.obj["api"], ctx.obj['deed_turtle'])

    ctx.obj['uuid_csv'] = retrieve_uuid_from_memorix(ctx.obj["api"])




@cli.command()
def process(ctx):
    ## process_logic comes here
    pass

@cli.command()
@click.argument("data")
@click.pass_context
def pipeline(ctx, data):

    # Store data globally
    ctx.obj["data"] = data
  
    click.echo("STEP 1: DOWNLOAD TURTLE DEED")

    retrieve_deed_turtle_from_memorix( ctx.obj["api"], ctx.obj["Deed"], ctx.obj['deed_turtle'])

    click.echo(f"Environment: {ctx.obj['env']}")
    click.echo(f"api: {ctx.obj['api']}")

    click.confirm(
        "Continue processing?",
        abort=True
    )

    click.echo("STEP 2: DOWNLOAD TURTLE CONCEPT")

    retrieve_concept_turtle_from_memorix( ctx.obj["api"], ctx.obj['vocabulair'], ctx.obj['concept_turtle'])

    click.echo(f"Environment: {ctx.obj['env']}")
    click.echo(f"api: {ctx.obj['api']}")

    click.confirm(
        "Continue processing?",
        abort=True
    )
    
    click.echo("STEP 3: DOWNLOAD UUIDS RECORD")
    
    retrieve_uuid_from_memorix( ctx.obj["api"], ctx.obj['deed_turtle'])

    click.echo(f"Environment: {ctx.obj['env']}")
    click.echo(f"api: {ctx.obj['api']}")

    click.confirm(
        "Continue processing?",
        abort=True
    )


    click.echo("STEP 2: PROCESSING")


    click.confirm(f"I am connected to: {ctx.obj['env']} Continue?", abort=True)


    click.echo('Done!')






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
    logging.info(f"START {row.uuid}")
    try:
        response = api.get_record(row.uuid)
        if response.status_code != 200:
            logging.error(f"Reading failed for {row.uuid}")
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
                    logging.info(f"{row.uuid}, Geen geregistreerde: {person_uuid}")

            data.loc[index, "geregistreerden"] = str(persons)
            logging.info(f"SUCCEED {row.uuid}")
    except:
        logging.error(f"FAILED TRANSFORMATION {row.uuid}")

### changes ###
'''

if __name__ == '__main__':
    cli(obj={})