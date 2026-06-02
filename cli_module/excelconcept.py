import pandas as pd
import json, sys, os
from pathlib import Path
import rdflib
import re
from modules import memorix
from modules import saa

def main(concept_turtle, excel_sheet):

    #WORK_REPO = Path("/opt/lampp/htdocs/saa-nexus-scripts") #### !!!! Adjust base path based on location !!!! ####
    #sys.path.append(str(WORK_REPO))
#
    ## prod or acc
    #settings = saa.readJsonFile('../settings.prod.json') 
    #api = memorix.ApiClient(settings)
#
    ## Conceptlist
    #vocabulair = 'a4863c0c-d9e5-3902-831a-d0960e381a41' #### !!!! Specify desired vocabulairy !!!! ####
#
    ## Paths
    #excel_sheet = "straten.xlsx"       #### !!!! Your desired excel output file name !!!! ####
#
    #response = api.list_concepts(vocabulair)
    #print(response.text,  file=open(concept_turtle, 'w', encoding='utf-8'))


    # Load RDF/concept_turtle 
    g = rdflib.Graph()
    g.parse(concept_turtle, format="ttl")

    # Namespace
    SKOS = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")

    rows = []
    for s in g.subjects(rdflib.RDF.type, SKOS.Concept):
        s_str = str(s)

        match = re.search(r'/vocabularies/concepts/([^/>]+)', s_str)
        uuid = match.group(1) if match else ""

        prefLabel = next((str(lab) for lab in g.objects(s, SKOS.prefLabel)), "")
        exactMatch = next((str(em) for em in g.objects(s, SKOS.exactMatch)), "") # <-- fout: want exactMatch kan nu meer dan 1 waarde hebben
        scopeNote = next((str(sn) for sn in g.objects(s, SKOS.scopeNote)), "")

        rows.append({
            "uuid": uuid,
            "prefLabel": prefLabel,
            "exactMatch": exactMatch,
            "scopeNote": scopeNote
        })

    # Create dataFrame and write to Excel
    df = pd.DataFrame(rows)
    df.to_excel(excel_sheet, index=False)

    return f"Done! exported to {excel_sheet}"

if __name__ == '__main__':
    main()