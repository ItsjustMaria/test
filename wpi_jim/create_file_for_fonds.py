import pandas as pd
import ast
import sys
from rdflib import Graph, URIRef, Literal, Namespace, RDF, BNode, XSD

### initialize things ###
csv = sys.argv[1]
toegangsnr = sys.argv[2]

df = pd.read_csv(csv, encoding='utf-8', na_filter=False)

df_dd = df[df["aggregatieniveau"] == "Dossier"]
df_dr = df[df["aggregatieniveau"] == "Record"]
df_db = df[df["aggregatieniveau"] == "Bestand"]
df_street = pd.read_csv("C:/Users/verhoe058/saa-nexus-scripts/cs_straat.csv", encoding='utf-8', na_filter=False)

document_mapping = {'Aanvraag': "b26c8d00-043f-4757-bf07-7d1336786701",
                    'Aanvraag; Advies': "b26c8d00-043f-4757-bf07-7d1336786701",
                    'Aanvraag; Advies; Bouwtekening': "b26c8d00-043f-4757-bf07-7d1336786701",
                    'Aanvraag; Berekening': "b26c8d00-043f-4757-bf07-7d1336786701",
                    'Aanvraag; Berekening; Overige': "b26c8d00-043f-4757-bf07-7d1336786701",
                    'Aanvraag; Beschikking': "b26c8d00-043f-4757-bf07-7d1336786701", 
                    'Aanvraag; Overige': "b26c8d00-043f-4757-bf07-7d1336786701", 
                    'Adresbeschikking': "24ed3153-3cb2-44cd-8459-f062ba96f3d8", 
                    'Adresbeschikking; Bouwtekening': "24ed3153-3cb2-44cd-8459-f062ba96f3d8", 
                    'Advies': "8d64f9c9-79db-4a86-916e-0b03455b5e99",
                    'Advies; Beschikking': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Beschikking; Bouwtekening; Overige': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Berekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Berekening; Bouwtekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Berekening; Constructietekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Berekening; Overige': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Bouwtekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Bouwtekening; Constructietekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Bouwtekening; Overige': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Constructietekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Overige': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Advies; Revisietekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99",
                    'Berekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99",
                    'Berekening; Constructietekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Berekening; Bouwtekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99",
                    'Berekening; Bouwtekening; Constructietekening': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Berekening; Bouwtekening; Overige': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Berekening; Overige': "8d64f9c9-79db-4a86-916e-0b03455b5e99", 
                    'Beschikking': "7bd8b5fc-f83f-46f4-a14e-e8b1b8479fdb", 
                    'Beschikking; Overige': "7bd8b5fc-f83f-46f4-a14e-e8b1b8479fdb", 
                    'Beschikking; Bouwtekening; Overige': "7bd8b5fc-f83f-46f4-a14e-e8b1b8479fdb",
                    'Beschikking; Bouwtekening': "7bd8b5fc-f83f-46f4-a14e-e8b1b8479fdb",
                    'Bouwtekening': "7a08380e-de15-4fe0-a658-42bb820d033a", 
                    'Bouwtekening; Constructietekening': "7a08380e-de15-4fe0-a658-42bb820d033a", 
                    'Bouwtekening; Constructietekening; Overige': "7a08380e-de15-4fe0-a658-42bb820d033a", 
                    'Bouwtekening; Overige': "7a08380e-de15-4fe0-a658-42bb820d033a", 
                    'Constructietekening': "8a0f62a6-31e1-41ef-ad0c-b3f2ef8f1ffb", 
                    'Overige': "b9954685-6a9e-4a6f-8fbe-4355ddb2c795", 
                    'Overige; Revisietekening': "b9954685-6a9e-4a6f-8fbe-4355ddb2c795", 
                    'Revisietekening': "2d94b07d-c12b-4696-9d6d-c66580d557fc"}

vormer_mapping = {"Stadsdeel Centrum": "5319453a-236e-4904-b474-f3d68a7a2f81",
                  "Stadsdeel Noord": "e26534b3-4412-41fa-b7cc-aea206b19cbd",
                  "Stadsdeel West": "e0edb0cb-d46c-72d6-e053-b784100affb0",
                  "Stadsdeel Zuid": "6e77fa16-6724-42a6-9ec0-dcc4b9eb6b1a",
                  "Stadsdeel Oost": "11d77213-93c8-4c52-a68a-47ebe6d4e2e5",
                  "Stadsdeel Nieuw-West": "d3799522-b511-4d2b-96e3-d5ee49e95725",
                  "Stadsdeel Zuidoost": "bb887b20-0d8b-4277-9993-e0cf96df1a85",
                  "Dienst Milieu en Bouwtoezicht": "d40a9607-c324-2841-e053-b784100aaebf"}

# Define a namespace
record = Namespace("/resources/records/")
rt = Namespace("/resources/recordtypes/")
rico = Namespace("https://www.ica.org/standards/RiC/ontology#")
mmx = Namespace("http://memorix.io/ontology#") 
saa_nm = Namespace("https://data.archief.amsterdam/ontology#")
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
concept = Namespace("/resources/vocabularies/concepts/")
vocabularies = Namespace("/resources/vocabularies/conceptschemes/")
dr = Namespace("/resources/recordtypes/DigitalRecord#")
dd = Namespace("/resources/recordtypes/DigitalDossier#")

for index, row_dd in df_dd.iterrows():
    g = Graph()

    g.bind("record", record)
    g.bind("rt", rt)
    g.bind("rico", rico)
    g.bind("memorix", mmx)
    g.bind("saa", saa_nm)
    g.bind("concept", concept)
    g.bind("vocabularies", vocabularies)
    g.bind("dd", dd)

    dd_uri = URIRef(record[row_dd["uuid"]])
    fonds_uri = URIRef(record[row_dd["parent_id"]])
    
    g.add((dd_uri, RDF.type, rt.DigitalDossier))
    g.add((dd_uri, RDF.type, mmx.Record))
    g.add((dd_uri, rico.isOrWasIncludedIn, fonds_uri))
    g.add((dd_uri, rico.identifier, Literal(row_dd["identificatiekenmerk"])))
    g.add((dd_uri, rico.title, Literal(row_dd["omschrijving"])))
    
    date = BNode()

    g.add((date, RDF.type, rico.DateRange))
    if row_dd["begin_datum"] != '':
        g.add((date, rico.hasBeginningDate, Literal(row_dd["begin_datum"], datatype=XSD.date)))
    if row_dd["eind_datum"] != '':
        g.add((date, rico.hasEndDate, Literal(row_dd["eind_datum"], datatype=XSD.date)))
    g.add((dd_uri, saa_nm.isAssociatedWithDate, date))

    if row_dd["naam"] == "Omgevingsvergunning":
        ov_uuid = "9c1b6de5-59a1-4eb8-bda7-4677f1495dbb"
        g.add((dd_uri, rico.hasDocumentaryFormType, URIRef(concept[ov_uuid])))
    if row_dd["naam"] == "Omgevingsvergunning, deelgoedkeuring":
        ovd_uuid = "0048ad86-8fbe-54d5-e063-894acd0aaec0"
        g.add((dd_uri, rico.hasDocumentaryFormType, URIRef(concept[ovd_uuid])))
    if row_dd["naam"] == "Overige bouwgerelateerde vergunningen":
        obv_uuid = "21bea570-f989-413f-af59-f0c5ce85e0db"
        g.add((dd_uri, rico.hasDocumentaryFormType, URIRef(concept[obv_uuid])))

    locatie_list = ast.literal_eval(row_dd["locaties"])
    for locatie in locatie_list:
        locaties = BNode()
        g.add((dd_uri, dd.isAssociatedWithAddress, locaties))
        g.add((locaties, RDF.type, saa_nm.Address))
        g.add((locaties, dd.streetTextualValue, Literal(locatie["straatnaam"])))
        if str(locatie["huisletter"]) != "" and str(locatie["huisnummertoevoeging"]) != "":
            g.add((locaties, saa_nm.houseNumberBegin, Literal(str(locatie["huisnummer"])+'-'+str(locatie["huisnummertoevoeging"])+str(locatie["huisletter"]))))
        elif str(locatie["huisletter"]) != "" and str(locatie["huisnummertoevoeging"]) == "":
            g.add((locaties, saa_nm.houseNumberBegin, Literal(str(locatie["huisnummer"])+'-'+str(locatie["huisletter"]))))
        elif str(locatie["huisletter"]) == "" and str(locatie["huisnummertoevoeging"]) != "":
            g.add((locaties, saa_nm.houseNumberBegin, Literal(str(locatie["huisnummer"])+'-'+str(locatie["huisnummertoevoeging"]))))
        else:
            g.add((locaties, saa_nm.houseNumberBegin, Literal(locatie["huisnummer"])))
        bag_id_list = ast.literal_eval(str(locatie["bag_id"]))
        for bag_id in bag_id_list:
            if bag_id is not None:
                if "|" in bag_id:
                    result = bag_id.split('|')
                    for id in result:
                        g.add((locaties, dd.hasOrHadSubjectBAGIdentifier, Literal(id)))
                else:
                    g.add((locaties, dd.hasOrHadSubjectBAGIdentifier, Literal(bag_id)))
        matched_street = df_street[df_street["prefLabel"].str.lower() == locatie["straatnaam"]]
        if not matched_street.empty:
            uri_suffix = matched_street.iloc[0]["uri"].replace("https://stadsarchiefamsterdam.memorix.io/resources/vocabularies/concepts/", "")
            g.add((locaties, saa_nm.street, URIRef(concept[uri_suffix])))  

    name_list = []
    externIdentificatiekenmerk_list = ast.literal_eval(row_dd["externIdentificatiekenmerk"])
    for item in externIdentificatiekenmerk_list:
        if item["kenmerkSysteem"] is not None:
            name = item["kenmerkSysteem"]+": "+item["nummerBinnenSysteem"]
            name_list.append(name)
        else:
            name = item["nummerBinnenSysteem"]
            name_list.append(name)
    rico_name = ' | '.join(name_list)
    g.add((dd_uri, rico.name, Literal(rico_name)))

    for item in externIdentificatiekenmerk_list:
        specification = BNode()
        g.add((specification, RDF.type, saa_nm.Specification))
        g.add((specification, saa_nm.specificationTextualValue, Literal(item["nummerBinnenSysteem"])))
        if item["kenmerkSysteem"] == "BWT nummer":
            bwt_uuid = "dfae0925-ca87-4837-a773-bd968f58fd94"
            g.add((specification, saa_nm.specificationType, URIRef(concept[bwt_uuid])))
        if item["kenmerkSysteem"] == "OLO loket":
            olo_uuid = "7f38c7db-195d-459d-b071-e606d29b2ede"
            g.add((specification, saa_nm.specificationType, URIRef(concept[olo_uuid])))
        if item["kenmerkSysteem"] == "Brugnummer":
            brug_uuid = "ee49395f-97ed-4191-bff6-a782a92dbf26"
            g.add((specification, saa_nm.specificationType, URIRef(concept[brug_uuid])))
        g.add((dd_uri, saa_nm.hasSpecification, specification))

    activiteiten_list1 = ast.literal_eval(str(row_dd["activiteiten"]))
    for activity in activiteiten_list1:
            specification2 = BNode()
            g.add((specification2, RDF.type, saa_nm.Specification))
            g.add((specification2, saa_nm.specificationType, URIRef(concept["f0ac6be3-c3ae-4352-9112-5ddd94ab4539"])))
            g.add((specification2, saa_nm.specificationTextualValue, Literal(activity)))
            g.add((dd_uri, saa_nm.hasSpecification, specification2))

    g.serialize(f"E:/wabo/bwt/{toegangsnr}/dossier/{row_dd["uuid"]}.ttl", format="turtle", encoding='utf-8')
    print(f"Turtle gemaakt voor dossier {row_dd["uuid"]}")
