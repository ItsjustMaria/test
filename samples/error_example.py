for p in Path(direcotory_to_read).glob('*.csv'):
    fileToRead = p.name
    
    # check filename PRK_R_001
    if recordtype_to_handle in fileToRead:

        print(f"Handling file {fileToRead}.")

        df = pd.read_csv(os.path.join(direcotory_to_read, fileToRead), delimiter='|', dtype={"guid": "string", "turtle": "string"})
        print(f'{len(df)} records to process.')

        good = 0
        bad = 0

        for row in df.itertuples(index=True, name='Pandas'):
            uuid = row.guid
            print(f"{row.Index} -- {uuid}")
            # print(f"turtle {row.turtle}.")
            
            response = api.update_ar_for_record(uuid, row.turtle)
            if response.status_code != 200:
                bad += 1
                #writer.writerow([uuid, result])
                logging.error(f'{uuid} -- {response.text}')
                #logging.error(f'Error creating (or updating) {uuid}. See the log')
            else :
                good += 1

        print(f"Done with file {fileToRead}. {good} succesfully parsed. {bad} errors.")

end = time.time()
print(f'Het verwerken van {direcotory_to_read} duurde:', (end-start), 'seconden')