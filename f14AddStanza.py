
#14. + stanza
  # Ici on ajoute les données de stanza
  # add stanza, , harmonise, test
dossier_temp = main_directory + "temp/"

# identifier les fichiers créés ajoutés au dossier (manuellement pr le moment)
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*600.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']

for f in range(len(theseFiles)):
  StanzaFile = theseFiles.iloc[f,0] 
  holdingFileIN = theseFiles.iloc[f,0].replace('600','903')
  holdingFileOut = theseFiles.iloc[f,0].replace('600','904')
  stanzaIn = pd.read_csv(StanzaFile, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  holdingData = pd.read_csv(holdingFileIN, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)

  QuadState = pd.merge(holdingData, stanzaIn, on="TokenID", how="left")
  QuadState.to_csv(holdingFileOut, sep="\t", encoding="UTF-8", index=False, header=True)
 # del(QuadState, holdingData, stanzaIn, theseFiles, holdingFileIN, holdingFileOut, StanzaFile)
