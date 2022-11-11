
#13. + TT

dossier_temp = main_directory + "temp/"

# identifier les fichiers créés ajoutés au dossier (manuellement pr le moment)
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*402.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
# for f =0:
for f in range(len(theseFiles)):
  ttTidyFile = theseFiles.iloc[f,0] 
  holdingFileIN = theseFiles.iloc[f,0].replace('402','902')
  holdingFileOut = theseFiles.iloc[f,0].replace('402','903')
  TT_tidy = pd.read_csv(ttTidyFile  , sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  TT_tidy.columns = ['FormeTTaf','XPOSttAF','LemmeTTaf', 'TokenID']  
  holdingData = pd.read_csv(holdingFileIN, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  TripleState = pd.merge(holdingData, TT_tidy, how = "left", on = 'TokenID')
  TripleState.to_csv(holdingFileOut, sep="\t", encoding="UTF-8", index=False, header=True)
  
  # del(ttTidyFile, TripleState,  TT_tidy, holdingData, holdingFileIN, holdingFileOut, theseFiles)
