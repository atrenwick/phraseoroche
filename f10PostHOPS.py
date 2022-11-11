
#- need checker to test if needed based on length of these_files
#10. HOPS post.


main_directory = f'{PathHead}/Dropbox/IRGA Phraseo 13-18/Pipeline/refresh/test2/'
dossier_temp = main_directory + "temp/"
HOPSdataColumns = ['hopsID', 'HOPSforme','TokenID']
allHOPScolumns =['hopsID', 'HOPSforme', 'TokenID', 'HOPS_UPOS', 'col4','col5','HOPSdepOn','HOPSrole','col8', 'col9']

## get list of files for which a 211 exists - those for which sentences had to be skipped
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*211.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']

i=0
for i in range(len(theseFiles)):
  skippedFileName = theseFiles.iloc[i,0]
  pairFileName = theseFiles.iloc[i,0].replace('211','202')
  skippedFile = pd.read_csv(skippedFileName, sep="\t", low_memory=False, skip_blank_lines=False, na_filter=True, encoding="UTF8", names=HOPSdataColumns)
  pairFile = pd.read_csv(pairFileName  , sep='\t', header = None, skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False, names=allHOPScolumns)

  HOPSall  = pd.concat([pairFile, skippedFile], axis = 0)
  HOPSall = HOPSall.sort_values('TokenID')
  HOPSallSaveName =  theseFiles.iloc[i,0].replace('211','203')
  HOPSall.to_csv(HOPSallSaveName, sep="\t", encoding = "UTF8", index=False, header=False)

# del(file_name, fileLGERM, holdingFile, HOPSdataColumns, HOPSinputFilename, HOPSall, HOPSallSaveName, skippedFileName, HOPSoutputFilename, pairFile, pairFileName, SaveName, SaveValue, skippedFile)


