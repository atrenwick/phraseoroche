
#11. Combiner LGERM + HOPS
# --currently will work with incomplete HOPS files caused by s length > 250
dossier_temp = main_directory + "/temp/"

theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*104.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
# f=0
for f in range(len(theseFiles)):
# for f in range(0,1):
  fileLGERM = theseFiles.iloc[f,0] 
  fileHOPS = theseFiles.iloc[f,0].replace('104.csv','203.csv') 
  
  holdingFile = theseFiles.iloc[f,0].replace('104','902')
  
  LGERM_all = pd.read_csv(fileLGERM, sep="\t", low_memory=False, header=None)
  LGERM_all.columns =['TokenID', 'LGERM_Forme', 'LGERM_XPOS', 'LGERM_lemme', 'LGERM_code','d','f','Pp','Para', 'TokenChap', 'FormeOut','hopsID','sentID'] 
  HOPS = pd.read_csv(fileHOPS  , sep='\t', header = None, skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  HOPS.columns =['hopsID', 'HOPSforme', 'TokenID', 'HOPS_UPOS', 'col4','col5','HOPSdepOn','HOPSrole','col8', 'col9'] 

  currentState = pd.merge(LGERM_all, HOPS, how="left", on=["TokenID"]) ## merge
  DoubleState = currentState.sort_values('TokenID')
  DoubleState = DoubleState.reset_index(drop=True)
  DoubleState.to_csv(holdingFile, sep="\t", encoding = "UTF8", index=False, header=True)
  # del(currentState, HOPS, LGERM_all, fileLGERM, fileHOPS, holdingFile)
  
