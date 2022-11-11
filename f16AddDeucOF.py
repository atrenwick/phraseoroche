
# 16. + DeucOF 
dossier_Deuc = main_directory + 'PourDeucalion'
  # identifier les fichiers créés ajoutés au dossier (manuellement pr le moment)
ofFiles = pd.DataFrame(glob.glob(dossier_Deuc +'/*320-out.txt'))
ofFiles = ofFiles.sort_values(0)
ofFiles.reset_index(inplace=True, drop=True)
ofFiles.columns=['Path']
# f=0
for f in range(len(ofFiles)):
  print(ofFiles.iloc[f,0])
  DeucOFfile = ofFiles.iloc[f,0] 
  DeucOFdata = pd.read_csv(DeucOFfile, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  DeucBinderName = DeucOFfile.replace('OF-320-out.txt','OF-320binder.csv').replace(dossier_Deuc,dossier_temp)
  ofBinder = pd.read_csv(DeucBinderName, encoding="UTF8", low_memory=False, sep="\t", header=None)
  ofBinder.columns = ['TokenID', 'LGERM_Forme']
  print(len(DeucOFdata) , len(ofBinder))
  ofDone = pd.concat([ofBinder, DeucOFdata], axis = 1)
  ofDone = ofDone.drop([ 'LGERM_Forme'], axis = 1)
  ofDone.columns = ['TokenID', 'token_deucOF', 'lemma_deucOF', 'POS_deucOF', 'morph_deucOF', 'treated_deucOF']
  
  ## when lengths not even, keep all cols...
# check for punct lines, such as –	–	CONcoo	MORPH=empty	–
  # emfDone.to_csv((dossier_Deuc + 'comparer.csv'), sep="\t", encoding="UTF-8", index=False, header=True)
## if each of last lines are nan, is not problem
  # (emfDone.iloc[len(emfDone)-1,1] is np.nan) & (emfDone.iloc[len(emfDone)-1,2] is np.nan) & (emfDone.iloc[len(emfDone)-2,1] ==  emfDone.iloc[len(emfDone)-2,2]) == True:

  holdingFileIN = DeucBinderName.replace('OF-320binder','905')
  holdingFileOut = holdingFileIN.replace('905','906')
  holdingData = pd.read_csv(holdingFileIN, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  HexState = pd.merge(holdingData, ofDone, on="TokenID", how="left")
  HexState.to_csv(holdingFileOut, sep="\t", encoding="UTF-8", index=False, header=True)
  HexState.shape
  # del(DeucOFdata, DeucOFfile, HexState, ofBinder, ofDone, ofFiles)
