
# 15. + DeucEMF 

dossier_Deuc = main_directory + 'PourDeucalion'
  # identifier les fichiers créés ajoutés au dossier (manuellement pr le moment)
emfFiles = pd.DataFrame(glob.glob(dossier_Deuc +'/*310-out.txt'))
emfFiles = emfFiles.sort_values(0)
emfFiles.reset_index(inplace=True, drop=True)
emfFiles.columns=['Path']
# f=1
for f in range(len(emfFiles)):
  print(emfFiles.iloc[f,0])
  DeucEMFfile = emfFiles.iloc[f,0] 
  DeucEMFdata = pd.read_csv(DeucEMFfile, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  DeucBinderName = DeucEMFfile.replace('EMF-310-out.txt','EMF-310binder.csv').replace(dossier_Deuc,dossier_temp)
  emfBinder = pd.read_csv(DeucBinderName, encoding="UTF8", low_memory=False, sep="\t", header=None)
  emfBinder.columns = ['TokenID', 'LGERM_Forme']
  print(len(DeucEMFdata) , len(emfBinder))
  emfDone = pd.concat([emfBinder, DeucEMFdata], axis = 1)
  emfDone = emfDone.drop([ 'LGERM_Forme'], axis = 1)
  emfDone.columns = ['TokenID', 'token_deucEMF', 'lemma_deucEMF', 'POS_deucEMF', 'morph_deucEMF', 'treated_deucEMF']
  
  ## when lengths not even, keep all cols...
# check for punct lines, such as –	–	CONcoo	MORPH=empty	–
  # emfDone.to_csv((dossier_Deuc + 'comparer.csv'), sep="\t", encoding="UTF-8", index=False, header=True)
## if each of last lines are nan, is not problem
  # (emfDone.iloc[len(emfDone)-1,1] is np.nan) & (emfDone.iloc[len(emfDone)-1,2] is np.nan) & (emfDone.iloc[len(emfDone)-2,1] ==  emfDone.iloc[len(emfDone)-2,2]) == True:
  
  holdingFileIN = DeucBinderName.replace('EMF-310binder','904')
  holdingFileOut = holdingFileIN.replace('904','905')
  holdingData = pd.read_csv(holdingFileIN, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  QuinState = pd.merge(holdingData, emfDone, on="TokenID", how="left")
  QuinState.to_csv(holdingFileOut, sep="\t", encoding="UTF-8", index=False, header=True)
  QuinState.shape
  # del(DeucEMFdata, DeucEMFfile, QuinState, emfDone, emfFiles)
