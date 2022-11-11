
# 17. + DeucFMod 
dossier_Deuc = main_directory + 'PourDeucalion'
  # identifier les fichiers créés ajoutés au dossier (manuellement pr le moment)
FmodFiles = pd.DataFrame(glob.glob(dossier_Deuc +'/*330-out.txt'))
FmodFiles = FmodFiles.sort_values(0)
FmodFiles.reset_index(inplace=True, drop=True)
FmodFiles.columns=['Path']
# f=0
for f in range(len(FmodFiles)):
  print(FmodFiles.iloc[f,0])
  DeucFmodFile = FmodFiles.iloc[f,0] 
  DeucFmodData = pd.read_csv(DeucFmodFile, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  DeucBinderName = DeucFmodFile.replace('FMod-330-out.txt','OF-320binder.csv').replace(dossier_Deuc,dossier_temp)
  FModBinder = pd.read_csv(DeucBinderName, encoding="UTF8", low_memory=False, sep="\t", header=None)
  FModBinder.columns = ['TokenID', 'LGERM_Forme']
  print(len(DeucFmodData) , len(FModBinder))
  FmodDone = pd.concat([FModBinder, DeucFmodData], axis = 1)
  FmodDone = FmodDone.drop([ 'LGERM_Forme'], axis = 1)
  FmodDone.columns = ['TokenID', 'token_deucFmod', 'lemma_deucFmod', 'POS_deucFmod', 'morph_deucFmod', 'treated_deucFmod']
  holdingFileIN = DeucBinderName.replace('OF-320binder','906')
  holdingFileOut = holdingFileIN.replace('906','907')
  holdingData = pd.read_csv(holdingFileIN, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  HeptState = pd.merge(holdingData, FmodDone, on="TokenID", how="left")
  HeptState.to_csv(holdingFileOut, sep="\t", encoding="UTF-8", index=False, header=True)

# del(holdingFileIN, FModBinder, FmodDone, FmodFiles, DeucBinderName, dossier_Deuc, DeucFmodData, DeucFmodFile)

