# 18. + UD_OF
dossier_UD = main_directory + "PourUD/"
temp_dossier = main_directory + "temp/"

UD_of_Files = pd.DataFrame(glob.glob(dossier_UD +'/*OF-511.conllu'))
UD_of_Files = UD_of_Files.sort_values(0)
UD_of_Files.reset_index(inplace=True, drop=True)
UD_of_Files.columns=['Path']

colNames = ['UDid','udFORME','udLEMME','udUPOStag','udXPOStag','udFEATS', 'udHEAD','udDEPREL','udDEPS','udMISC']

# f=1
# for f in range(11):
for f in range(len(UD_of_Files)):
  # print(UD_of_Files.iloc[f,0])
  FilenameFROMudRaw = UD_of_Files.iloc[f,0] 
  FilenameFROMudRawShifted = FilenameFROMudRaw.replace('PourUD', 'temp')
  FilenameFROMudTidy = FilenameFROMudRawShifted.replace("OF-511.conllu","-512.csv")
  FilenameUDfinal = FilenameFROMudRawShifted.replace("OF-511.conllu","-513.csv")
  BinderFileName = FilenameFROMudRawShifted.replace("OF-511.conllu","-500.csv")
  holdingFileIN = FilenameFROMudRawShifted.replace("OF-511.conllu",'-907.csv')
  holdingFileOut = FilenameFROMudRawShifted.replace("OF-511.conllu",'-908.csv')
  
  rawConnl = open(FilenameFROMudRaw, "rt", encoding="UTF8")
  cleanedConnl = rawConnl.read().replace('^#.+?$\n', '')
  rawConnl.close()
  
  fin = open(FilenameFROMudTidy, "wt", encoding="UTF8")
  fin.write(cleanedConnl)
  fin.close()
  
  UD_data = pd.read_csv(FilenameFROMudTidy, sep="\t", encoding="UTF8", names =   colNames )
  # UD_data = UD_data[['UDid','udFORME','udUPOStag','udXPOStag','udFEATS', 'udHEAD','udDEPREL']]
  
  UD_data_trimmed = UD_data.drop([0,1,2,3,4,5])
  # remove sent_id entries
  for i in range(len(UD_data_trimmed)-1):
    if UD_data_trimmed.iloc[i+1,0].startswith("# sent_id") == True:
       UD_data_trimmed.iloc[i+1,0] = ""
  
  # remove fullstops from words at end of sentences
  for i in range(len(UD_data_trimmed)-1):
    if pd.api.types.is_string_dtype(type(UD_data_trimmed.iloc[i,1])) == True:
      if UD_data_trimmed.iloc[i,1].endswith(".") == True:
         UD_data_trimmed.iloc[i,1] = UD_data_trimmed.iloc[i,1].replace('.','')
  
  UD_dataOut = UD_data_trimmed.reset_index(drop=True)
  
  udBinder = pd.read_csv(BinderFileName, sep="\t", encoding="UTF8")
  udBinder.columns = ['TokenID_udOF','LGERM_Forme_udOF']
  
  
  thisCount = len(udBinder)-1
  # print(thisCount, len(udBinder))
  # if udBinder.iloc[thisCount,1]== ".":
  #   udBinder = udBinder.drop(thisCount)
  
  UDoutput = pd.concat([udBinder,UD_dataOut], axis=1)
  
  UDoutput.to_csv(FilenameUDfinal, sep="\t", encoding="UTF8", index=False)
  # UDoutput = UDoutput.drop(['Forme'], axis=1)
  
  holdingData = pd.read_csv(holdingFileIN, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)

  ## now can join to hept
  EightState = pd.merge(holdingData, UDoutput, left_on="TokenID", right_on = "TokenID_udOF", how="left")

  EightState.to_csv(holdingFileOut, encoding="UTF8", sep="\t", index=False)

 # del(HeptState, FilenameFROMudRaw, FilenameFROMudRawShifted, FilenameFROMudTidy, fin)
# del(BinderFileName, cleanedConnl, EightState, UD_of_Files)
