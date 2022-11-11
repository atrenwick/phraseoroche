
#19 + UD_Fmod_GSD 
# add UD_FMOD
dossier_UD = main_directory + "PourUD/"
temp_dossier = main_directory + "temp/"

## define input files 
UD_Fmod_Files = pd.DataFrame(glob.glob(dossier_UD +'/*MF-521.conllu'))
UD_Fmod_Files = UD_Fmod_Files.sort_values(0)
UD_Fmod_Files.reset_index(inplace=True, drop=True)
UD_Fmod_Files.columns=['Path']

## name columns
colNames = ['UDid_Fmod','udFORME_Fmod','udLEMME_Fmod','udUPOStag_Fmod','udXPOStag_Fmod','udFEATS_Fmod', 'udHEAD_Fmod','udDEPREL_Fmod','udDEPS_Fmod','udMISC_Fmod']

# f=1
# for f in range(11):
for f in range(len(UD_Fmod_Files)):
  # print(UD_of_Files.iloc[f,0])
  # get current file
  FilenameFROMudRaw = UD_Fmod_Files.iloc[f,0] 
  ## get current filename and change path to temp folder to name copy of input file
  FilenameFROMudRawShifted = FilenameFROMudRaw.replace('PourUD', 'temp')
  ## get current filename and change path and suffix to name tidy file
  FilenameFROMudTidy = FilenameFROMudRawShifted.replace("MF-521.conllu","-522.csv")
  # make name for final output file
  FilenameUDfinal = FilenameFROMudRawShifted.replace("MF-521.conllu","-523.csv")
# name for binder file
  BinderFileName = FilenameFROMudRawShifted.replace("MF-521.conllu","-500.csv")
# get name of file with 8 outputs
  holdingFileIN = FilenameFROMudRawShifted.replace("MF-521.conllu",'-908.csv')
## set name of output file with 9 outputs
  holdingFileOut = FilenameFROMudRawShifted.replace("MF-521.conllu",'-909.csv')
  
## read raw connl in as UTF8
  rawConnl = open(FilenameFROMudRaw, "rt", encoding="UTF8")
## tidy raw connl replacing numbered lines with blanks, this removes sentence numbers, etc
  cleanedConnl = rawConnl.read().replace('^#.+?$\n', '')
  rawConnl.close()

# create a nez file to save cleaned connl to, write to it, and close it  
  fin = open(FilenameFROMudTidy, "wt", encoding="UTF8")
  fin.write(cleanedConnl)
  fin.close()
  
# read in the clean connl data as a df
  UD_data = pd.read_csv(FilenameFROMudTidy, sep="\t", encoding="UTF8", names =   colNames )
  # UD_data = UD_data[['UDid_Fmod','udFORME_Fmod','udLEMME_Fmod', 'udUPOStag_Fmod','udXPOStag_Fmod','udFEATS_Fmod', 'udHEAD_Fmod','udDEPREL_Fmod', 'udDEPS_Fmod']]
  
  ## drop first 6 ros, which contain junk on line numbers, paras...
  UD_data_trimmed = UD_data.drop([0,1,2,3,4,5])
  # for and if loop to remove sent_id entries by setting them to blanks
  for i in range(len(UD_data_trimmed)-1):
    if UD_data_trimmed.iloc[i+1,0].startswith("# sent_id") == True:
       UD_data_trimmed.iloc[i+1,0] = ""
  
  # remove fullstops from words at end of sentences
  # for i in the df
  for i in range(len(UD_data_trimmed)-1):
    # if the dtype is string for col2, AND if the form column ends with a fullstop, replace fullstop with blank
    if pd.api.types.is_string_dtype(type(UD_data_trimmed.iloc[i,1])) == True:
      if UD_data_trimmed.iloc[i,1].endswith(".") == True:
         UD_data_trimmed.iloc[i,1] = UD_data_trimmed.iloc[i,1].replace('.','')
  
  # reset index with drop
  UD_dataOut = UD_data_trimmed.reset_index(drop=True)
  
  # read in binder file, and name columns
  udBinder = pd.read_csv(BinderFileName, sep="\t", encoding="UTF8")
  udBinder.columns = ['TokenID_udFMod','LGERM_Forme_udOF']

# get length of udBinder -1 ; need to be 1 less as need to look ahead to next row to see if contains .
  thisCount = len(udBinder)-1
  # print(thisCount, len(udBinder))
  # if udBinder.iloc[thisCount,1]== ".":
  #   udBinder = udBinder.drop(thisCount)
  
  # concatenate dfs
  UDoutput = pd.concat([udBinder,UD_dataOut], axis=1)
  
# save tidy ud data to csv
  UDoutput.to_csv(FilenameUDfinal, sep="\t", encoding="UTF8", index=False)
  # UDoutput = UDoutput.drop(['Forme'], axis=1)
  
## read in 7 state data
  holdingData = pd.read_csv(holdingFileIN, sep='\t',  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)

  ## merge(left join) 8state data with tidy UD FMod, matching on left TokenID equal to R TokenidFMod
  NineState = pd.merge(holdingData, UDoutput, left_on="TokenID", right_on = "TokenID_udFMod", how="left")
  # del(HexState, DeucalionDone)
  # save 8 state data to csv
  NineState.to_csv(holdingFileOut, encoding="UTF8", sep="\t", index=False)
  # NineState.shape

# del(UD_data, UD_data_trimmed, UD_dataOut, dossier_UD, holdingData, holdingFileIN, holdingFileOut, NineState)

