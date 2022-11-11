#f20 

# dictionnary to map AUX to verb
HOPS_StanzaRemapDict = {"AUX" : 'VERB'}
# dictionary to map XPOS to UPOS
XposUposRemapDict = {"ADJind" : 'ADJ',"ADJord" : 'ADJ',"ADJpos" : 'ADJ',"ADJqua" : 'ADJ',"ADVgen" : 'ADV',"ADVint" : 'ADV',"ADVneg" : 'ADV',"ADVsub" : 'ADV',"AUX" : 'VERB',"CONcoo" : 'CCONJ',"CONsub" : 'SCONJ',"DETcar" : 'DET',"DETdef" : 'DET',"DETdem" : 'DET',"DETind" : 'DET',"DETndf" : 'DET',"DETpos" : 'DET',"DETrel" : 'DET',"INJ" : 'INTJ',"NOMcom" : 'NOUN',"NOMpro" : 'PROPN',"PRE" : 'ADP','PRE.DETdef':"ADP" ,"PROadv" : 'ADV',"PROdem" : 'PRON',"PROind" : 'PRON',"PROper" : 'PRON',"PROrel" : 'PRON',"VERcjg" : 'VERB',"VERinf" : 'VERB',"VERppa" : 'VERB',"VERppe" : 'VERB', "ADJcar" : 'ADJ', "ADJqual":"ADJ", "CON":"CCONJ", "DETint":"DET", "Nomcom":"NOUN", "NOMcompro":"NOUN","PROint":"PRON","PROord":"PRON","PROpos":"PRON","PROrel.PROper":"PRON","verbe":"VERB"}

## dictionary to map elided forms to elided forms with apostrophes
ElisionDictForms = {"c":'c\'', "d":'d\'', "j":'j\'', "l":'l\'', "m":'m\'', "n":'n\'', "s":'s\'', "t":'t\'', "q":'q\'', "qu":'qu\'', "jourd":'jourd\'', "C":'C\'', "D":'D\'', "J":'J\'', "L":'L\'', "M":'M\'', "N":'N\'', "S":'S\'', "T":'T\'', "Q":'Q\'', "Qu":'Qu\'',"jusqu":'jusqu\'', "JUSQU":'JUSQU\'' ,"QU":'QU\'', "Jourd":'Jourd\''}

##Col lists
# list of columns with AUX needing to be remapped to VERB
RemapAUXverbColumns = ['StanzaUPOS', 'HOPS_UPOS', 'udUPOStag', 'udUPOStag_Fmod', 'token_deucOF', 'treated_deucFmod']

## list of target columns into which XPOS tags will be mapped
XPOSremapColumns = ['LGERM_UPOS', 'UPOSttAF', 'DeucOF_UPOS', 'DeucEMF_UPOS','Deuc_FModUPOS']
## list of source columns of XPOS tags to map
XPOSremapColumnSources = ['LGERM_XPOS', 'XPOSttAF', 'POS_deucOF', 'POS_deucEMF','POS_deucFmod']

## list of all UPOS tag columns as well as reference info
POS_Cols = ['TokenID','LGERM_Forme','LGERM_code','LGERM_UPOS','HOPS_UPOS','UPOSttAF','StanzaUPOS', 'udUPOStag',   'udUPOStag_Fmod','DeucOF_UPOS', 'DeucEMF_UPOS', 'Deuc_FModUPOS', 'sentID']

# list of columns with formes for each tool and reference info
Formes_Cols = ['TokenID', 'LGERM_Forme', 'LGERM_code',  'FormeOut', 'HOPSforme','FormeTTaf','StanzaForme', 'treated_deucOF', 'token_deucFmod',  'token_deucEMF','treated_deucEMF', 'token_deucOF', 'LGERM_Forme_udOF_x', 'LGERM_Forme_udOF_y','treated_deucFmod',   'udFORME' , 'udFORME_Fmod']

## columns of forms from tools that don't deal with elision apostrophes, for comparison of input with output
ElisionCols = ['treated_deucOF', 'token_deucFmod', 'token_deucEMF', 'treated_deucEMF', 'token_deucOF', 'treated_deucFmod']

  ##  names of columns containing lemmas. These are used to create the DF
LemmesCols = ['TokenID', 'LGERM_Forme', 'LGERM_code', 'LGERM_lemme','LemmeTTaf','lemma_deucEMF', 'lemma_deucOF', 'lemma_deucFmod','udLEMME_Fmod']
## names of columns containing lemmas to be used as list of columns to drop once lemmes have been tidied.
LemmesColsToDrop = [ 'LGERM_lemme','LemmeTTaf','lemma_deucEMF', 'lemma_deucOF', 'lemma_deucFmod','udLEMME_Fmod']

###### end of declatations, start of loop:
# f=0
## define directories and input files
temp_dossier = main_directory + "temp/"
NineStateFiles = pd.DataFrame(glob.glob(temp_dossier +'*909.csv'))
NineStateFiles = NineStateFiles.sort_values(0)
NineStateFiles.reset_index(inplace=True, drop=True)
NineStateFiles.columns=['Path']

# for each file, create name for output excel file
for f in range(len(NineStateFiles)):
  ninestateFile = NineStateFiles.iloc[f,0] 
  ExcelOutputName = ninestateFile.replace('909.csv','neufState.xlsx')
  
  # import ninestateFile
  NineState = pd.read_csv(f'{PathHead}/test2/temp/AF01-909.csv', encoding = "UTF8", sep="\t", skip_blank_lines=False, na_filter=True, low_memory=False)
  # NineState.columns
  # supprimer les colonnes non-nécessaires
  # NineState = NineState.drop(NineState.columns[[0,6,7,9, 10, 12, 16, 17, 20,21, 25, 47, 55 ]], axis=1)
  
  ## remap AUX to VERB pour UD, Stanza, HOPS pour ne pas les distinguer, using list comprehension and map
  for i in range(len(RemapAUXverbColumns)):
    currentColumn =RemapAUXverbColumns[i]
    NineState[currentColumn] =  NineState[currentColumn].map(HOPS_StanzaRemapDict).fillna(NineState[currentColumn])

  # remap XPOS to UPOS with list comprehension and map
  for i in range(len(XPOSremapColumns)):
    currentColumn =XPOSremapColumns[i]
    NineState[currentColumn] =  NineState[XPOSremapColumnSources[i]].map(XposUposRemapDict).fillna(NineState[XPOSremapColumnSources[i]])
  
  
  #Créer df des tags UPOS
  POS_df    = NineState[POS_Cols] 
  ## set check to true if lgermcode is punctuation or if all columns give same POS ; else error
  POS_df['Check'] = ['True' if ((POS_df.iloc[i,2] == 'ponctuation') | (POS_df.iloc[i,3] == POS_df.iloc[i,4] == POS_df.iloc[i,5] == POS_df.iloc[i, 6]== POS_df.iloc[i, 7]== POS_df.iloc[i, 8] == POS_df.iloc[i, 9] == POS_df.iloc[i, 10] == POS_df.iloc[i, 11])) else "ERROR" for i in range(len(POS_df))]
  # POS_df['Check'].value_counts() # 23k error, 37k T
  # LC to add true if all POS agree for non-mod FR tools, get value counts
  # POS_df['Check_NoMod'] = ['True' if ((POS_df.iloc[i,2] == 'ponctuation') | (POS_df.iloc[i,3] == POS_df.iloc[i,4] == POS_df.iloc[i,5] == POS_df.iloc[i, 6]== POS_df.iloc[i, 7]==  POS_df.iloc[i, 9] == POS_df.iloc[i, 10] )) else "ERROR" for i in range(len(POS_df))]
  # POS_df['Check_NoMod'].value_counts() ## 41kT, 19kF
  # 
  # LC to add OFOnly if OF only agree and prive value counts
  # POS_df['OF_only'] = ['True' if ((POS_df.iloc[i,2] == 'ponctuation') | (POS_df.iloc[i,3] == POS_df.iloc[i,4] == POS_df.iloc[i,5] == POS_df.iloc[i, 6]== POS_df.iloc[i, 7]==  POS_df.iloc[i, 9] )) else "ERROR" for i in range(len(POS_df))]
  # POS_df['OF_only'].value_counts() ## 47kT, 14kF

  # create df of formes, to verify form output by each tool = input and that these correspond
  Formes_df = NineState[Formes_Cols]
  # LC to add TRUE if forme is punctuation OR if all forms are same
  Formes_df['Check'] = ['True' if ((Formes_df.iloc[i,2] == 'ponctuation')  |(Formes_df.iloc[i,3] == Formes_df.iloc[i,4] == Formes_df.iloc[i,5] == Formes_df.iloc[i, 6]== Formes_df.iloc[i, 7]== Formes_df.iloc[i, 8] == Formes_df.iloc[i, 9] == Formes_df.iloc[i, 10] == Formes_df.iloc[i, 11] == Formes_df.iloc[i, 12] == Formes_df.iloc[i, 13] == Formes_df.iloc[i, 14] == Formes_df.iloc[i, 15] == Formes_df.iloc[i, 16])) else "ERROR" for i in range(len(Formes_df))]
  # 
  ## use LC and map to map elided forms what does this achieve, since elisions shoudl be taken care of individually plus haut ?
  for i in range(len(ElisionCols)):
    currentColumn =ElisionCols[i]
    Formes_df[currentColumn] = Formes_df[currentColumn].map(ElisionDictForms).fillna(Formes_df[currentColumn])
    # POS_df.to_csv(main_directory + "AF03_POS.csv", sep="\t", encoding= "UTF8")

  # create new DF containing lemma info
  Lemmes_df= NineState[LemmesCols]   
  
  # loop over columns with lemmes, replacing and lowering strings ; range must start at 3 to not modify tokenIDs, etc ; name new col by adding tidy to name of old column, and tidy lemme by lower and removing digits (notably present in LGERM lemmes)
  for i in range(3, len(LemmesCols)):
    NewName = (LemmesCols[i]) + "_tidy"
    Lemmes_df[NewName] = Lemmes_df[LemmesCols[i]].str.lower().str.replace('\d+','')
    
  # sent tidy data to new df and drop raw lemma cols
  LemmesTidyDF = Lemmes_df.drop(labels = LemmesColsToDrop, axis=1)
  # LemmesTidyDF.columns

# add true if all lemmes agree OR if col2 is punctuation; print value counts
  LemmesTidyDF['Check'] = ['True' if ((LemmesTidyDF.iloc[i,2] == 'ponctuation') | (LemmesTidyDF.iloc[i,3] == LemmesTidyDF.iloc[i,4] ==LemmesTidyDF.iloc[i,5] == LemmesTidyDF.iloc[i,6] == LemmesTidyDF.iloc[i,7] == LemmesTidyDF.iloc[i,8] )) else "ERROR" for i in range(len(LemmesTidyDF))]
  # LemmesTidyDF['Check'].value_counts() #33k T v 27kF

  # LemmesTidyDF.to_csv(main_directory + "AF03lemmes.csv", sep="\t", encoding= "UTF8")
  # example LC to add custom value to output ; here, basis is that all tools agree save one
  # LemmesTidyDF['No_DeucOF'] = ['True' if ((LemmesTidyDF.iloc[i,2] == 'ponctuation') | (LemmesTidyDF.iloc[i,3] == LemmesTidyDF.iloc[i,4] ==LemmesTidyDF.iloc[i,5] ==  LemmesTidyDF.iloc[i,7] == LemmesTidyDF.iloc[i,8] )) else "ERROR" for i in range(len(LemmesTidyDF))]
  # # LemmesTidyDF['No_DeucOF'].value_counts() # 33T v 27F
 
  ## create writer object, define name of first sheet; and define formats, then define shape of worksheet, and finally conditional formats to highlight rows if cols M, N or O contian error:
  writer = pd.ExcelWriter(f'{ExcelOutputName}', engine='xlsxwriter')
  ## df to sent to excel sheet
  POS_df.to_excel(writer, sheet_name='POS', index=False)
  workbook = writer.book
  worksheet = writer.sheets['POS']
  format1 = workbook.add_format({'bg_color': 'yellow'})
  format2 = workbook.add_format({'bg_color': 'green'})
  format3 = workbook.add_format({'bg_color': 'blue'})
  
  (max_row, max_col) = POS_df.shape
  worksheet.conditional_format(1, 0, max_row, max_col, {'type': 'formula', 'criteria': '$M2="ERROR"', 'format':format1}) ## 
  worksheet.conditional_format(1, 0, max_row, max_col, {'type': 'formula', 'criteria': '$N2="ERROR"', 'format':format2}) ## 
  worksheet.conditional_format(1, 0, max_row, max_col, {'type': 'formula', 'criteria': '$O2="ERROR"', 'format':format3}) ## 
  ## df to send to excel worksheet, with shape, name, and criteria for conditional format to highlight rows with error
  LemmesTidyDF.to_excel(writer, sheet_name='Lemmes', index=False)
  worksheet = writer.sheets['Lemmes']
  (max_row, max_col) = Lemmes_df.shape
  worksheet.conditional_format(1, 0, max_row, max_col, {'type': 'formula', 'criteria': '$J2="ERROR"', 'format':format1}) ## 3 agreements
  worksheet.conditional_format(1, 0, max_row, max_col, {'type': 'formula', 'criteria': '$K2="ERROR"', 'format':format2}) ## 3 agreements
  ### save excel
  writer.save()

   
del(max_col, max_row, XPOSremapColumns, XPOSremapColumnSources, format1, format2, format3, Formes_Cols, LemmesCols, LemmesColsToDropHOPS_StanzaRemapDict, XposUposRemapDict, ElisionDictForms, RemapAUXverbColumns, writer, worksheet, workbook, ExcelOutputName, currentColumn, POS_Cols)

