#7. Export UD cette fonction créera les fichiers que UDPipe prendra comme entrée, ainsi que le fichier binder qui permettra de réaligner la sortie de ces traitements.

## ######## create for UDa,b
  ## set output filenames
### define elements for name builder - lang states for OF and MF, extensions for each
import numpy as np
lState= ['OF','MF']
udExtensions = ["-511.txt", "-521.txt"]
main_directory = f'{PathHead}'/refresh/test2/'
dossier_UD = main_directory + 'PourUD/'
dossier_temp = main_directory + "temp/"
# get input file list as df
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
# file_name = LGERMtidyName
# identifier les fichiers créés par f1 dans le dossier_temp
## make empty df to store results
UDMakerResults = pd.DataFrame()
for f in range(len(theseFiles)):
  startTime = time.time()
# get input filename
  file_name = theseFiles.iloc[f,0] 
# extract code
  currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
# get name of file containing body of text
  BodyFile = file_name
# get name of file containing punctuation only
  PunctFile = file_name.replace('103','102')
# name for file containing all
  nameForLGERMallNew = file_name.replace('103','104')
# name and path for binder file
  forUDbinder = dossier_temp + '/' + currentCode + "-500.csv"
# name and path for raw output file
  forUDRaw = dossier_temp + '/' + currentCode + "-501.txt"
  
  ## get tidy LGERM
  LGERMBody = pd.read_csv(file_name, sep="\t", low_memory=False)
  # name columns
  LGERMBody.columns =['TokenID', 'LGERM_Forme', 'LGERM_XPOS', 'LGERM_lemme', 'LGERM_code','d','f','Pp','Para', 'TokenChap', 'FormeOut','hopsID','sentID'] # are these in the output ? can save them there, non ???
  
  # charger le fichier contenant la ponctuation
  LGERM_Ponct = pd.read_csv(PunctFile, sep=",", low_memory=False)
## add col names
  LGERM_Ponct.columns =['TokenID', 'LGERM_Forme', 'LGERM_XPOS', 'LGERM_lemme', 'LGERM_code','d','f','Pp','Para', 'TokenChap', 'FormeOut'] 
## concat dfs
  LGERM_all = pd.concat([LGERMBody, LGERM_Ponct], axis = 0)
# sort by tokenID in place
  LGERM_all.sort_values(['TokenID'], inplace=True)
# save whole file as output as csv 
  ## save LGERM_all
  LGERM_all.to_csv(nameForLGERMallNew, sep="\t", encoding = "UTF8", index=False, header=False)
  
  #sort and send Forme column to forUDPipe
  LGERM_all = LGERM_all.sort_values(by="TokenID")  
  forUDpipe = LGERMBody[['LGERM_Forme']]
# for each row, if the form is a fullstop, add the fullstop to the previous word
  for p in range(len(forUDpipe)):
    if forUDpipe.iloc[p,0] ==".":
      forUDpipe.iloc[(p-1),0] = forUDpipe.iloc[(p-1),0] + "."
  
  # if fullstop is only char, replace with np.nan
  forUDpipe = forUDpipe.replace({".":np.nan}) 
  ## write raw data as csv
  forUDpipe.to_csv(forUDRaw ,sep="\t", encoding = "UTF8", index=False, header=False)
## isolate data to make binder  and save csv
  udBinder = LGERMBody[['TokenID', 'LGERM_Forme']]
  udBinder.to_csv(forUDbinder, sep="\t", encoding = "UTF8", index=False, header=True)

  ## read in raw data just written, as file and tidy by replacing entries where open+close quote marks are the entire string, then close raw file
  fin = open(forUDRaw, "rt", encoding = "UTF8")
  data = fin.read().replace('""', '')
  fin.close()
  
  ## take tidy  data, and save for each state and extension in lists
  for q in range(len(lState)): ### lState is list of language states for which models exist
    currentName = dossier_UD +   currentCode  + lState[q] + udExtensions[q]
    ## make name from path, text code, tagger, language state, and extension
    fin = open(currentName, "wt",  encoding = "UTF8")
    fin.write(data)
    fin.close()
  # os.remove(FilenameForUDRaw) ## delete raw file optional autodelete of raw file
  endTime = time.time()
## print speed results and make df of same
  # result = {'code':[currentCode], 'wCount':[len(udBinder)], 'start':[startTime],'end':[endTime], 'delta':[endTime - startTime], 'speed':[len(udBinder)/(endTime - startTime) ]}
  # result = pd.DataFrame(result)
  ## append results
  # UDMakerResults = UDMakerResults.append(result)
#     
# print(UDMakerResults)
# ## runs at abotu 29k/s

# del(BodyFile, currentCode, currentName, data, dossier_temp, dossier_UD, f, file_name, fin, forUDbinder, forUDpipe, forUDRaw, LGERM_all, LGERM_Ponct, LGERMBody, lState, nameForLGERMallNew, p, PunctFile, q, theseFiles, udBinder, udExtensions)
# set target link to run udpipe on server
targetLink = "https://lindat.mff.cuni.cz/services/udpipe/run.php"
# open twb (2, window would be 1?) with link to run by uploading file
webbrowser.open(targetLink, new=2)

## display message:
## set model = 2.10
## old_french if 510, french_gsd if 520
## advanced options : UDPipe version : 2 (default)
##                    Input : VERTICAL
##                   Tokenizer : Save Toekn Ranges (default)
# input file > Load file : choose 510, 610
# Click 'Process Input'
# when finihsed : Save OutputFile : move from downloads folder to target folder, AND give same name (copy + paste), add out
## 

