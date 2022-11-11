#4. Export for HOPS old version
#### ancienne version

main_directory = '{PathHead}}Pipeline/refresh/test2/'
dossier_temp = main_directory + "temp/"

# identifier les fichiers créés par f2 dans le dossier_temp
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
myResults = pd.DataFrame()
#f=12
HOPS_MakerResults= pd.DataFrame()
for f in range(len(theseFiles)):
  startTime = time.time()
  # file_name = LGERMtidyName
  # load iteration of file
  file_name = theseFiles.iloc[f,0] 
  currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
  # nameForhopsOutput = dossier_temp + '/' + currentCode + "-201.csv"
  # nameForhopsExcluded = dossier_temp + '/' + currentCode + "-211.csv"
  counter = 0
  # calculate names for included and excluded file
  nameForhopsInclu = dossier_temp + '/' + currentCode + "-test201a.txt"
  nameForhopsEXCLU = dossier_temp + '/' + currentCode + "-test211a.csv"
  # laod data
  step2 = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)
  # restrict to 4 cols
  HOPS_initial =   step2[['hopsID', 'FormeOut','TokenID', 'sentID']]
  # calculate HOPS ids, TokenIDs and FormeOut selon nécessités de HOPS 
  # option to calculate inidividually
  # hopsOutCorrect= pd.DataFrame([HOPS_initial.iloc[j,0] if HOPS_initial.iloc[j,0]!= 0 else "" for j in range(len(HOPS_initial))])
  # FormeOutCorrect= pd.DataFrame([HOPS_initial.iloc[j,1] if HOPS_initial.iloc[j,0]!= 0 else "" for j in range(len(HOPS_initial))])
  # TokenIDCorrect= pd.DataFrame([HOPS_initial.iloc[j,2] if HOPS_initial.iloc[j,0]!= 0 else "" for j in range(len(HOPS_initial))])
  # sentID= pd.DataFrame([HOPS_initial.iloc[j,3]  for j in range(len(HOPS_initial))])
  
  HOPS_tout= pd.DataFrame([(HOPS_initial.iloc[j,0],HOPS_initial.iloc[j,1],HOPS_initial.iloc[j,2],HOPS_initial.iloc[j,3]) if HOPS_initial.iloc[j,0]!= 0 else "" for j in range(len(HOPS_initial))])
  
  HOPS_tout['Owner'] = pd.DataFrame([HOPS_initial.iloc[j,3] if (HOPS_initial.iloc[j,3]).dtype == "int64" else HOPS_initial.iloc[j-1,3] for j in range(len(HOPS_initial))])

  # mettre données correctes dans un df
  # HOPS_tout = pd.concat([hopsOutCorrect, FormeOutCorrect, TokenIDCorrect, sentID], axis=1)
  # nommer ses colonnes
  HOPS_tout.columns =   ['hopsID', 'FormeOut','TokenID', 'sentID', 'Owner']
  ## on a toutes les données, il faut juste exclure les cas où les phrases sont trop longues.
  
  DataHOPSexclu = pd.DataFrame()
  # créer un DF pour stocker les données sur la longueur des phrases
  resultFrame = pd.DataFrame()
  # for range of 1 to max of sentID +1 to use last real value
  myRange = range(1, int(HOPS_tout['sentID'].max()+1 ))
  # for each value of s, soit la numérotation de la phrase, faites comme qui suit :
  # for s in range(1, int(HOPS_tout['sentID'].max()+1 )):
  for s in myRange:
    if s in set(HOPS_tout['sentID']):
      segment = HOPS_tout.loc[HOPS_tout['Owner']==s]  ## délimiter à la phrase
      # if   len(segment) > 1:
        ### sentence of null length??
      Bound = len(segment)   ### trouver la longueur de la phrase 
      Word = segment.iloc[(len(segment)-2),2]  # extraire la numérotation du dernier mot de la phrase
      result = {'Sent':[s], 'max':[Bound], 'final':[Word]}  ## créer un DF avec ces résultats
      resultFrame = resultFrame.append(pd.DataFrame(result))  # ajouter aux résultats existants
    else:
      counter = counter +1
      continue
    
    # déclarer que toute phrase de longueur supérieur à 460 mots == problem
  problems = resultFrame.loc[resultFrame['max'] > 460]
  problems = list(problems['Sent'])  #list of items in problems 

## above runs, but removes blank lines needed by HOPS
###  
  # my range, from above, is list of all valid sentence numbers. Problems is list of sentences that are too long. Reste is the list of all sentences without problems
  reste = list(set(list(myRange)) - set(problems))
  
  # LC to get df of all cases where i,3 == sentID is not in reste
  DataHOPSinclus = pd.DataFrame(  [(HOPS_tout.iloc[i,0],HOPS_tout.iloc[i,1],HOPS_tout.iloc[i,2], HOPS_tout.iloc[i,3]) for i in range(len(HOPS_tout)) if (HOPS_tout.iloc[i, 4]  in reste)])
  # name columns
  columns = ['hopsID', 'FormeOut','TokenID', 'sentID']
  
  DataHOPSinclus['hopsID'] = DataHOPSinclus['hopsID'].astype('Int64')
  DataHOPSinclus['TokenID'] = DataHOPSinclus['TokenID'].astype('Int64')
  
  # LC to get df of cases where [i,3] == sentID IN reste == excluded sentences
  if len(problems) > 0:
    DataHOPSexclu = pd.DataFrame( [(HOPS_tout.iloc[i,0],HOPS_tout.iloc[i,1],HOPS_tout.iloc[i,2], HOPS_tout.iloc[i,3]) for i in range(len(HOPS_tout)) if (HOPS_tout.iloc[i, 4] not in reste)])
    DataHOPSexclu.columns = ['hopsID', 'FormeOut','TokenID', 'sentID']
    DataHOPSexclu['hopsID'] = DataHOPSexclu['hopsID'].astype('Int64')
    DataHOPSexclu['TokenID'] = DataHOPSexclu['TokenID'].astype('Int64')
    

  # garde-fou : vérifie que la longueur des deux parties == la longueur du tout
  print(currentCode, len(DataHOPSinclus), '+', len(DataHOPSexclu), '+',  '==', len(HOPS_tout))
  
  # exporter les données de manière homogène, comme les veut HOPS
  DataHOPSinclus = DataHOPSinclus.fillna(0)


  ForHOPSoutputINC = DataHOPSinclus[['hopsID', 'FormeOut','TokenID']]
  DataHOPSinclus.to_csv(nameForhopsInclu, index = False, header = False, sep="\t", encoding='UTF8') # output 
  if len(problems) > 0:
    ForHOPSoutputExclu = DataHOPSexclu[['hopsID', 'FormeOut','TokenID']]
    ForHOPSoutputExclu.to_csv(nameForhopsEXCLU, index = False, header = False, sep="\t", encoding='UTF8')
  # 
  endTime = time.time()
  result = {'code':[currentCode], 'wCount':[len(HOPS_tout)], 'start':[startTime],'end':[endTime], 'delta':[endTime - startTime], 'speed':[len(HOPS_tout)/(endTime - startTime) ]}
  result = pd.DataFrame(result)
  HOPS_MakerResults = HOPS_MakerResults.append(result)
print(HOPS_MakerResults)

# del(main_directory, dossier_temp, theseFiles,file_name, currentCode, nameForhops, step2, outputdf,   hopsOutCorrect, j,   FormeOutCorrect, TokenIDCorrect,  HOPSoutFinal)

# del(LGERMdelimiter, counter, closeG, CanonicForms, tidyEncoding, thisPunct, startTime, endTime)

step2.to_csvf'{PathHead}'/refresh/test2/af01step2.csv", encoding="UTF8", sep="\t", index=False, header=True)

