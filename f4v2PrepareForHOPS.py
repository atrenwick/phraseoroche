#Note on HOPS
#HOPS dit avoir une limite de 510 tokens pour pouvoir parser une phrase. Cependant, cette limite ne correspond pas parfaitement au décompte d'unités lexicales telles que LGERM les compte, la différence pouvant atteindre 10 % de la longueur de la phrase. Une valeur conservatrice de 460 a été utilisée dans ce script, qui va vérifier que les phrases ont une longueur inférieure à cette limite. Si toutes les phrases répondent à ce critère, 1 fichier sera crée, avec suffixe 201. Si des phrases trop longuers pour être traitées par HOPS, celles-ci seront exportées séparamment dans un fichier -211.csv, car HOPS s'arrête en cas de phrase trop longue.
#4. Export Pour HOPS new


main_directory = f'{PathHead}/Pipeline/refresh/test2/'
dossier_temp = main_directory + "temp/"
## limite de longueur pour HOPS
sLengthLimit = 460

# identifier les fichiers créés par f2 dans le dossier_temp
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
myResults = pd.DataFrame()
#f=12
# make empty df to hold results
HOPS_MakerResults= pd.DataFrame()
for f in range(len(theseFiles)):
  startTime = time.time()
  # file_name = LGERMtidyName
  # load iteration of file
  file_name = theseFiles.iloc[f,0] 
  currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
  counter = 0
  # calculate names for included and excluded file
  nameForhopsInclu = dossier_temp + '/' + currentCode + "-test201.txt"
  nameForhopsEXCLU = dossier_temp + '/' + currentCode + "-test211.csv"
  # laod data
  step2 = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)
  # restrict to 4 cols
  HOPS_initial =   step2[['hopsID', 'FormeOut','TokenID', 'sentID']]
  # calculate HOPS ids, TokenIDs and FormeOut selon nécessités de HOPS 

# list comprehension to calculate TokenID and sentID columns
### output columns 0,1,2,3 if col0 is not 0, otherwise blank  for all rows of df
  HOPS_tout= pd.DataFrame([(HOPS_initial.iloc[j,0],HOPS_initial.iloc[j,1],HOPS_initial.iloc[j,2],HOPS_initial.iloc[j,3]) if HOPS_initial.iloc[j,0]!= 0 else "" for j in range(len(HOPS_initial))])
  
  # copy the sentence number to the column 'owner' if col3 is an INT, otherwise put value of col3, for all rows of df
  HOPS_tout['Owner'] = pd.DataFrame([HOPS_initial.iloc[j,3] if (HOPS_initial.iloc[j,3]).dtype == "int64" else HOPS_initial.iloc[j-1,3] for j in range(len(HOPS_initial))])
# name columns
  HOPS_tout.columns =   ['hopsID', 'FormeOut','TokenID', 'sentID', 'Owner']

  # make df to store lines to be excluded
  DataHOPSexclu = pd.DataFrame()
  # créer un DF pour stocker les données sur la longueur des phrases
  resultFrame = pd.DataFrame()
  
  # créer le set de toutes les valeurs uniques de numérotation de phrase, exclure les NA
  set1 = set(HOPS_tout['sentID'].dropna())

  # make df of s, len(s) to test if any sent too long for HOPS to parse
  testResults = pd.DataFrame( [[ s, len(HOPS_tout.loc[HOPS_tout['sentID'] == s])] for s in set1 ] )
  # nommer les colonnes
  testResults.columns = ['sentID','len']
  # créer un groupe, fast, avec les phrases moins longues que la limite, et faire un set des valeurs de numérotation de ces phrases
  fastgroup = testResults.loc[testResults['len'] < sLengthLimit]
  fastgroup = set(fastgroup['sentID'])

  # LC to get df of all cases where i,4 == sentID is in fastghroup : i,4 is Owner, so includes final punct in sents, to preserve blank lines for HOPS
  DataHOPSinclus = pd.DataFrame(  [(HOPS_tout.iloc[i,0],HOPS_tout.iloc[i,1],HOPS_tout.iloc[i,2], HOPS_tout.iloc[i,3]) for i in range(len(HOPS_tout)) if (HOPS_tout.iloc[i, 4]  in fastgroup)])
  DataHOPSinclus.columns = ['hopsID', 'FormeOut','TokenID', 'sentID']
  ## définir les datatypes  
  DataHOPSinclus['hopsID'] = DataHOPSinclus['hopsID'].astype('Int64')
  DataHOPSinclus['TokenID'] = DataHOPSinclus['TokenID'].astype('Int64')

  # créer un set avec les phrases qui ne font pas partie du groupe précédent, et si la longueur de ce set et superieure à 0, préparer les contenus  
  SlowGroup = set1 - fastgroup ## with sets, - removes items

  if len(SlowGroup) > 0:
    DataHOPSexclu = pd.DataFrame( [(HOPS_tout.iloc[i,0],HOPS_tout.iloc[i,1],HOPS_tout.iloc[i,2], HOPS_tout.iloc[i,3]) for i in range(len(HOPS_tout)) if (HOPS_tout.iloc[i, 4]  in SlowGroup)])
    DataHOPSexclu.columns = ['hopsID', 'FormeOut','TokenID', 'sentID']
    DataHOPSexclu['hopsID'] = DataHOPSexclu['hopsID'].astype('Int64')
    DataHOPSexclu['TokenID'] = DataHOPSexclu['TokenID'].astype('Int64')


  # garde-fou : vérifie que la longueur des deux parties == la longueur du tout
  questionSents = list(set(range(1,  int(max(set1)+1) )) - set1) 
  checkSum = 0
  for i in range(len(questionSents)):
    checkSum = checkSum + len(HOPS_tout.loc[HOPS_tout['sentID'] == questionSents[i]])

# imprimer un résumé de la répartition des phrases
  print('import == '+ str(len(HOPS_tout)) +  ' lignes dont ' +   str(int(max(HOPS_tout['sentID']))) + ' phrases' + '\n' + 'export == ' + str(len(DataHOPSinclus) + len(DataHOPSexclu)) + ' lignes dont ' + str  (len(SlowGroup) + len(fastgroup)) + ' phrases pleines' + '\n'+ '          plus ' + str(len(questionSents))  + ' lignes à raison d\'une ligne par phrase sans contenus - c\'est des ellipses' )
  print('donc pour LONGUEUR input = ' + str(len(HOPS_tout)) +  ' output = ' + str(len(DataHOPSinclus) + len(DataHOPSexclu) + (len(questionSents))))
  print('donc pour PHRASES input = ' + str(int(max(HOPS_tout['sentID']))) +  '  output = ' + str(len(SlowGroup) + len(fastgroup) + len(questionSents)))

  # exporter les données de manière homogène, comme les veut HOPS, avec fillna as 0
  DataHOPSinclus = DataHOPSinclus.fillna(0)
  ForHOPSoutputINC = DataHOPSinclus[['hopsID', 'FormeOut','TokenID']]
  
  # export file
  DataHOPSinclus.to_csv(nameForhopsInclu, index = False, header = False, sep="\t", encoding='UTF8') # output 
  # if there are any sent that are too long, get them and outputthem to 211 file : not necessary to do fillna0???
  if len(problems) > 0:
    ForHOPSoutputExclu = DataHOPSexclu[['hopsID', 'FormeOut','TokenID']]
    ForHOPSoutputExclu.to_csv(nameForhopsEXCLU, index = False, header = False, sep="\t", encoding='UTF8')
  # 
  endTime = time.time()
  # infos sur la vitesse de traitement : détail puis résumé ajouté à la DF
  result = {'code':[currentCode], 'wCount':[len(HOPS_tout)], 'start':[startTime],'end':[endTime], 'delta':[endTime - startTime], 'speed':[len(HOPS_tout)/(endTime - startTime) ]}
  result = pd.DataFrame(result)
  HOPS_MakerResults = HOPS_MakerResults.append(result)
print(HOPS_MakerResults)

# del(main_directory, dossier_temp, theseFiles,file_name, currentCode, nameForhops, step2, outputdf,   hopsOutCorrect, j,   FormeOutCorrect, TokenIDCorrect,  HOPSoutFinal)

# del(LGERMdelimiter, counter, closeG, CanonicForms, tidyEncoding, thisPunct, startTime, endTime)

