#5. Export for TT
##### make for TT
## déclarer les dossiers de travail
main_directory = f'{PathHead}'/refresh/test2/'
dossier_temp = main_directory + "temp/"
dossier_tt = main_directory + 'PourTreeTagger/'

# identifier les fichiers créés par f2 dans le dossier_temp
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
# make df to store output
TTmakerResults = pd.DataFrame()
for f in range(len(theseFiles)):
  # file_name = LGERMtidyName
  startTime = time.time()
  # prendre le fichier actuel
  file_name = theseFiles.iloc[f,0] 
  # extraire le code du nom du fichier
  currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
# construire le nom du fichier de sortie
  nameForTT = dossier_tt + '/' + currentCode + "-400.txt"
# importer les données de LGERM
  step2 = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)
## define cols to send to treetagging
  TreeTagging = step2[['FormeOut','TokenID']]
# get cases where form is not null
  toTreeTag = TreeTagging.loc[TreeTagging['FormeOut'] != ""]
  # add noise as _xkw + row number to force tagging with noisy data
  toTreeTag['TagID'] = [str("_xkw") + str(int(toTreeTag.iloc[i, 1])) for i in range(len(toTreeTag))  ]
# drop tokenID col and save to csv
  toTreeTag = toTreeTag.drop('TokenID', axis=1)
  toTreeTag.to_csv(nameForTT, sep='\t' , header=False, index=False)

## results
#   endTime = time.time()
#   result = {'code':[currentCode], 'wCount':[len(toTreeTag)], 'start':[startTime],'end':[endTime], 'delta':[endTime - startTime], 'speed':[len(toTreeTag)/(endTime - startTime) ]}
#   result = pd.DataFrame(result)
#   TTmakerResults = TTmakerResults.append(result)
# print(TTmakerResults)
# runs at about 38k words/s

# del(currentCode, dossier_temp, dossier_tt, file_name,  main_directory, nameForTT, step2, theseFiles, toTreeTag, TreeTagging)

