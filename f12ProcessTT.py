#12. Nettoyer TT
dossier_temp = main_directory + "temp/"
dossier_tt = main_directory + 'PourTreeTagger/'
TTencoding = "UTF8"
thisDelimiter = '\t'

# identifier les fichiers créés ajoutés au dossier (manuellement pr le moment)
theseFiles = pd.DataFrame(glob.glob(dossier_tt +'/*400.xml-out.tt'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
# f=1
for f in range(len(theseFiles)):
  file_name = theseFiles.iloc[f,0] 
  # GlobalStart = time.time()
  ### 400 input = with xkd prefixes
  TTtidy = file_name.replace('400.xml-out.tt','402.csv').replace(dossier_tt, dossier_temp)
  ## tidy ttfile == preprocessing 
  ### créer un objet en lisant le fichier en mode lecture seule
  TTin = open(file_name, "rt", encoding = TTencoding)
  ## lire les données en remplacant les marqueurs de début et de fin de phrase <s> et </s> et le retour chariot qui les suit par rien. Dans le cas des lignes ne contenant qu'un point, une tabulation, PONfrt, une tabulation puis un point suivi par un retour de chariot, remplace des lignes par rien. Cela évitera les problèmes liées aux emplois différents des points. Enfin, chaque fois qu'il y a un retour chariot suivi immédiatement par un _xkw, remplace le retour + cette suite de caractères par une tabulation, pour rétablir les TokenID sur la même ligne que le token ; Treetagger les a reconnus comme des mots indépendents, mais on sait que c'est du bruit, aucun mot en ancien français ne commençant par _xkw
  TT_data = TTin.read().replace('<s>\n', '').replace('</s>\n', '').replace('\.\tPONfrt\t\.\n', '').replace('\n_xkw', '\t')
  ### maintenant qu'on a défini les modifications à apporter, on ouvre un nouveau doc en mode écriture. 
  TTin = open(TTtidy, "wt", encoding = TTencoding)
  ## on écrit nos données traitées
  TTin.write(TT_data)
  ## et on ferme le fichier  
  TTin.close()

  ## import ttagged data ## may need to add empty tabs to first line of input file to get it to recognise 6 cols for each line
  col_names = ['FormeTTaf','POSttAF','LemmeTTaf','ID','temp1','temp2']
  TreTaggeddata = pd.read_csv(TTtidy, encoding = TTencoding, delimiter = thisDelimiter, quotechar = '\"', skip_blank_lines=False, na_filter=False, header=None, names = col_names) ## af07,13 needs head false arg
  ## nommer les colonnes
  ## faire la liste des cas où la forme est le point
  TTstops = TreTaggeddata.loc[TreTaggeddata['FormeTTaf'] == "."]
  ## enlever ces rangs du df
  TTdatacleaner = TreTaggeddata.drop(TTstops.index)
  
  ## deal with isolated apostrophes by binding form + apostrophe
  ### ici on gère les apostrophes marquant l'élision
  ### pour chaque rang, si la colonne 0, qui contient la forme à l'etude, comprend uniquement une apostrophe, on collera cet apostrophe à la fin du mot dans le rang immédiatement au-dessus, et on remplacera les valeurs des colonnes 3 et 5 dans le rang au-dessus. On met ainsi comme TOkenID le TokenID correct, qui correspond au forme élidé et non pas à la ponctuation.
  ## note que cela ne fonctionne que dans les cas où il y a une elision, pas deux consecutives, donc des cas de aujour-d-'hui en 3 mots avec 2 elisions font erreur
  for j in range(len(TTdatacleaner)):
    if TTdatacleaner.iloc[j, 0] == "'":
      TTdatacleaner.iloc[j-1, 0] = str(TTdatacleaner.iloc[j-1, 0]) + str("\'") ## add ' to prev word
      TTdatacleaner.iloc[j-1, 3] = int(TTdatacleaner.iloc[j, 3]) ## move value of col4 from j to j-1
      TTdatacleaner.iloc[j-1, 5] = str(TTdatacleaner.iloc[j, 5]) ## move value of col4 from j to j-1
  ### on définit les lignes où l'apostrophe d'élision est la totalité de la forme  
  TTdeleteMe = TTdatacleaner.loc[TreTaggeddata['FormeTTaf'] == "\'"]
  ## on utilise leur index pour supprimer ces lignes de notre df
  TTdatCleaned = TTdatacleaner.drop(TTdeleteMe.index)
  ## TTdatCleaned = TTdatacleaner

  ## sanity check. on vérifie que les données sont croisées correctement. En supprimant le préfixe dans la colonne temp2, on devrait avoir une colonne de valeurs numériques, identiques aux TokenID et temp1, ajouté par TT
  TTdatCleaned['temp2'] = (TTdatCleaned['temp2'].str.replace("_xkw","")) 
  ## on supprime la colonne inutile, en spécifiant son axe verticale
  TTdatCleaned = TTdatCleaned.drop([ 'temp1'], axis = 1)
  ## supprime les rangs où la valeur de la colonne temp2 est nulle. Ces rangs contiennent la ponctuation, non traitée TT
  TTdatCleaned['temp2'] = TTdatCleaned['temp2'].dropna()
  #### on supprime les valeurs nulles de la colonne temp2
  TTdatCleaned = TTdatCleaned.dropna(subset=['temp2'])
  #### enfin on se limite aux rangs où la colonne ID n'est pas nulle ou vide
  TTdatCleaned = TTdatCleaned.loc[TTdatCleaned['ID'] != ""]
  ## on spécifie que cette colonne contient des entiers
  TTdatCleaned['ID'] = TTdatCleaned['ID'].astype('int64')
  ## on supprime la colonne temp2 qui nous a montré la correspondance souhaitée entre TOkenID et temp2
  TTdatCleaned = TTdatCleaned.drop([ 'temp2'], axis = 1)
  ## nommer les colonnes restantes
  TTdatCleaned2 = TTdatCleaned
  TTdatCleaned2.columns = ['FormeTTaf','POSttAF','LemmeTTaf', 'TokenID']
  ####  retenir  les lignes si sa forme est quelque chose d'autre qu'une apostrophe
  TTdatCleaned2 = TTdatCleaned2.loc[TTdatCleaned2['FormeTTaf'] != "\'"]
  ## save tidy TT data  
  TTdatCleaned2.to_csv(TTtidy, sep="\t", encoding="UTF-8", index=False, header=True)
  # print(f, "\n")  
  
# del(col_names, dossier_tt, file_name, j, theseFiles, thisDelimiter, TreTaggeddata, TT_data, TTdatacleaner, TTdatCleaned, TTdeleteMe, TTencoding, TTin, TTstops, TTtidy)

