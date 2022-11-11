# 2. Nettoyer LGERM
:: prend comme entrée un fichier csv exporté par LGERM, avec un code de 2 lettres + 2 chiffres, qui sert comme ID de chaque texte, et d'un suffixe '-100'

# EN define paths to main_directory and working directories for temporary files, treetagger, Deucalion and UD.
#FR définir les chemins vers les dossiers de travail et de fichiers temporaires, aussi bien que ceux pour TreeTagger, Deucalion et UD
main_directory = f'{pathHead}/test2/'
dossier_temp = main_directory + "temp/"
dossier_tt = main_directory + 'PourTreeTagger/'
dossier_Deuc = main_directory + 'PourDeucalion/'
dossier_UD = main_directory + 'PourUD/'

# EN. create sorted dataframe of files to process
# FR créer une dataframe des fichiers à traiter, et trier-la
theseFiles = pd.DataFrame(glob.glob(main_directory +'/*100.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']

#EN declarations for import : encoding of LGERM exported csv file and separation character
# déclarations pour l'importation : encodage qu'emploie LGERM en exportant son CSV et le caractère de délimitation
LGERMinCode = "ISO-8859-1"
LGERMdelimiter = '\t'

# EN create list of temporary folders, and create each folder if it doesn't exist
# FR créer une liste des dossiers temporaires et de travail, et crée chaque dossier s'il n'existe pas
paths = [dossier_temp,  dossier_tt, dossier_Deuc, dossier_UD]
for p in range(len(paths)):
  if os.path.exists(paths[p])==False:
    os.makedirs(paths[p])

# EN loop to process files, operating one file at a time, from the sorted dataframe of filenames
# FR boucle pour traiter les fichiers d'entrée, un fichier à la fois, à partir de la dataframe des noms de fichiers
for i in range(len(theseFiles)):
  ## EN get current file from the dataframe theseFiles
  ## FR obtenir le nom du fichier i de la dataframe
  file_name = theseFiles.iloc[i,0]
  #EN extract the text's ID code by removing the path and the suffix
  #FR extraire l'identifiant du texte en supprimant le chemin et le suffixe du nom du fichier
  currentCode = file_name.replace(main_directory,'').replace('-100.csv','')
  # EN define name and path for the file that this function will output.
  # FR définir le nom et le chemin pour le fichier que cette fonction va créer
  LGERMtidyName =  dossier_temp + '/' + currentCode + '-101.csv'

# EN read input file replacing any double tabulations with single tabulations
# FR lire le fichier d'entrée en remplacant les double tabulations par les tabulations simples
  fin = open(file_name, "rt", encoding = LGERMinCode)
  data = fin.read().replace('\t\t', '\t')
  fin.close()
# EN write tidy version of input file
# FR écrire la version propre du fichier d'entrée
  fin = open(LGERMtidyName, "wt", encoding = LGERMinCode)
  fin.write(data)
  fin.close()
  
# EN import tidy version of input file. Eight columns should be present, but function has been tested with all texts present in LGERM at 2022-10-31 and can process the rare cases where 6 or 7 columns are present.
# FR importer la version propre du fichier d'entrée. Huit colonnes devraient être présentes, mais cette fonction a été testée avec tous les textes présents dans LGERM au 2022-10-31 et peut prendre en charge les cas où six ou sept colonnes sont présentes.

# EN LGERM's output has nested data within the column named 'expandMe', which this subroutine will de-nest. Column TokenID contains the token ID assigned by LGERM. Forme contains the forme analysed. Columns named 'supp' are to be deleted, as they contain either no information or no useful information. expandMe contains nested information. etiq contains UPOS tags. Lemme contains the lemma. code contains LGERM codes distinguishing punctuation, notably.

# FR La sortie de LGERM contient des données imbriquées dans la colonne nommée 'expandMe', que cette sous-partie va extraire. La colonne TokenID contient le numéro du token attribué par LGERM. Forme contient la forme analysée. Les colonnes nommées 'supp' sont à supprimer, car elles sont soit vides, soit ne contiennent d'informations utiles. 'expandMe' contient les données imbriquées. 'etiq' contient les étiquettes UPOS. Lemme contient le lemme. 'code' contient le code qu'utilise LGERM pour distinguer, notamment, les différents types de ponctuation.

  data = pd.read_csv(LGERMtidyName, encoding = "Windows-1252", delimiter = LGERMdelimiter, header=None, quotechar = '\"')
  if len(data.columns) == 8:
    data.columns =['TokenID', 'Forme', 'supp', 'expandMe', 'etiq', 'lemme','code','supp'] 
    data = data.drop('supp', 1) 
  elif len(data.columns) == 7:
    data.columns =['TokenID', 'Forme', 'expandMe', 'etiq', 'lemme','code','supp'] 
    data = data.drop('supp', 1) 
  elif len(data.columns) == 6:
    data.columns =['TokenID', 'Forme', 'expandMe', 'etiq', 'lemme','code'] 

# EN some LGERM exports have alignment problems with English double quotation marks, with the expandMe column empty as the quotation marks in the text are not escaped. This subroutine takes these into account by checking if the expandMecolumn contains NA values .
# FR Certains fichiers exportés par LGERM ont des problèmes d'alignement en raison des doubles guillemets anglais qui ne sont pas précédés du caractère \. Ce partie ci-dessous tiend compte de cela en vérifiant si la colonne 'expandMe' contient des NA.

  nan_len = len(data[data['expandMe'].isnull()])
  if nan_len >=1:
    #EN  create a df to hold the correctly processed lines
    # FR créer une dataframe où on stockera les lignes sans erreurs
    holding = data
    ## EN split the Forme column at tabulations if any present, putting data into new columns a-e
    ## FR scinder la colonne Forme au caractère tabulation, mettant les suites de caractère dans les nouvelles colonnes a-e.
    holding[['a','b','c','d','e']] = holding['Forme'].str.split('\t', -1, expand=True)
    ## EN restrict the dataframe 'holding' to rows where the newly created column is not empty
    ## FR délimiter la dataframe holding aux rangs dans lesquels la nouvelle colonne b n'est pas vide
    holding = holding[~holding['b'].isnull()]
    # EN delete unneeded columns as specified in list in square brackets. 1 after list indicates vertical axis
    # FR supprimer les colonnes non-nécessaires, à l'aide de la liste entre [ ] ; axe = vertical = 1
    holding = holding.drop(['a', 'b','e', 'Forme', 'expandMe','etiq', 'lemme','code'], 1)
    # EN name the remaining columns
    # FR nommer les colonnes restantes
      holding.columns =['TokenID','expandMe', 'etiq']
    
    #EN In cases where there is an alignment issue, it can be rectified by adding the missing double quotation mark. Add a column for the form, specifying its position in the order of columns, and add the double quotation mark as the form and the lemma, and 'ponctuation' as its POS. 
    # FR Dans les cas où il y a un problème d'alignement, ce problème peut être géré en ajoutant le double guillement anglais qui manque. Ajouter une colonne pour la forme, en spécifiant son positionnement dans l'ordre des colonnes, et en ajoutant un double guillement, pour tenir compte de celui déjà présent dans la colonne suivante. Ajouter le double guillemet comme la forme et comme le lemme, et ajouter 'ponctuation' comme sa catégorie grammaticale.
    holding.insert(1, 'Forme', '"')
    holding.insert(4, 'lemme', '"')
    holding.insert(5, 'code', 'ponctuation')

    # EN get the dataframe holding and send it to addMe
    # FR prendre la dataframe 'holding' et assigne-la à addMe
    addMe = holding
    # EN restrict the dataframe 'data' to cases where the expandMe column is not null, to avoid repetition of lines.
    ## FR exclure de la dataframe 'data' les rangs où la valeur de la colonne expandMe n'est pas null. Cela évitera l'ajout de doublons des lignes présentes lors de l'ajout des données nettoyées dans addMe
    df_filtered = data[data['expandMe'].notnull() == True]
    # EN concaténer les df dans la liste : 
   # FR faire une concaténation des dataframes indiquées dans la liste.
    combined = pd.concat([df_filtered, addMe])
    # EN sort the concatenated dataframe by increasing order of TokenID column, to restore the correct order of the text
    # FR trier la df combinée par l'ordre croissant de la colonne TokenID, pour que tout soit en l'ordre correct
    combined.sort_values(['TokenID'], inplace=True)
  
  # EN in the case that there are no alignment issues due to double quotation marks, we can do straight to expanding the data in the expandMe column.
  # FR dans le cas où il n'y a pas de problème avec les double guillemets anglais, on peut passer directement à la scission des données dans la colonne expandMe.
  if nan_len ==0:
    combined = data
  ## execution prob with indents here, odd....


  ## EN split column expandMe at [, sending strings to columns a-f
  ## FR scinder la colonne expandMe au crochet gauche, mettant les chaines de caractères résultantes dans les colonnes a-f
  combined[['a','b','c','d','e','f']] = combined["expandMe"].str.split('\[',-1, expand=True)
  ## EN drop newly created empty columns
  ## FR supprimer les colonnes vides 
  combined = combined.drop(['expandMe','a','c'],1)

# EN  in columns b,e,f,b, remove the ]
## FR dans les colonnes restantes, remplacer le crochet droite par rien pour le supprimer
  combined['b'] = combined['b'].str.replace('\]','')
  combined['e'] = combined['e'].str.replace('\]','')
  combined['f'] = combined['f'].str.replace('\]','')
  combined['d'] = combined['d'].str.replace('\]','')
  step3 = combined


  # EN split column b at colon, sending strings to columns g, h and i
  # FR scinder la colonne B aux deux-points, renvoyant les chaines de caractères aux nouvelles colonnes g-i
  step3[['g','h','i']] = step3["b"].str.split('\:', -1, expand=True)
  step4 = step3.drop(['b'],1)

  ## EN remove double underscores and leading underscores in column g (regex)
  ## FR supprimer les double tiret du huit en colonne G, aussi bien que l'underscore lorsqu'il est premier caractère d'uen chaine (regex)
  step4['g'] = step4['g'].str.replace('__','')
  step4['g'] = step4['g'].str.replace('^_','')
  step5 = step4

  # EN check number of columns
  # FR vérifier le nombre de colonnes
  w = len(step5.columns)

  #EN  column w-3 contains information in pagination, , w-2 on paragraphs, and w-1 on token counts within chapters
  ## FR colonne w-3 contient des informations sur la pagination, w-2 sur les paragraphes et w-1 sur le décompte de tokens à l'intérieur des chapitres
  mapping = {step5.columns[w-3]: 'Pp', step5.columns[w-2]: 'Para', step5.columns[w-1]: 'TokenChap'}

  ## EN rename columns using mapping
  ## FR renommer les colonnes utilisant le schéma défini

  step5 = step5.rename(columns=mapping)


  # EN drop unneeded column
  ## FR supprimer la colonne inutile
  step6 = step5.drop(['e'], 1)
  
  ## EN LGERM data is tidy now, so save  101 file in temp directory
  ## FR données le LGERM sont propres, enregistrer le fichier 101 dans le dossier temporaire
  step6.to_csv(LGERMtidyName, sep="\t", encoding = "UTF8", index=False)
  
# del(addMe, combined, currentCode, data, deleteMeC, deleteMeP, deleteMeQ, df_filtered, dossier_Deuc, dossier_temp, dossier_tt, dossier_UD, file_name, holding, LGERMdelimiter, LGERMinCode, LGERMtidyName, main_directory, mapping, nan_len, paths, restrictMeP, restrictMeQ, restrictMeS, step3, step4, step5, step6, theseFiles, w)
