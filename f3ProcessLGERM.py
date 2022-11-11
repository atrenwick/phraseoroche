# 3. Traiter LGERM
### fonction qui prépare les fichiers que traiteront HOPS, TT, Stanza, UD, Deuc…

## EN define paths to working folders
## FR définir les dossiers de travail explicitement
main_directory = f'{PathHead}/refresh/test2/'
dossier_temp = main_directory + "temp/"
dossier_tt = main_directory + 'PourTreeTagger/'
dossier_Deuc = main_directory + 'PourDeucalion/'
dossier_UD = main_directory + 'PourUD/'

## EN create list of LGERM files tidied in previous function
# identifier les fichiers créés par f1 dans le dossier_temp
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*101.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']

## EN declarations for import
## FR déclarations de codage et de caractère de délimitation
LGERMinCode = "ISO-8859-1"
LGERMdelimiter = '\t'

## EN define specific types of quotation marks
## FR définir des différents types de guillemets
openG = html.unescape('&laquo;')
closeG = html.unescape('&raquo;')
apos1 = html.unescape('&apos;')
apos2 = html.unescape('&prime;') 
apos3 = html.unescape('&lsquo;')
apos4 = html.unescape('&rsquo;')

## EN specify encoding for output
## FR définir le codage pour le document de sortie
tidyEncoding = "UTF-8"

# EN list all possible punctuation tags LGERM gives
## FR lister toutes les différentes étiquettes morphosyntaxiques que LGERM attribue
PONtypes = {"PON", "PONfbl", "PONfbl%", "PONpdr", "PONpga", "PONpxx"}

# EN specify punctuation to search for, including HTML-like elements
## FR lister les différents caractères de ponctuation, dont des éléments HTML
PONtypesExamples = ["'", "\"",closeG,openG,"(",")",":",";","!","?",">","<", "div", "type=",  ","]


# EN specify elided forms to take account of cases where the apostrophe will be absent or moved to following line
## FR spécifier les différentes formes qui peuvent être élidées, pour prendre en charge les cas où l'apostrophe serait absente ou présente sur la ligne suivante
CanonicForms = ["c\'","?\'","d\'","j\'","l\'","m\'","n\'","s\'","t\'","qu\'", "jourd\'", "aujourd\'"]

#EN  specify punctuation that ends sentences
## FR spécifier la ponctuation qui met fin à la phrase.
thisPunct ={ "!","?","."}
#f=10


for f in range(len(theseFiles)):
## EN create six lists in which various values will be stored - likes to skip, etc
## FR créer six listes pour stocker des valeurs -lignes à sauter, etc
  deleteMeQ = []
  deleteMeP = []
  restrictMeP = []
  deleteMeC = []
  restrictMeS = []
  restrictMeQ = []
  # file_name = LGERMtidyName
  # EN get name of current file from dataframe, and then extract text code from name
  ## FR trouver le nom du fichier à traiter depuis la dataframe, puis isoler l'ID du texte à partir du nom du fichier.
  file_name = theseFiles.iloc[f,0] 
  currentCode = file_name.replace(dossier_temp,'').replace('-101.csv','')

# EN define names for output files for punctuation only, body only and all.
## FR créer des noms pour les fichiers de sortie : ponctuation, corps de texte, et tout.
  nameForLGERMpunct = dossier_temp + '/' + currentCode + "-102.csv"
  nameForLGERMbody = dossier_temp + '/' + currentCode + "-103.csv"
  nameForLGERMallNew = dossier_temp + '/' + currentCode + "-104.csv"

## EN import current file
## FR importer le fichier actuel
  step1 = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)

## EN list comprehensions to remove left and then right square brackets from the Forme column for each row of dataframe. If no squqre bracket present, leave contents unchanged
## FR 2 compréhensions de liste pour supprimer le crochet de gauche, puis de droite, de la colonne Forme pour chaque rang de la dataframe. Si aucun crochet n'est présent, laisser le contenu de la colonne Forme sans modification
  step1['Forme'] = pd.DataFrame([ step1.iloc[b,1].replace('[','')  if  (("[" in step1.iloc[b,1])) else step1.iloc[b,1] for b in range(len(step1)) ])
  step1['Forme'] = pd.DataFrame([ step1.iloc[b,1].replace(']','')  if  (("]" in step1.iloc[b,1])) else step1.iloc[b,1] for b in range(len(step1)) ])

#### commented up to here
### this subsection deals with apostrophes and has X sections. the first (101) deals with cases where a double quote has been recognised as two single apostrophes ; these are combined into a double quotation mark, deleting the unnecessary line. The second (102) deals with cases where the apostrophe marking elision is present on the following line, and moves this apostrophe to the end of the correct word in the previous line. 103 deals with the forms in the canonic forms list removing the following lines' apostrophe. The last section, 104, treats cases where a verb is followed by an apostrophe, converting this apostrophe into a ". After each step, the cases dealt with in the step are removed from the list of cases to deal with, and the index of this list re-sent to the variable DevApostrophesI. 

## find all cases where the form is apos4
  DevApostrophes = step1.loc[step1['Forme'] == apos4] #### '
 ## set tokenID column to index
  step1['TokenID'] = step1.index
    
    # set variable to Index of DevApostrophes
  DevApostrophesI = DevApostrophes.index
  # step1['type'] = ""
  ## sous-secction 101 - exemple = '' au lieu de " pour fermer une citation
  # add Q where need double quotes, del, add to restrict list to remove from current processing list, add to delete list for same AND to delete from input file
  for l in DevApostrophesI:    # for line l in DevApostrophesIndex
    if l < max(DevApostrophesI):    ### if L is less than the maximum of DevApostrophesIndex
      if (step1.iloc[l+1,1] == "'") & (step1.iloc[l,1] == "'"):    ### if Forme of current line is apostrophe AND forme of previous line is apostrophe
        step1.iloc[l, 1] = "\""    ### then set forme of line l to double quotationmark
        deleteMeQ.append(l+1)    ### add l+1 to list of lines to deleteMeQ = list of lines to delete due to quotation marks
        restrictMeQ.append(l)  ## add l to list RestrictMe
    else:   ### else
      if (step1.iloc[l,1] == "'") & (step1.iloc[l-1,1] == "'"):   ### if forme of line l is apostrophe AND previous line is also apostrophe
        step1.iloc[l, 1] = "\""    ## set line l form to double quotation mark
        deleteMeQ.append(l+1)    ## add L+1 to deleteMeQ list 
        restrictMeQ.append(l)    ### add L to list RestrictMeQ
  # drop from lists and reset index
  working = DevApostrophes.drop(deleteMeQ)   ### remove lines to delete from working list
  DevApostrophes = working.drop(restrictMeQ)  ### remove restricted = lines processed - from working list
  DevApostrophesI = DevApostrophes.index #### set DevApostrophesI to index of I ; needs to be redone as lines have been excluded and removed
  
    ## sous-secction 102 : exemple = qu with apostrophe on next line
  ## for line l in DevApostrophesI : deal with cases where apostrophe marking elision is on following line : move apostrophe to previous line
  for l in DevApostrophesI:
    if  (step1.iloc[l-1,1].endswith("'") == False) & ((step1.iloc[l-1,1].lower()+ "'") in CanonicForms):    ##### if the form of the preceding line doesn't end with an apostrophe and the lowercase text of this previous line followed by an apostrophe is absent from the list CanonicForms
      deleteMeP.append(l)   ### add l to list deleteMeP
      step1.iloc[l-1,1] = (str(step1.iloc[l-1,1]) + str(step1.iloc[l,1]) ) ## combine forms of L and l-1 in l-1
  
  working = DevApostrophes.drop(deleteMeP) ## remove lines deletemeP from list working
  DevApostrophes = working # set devapostrophes to working
  DevApostrophesI = DevApostrophes.index ## get index of updated devapostrophes and send to DevApostrophesI
  
# sous-secction 103 exemple = qu'
  for l in DevApostrophesI:  ### for each line L in devapostrophesI
    if step1.iloc[l-1,1].lower() in CanonicForms:   ### if the lowercase of the form is in CanonicForms list
      deleteMeC.append(l) ## add l to list deleteMeC
  
  working = DevApostrophes.drop(deleteMeC) ## set working to devapostrophes after dropping elemenets present in DeleteMeC
  DevApostrophes = working ## set DevApostrophes to updated version of working, get index
  DevApostrophesI = DevApostrophes.index
  
  # sous-secction 104 exemple = aller' devient aller"

  ## for line L in DevApostrophesI of previous word has POS beginning with VER, then delete final apostrophe, and set next item Forme to quotation mark, then add L to list restrictMeS
  for l in DevApostrophesI:
    if step1.iloc[l-1,2].startswith("VER"):
      step1.iloc[l-1,1] = step1.iloc[l-1,1].replace("\'","")
      step1.iloc[l,1] = "\""
      restrictMeS.append(l)
  
  ## remove processed lines from working
  working = DevApostrophes.drop(restrictMeS)
  ## check completion ; should have reduced length of list of cases to process to 0
  if len(working) == 0:
    print("success")

## tested with all texts in LGERM at 31-10-2022, and works correctly, with result of 0 for all texte.

## lines to delete are then used to drop lines from dataframe for 1x,y then z.
  step1x = step1.drop(deleteMeQ)
  step1y = step1x.drop(deleteMeP)
  step1z = step1y.drop(deleteMeC)
  
  ## define a dataframe with all cases where the etiq column is not in the list PONtypes. This gives us the body of the text
  step2a =step1z.loc[~step1z['etiq'].isin(PONtypes)] 
  ## items from the  PONtypesExamples are removed
  step2b =step2a.loc[~step2a['Forme'].isin(PONtypesExamples)]
  ## index is reset dropping old index
  step2b = step2b.reset_index(drop=True)
  
  ## list comprehension to add noise "zzblank" to column FormeOut if the LGERM code is 0 ; as LGERM gives code of 0 to any punctuation, this adds zzblank to the FormeOut column if punctuation, and text if there is text.
  step2b['FormeOut'] = pd.DataFrame([ "zzblank"  if  (step2b.iloc[b, 4] == "0") else step2b.iloc[b,1] for b in range(len(step2b)) ])
    #ignore # dev note : if need to remove fullstops from FormeOut, can add here in LC

## list comprenehnsioncreate df of rows where code is ponctuation and etiq is PONfrt and forme is not in list thisPunct: this gives us sentence boundaries
  ToRemove = step2b.loc[(step2b['code'] =="ponctuation") &   (step2b['etiq'] =="PONfrt") & (~step2b['Forme'].isin(thisPunct)) ]
  # drop the rows in 2b by the index of ToRemove --- what does this achieve ,?????
  step2c = step2b.drop(ToRemove.index, axis = 0)
## tidy name
  step2 = step2c

  ### enregistrer la ponctuation
  ## make df of all rows where the etiq column value is in the PONtypes list
  puncttypes =step1.loc[step1['etiq'].isin(PONtypes)]
## make df of rows where forme column values are in PONtypesExamples list
  PunctSectionWorkerA = step2a.loc[step2a['Forme'].isin(PONtypesExamples)]
## concatenate the two dfs
  PunctSectionWorker = pd.concat([puncttypes,ToRemove, PunctSectionWorkerA], axis=0)
## drop the rows identified earlier as deleteMeQ,P et C to ignore unneeded punctuation
  PunctSectionWorker2 = PunctSectionWorker.drop(deleteMeQ, axis = 0)
  PunctSectionWorker3 = PunctSectionWorker2.drop(deleteMeP, axis = 0)
  PunctSection = PunctSectionWorker3.drop(deleteMeC, axis = 0)
## write file of just punctuation to specified path in temp folder; eporting with no index
  PunctSection.to_csv(nameForLGERMpunct, index =False)


### to ensure alignment of data output by HOPS and LGERM, we need IDs for each item, as well as for each sentence
  step2['hopsID'] = 1 # create column with NaN or 1
  step2['sentID'] = 1 # create column with NaN or 1
  SentValueCurrent = 1
  ## pour i dans la longueur du texte, en commencant à partir du 2e item (indexation d'ordre 0), si l'item actuel n'est pas la ponctuation, on ajout 1 à la valeur HOPSid de l'item précédant, et saisit la valeur actuelle du numérotateur de phrase. Si, par contre, l'item est un point (simple, d'exclamation ou d'interrogation), met 0 comme HOPSid, car on est à la fin d'une phrase, met la valeur actuelle du numérotateur de phrase, puis ajoute 1 à cette valeur pour la phrase suivante
  for k in range(1, len(step2)): ## for k in 1-length of step3
    if step2.iloc[k,4] != "ponctuation": #### if the LGERM POS is not ponctuation
      step2.iloc[k,11] = int(step2.iloc[k-1,11])+1 # mettre HOPS id comme la valeur du rang au-dessus + 1
      step2.iloc[k,12] = SentValueCurrent
    elif step2.iloc[k,1] in ".|!|?": # if the forme is a fullstop
      step2.iloc[k,11] = 0   ### set the HOPS id to 0
      step2.iloc[k,12] = SentValueCurrent
      SentValueCurrent = SentValueCurrent +1
#draft lc
  #### enregistrer lgermtidyBody with HOPS and SENTids ; this doesn't exist for ALL
  step2.to_csv(nameForLGERMbody, sep="\t", encoding = "UTF8", index=False)


## EN option sous MacOS :: have system announce that processing has finished. Default = non-active. Text can be defined, with escape characters as necessary
## FR option sos MacOS ::  faire annoncer la fin du traitement du fichier par le système. Par défaut, non-actif. Texte à dire peut être personnalisé, en utilisant les escapes lorsque nécessaire
# os.system('say "J\'ai fini"')

# del(apos1, apos2, apos3, apos4, CanonicForms, closeG, currentCode, deleteMeC, deleteMeP, deleteMeQ, DevApostrophes, DevApostrophesI, dossier_Deuc, dossier_temp, dossier_tt, dossier_UD, file_name, k, l, LGERMdelimiter,LGERMinCode, LGERMtidyName, main_directory, nameForLGERMallNew, nameForLGERMbody, nameForLGERMpunct, openG, PONtypes, PONtypesExamples, PunctSection, PunctSectionWorker, PunctSectionWorker2, PunctSectionWorker3, PunctSectionWorkerA, puncttypes, restrictMe, restrictMeQ, restrictMeS, SentValueCurrent, step1, step1x, step1y, step1z, step2, step2a, step2b, step2c, theseFiles, thisPunct, tidyEncoding, ToRemove, working)
