---
title: "PipelineGMs"
output: html_document
date: '2022-12-12'
editor_options: 
  chunk_output_type: console
chunk_output_type: console
---
#00 Contents
```{text}

01. Import libraries, global variables
02. Tidy LGERM
    2h. Zip 100.csv
03   Traiter LGERM
    3h zip 101
04   Convert 103.csv ISO-8859-1 to UTF8
05   HOPS preparation
    ########### to do : add command maker here
06   TT preparation
    6h TT archiver
07   Prepare Deucalion
    ########### to do : add command maker here
08   Prepare UD
09   Prepare, Process Stanza


15 : HOPS processing
16. TT processing
    16h : Archiver
17 Deuc doing
    17n pieExtended
18 UD doing

25 HOPS post
26    TT post
27 DeucPost
28    UDpost
29    StanzaPost

```



#01. Importer les libraries, définir les répertoires, les variables....
```{python}
# import webbrowser
import pandas as pd
import platform
import glob
import os
import html
import numpy as np
import stanza
from stanza.utils.conll import CoNLL
import codecs
import re
import ast
import conllu
import shutil
import pathlib
# import json

from zipfile import ZipFile, ZIP_DEFLATED

#-- need to make the following available to all functions::

##  EN declare paths to home and main directory folders, which will be inherited by all other functions
## FR Déclarations pour système d'exploitation, chemin et fichiers d'entrée, qui seront passées à toutes les autres fonctions

# À faire avant une première utilisation :

## EN On MacOS, add name of user account in place of nom_utilisateur   
## FR Sous MacOS, ajouter le nom du compte utilisateur à la place de nom_utilisateur 

## On Windows or Linux, replace nom_utilisateur with the name of the user account, verifying that the correct drive letter is present in PathHead
## Sous Windows ou Linux, remplacer nom_utilisateur par le nom du compte utilisateur en veillant à indiquer la bonne lettre pour le disque dans PathHead
if platform.system() == "Darwin":
  PathHead = "/Users/username/"
elif platform.system() == "Windows":
  PathHead = "C:/Users/username/"
elif platform.system() == "Linux":
  PathHead = "C:/Users/nom_utilisateur/"
main_directory = f'{PathHead}/PhraseoRoChe/Pipeline/refresh/'
# HOPSparserLocation = f'{PathHead}Downloads/MedJournalDownloads/AGOS/UD_Old_French-SRCMF-flaubert+fro/'

## EN provide path to folder than will serve as the main directory. Inside this folder, temporary folders will be created, and processed files will be created here.
## FR indiquer le chemin au dossier qui sevira comme dossier principal. À l'intérieur de celui-ci seront créés des dossiers temporaires, et les fichiers créés seront stockés ici.
dossier_tt = main_directory + 'PourTreeTagger/'
dossier_Deuc = main_directory + 'PourDeucalion/'
dossier_UD = main_directory + 'PourUD/'
dossier_temp = main_directory + "temp/"
dossier_Stanza = main_directory + 'PourStanza/'
HOPSfolder = main_directory + "HOPS/"

# EN create list of temporary folders, and create each folder if it doesn't exist
# FR créer une liste des dossiers temporaires et de travail, et crée chaque dossier s'il n'existe pas
paths = [dossier_temp,  dossier_tt, dossier_Deuc, dossier_UD, dossier_Stanza, HOPSfolder]
for p in range(len(paths)):
  if os.path.exists(paths[p])==False:
    os.makedirs(paths[p])

```

#02 LGERM Prepare
```{python}
'''prend comme entrée un fichier csv exporté par LGERM, dont le nom comprend déjà un code de 2 lettres + 2 chiffres, qui sert comme ID de chaque texte, et d'un suffixe -100'''

def traiter_tabs(file_name, LGERMinCode):
  '''fonction qui remplace les double-tabulations par les tabulation simples et nommer le fichier créé'''
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

def gerer_colonnes(LGERMtidyName, LGERMinCode, LGERMdelimiter):  
  # EN import tidy version of input file. Eight columns should be present, but function has been tested with all texts present in LGERM at 2022-10-31 and can process the rare cases where 6 or 7 columns are present.
  # FR importer la version propre du fichier d'entrée. Huit colonnes devraient être présentes, mais cette fonction a été testée avec tous les textes présents dans LGERM au 2022-10-31 et peut prendre en charge les cas où six ou sept colonnes sont présentes.
  # EN LGERM's output has nested data within the column named 'expandMe', which this subroutine will de-nest. Column TokenID contains the token ID assigned by LGERM. Forme contains the forme analysed. Columns named 'supp' are to be deleted, as they contain either no information or no useful information. expandMe contains nested information. etiq contains UPOS tags. Lemme contains the lemma. code contains LGERM codes distinguishing punctuation, notably.
  # FR La sortie de LGERM contient des données imbriquées dans la colonne nommée 'expandMe', que cette sous-partie va extraire. La colonne TokenID contient le numéro du token attribué par LGERM. Forme contient la forme analysée. Les colonnes nommées 'supp' sont à supprimer, car elles sont soit vides, soit ne contiennent d'informations utiles. 'expandMe' contient les données imbriquées. 'etiq' contient les étiquettes UPOS. Lemme contient le lemme. 'code' contient le code qu'utilise LGERM pour distinguer, notamment, les différents types de ponctuation.
  data = pd.read_csv(LGERMtidyName, encoding = LGERMinCode, delimiter = LGERMdelimiter, header=None, quotechar = '\"', low_memory=False)
  if len(data.columns) == 8:
    data.columns =['TokenID', 'Forme', 'supp', 'expandMe', 'etiq', 'lemme','code','supp'] 
    data = data.drop('supp', 1)
  
  if len(data.columns) == 7:
    data.columns =['TokenID', 'Forme', 'expandMe', 'etiq', 'lemme','code','supp'] 
    data = data.drop('supp', 1)
  
  if len(data.columns) == 6:
    data.columns =['TokenID', 'Forme', 'expandMe', 'etiq', 'lemme','code'] 

  # EN some LGERM exports have alignment problems with English double quotation marks, with the expandMe column empty as the quotation marks in the text are not escaped. This subroutine takes these into account by checking if the expandMecolumn contains NA values .
  # FR Certains fichiers exportés par LGERM ont des problèmes d'alignement en raison des doubles guillemets anglais qui ne sont pas précédés du caractère \. Ce partie ci-dessous tiend compte de cela en vérifiant si la colonne 'expandMe' contient des NA.

  nan_len = len(data[data['expandMe'].isnull()])
  if nan_len >=1:
    ## EN create a df to hold the correctly processed lines. split the Forme column at tabulations if any present, putting data into new columns a-e, then restrict the dataframe  to rows where the newly created column is not empty
    ## FR sréer une dataframe où on stockera les lignes sans erreurs. scinder la colonne Forme au caractère tabulation, mettant les suites de caractère dans les nouvelles colonnes a-e, puis limiter la dataframe holding aux rangs dans lesquels la nouvelle colonne b n'est pas vide
    holding = data
    holding[['a','b','c','d','e']] = holding['Forme'].str.split('\t', -1, expand=True)
    holding = holding[~holding['b'].isnull()]
    # EN delete unneeded columns as specified in list in square brackets. 1 after list indicates vertical axis. name the remaining columns
    # FR supprimer les colonnes non-nécessaires, à l'aide de la liste entre [ ] ; axe = vertical = 1. nommer les colonnes restantes
    holding = holding.drop(['a', 'b','e', 'Forme', 'expandMe','etiq', 'lemme','code'], 1)
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
  return combined

def de_nest(combined, LGERMtidyName):
  '''fonction qui remet dans sa propre colonne les informations se trouvant dans colonnes imbriquées'''
  # EN split column expandMe at [, sending strings to columns a-f
  # FR scinder la colonne expandMe au crochet gauche, mettant les chaines de caractères résultantes dans les colonnes a-f
  combined[['a','b','c','d','e','f']] = combined["expandMe"].str.split('[',-1,expand=True)
  # EN drop newly created empty columns
  # FR supprimer les colonnes vides 
  combined = combined.drop(['expandMe','a','c'],1)
  
  # EN  in columns b,e,f,b, remove the ]
  # FR dans les colonnes restantes, remplacer le crochet droite par rien pour le supprimer
  combined['b'] = combined['b'].str.replace(']','')
  combined['e'] = combined['e'].str.replace(']','')
  combined['f'] = combined['f'].str.replace(']','')
  combined['d'] = combined['d'].str.replace(']','')
  step3 = combined
  
  # EN split column b at colon, sending strings to columns g, h and i
  # FR scinder la colonne B aux deux-points, renvoyant les chaines de caractères aux nouvelles colonnes g-i
  step3[['g','h','i']] = step3["b"].str.split(':', -1, expand=True)
  step4 = step3.drop(['b'],1)
  
  ## EN remove double underscores and leading underscores in column g (regex)
  ## FR supprimer les double tiret du huit en colonne G, aussi bien que l'underscore lorsqu'il est premier caractère d'uen chaine (regex)
  step4['g'] = step4['g'].str.replace('__','')
  step4['g'] = step4['g'].str.replace('^_','')
  step5 = step4
  
  # EN check number of columns, assign names. column w-3 contains information in pagination, , w-2 on paragraphs, and w-1 on token counts within chapters
  # FR vérifier le nombre de colonnes, nommer-les. colonne w-3 contient des informations sur la pagination, w-2 sur les paragraphes et w-1 sur le décompte de tokens à l'intérieur des chapitres
  w = len(step5.columns)
  
  ## EN rename columns using mapping, drop unneeded column. LGERM data is tidy, so save  101 file in temp directory
  ## FR renommer les colonnes utilisant le schéma défini, supprimer la colonne inutile. données le LGERM sont propres, enregistrer le fichier 101 dans le dossier temporaire

  mapping = {step5.columns[w-3]: 'Pp', step5.columns[w-2]: 'Para', step5.columns[w-1]: 'TokenChap'}
  step5 = step5.rename(columns=mapping)
  step6 = step5.drop(['e'], 1)
  step6.to_csv(LGERMtidyName, sep="\t", encoding = LGERMinCode, index=False)
  print("Fichier exporté vers " + LGERMtidyName)

def zip101Files(dossier_temp):
  with ZipFile((main_directory + "101.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(dossier_temp ).rglob("*101*"):
           archive.write(file_path, arcname=file_path.relative_to(dossier_temp))

#### end of definitions, 
def doLGERMprepare(main_directory, dossier_temp):
  #EN declarations for import : encoding of LGERM exported csv file and separation character
  theseFiles = pd.DataFrame(glob.glob(main_directory +'/temp/*100.csv'))
  theseFiles = theseFiles.sort_values(0)
  theseFiles.reset_index(inplace=True, drop=True)
  theseFiles.columns=['Path']
  LGERMinCode = "ISO-8859-1"
  LGERMdelimiter = '\t'
  # EN loop to process files, operating one file at a time, from the sorted dataframe of filenames
  for i in range(len(theseFiles)):
    ## EN get current file from the dataframe theseFiles
    ## FR obtenir le nom du fichier i de la dataframe
    file_name = theseFiles.iloc[i,0]
    #EN extract the text's ID code by removing the path and the suffix
    #FR extraire l'identifiant du texte en supprimant le chemin et le suffixe du nom du fichier
    currentCode = file_name.replace(main_directory,'').replace('-100.csv','')
    # EN define name and path for the file that this function will output.
    # FR définir le nom et le chemin pour le fichier que cette fonction va créer
    LGERMtidyName =  file_name.replace('100', '101')
    traiter_tabs(file_name, LGERMinCode)
    combined = gerer_colonnes(LGERMtidyName, LGERMinCode, LGERMdelimiter)  
    de_nest(combined, LGERMtidyName)
  zip101Files(dossier_temp)
  

## single line to run

doLGERMprepare(main_directory, dossier_temp)

```

#03 LGERM Process
```{python}
'''fonction qui fait toutes les déclarations que nécessitent les transformations des données de LGERM'''
## TO DO : change path of filename created in 101 to /temp
## define custom functions this function will call

def LGERMaligner(file_name):  
  ## EN create six lists in which various values will be stored - likes to skip, etc
  ## FR créer six listes pour stocker des valeurs -lignes à sauter, etc
  deleteMeQ = []
  deleteMeP = []
  restrictMeP = []
  deleteMeC = []
  restrictMeS = []
  restrictMeQ = []
  ## EN import current file
  ## FR importer le fichier actuel
  step1 = pd.read_csv(file_name, sep="\t", encoding = LGERMinCode, low_memory=False)
  
  ## EN list comprehensions to remove left and then right square brackets from the Forme column for each row of dataframe. If no squqre bracket present, leave contents unchanged
  ## FR 2 compréhensions de liste pour supprimer le crochet de gauche, puis de droite, de la colonne Forme pour chaque rang de la dataframe. Si aucun crochet n'est présent, laisser le contenu de la colonne Forme sans modification
  step1['Forme'] = pd.DataFrame([ step1.iloc[b,1].replace('[','')  if  (("[" in step1.iloc[b,1])) else step1.iloc[b,1] for b in range(len(step1)) ])
  step1['Forme'] = pd.DataFrame([ step1.iloc[b,1].replace(']','')  if  (("]" in step1.iloc[b,1])) else step1.iloc[b,1] for b in range(len(step1)) ])
  
  ### this subsection deals with apostrophes and has X sections. the first (101) deals with cases where a double quote has been recognised as two single apostrophes ; these are combined into a double quotation mark, deleting the unnecessary line. The second (102) deals with cases where the apostrophe marking elision is present on the following line, and moves this apostrophe to the end of the correct word in the previous line. 103 deals with the forms in the canonic forms list removing the following lines' apostrophe. The last section, 104, treats cases where a verb is followed by an apostrophe, converting this apostrophe into a ". After each step, the cases dealt with in the step are removed from the list of cases to deal with, and the index of this list re-sent to the variable DevApostrophesI. 
  
  ##EN identify all cases where the form is apos4, a curved apostrophe, is present
  ## FR identifier tous les cas où apos4 est présente, une apostrophe recourbée
  DevApostrophes = step1.loc[step1['Forme'] == apos4] #### '
  
  ## EN send index of step1 to  tokenID
  step1['TokenID'] = step1.index
    
  # set variable to Index of DevApostrophes
  DevApostrophesI = DevApostrophes.index
  
  ## EN subroutine to identify cases where consecutive apostrophes are used to close a citation rather than double quotes. For + if loops to look through list DevApostrophes, and if there are two consecutive apostrophes in current and following row, replace them with 1 double quotation mark, and add the following row to the list of rows to delete, and remove these lines from the list of lines to process ; else checks if the preceding item is an apostrophe as well as the current one, and if so, replaces them with double quotation mark. Add line to delete to list of lines to delete, remove these lines from list of lines to process
  ## FR sous-ensemble pour identifier les cas où des apostrophes consecutives sont utilisées pour fermer une citation au lieu de guillemets doubles. Des boucles FOR et IF traitent les items de la liste DevApostrophes? et si une apostrophe est préesente dans le rang actuel aussi bien que dans le suivant, elles sont remplacées par un guillemet double ; le rang suivant est ajouté à la liste de ceux à supprimer, les deux rangs sont ajoutés à la liste des rangs de traités. ELSE va faire la même chose en regardant le rang d'avant.
  for l in DevApostrophesI:
    if l < max(DevApostrophesI):
      if (step1.iloc[l+1,1] == "'") & (step1.iloc[l,1] == "'"):
        step1.iloc[l, 1] = "\""
        deleteMeQ.append(l+1)
        restrictMeQ.append(l)
    else: 
      if (step1.iloc[l,1] == "'") & (step1.iloc[l-1,1] == "'"):
        step1.iloc[l, 1] = "\""
        deleteMeQ.append(l+1)
        restrictMeQ.append(l)
  
  ## EN drop processed and to-delete rows, then reset index of DevApostrophesI to reflect changes made
  ## FR exclure les rangs traités et ceux à supprimer, rétablir l'index de DevApostrophesI pour tenir compte de ces modifications
  working = DevApostrophes.drop(deleteMeQ)
  DevApostrophes = working.drop(restrictMeQ)
  DevApostrophesI = DevApostrophes.index
  
  ## EN subroutine to deal with cases where apostrophe marking elision is present as its own token in the following row. FOR-IF loops to find cases where the preceding form does not end in an apostrophe but where the lower case form of this is in the CanonicForms list when followed by an apostrophe. The apostrophe is added to the preceding line, and the current line added to list to delete. Both lines are then added to list of lines processed, then these lines are removed from the list of lines to process, and the index of this list is reset.
  ## FR sous-ensemble pour prendre en charge les cas où l'apostrophe marquant l'élision est présent comme son propre token dans le rang suivant. Des boucles FOR-IF recherchent des cas où la forme du rang précédant ne se termine pas par une apostrophe et mais que cette forme suivie par une apostrophe figure dans la liste CanonicForms. L'apostrophe est ajoutée au rang suivant, la ligne actuelle est ajoutée à la liste des lignes à supprimer, les deux à la liste des lignes de traitées. Ces lignes sont supprimées de la liste des lignes à traiter, puis l'index de celle-ci rétalie.
  
  for l in DevApostrophesI:
    if  (step1.iloc[l-1,1].endswith("'") == False) & ((step1.iloc[l-1,1].lower()+ "'") in CanonicForms):
      deleteMeP.append(l)   
      step1.iloc[l-1,1] = (str(step1.iloc[l-1,1]) + str(step1.iloc[l,1]) ) ## combine forms of L and l-1 in l-1
  
  working = DevApostrophes.drop(deleteMeP) ## remove lines deletemeP from list working
  DevApostrophes = working # set devapostrophes to working
  DevApostrophesI = DevApostrophes.index ## get index of updated devapostrophes and send to DevApostrophesI
  
  ## EN if the current form is an apostrophe and the form on the preceding row is in the list CanonicForms, delete the current apostrophe and add the line to list of lines processed and to delete. List of lines to process is then trimmed and index reset.
  ## FR si la forme actuelle est une apostrophe et que la forme du rang précédant figure dans la liste CanonicFormes, supprimer l'apostrophe actuelle, et ajoute ce rang à la liste des rangs traités et à supprimer. La liste des rangs à traité est mise à jour et l'index rétabli
  for l in DevApostrophesI:  
    if step1.iloc[l-1,1].lower() in CanonicForms:   
      deleteMeC.append(l) 
  
  working = DevApostrophes.drop(deleteMeC) 
  DevApostrophes = working 
  DevApostrophesI = DevApostrophes.index
  
  ## EN process cases where verb conjugations are followed by apostrophes rather than double quotations. Next form is set to double quote mark, current form has apostrophe removed. Line added to list of lines processed, and removed from list of lines to process.
  ## FR traiter les cas où la conjugation d'un verbe se suit d'une apostrophe au lieu d'un guillemet anglais double. La forme suivante devient le guillemet double, l'apostrophe est supprimée, et la ligne ajoutée à celles de traitées et retirée de la liste des lignes à taiter.
  for l in DevApostrophesI:
    if step1.iloc[l-1,2].startswith("VER"):
      step1.iloc[l-1,1] = step1.iloc[l-1,1].replace("\'","")
      step1.iloc[l,1] = "\""
      restrictMeS.append(l)
  
  working = DevApostrophes.drop(restrictMeS)
  
  ## EN verification tha all lines have been processed - length should be 0
  ## FR vérification que toutes les lignes nécessaires ont été traitées - la longueur de working devrait être égale à 0
  ## check completion ; should have reduced length of list of cases to process to 0
  if len(working) == 0:
    print("success")
  
  ## tested with all texts in LGERM at 31-10-2022, and works correctly, with result of 0 for all texte.
  
  ## EN lines to delete are then used to drop lines from dataframe for 1x,y then z.
  ## FR utiliser les listes deleteMeQ,P et C pour supprimer les lignes de la dataframe
  step1x = step1.drop(deleteMeQ)
  step1y = step1x.drop(deleteMeP)
  aligned_df = step1y.drop(deleteMeC)
  return aligned_df

# EN define a dataframe with all cases where the value in the etiq column is not in the list PONtypes. This gives us the body of the text, then remove lines where the value in the Forme column is in the PONtypesExamples list, then reset index
  ## FR créer une datefrale avec tous les rangs dont la valeur de la colonne etiq est absente de la liste PONtypes, puis exclure les rangs dont la valeur de la colonne Forme ne figure pas dans la liste PONtypesExamples, pour donner le corps du texte uniquement. Puis, rétablir l'index


def special_deleter(aligned_df):
# '''fonction qui supprimera les crochets ajoutés au texte pour indiquer des interventions éditoriales ayant échappés aux relectures, en attente du reformatage définitif des textes LGERM'''
  specialExcludeList = [ ']','['  ]
  exclude_these = [i for i in range(len(aligned_df)) if aligned_df.iloc[i,3] in   specialExcludeList] 
  aligned_df  = aligned_df.drop(exclude_these)
  return aligned_df

def LGERMidGen(aligned_df):
  """docstring for LGERMidGen"""
  # create numpy array on values with LC testing if current token is in EOS_types and following token is »; these are EOS_complex, 
  EOS_complex = np.array([i+1 for i in range(len(aligned_df)-1) if ((aligned_df.iloc[i,1] in EOS_types) & (aligned_df.iloc[i+1,1] == "»"))])
  # df of values where token is in EOS but i+1 not in EOS_complex; these are EOS_simple. Name column
  EOS_simple = pd.DataFrame([[i ] for i in range(len(aligned_df)) if ((aligned_df.iloc[i,1] in EOS_types) & ((i +1) not in EOS_complex))])
  EOS_simple.columns = ['value']
  # create set of values in EOS simple, and set of values in EOS_simple_exclude, and subtract EOS_simple_exclude_set from EOS_simple_set
  EOS_simple_set = set(EOS_simple['value'].to_list()  )
  ## convert EOS_complex to set, then get union of simple and complex sets
  EOS_complex_set = set(EOS_complex)
  allSentEnds = EOS_complex_set.union(EOS_simple_set)
  # TODO: write code...
  aligned_df['ss_intID'] =0 
  aligned_df['ssid'] =1
  SentValueCurrent =1
  TokenValueCurrent = 0

  for k in range(len(aligned_df)):
    if k in allSentEnds:
      aligned_df.iloc[k,10] = TokenValueCurrent +1   ### add1 to tokenID
      aligned_df.iloc[k,11] = SentValueCurrent      ##### set sentID to ucrrentvalue
      SentValueCurrent = SentValueCurrent +1          ### add1 to sentID
      TokenValueCurrent = 0                         #### set toeknID to 0
    else:
      aligned_df.iloc[k,10] = TokenValueCurrent +1   ### add1 to tokenID
      aligned_df.iloc[k,11] = SentValueCurrent       ##### keep sentID same
      TokenValueCurrent = TokenValueCurrent +1

  aligned_id_df = aligned_df
  return aligned_id_df

#### function to add blanks at sentence boundaries
def LGERMconcat(aligned_id_df):
  workingFrame = aligned_id_df
  workingFrame['FormeNoP'] = pd.DataFrame([ "zzblank"  if  (workingFrame.iloc[b, 6] == 0) else workingFrame.iloc[b,1] for b in range(len(workingFrame)) ]) 
  sentValues = set(workingFrame['ssid'].values)
  tidyFrame = pd.DataFrame()
  spacer = pd.DataFrame([{'TokenID':"zzkz",'Forme':"zzkz",'etiq':"zzkz",'lemme':"zzkz",'code':"zzkz",'d':"zzkz",'f':"zzkz",'Pp':"zzkz",'Para':"zzkz",'TokenChap':"zzkz",'ss_intID':  "zzkz",'ssid':"zzkz",'FormeNoP':"zzkz"}])
## > changes here
  for s in sentValues:
      currentSentence = workingFrame.loc[workingFrame['ssid']== s]
      # currentOut = currentSentence.append(spacer, ignore_index=True)
      currentOut = pd.concat([currentSentence, spacer], axis=0)
      tidyFrame = pd.concat([tidyFrame, currentOut], axis=0)
      workingFrame = workingFrame.drop(currentSentence.index, axis=0)
  aligned_id_concat_df= tidyFrame
  return aligned_id_concat_df

def LGERMexport(aligned_id_concat_df, file_name):
  currentCode = file_name.replace(dossier_temp,'').replace('-101.csv','')
  if platform.system() in ['Windows','windows']: 
    currentCode = file_name.replace(main_directory.replace('\\',''),'').replace('temp','').replace('\\','').replace('-101.csv','')
  aligned_id_concat_df['UUID'] = [str(currentCode) + "-" + str(i).zfill(5) for i in range(len(aligned_id_concat_df)) ]
  df_export = aligned_id_concat_df
  ## EN define names for output files for punctuation only, body only and all.
  ## FR créer des noms pour les fichiers de sortie : ponctuation, corps de texte, et tout.
  df_export.to_csv(file_name.replace('101','103-8859'), sep="\t", encoding = LGERMinCode, index=False)
  return(file_name.replace('101','103'))


def zip103Files(dossier_temp, main_directory):
  with ZipFile((main_directory + "103-8859.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(dossier_temp ).rglob("*103*"):
           archive.write(file_path, arcname=file_path.relative_to(dossier_temp))


#declarations
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

# EN list all possible punctuation tags LGERM gives
## FR lister toutes les différentes étiquettes morphosyntaxiques que LGERM attribue
PONtypes = {"PON", "PONfbl", "PONfbl%", "PONpdr", "PONpga", "PONpxx"}
EOS_types = {".", ";" ,":", "!", "?" }

# EN specify punctuation to search for, including HTML-like elements
## FR lister les différents caractères de ponctuation, dont des éléments HTML
# PONtypesExamples = ["'", "\"",closeG,openG,"(",")",":",";","!","?",">","<", "div", "type=",  ","]
PONtypesExamples = ["'", "\"",closeG,openG,"(",")",">","<", "div", "type=" ,","]

# EN specify elided forms to take account of cases where the apostrophe will be absent or moved to following line
## FR spécifier les différentes formes qui peuvent être élidées, pour prendre en charge les cas où l'apostrophe serait absente ou présente sur la ligne suivante
CanonicForms = ["c\'","?\'","d\'","j\'","l\'","m\'","n\'","s\'","t\'","qu\'", "jourd\'", "aujourd\'"]

##EN  specify punctuation that ends sentences
## FR spécifier la ponctuation qui met fin à la phrase.
thisPunct ={ "!","?","."} 


for f in range(len(theseFiles)):
  file_name= theseFiles.iloc[f,0]
  aligned_df = LGERMaligner(file_name)
  aligned_df  = special_deleter(aligned_df)
  aligned_id_df = LGERMidGen(aligned_df)
  aligned_id_concat_df = LGERMconcat(aligned_id_df)
  LGERMexport(aligned_id_concat_df, file_name)
  print(str(f+1) + " sur " + str(len(theseFiles)) +  " == " + file_name.replace(dossier_temp,'') + "  ==> exporté vers " + file_name.replace('101','103-8859').replace(dossier_temp,'') , flush=True)

  zip103Files(dossier_temp, main_directory)

## single line to run:


```

#04 LGERM Convert
```{python}

def zip103Files(dossier_temp, main_directory):
  with ZipFile((main_directory + "103.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(dossier_temp ).rglob("*103.csv"):
           archive.write(file_path, arcname=file_path.relative_to(dossier_temp))

def doLGERMconvert(dossier_temp, main_directory):
  theseFiles = pd.DataFrame(glob.glob(dossier_temp + '*103-8859.csv'))
  theseFiles = theseFiles.sort_values(0)
  theseFiles.reset_index(inplace=True, drop=True)
  theseFiles.columns=['Path']

  for i in range(len(theseFiles)):
    inputFile = theseFiles.iloc[i,0]
    outputFile = inputFile.replace('-8859.csv','.csv')
    ## convert to UTF8 with new filename
    BLOCKSIZE = 1048576 # or some other, desired size in bytes
    with codecs.open(inputFile, "r", "ISO-8859-1") as sourceFile:
      with codecs.open(outputFile, "w", "utf-8") as targetFile:
          while True:
              contents = sourceFile.read(BLOCKSIZE)
              if not contents:
                  break
              targetFile.write(contents)
  
  zip103Files(dossier_temp, main_directory)

#single line
doLGERMconvert(dossier_temp, main_directory)


```


#101. combiner les saucisses
```{python}
# function to get all AF04, AF05 and AF06 files : parts 1, 2, 3 of same text, and move to subfolder, of456, then rename AF02 to AF03
def moveAFparts(dossier_temp):
  targets = pd.DataFrame()
  extraTargets = pd.DataFrame(glob.glob(dossier_temp +'/*AF04*'))
  targets = pd.concat([targets, extraTargets], axis=0)
  extraTargets = pd.DataFrame(glob.glob(dossier_temp +'/*AF05*'))
  targets = pd.concat([targets, extraTargets], axis=0)
  extraTargets = pd.DataFrame(glob.glob(dossier_temp +'/*AF06*'))
  targets = pd.concat([targets, extraTargets], axis=0)
  targets = targets.reset_index(drop=True)
  currentPath = dossier_temp + 'combining/' 
  
  if os.path.exists(currentPath)==False:
    os.makedirs(currentPath)

  for t in range(len(targets)):
    targetFile = targets.iloc[t,0].replace(dossier_temp, currentPath)
    SourceFile = targets.iloc[t,0]
    shutil.move(SourceFile, targetFile)

def combineAFparts(dossier_temp):
  '''function to combine AF4-5-6 and make sent num sequential '''
  ### to to : are UUIDs sequential afterwards??
  targets = pd.DataFrame()
  extraTargets = pd.DataFrame(glob.glob(dossier_temp +'/combining/*AF04*'))
  targets = pd.concat([targets, extraTargets], axis=0)
  extraTargets = pd.DataFrame(glob.glob(dossier_temp +'/combining/*AF05*'))
  targets = pd.concat([targets, extraTargets], axis=0)
  extraTargets = pd.DataFrame(glob.glob(dossier_temp +'/combining/*AF06*'))
  targets = pd.concat([targets, extraTargets], axis=0)
  targets = targets.reset_index(drop=True)
  correctCode = 'AF04-'

  baseIn = pd.read_csv(targets.iloc[0,0], sep="\t", skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  sentMax = baseIn.iloc[len(baseIn)-2,11]
  tokenMax = int(str(baseIn.iloc[len(baseIn)-1,13]).replace(correctCode,''))+1
  concatenatedInputs = baseIn

  for t in range(1,len(targets)):
    currentInput = pd.read_csv(targets.iloc[t,0], sep="\t", skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
    ## update SSIDs
    currentInput['ssid'] = pd.DataFrame([currentInput.iloc[c,11] if currentInput.iloc[c,11] == 'zzkz' else (int(currentInput.iloc[c,11]) + int(sentMax))  for c in range(len(currentInput)) ])
    currentInput['ssid'] = currentInput['ssid'].apply(pd.to_numeric, errors = 'coerce')
    sentMax = currentInput['ssid'].max()

    ## update UUIDs
    currentInput['UUID'] = pd.DataFrame([correctCode + str(int(currentInput.iloc[c,13][-5:]) + tokenMax) for c in range(len(currentInput))])
    tokenMax = int(str(currentInput.iloc[len(currentInput)-1,13]).replace(correctCode,''))+1
    
    concatenatedInputs = pd.concat([concatenatedInputs, currentInput], axis=0)
  concatenatedInputs.to_csv(targets.iloc[0,0].replace('combining/',''), sep="\t", index=False)

combineAFparts(dossier_temp)
moveAFparts(dossier_temp)

```



#05 HOPSprepare
```{python}
## EN state sentence length limit
## FR limite de longueur pour HOPS

def testSentLen(HOPS_initial, EOStypes, sLengthLimit):
  testResults = pd.DataFrame( [ [HOPS_initial.iloc[i,0],HOPS_initial.iloc[i,3]]  for i in range(len(HOPS_initial)) if  HOPS_initial.iloc[i,1] in EOStypes ])
  testResults.columns = ['len','ssid']
  testResults['len'] = testResults['len'].astype("int64")
  includeSet = set((testResults.loc[testResults['len'] < sLengthLimit]['ssid']).astype("int64"))
  excludeSet = sentValues - includeSet
  return includeSet, excludeSet

def ProcessInclus(IncludeSet):
  ## add zzkz to NA lines in ssid col
  ## EN list comprehension to take the 4 columns of data and put blank entries to serve as blank lines marking sentence boundaries if HOPSid is 0. If HOPSid is not 0, then the content is present
  ## FR compréhension de liste pour prendre les 4 colonnes de données et de mettre des vides pour servir comme lignes vides démarcant les fin de phrases si le HOPSid est égal à 0. Sinon, le contenu sera présent
  HOPS_initial['ssid'] = pd.DataFrame([HOPS_initial.iloc[j-1,3] if (HOPS_initial.iloc[j,3])  == "zzkz" else HOPS_initial.iloc[j,3] for j in range(len(HOPS_initial))])
  HOPS_initial['ssid'] = HOPS_initial['ssid'].astype("int64")
  
  ## LC to get df of all cases where the sentID of the current sentence is  fastgroup. [i,4] is Owner, so includes final punct in sents, to preserve blank lines for HOPS
  ## FR compréhension de liste pour obtenir toutes les phrases dont le sentID figure dans fastgroup.  [i,4] est la colonne Owner, pour comprendre la ponctuation de fin de phrase, pour avoir des lignes vides que nécessite HOPS en fin de phrase.
  DataHOPSinclus = pd.DataFrame(  [(HOPS_initial.iloc[i,0],HOPS_initial.iloc[i,1],HOPS_initial.iloc[i,2], HOPS_initial.iloc[i,3]) for i in range(len(HOPS_initial)) if (HOPS_initial.iloc[i, 3]  in IncludeSet)  ])
  
  ## EN name columns
  ## FR nommer les colonnes
  DataHOPSinclus.columns = ['ss_intID', 'Forme','UUID', 'ssid']
    
    ## what does this do ? :: sets zzkz to blank for export
  HOPS_out= pd.DataFrame([(DataHOPSinclus.iloc[t,0],DataHOPSinclus.iloc[t,1],DataHOPSinclus.iloc[t,2]) if DataHOPSinclus.iloc[t,0] != "zzkz" else "" for t in range(len(DataHOPSinclus))])
  
  ## EN export file with fast group sentences
  ## FR exporter le fichier avec les phrases du fastgroup
  HOPS_out.to_csv(nameForhopsInclu, index = False, header = False, sep="\t", encoding='UTF8') # output 

def ProcessExclus(ExcludeSet):
  ## add zzkz to NA lines in ssid col
  ## EN list comprehension to take the 4 columns of data and put blank entries to serve as blank lines marking sentence boundaries if HOPSid is 0. If HOPSid is not 0, then the content is present
  ## FR compréhension de liste pour prendre les 4 colonnes de données et de mettre des vides pour servir comme lignes vides démarcant les fin de phrases si le HOPSid est égal à 0. Sinon, le contenu sera présent
  HOPS_initial['ssid'] = pd.DataFrame([HOPS_initial.iloc[j-1,3] if (HOPS_initial.iloc[j,3])  == "zzkz" else HOPS_initial.iloc[j,3] for j in range(len(HOPS_initial))])
  HOPS_initial['ssid'] = HOPS_initial['ssid'].astype("int64")

  ## LC to get df of all cases where the sentID of the current sentence is  fastgroup. [i,4] is Owner, so includes final punct in sents, to preserve blank lines for HOPS
  ## FR compréhension de liste pour obtenir toutes les phrases dont le sentID figure dans fastgroup.  [i,4] est la colonne Owner, pour comprendre la ponctuation de fin de phrase, pour avoir des lignes vides que nécessite HOPS en fin de phrase.
  DataHOPSexclus = pd.DataFrame(  [(HOPS_initial.iloc[i,0],HOPS_initial.iloc[i,1],HOPS_initial.iloc[i,2], HOPS_initial.iloc[i,3]) for i in range(len(HOPS_initial)) if (HOPS_initial.iloc[i, 3]  in ExcludeSet)  ])
  ## EN name columns
  ## FR nommer les colonnes
  DataHOPSexclus.columns = ['ss_intID', 'Forme','UUID', 'ssid']
    
    ## what does this do ? :: sets zzkz to blank for export
  HOPS_outExclu= pd.DataFrame([(DataHOPSexclus.iloc[t,0],DataHOPSexclus.iloc[t,1],DataHOPSexclus.iloc[t,2]) if DataHOPSexclus.iloc[t,0] != "zzkz" else "" for t in range(len(DataHOPSexclus))])
  
  ## EN export file with fast group sentences
  ## FR exporter le fichier avec les phrases du fastgroup
  HOPS_outExclu.to_csv(nameForhopsEXCLU, index = False, header = False, sep="\t", encoding='UTF8') # output 

def hops_preparer(file_name):
  currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
  if platform.system() in ['Windows','windows']: 
    currentCode = file_name.replace(main_directory.replace('\\',''),'').replace('/temp','').replace('\\','').replace('-103.csv','')
    
  nameForhopsInclu = file_name.replace('103.csv','201.txt')
  nameForhopsEXCLU = file_name.replace('103.csv','211.txt')
  ## EN load current file and restrict to the 4 columns indicated
  ## FR charger le fichier actuel puis le restreindre aux 4 colonnes
  step2 = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)
  HOPS_initial = step2[['ss_intID', 'Forme','UUID', 'ssid']]

  ## EN create the set of all the sentence numbers (not all sentences are necessarily present due to ellipses rendered as sequences of three fullstops being recognised as sentences)
  ## FR créer une liste de toutes les sentID (certaines valeurs peuvent être absentes en raison des ellipses rendues avec trois points éventuellement identifiées comme des fin de phrases)
  sentValues = set((HOPS_initial.loc[HOPS_initial['ssid'] != "zzkz"]['ssid'].values).astype("int64"))
  TokenValues = set((HOPS_initial.loc[HOPS_initial['ss_intID'] != "zzkz"]['ss_intID'].values).astype("int64"))

## longest sent: 
  MaxTokenID = list(TokenValues)[-1] 
  return MaxTokenID, HOPS_initial, sentValues, TokenValues, nameForhopsInclu, nameForhopsEXCLU
   

## make commands for HOPS
def prepareHOPScommands(inputFile, HOPS_model_data):
  '''function to create commands for HOPS for all models in HOPS_model_data for all files '''
  # replace spaces in filepaths with literals
  inputFileTidy = inputFile.replace(' ','\ ')
  ModelsFolder = HOPS_model_data.iloc[i,5].replace(' ','\ ')[:-3]
  commands = pd.DataFrame([str("hopsparser parse " + HOPS_model_data.iloc[i,5].replace(' ','\ ') + "/ " + inputFileTidy +  " "+ inputFileTidy.replace('-201.txt','') + str(HOPS_model_data.iloc[i,5].replace(' ','\ ')).replace(ModelsFolder,'-') + '.csv') for i in range(0, len(HOPS_model_data))]) ## leave as 1 to omit oldhopsModel
  #os.system(commands.iloc[0,0])
  return commands

## zip HOPSPreparedFiles
def zipHOPSPrepared(main_directory):
  ''' function to zip files prepared for HOPS'''
  with ZipFile((main_directory + "201asdfasdfa.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(main_directory + 'HOPS/').rglob("*2*1.txt"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(main_directory + 'HOPS/')))

def runHOPSpreparers(main_directory, dossier_temp):
  '''function that combines all functions into a single command'''
  ## EN declare directories and get list of files to process
  ## FR identifier les fichiers créés par f2 dans le dossier_temp et déclarer les chemins nécesaires
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
sLengthLimit = 400
EOStypes = ["!","?",".",":",";"]
HOPS_model_data = pd.read_csv("/Users/username/Desktop/HOPS_models/HOPS_models.csv", sep=";")
allCommands = pd.DataFrame()

for f in range(0, len(theseFiles)):
  file_name = theseFiles.iloc[f,0]
  ## get info on sentences, get df, and values to determine whether to apply  IF or the ELSE 
  MaxTokenID, HOPS_initial, sentValues, TokenValues, nameForhopsInclu, nameForhopsEXCLU = hops_preparer(  file_name)
  if MaxTokenID <= sLengthLimit:
    IncludeSet = sentValues
    ProcessInclus(IncludeSet)
  else:
    IncludeSet, ExcludeSet = testSentLen(HOPS_initial, EOStypes, sLengthLimit)
    ProcessInclus(IncludeSet)
    ProcessExclus(ExcludeSet)
  print("Processing " + str(f+1) + " sur " + str(len(theseFiles)) +  " == " + file_name.replace(dossier_temp,'') + "  ==> exporté vers " + file_name.replace(dossier_temp,'').replace('103','201'), flush=True)
  commands = prepareHOPScommands(file_name, HOPS_model_data)
  allCommands = pd.concat([allCommands, commands], axis=0)  

  allCommands.to_csv(main_directory + 'HOPS//HOPScommands.csv', index=False, header=False)
  zipHOPSPrepared(main_directory)
  #### now have 201, 211 files exported as well as a list of commands
######## end  definitions


## single line to run all HOPS prep
runHOPSpreparers(main_directory, dossier_temp)


```

#05n HOPS note
```{text}
Préparation pour HOPS 
EN Note on HOPS
HOPS' documentation mentions a limit of 510 tokens when tokenising sentences. However, this limit does not correspond precisely with LGERM's tokenisation, the difference between the two approaching 30% of the length of the sentence. A conservative value of 400 has been used in the function below, which will check that the number of tokens in each sentence is less than this limit. If all sentences fall into this category, one file will be created, with a 201 suffix. If any sentences containing more tokens than the defined limit, these will be exported in a separate file with a -211.csv suffix, because HOPS stops all processing if a sentence is too long.

FR Note sur HOPS
HOPS dit avoir une limite de 510 tokens pour pouvoir parser une phrase. Cependant, cette limite ne correspond pas parfaitement au décompte d'unités lexicales telles que LGERM les compte, la différence pouvant atteindre 30  % de la longueur de la phrase : une phrase avec 417 tokens donne lieu au message que la phrase comprend 610 tokens. Une valeur conservatrice de 400 a été utilisée dans ce script, qui va vérifier que les phrases ont une longueur inférieure à cette limite. Si toutes les phrases répondent à ce critère, 1 fichier sera crée, avec suffixe 201. Si des phrases trop longues pour être traitées par HOPS, celles-ci seront exportées séparamment dans un fichier -211.csv, car HOPS s'arrête en cas de phrase trop longue.


```

#06. TT Prepare
```{python}
'''préparer les données pour TreeTagger'''
# :: TO DO :: rewrite prepareTT() to use TT dossier and tidy names
# new names

def prepareTT(file_name):
  '''Prepare data for TreeTagger '''
  ## EN take current file,  construct names of files to output
  ## FR prendre le fichier actuel et construire les nom des fichiers à sortir
  currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
  nameForTTBinder = file_name.replace('-103.csv','-400binder.csv').replace(dossier_temp, dossier_tt)
  nameForTTdata = nameForTTBinder.replace('-400binder.csv','-401.txt')

  ## EN import current file, restrict to specified columns
  # FR importer le fichier actuel, en les limitant aux colonnes précisées
  step2 = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)
  if currentCode[:2] in ['CA','CM']:
    formColName = 'C_FORME'
  else:
    formColName = 'Forme'
    
  TreeTagging = step2[[formColName,'UUID']]
  
  ## EN  get cases where form is neither zzkz ou zzblank then set TreeTagBinder to toTreeTag  with 2 cols, then drop TokenID column to leave data to toTreeTag 
  ## FR prendre les cas où la forme n'est égale ni à zzkz ou zzblank, et créer une df TreeTagBinder avec formes + TokenID, puis supprimer la colonne des TokenID en toTreeTag
  toTreeTag = TreeTagging.loc[TreeTagging[formColName] != "zzkz"]
  toTreeTag = toTreeTag.loc[toTreeTag[formColName] != "zzblank"]
  toTreeTag = toTreeTag.loc[toTreeTag[formColName] != "»"]
  toTreeTag = toTreeTag.loc[toTreeTag[formColName] != "«"]
  toTreeTag = toTreeTag.loc[toTreeTag[formColName] != ";"]
  toTreeTag = toTreeTag.loc[toTreeTag[formColName] != ":"]
  toTreeTag = toTreeTag.loc[toTreeTag[formColName] != "\'"]
  
  # add noise as _xkw + row number to force tagging with noisy data
  # toTreeTag['TagID'] = [str("_xkw") + str(int(toTreeTag.iloc[i, 1])) for i in range(len(toTreeTag))  ]
  TreeTagBinder = toTreeTag
  toTreeTag = toTreeTag.drop('UUID', axis=1)
  
  ## EN export data , using the filenames created above, which includes the .txt extension. 
  ## FR exporter les données en utilisant les noms de fichier créés plus haut qui contiennent l'extension .txt.
  toTreeTag.to_csv(nameForTTdata, sep='\t' , header=False, index=False, encoding = "UTF8")
  TreeTagBinder.to_csv(nameForTTBinder, sep='\t' , header=False, index=False)
  
  message = ("Done " + currentCode)
  return(message)

## zip TTPreparedFiles
def zipTTPrepared(main_directory):
  ''' function to zip files prepared for TT'''
  with ZipFile((main_directory + "400.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(main_directory + 'PourTreeTagger/').rglob("*"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(main_directory + 'PourTreeTagger/')))

## end of definitions

## run section for above function

def runTTpreparers(main_directory, dossier_temp):
  ## EN identify files containing body of text and organise them in a dataframe
  # FR identifier les fichiers créés par f2 dans le dossier_temp et organiser ces noms dans une dataframe
  theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
  theseFiles = theseFiles.sort_values(0)
  theseFiles.reset_index(inplace=True, drop=True)
  theseFiles.columns=['Path']
  
  ## EN loop to process each file individually
  ## boucle pour traiter chaque fichier  individuellement
  for f in range(len(theseFiles)):
    # startTime = time.time()
    file_name = theseFiles.iloc[f,0] 
    prepareTT(file_name)
    zipTTPrepared(main_directory)
  
## single line to run all TT prep
runTTpreparers(main_directory, dossier_temp)

  
```

#07. Deuc Prepare
```{python}
'''préparer les données pour les traitements par les modèles de Deucalion '''

def prepareDeucDossiers(dossier_Deuc, deucParams):
  ##EN  make subfolders 
  ## FR créer les sous-dossiers de stockage
  for m in range(len(deucParams)):
    currentPath = dossier_Deuc  + deucParams.iloc[m,0] + "/"
    if os.path.exists(currentPath)==False:
      os.makedirs(currentPath)

def prepare_deucalionFiles(file_name):
  DeucMaking = pd.read_csv(file_name, sep="\t", usecols = [1,13], encoding = 'UTF8')
  DeucBinder = pd.DataFrame([ [DeucMaking.iloc[i,0], DeucMaking.iloc[i,1]] for i in range(len(DeucMaking))  if (DeucMaking.iloc[i,0] != "zzkz")])
  DeucBinderFile = file_name.replace('103','300-binder').replace('temp', 'PourDeucalion')
  DeucDataFile = file_name.replace('103','300').replace('temp', 'PourDeucalion')
  DeucBinder.to_csv(DeucBinderFile, index=False, sep="\t", header=None)
  DeucBinder.drop([1], axis=1).to_csv(DeucDataFile, index=False, sep="\t", header=None)
  inputFileList = [DeucBinderFile, DeucDataFile]
  deucCopier(inputFileList)
  return DeucDataFile

# copy files where necessary  
def deucCopier(inputFileList):
  for i in inputFileList:
    shutil.copy(i, i.replace('PourDeucalion','PourDeucalion/freem'))
    shutil.copy(i, i.replace('PourDeucalion','PourDeucalion/fr'))
    shutil.move(i, i.replace('PourDeucalion','PourDeucalion/fro'))

       
def zipDeucFiles(dossier_Deuc):
  with ZipFile((main_directory + "DeucInputs.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(dossier_Deuc + 'fro/').rglob("*"):
           archive.write(file_path, arcname=file_path.relative_to(dossier_Deuc))

## function to make list of commands
def prepareDeucCommands(DeucDataFile, deucParams, pieHeader):
  DeucDataFileTidy = DeucDataFile.replace(' ','\ ')
  DeucCommands = pd.DataFrame([str(pieHeader + deucParams.iloc[i,0] + " " + DeucDataFileTidy.replace('PourDeucalion/', str('PourDeucalion/' + deucParams.iloc[i,0] + '/'))) for i in range(len(deucParams))])
  return DeucCommands

#### declarations
# files to process : 103.csv files in temp dossier

def doDeucPrepare(dossier_temp, dossier_Deuc):
  '''function to run deuc preparation functions in sequence '''
  ## declarations
deucParams = pd.DataFrame([{'model':"fro",'code':"310"},{'model':"freem",'code':"320"},{'model':"fr",'code':"330"}])
pieHeader = f'pie-extended tag --no-tokenizer '
allDeucCommands = pd.DataFrame()
inputFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
inputFiles = inputFiles.sort_values(0)
inputFiles.reset_index(inplace=True, drop=True)
inputFiles.columns=['Path']
## call functions
prepareDeucDossiers(dossier_Deuc, deucParams)   ### makes subfolders
for f in range(len(inputFiles)):              #### for each file
  file_name = inputFiles.iloc[f,0]            #### get path
  DeucDataFile = prepare_deucalionFiles(file_name)          #### prepare binder + data file, copy, move
  DeucCommands = prepareDeucCommands(DeucDataFile, deucParams, pieHeader) ## make commands
  allDeucCommands = pd.concat([DeucCommands, allDeucCommands], axis=0) ## concat commands
allDeucCommands.to_csv(dossier_Deuc + "deucCommands.csv", index=False, header=None, sep="\t", encoding='UTF8') ## export list of commands
  zipDeucFiles(dossier_Deuc)   ## zip all outputfiles


#### 1 line to run:
doDeucPrepare(dossier_temp, dossier_Deuc)
dossier_temp = "/Users/username/PhraseoRoChe/Pipeline/refresh/scores/correct"
dossier_Deuc = "/Users/username/PhraseoRoChe/Pipeline/refresh/PourDeucalion"

```

#08. UD Prepare
```{python}
## declare function
def prepareUD(file_name):
  '''cette fonction créera les fichiers que UDPipe prendra comme entrée, ainsi que le fichier binder qui permettra de réaligner la sortie de ces traitements.'''
  UDBinderName = file_name.replace('.csv','-500-binder.csv').replace(dossier_temp,dossier_UD)
  UDoutputRaw = UDBinderName.replace('.csv','-500.txt')
  UDoutputTidy =UDoutputRaw.replace('500','502')

  ## EN read tidy LGERM body data, name columns
  ## FR charger les données LGERM propres pour le corps du texte, nommer colonnes
  LGERMBody = pd.read_csv(file_name, sep="\t", low_memory=False, encoding="UTF8", skip_blank_lines=False)
  LGERMBody.columns = ['TokenID', 'LGERM_Forme', 'LGERM_XPOS', 'LGERM_lemme', 'LGERM_code','d','f','Pp','Para', 'TokenChap','ss_intID' ,'ssid' ,'FormeOut','UUID'] 
  
  ## isolate columns to use in binder and save csv
  ## Restreindre aux colonnes à mettre dans le fichier binder et enregistrer sous csv
  udBinder = LGERMBody[['UUID', 'LGERM_Forme']]
  
  udBinder.to_csv(UDBinderName, sep="\t", encoding = "UTF8", index=False, header=None)
  udBinder.drop(['UUID'], axis=1).to_csv(UDoutputRaw, index=False, sep="\t", header=None)

  ## Read in raw data just written, as file and tidy by replacing entries where open+close quote marks are the entire string, then close raw file
  ## Lire les données qui viennent d'être écrites comme fichier, puis les toiletter en remplaçant par rien chaque cas où la chaine de caractères est simplement l'ouverture puis la fermeture de double guillemets. Fermer le fichier.
  fin = open(UDoutputRaw, "rt", encoding = "UTF-8")
  binder_data = fin.read().replace('\n""','\n').replace('\nzzkz\tzzkz','\n') 
  fin.close()
  # export binder final version
  fin = open(UDoutputTidy, "w", encoding = "UTF-8")
  fin.write(binder_data)
  fin.close()

#### end of defintiion
## Declare directories, paths and files. Set list elements to create specifc versions, et URL

def runUDpreparers(dossier_temp, dossier_UD):
  '''Function that takes 103.csv files, iterates over them, calling the prepareUD function, and creates 500.zip upon completion ; 'dossier_temp' contains 103.csv files ; 
  'dossier_UD' is  output destination '''
  ## FR Déclarer les dossiers, chemins et fichiers. Déclarer des éléments pour la création de fichiers de sortie spécifiques et l'URL
  theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
  theseFiles = theseFiles.sort_values(0)
  theseFiles.reset_index(inplace=True, drop=True)
  theseFiles.columns=['Path']
  
  ## EN Loop over files to get current filename, extract code, name, create names, paths for binder, output as well as punctuation file to import 
  for f in range(len(theseFiles)):
    file_name = theseFiles.iloc[f,0] 
    prepareUD(file_name)
  
  # make archive of PourUDfolder
  with ZipFile((main_directory + "500.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(dossier_UD).rglob("*"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(dossier_UD)))


## 1 line:
runUDpreparers(dossier_temp, dossier_UD)
  dossier_temp = "/Users/username/PhraseoRoChe/Pipeline/refresh/temp/"
  
```

#09 Stanza Prepare
```{python}
#  :stanza prepare
def prepareStanza(inputFile):
  '''préparer les textes pour stanza : construire les phrases et les suites de UUIDs'''
  aligned_df = pd.read_csv(inputFile, skip_blank_lines=False, sep="\t")
  challenge_set = [i  for i in range(len(aligned_df)) if ((aligned_df.iloc[i,4] == "ponctuation")  )]
  restricted_df = aligned_df.drop(challenge_set)
  restricted_df = restricted_df.reset_index(drop=True)
  
  ## set EOS chars to blanks
  restricted_df['FORME'] = pd.DataFrame([""  if (restricted_df.iloc[i,1] in ["zzkz",".","!","?",":",";"]) else restricted_df.iloc[i,1] for i in range(len(restricted_df))])
  restricted_df = restricted_df.dropna()
  #### set dtypes
  restricted_df['ssid'] = restricted_df['ssid'].apply(pd.to_numeric, errors = 'coerce')
  restricted_df['ssid'] = restricted_df['ssid'].fillna(0).astype("int64")
  ## EN find number of sentences in text, create df to hold processed sentences
  ## FR trouver le nombre de phrases dans le texte, créer une dataframe pour stocker les phrases traitées
  restricted_df = restricted_df[['TokenID', 'FORME', 'UUID', 'ss_intID', 'ssid']]
  restricted_df = restricted_df.reset_index(drop=True)
  
  ##### make df of sentences to pass to stanza
  corpus = restricted_df
  
  senSet = set(corpus['ssid'].to_list())
  senSet = senSet - {0}
  allOutput = pd.DataFrame()
  
  for s in senSet:
    sentence = corpus.loc[corpus['ssid']== s ]
    thisMin = (sentence.index).min()
    thisMax = (sentence.index).max()+1
    sentenceA = corpus[thisMin:thisMax]
    sentenceUUIDs = list((sentenceA['UUID']))
    uuid_len = len(sentenceUUIDs)
    sentenceUUIDsConverted = str(sentenceUUIDs)
    sentenceA = sentenceA[['FORME']]
    
    currentSentence = ""
    for w in range(len(sentenceA)):
      thisWord = str(sentenceA.iloc[w,0] )
      if thisWord == '\x85':
        thisWord = html.unescape('&hellip;')
      currentSentence = str(currentSentence) + " " + thisWord
    
    currentOutput = {'sentID':s, 'text':currentSentence, 'sMin':thisMin, 'sMax':thisMax,'s':sentenceUUIDsConverted}#, 'uuid_len':uuid_len, 'delta':thisMax-thisMin}
    output = pd.DataFrame(currentOutput, index = [s])
    allOutput = pd.concat([allOutput, output], axis=0)
  
  if os.path.exists(main_directory + 'PourStanza/')==False:
    os.makedirs(main_directory + 'PourStanza/')

  allStanzaOutputname = inputFile.replace('temp', 'PourStanza').replace('103','601')
  allOutput.to_csv(allStanzaOutputname, sep="\t", index=False)

def prepareStanzaForCAfiles(inputFile):
  '''préparer les textes pour stanza : construire les phrases et les suites de UUIDs'''
  aligned_df = pd.read_csv(inputFile, skip_blank_lines=False, sep="\t")
  challenge_set = [i  for i in range(len(aligned_df)) if ((aligned_df.iloc[i,3] == "PUNCT")  )]
  restricted_df = aligned_df.drop(challenge_set)
  restricted_df = restricted_df.reset_index(drop=True)
  
  ## set EOS chars to blanks
  restricted_df['C_FORME'] = pd.DataFrame([""  if (restricted_df.iloc[i,1] in ["zzkz",".","!","?",":",";"]) else restricted_df.iloc[i,1] for i in range(len(restricted_df))])
  # restricted_df = restricted_df.dropna()
  #### set dtypes
  restricted_df['ssid'] = restricted_df['ssid'].apply(pd.to_numeric, errors = 'coerce')
  restricted_df['ssid'] = restricted_df['ssid'].fillna(0).astype("int64")
  ## EN find number of sentences in text, create df to hold processed sentences
  ## FR trouver le nombre de phrases dans le texte, créer une dataframe pour stocker les phrases traitées
  restricted_df = restricted_df[['C_id', 'C_FORME', 'UUID', 'ss_intID', 'ssid']]
  restricted_df = restricted_df.reset_index(drop=True)
  
  ##### make df of sentences to pass to stanza
  corpus = restricted_df
  
  senSet = set(corpus['ssid'].to_list())
  senSet = senSet - {0}
  allOutput = pd.DataFrame()
  
  for s in senSet:
    sentence = corpus.loc[corpus['ssid']== s ]
    thisMin = (sentence.index).min()
    thisMax = (sentence.index).max()+1
    sentenceA = corpus[thisMin:thisMax]
    sentenceUUIDs = list((sentenceA['UUID']))
    uuid_len = len(sentenceUUIDs)
    sentenceUUIDsConverted = str(sentenceUUIDs)
    sentenceA = sentenceA[['C_FORME']]
    
    currentSentence = ""
    for w in range(len(sentenceA)):
      thisWord = str(sentenceA.iloc[w,0] )
      if thisWord == '\x85':
        thisWord = html.unescape('&hellip;')
      currentSentence = str(currentSentence) + " " + thisWord
    
    currentOutput = {'sentID':s, 'text':currentSentence, 'sMin':thisMin, 'sMax':thisMax,'s':sentenceUUIDsConverted}#, 'uuid_len':uuid_len, 'delta':thisMax-thisMin}
    output = pd.DataFrame(currentOutput, index = [s])
    allOutput = pd.concat([allOutput, output], axis=0)
  
  if os.path.exists(main_directory + 'PourStanza/')==False:
    os.makedirs(main_directory + 'PourStanza/')

  allStanzaOutputname = inputFile.replace('temp', 'PourStanza').replace('103','601')
  allOutput.to_csv(allStanzaOutputname, sep="\t", index=False)

## zip stanzaPreparedFiles
def zipStanzaPrepared(main_directory):
  ''' function to zip files prepared for Stanza'''
  with ZipFile((main_directory + "600.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(main_directory + 'PourStanza/').rglob("*610*"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(main_directory + 'PourStanza/')))

# get files to process


def runStanzaPrepares(dossier_temp, dossier_Stanza, main_directory):
  '''Function that takes 103.csv files, iterates over them, calling the prepareStanza function, and creates 600.zip upon completion ; 'dossier_temp' contains 103.csv files ; 
  'dossier_Stanza' is  output destination '''
  ## FR Déclarer les dossiers, chemins et fichiers. Déclarer des éléments pour la création de fichiers de sortie spécifiques et l'URL
  theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
  theseFiles = theseFiles.sort_values(0)
  theseFiles.reset_index(inplace=True, drop=True)
  theseFiles.columns=['Path']
  
  ## EN Loop over files to get current filename, extract code, name, create names, paths for binder, output as well as punctuation file to import 
  for f in range(len(theseFiles)):
    inputFile = theseFiles.iloc[f,0] 
    prepareStanza(inputFile)
    # prepareStanzaForCAfiles(inputFile)
    zipStanzaPrepared(main_directory)

## single line:
runStanzaPrepares(dossier_temp, dossier_Stanza, main_directory)
dossier_temp = "/Users/username/Dropbox/__doodling/inputForR/temp"
main_directory = "/Users/username/Dropbox/__doodling/inputForR/"
dossier_Stanza = "/Users/username/Dropbox/__doodling/inputForR/"

```

# 15 HOPS processing
```{python}

# def processHOPS(HOPScommandfile):
#   '''function to load csv of HOPS commands as df, and iterate over HOPS commands. Runs all commands sequentially in base terminal environment on CPU. For many files, prefer multiple simultaneous instances of virtual terminal in a command line'''
#   HOPScommands = pd.read_csv(HOPScommandfile, header=None)
#   for i in range(len(HOPScommands)):
#     currentCommand = HOPScommands.iloc[i,0]
#     os.system(currentCommand)
# 
# processHOPS(main_directory + 'HOPS/HOPScommands.csv')

```

# 16 TT processing  
```{python}
def process_TT(file_name):
  tt_AbsPath = "/Users/username/Dropbox/models/tree-tagger-MacOSX-Intel-3.2.3/cmd" # declare absolute path to folder containing tree-tagger-old-french; don't added final slash
  tt_AbsPathForTerminal = tt_AbsPath.replace(' ','\ ')
  file_name_forTerminal = file_name.replace(' ','\ ')
  tt_header = 'cat '
  tt_chunk = f' | {tt_AbsPathForTerminal}/tree-tagger-old-french > '  
  file_name_out = file_name_forTerminal.replace('401','402')
  command = f'{tt_header}{file_name_forTerminal}{tt_chunk}{file_name_out}'
  os.system(command)
file_name_forTerminal = '/Users/username/Desktop/test.txt'
def runTTprocessors(main_directory, dossier_tt):
  ## EN loop to process each file individually
  ## boucle pour traiter chaque fichier  individuellement
  theseFiles = pd.DataFrame(glob.glob(dossier_tt +'/*401.txt'))
  theseFiles = theseFiles.sort_values(0)
  theseFiles.reset_index(inplace=True, drop=True)
  theseFiles.columns=['Path']
  
  for f in range(len(theseFiles)):
    # startTime = time.time()
    file_name = theseFiles.iloc[f,0] 
    process_TT(file_name)
  
  with ZipFile((main_directory + "402.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(dossier_tt).rglob("*402.txt"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(dossier_tt)))

# 1 line:
run TTprocessors(main_directory, dossier_tt)

```

#16n.TT notes 
```{text}

OSX_tagger = 'https://cis.uni-muenchen.de/~schmid/tools/TreeTagger/data/tree-tagger-MacOSX-Intel-3.2.3.tar.gz'
tagger_scripts = 'https://cis.uni-muenchen.de/~schmid/tools/TreeTagger/data/tagger-scripts.tar.gz'
installation_script = 'https://cis.uni-muenchen.de/~schmid/tools/TreeTagger/data/install-tagger.sh'
OF_params = 'https://cis.uni-muenchen.de/~schmid/tools/TreeTagger/data/old-french.par.gz'


download all files to pwd
cd to pwd, then run install.sh
decompress OFpars, move to lib
decompress scripts, move OF elements to /cmd
ensure that utf8-tokenize.perl is in CMD
run test : echo 'le chat est grand' | ./cmd/tree-tagger-old-french > ./testout.txt
hardcode change of source directory : open /cmd/tree-tagger-old-french and add absolute path, no spaces, for BIN, CMD and LIB, with leading slashes
 
# to deactivate the tokeniser:
== delete the following
@ line 11 : TOKENIZER=${CMD}/utf8-tokenize.perl
@ line 9  : -token
@ line 16 : entire line
# save. this removes tokenizer from pipeline, and will output only POS, lemmes based on lines. 


```

# 17 Deuc processing 
```{python}
# deactivated to allow use multiple venvs
# def processDeuc(DeucCommandfile):
#   '''function to load csv of Deuc commands as df, and iterate over Deuc commands. Runs all commands sequentially in base terminal environment on CPU. For many files, prefer multiple simultaneous instances of virtual env in a command line'''
#   DeucCommands = pd.read_csv(DeucCommandfile, header=None)
#   for i in range(len(DeucCommands)):
#     currentCommand = DeucCommands.iloc[i,0]
#     os.system(currentCommand)
# 
# processDeuc(dossier_Deuc+ "deucCommands.csv")

```


#17n Deuc notes
```{text}
It may be useful to use the Deucalion processing pipelines locally, and to install pie-extended in a virtual environment (import virtualenv; virtualenv -p python3 deucalionEnv; activate and deactivate env before and after use…) but precisely how depends on platform, py version/installation…, advantage of virtual envs is that they can be instantiated multiple times, and process several texts in parallel, as each instance doesn't push CPU to 100%
# import virtualenv
# virtualenv -p python3 freemdeuc
# virtualenv mytest
# 
# virtualenv env

###### setup -env, dls -- install errors on pc/windows
## terminal - OSX
conda create -n deucProcessor python=3.9 anaconda ## create env
conda activate deucProcesser
pip install pie_extended
#when done : conda deactivate

To download pie-extended : pip install pie_extended
Then download modules for each language state
pie-extended download freem ##128Mo
pie-extended download fro  ## 348 mo
pie-extended download fr  ## 282 Mo

If git is not installed locally, an error message can appear, saying this is fatal, but it is not.
The progress bar does not advance, but the file being written can be read to determine progress.

# /source env/bin/activate

## terminal - Windows
## note : windows version seems to have charset errors when taking UTF8, may need to add converter
virtualenv -p python3 deuc
.\deuc\Scripts\activate
pip install pie_extended
# when done : deactivate
note@ ATR : use in base env

```

#18 UD Process::  TO DO  ::
```{python}

def buildText(inputfile):
  '''take Input created for Stanza, and concatenate sentences with double line breaks'''
  TextCode = inputfile[-12:-8]
  inputData = pd.read_csv(inputfile,  sep="\t")
  Text = inputData.iloc[0,1]
  for i in range(1, len(inputData)):
    Text = Text + "\n\n" + inputData.iloc[i,1]
  return TextCode, Text

def makeStatements(all_results, Text, TextCode):
  '''make a df of commands to send to commandline for the current text '''
  ModelsList = ['old_french', 'french-gsd','french-parisstories','french-partut','french-rhapsodie','french-sequoia']

  for i in ModelsList:
    thisModel = i
    SaveFile = dossier_Stanza + f'601/{TextCode}-{thisModel}.json'
    SaveFileT = SaveFile.replace(' ','\ ')
    theCommand = f'curl -F data="""{Text}""" -F model={thisModel} -F tokenizer= -F tagger= -F parser= http://lindat.mff.cuni.cz/services/udpipe/api/process > {SaveFileT}'
    result = pd.DataFrame([{'Model':thisModel,'SaveFile':SaveFile, 'SaveFileT':SaveFileT,'command':theCommand}])
    all_results = pd.concat([all_results, result])
  return all_results.iloc[13,2]
# all_results.to_csv('/Users/username/Desktop/allResults.csv', header=True, index=False, sep="\t")
## need to run above for all texts, then run all these get commands
def useGETapi(all_results):
  for i in range(len(all_results)):
    currentCommand = all_results.iloc[i,3]
    SaveFile = all_results.iloc[i,2]
    os.system(currentCommand)
    processJSON(SaveFile, dossier_Stanza)
# # moved to UDpost
# # def processJSON(SaveFile, dossier_Stanza):
# #   myInput = open(SaveFile)
# #   result = json.load(myInput)['result']
# #   myInput.close()
# 
#   with open(dossier_Stanza + "parsedSent.csv", 'w') as fileA:
#     cleanedResult = re.sub('\n# .+\n','\n',result)
#     fileA.writelines(cleanedResult)
#   myLines =  open(dossier_Stanza + "parsedSent.csv").readlines()[4:-1]
#   with open(dossier_Stanza + "parsedSent.csv", 'w') as fileA:
#     fileA.writelines(myLines)
#   
#   table = pd.read_csv(dossier_Stanza + "parsedSent.csv", header=None, sep="\t")
#   table.columns = ['UDid', 'UDforme','supp','UDpos', 'UDXPOS','UD_Feats','UD_dep','UD_role','supp','supp']
#   table = table.drop("supp", axis=1)
#   table = table.dropna(subset=['UD_role'])
#   table.to_csv(SaveFile.replace('json','csv'), sep="\t", index=False, encoding="UTF8")
  
def do_building(all_results, dossier_Stanza):
  for inputfile in glob.glob(dossier_Stanza + '/601/*-601.csv'):
    TextCode, Text = buildText(inputfile)
    all_results = makeStatements(all_results, Text, TextCode)
  return(all_results)

def doUDpipeprocessing()
  all_results = pd.DataFrame():
  all_results = do_building(all_results, dossier_Stanza)
  useGETapi(all_results)


 

```

#19 Stanza Process
```{python}
# file_name = file_name.replace('CA17','CM03')
def Stanza_tag_FM(file_name):
  '''function to take allOutput files and process with Modern French pipeline'''
  allOutput = pd.read_csv(file_name, sep="\t")
  allOutput['s'] = allOutput['s'].apply(lambda x: ast.literal_eval(x))
  processed_FM = pd.DataFrame()
  for s in range(len(allOutput)):
  # s = len(allOutput)-1
    currentSentence = allOutput.iloc[s,1]
    currentDict = allOutput.iloc[s,4]
    doc_FMod = nlp_FMod(currentSentence)
    dict_FMod = doc_FMod.to_dict()
    conll_FMod = CoNLL.convert_dict(dict_FMod)
    FMod_out  = pd.DataFrame(conll_FMod[0]) 
    FMod_out.columns = StanzaColNamesFMod
    FMod_out = FMod_out.drop(['blank1', 'blank3', 'start_end_char'], axis=1)
    FMod_out = FMod_out.astype(myDtypesFMod)
    FMod_out['TokenID'] = currentDict #= currentDict[:-1]
  # FMod_out.to_csv("/Users/username/Desktop/sentOut.csv", sep="\t", index=False)

  ##EN add the processed sentence to the dataframe of processed sentences and save to disk with correct name + path. Loop continues with next sentence then next text.
  ##FR ajouter la phrase traitée en bas de la dataframe contenant les phrases traitées puis exporter ce document vers csv, avec le nom + chemin crées plus haut. La boucle se poursuivra avec les phrases suivantes, puis les textes suivants.
  processed_FM = pd.concat([processed_FM, FMod_out])

  StanzaOutputNameFMod = file_name.replace('601','620')
  processed_FM.to_csv(StanzaOutputNameFMod, sep="\t", index=False, header=True)

def Stanza_tag_OF(file_name):
  '''function to take allOutput files and process with Old French pipeline'''
  allOutput = pd.read_csv(file_name, sep="\t")
  allOutput['s'] = allOutput['s'].apply(lambda x: ast.literal_eval(x))
  processed_OF = pd.DataFrame()
  for s in range( len(allOutput)):
    currentSentence =  allOutput.iloc[s,1]
    currentDict = allOutput.iloc[s,4]
    doc_OF = nlp_OF(currentSentence)
    dicts_OF = doc_OF.to_dict() 
    conll_OF = CoNLL.convert_dict(dicts_OF)
    OF_out  = pd.DataFrame(conll_OF[0]) 
    # len(OF_out)
    # len(currentDict)
    OF_out.columns =StanzaColNamesOF
    OF_out = OF_out.drop(['blank1', 'blank3', 'start_end_char'], axis=1)
    OF_out = OF_out.astype(myDtypesOF)
    OF_out['TokenID'] = currentDict 

    ##EN add the processed sentence to the dataframe of processed sentences and save to disk with correct name + path. Loop continues with next sentence then next text.
    ##FR ajouter la phrase traitée en bas de la dataframe contenant les phrases traitées puis exporter ce document vers csv, avec le nom + chemin crées plus haut. La boucle se poursuivra avec les phrases suivantes, puis les textes suivants.
    processed_OF = pd.concat([processed_OF, OF_out])

  StanzaOutputNameOF = file_name.replace('601','610')
  processed_OF.to_csv(StanzaOutputNameOF, sep="\t", index=False, header=True)

## zip StanzaPreparedFiles
def zipStanzaProcessed(main_directory):
  ''' function to zip files prepared for Stanza'''
  with ZipFile((main_directory + "620asdfasdf.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(main_directory + 'PourStanza/').rglob("*620*"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(main_directory + 'PourStanza/')))
  with ZipFile((main_directory + "610asdfas.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(main_directory + 'PourStanza/').rglob("*610*"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(main_directory + 'PourStanza/')))


#### declarations
# dictionaries
myDtypesOF = {"StanzaID": int, "StanzaForme": object, "StanzaUPOS_OF": object, "StanzaXPOS_OF": object, "StanzaFeats_OF": object, "StanzaDepOn_OF": int, "StanzaRel_OF": object}
myDtypesFMod = {"StanzaID": int, "StanzaForme": object, "StanzaUPOS_FMod": object, "StanzaXPOS_FMod": object, "StanzaFeats_FMod": object, "StanzaDepOn_FMod": int, "StanzaRel_FMod": object}

## colnames
StanzaColNamesOF =['StanzaID', 'StanzaForme', 'blank1', 'StanzaUPOS_OF', 'StanzaXPOS_OF','StanzaFeats_OF', 'StanzaDepOn_OF','StanzaRel_OF','blank3','start_end_char']
StanzaColNamesFMod =['StanzaID', 'StanzaForme', 'blank1','StanzaUPOS_FMod','StanzaXPOS_FMod','StanzaFeats_FMod','StanzaDepOn_FMod','StanzaRel_FMod','blank3','start_end_char']


def ProcessStanzaAll(main_directory):
theseFiles = pd.DataFrame(glob.glob(main_directory + 'PourStanza/' + "*601.csv"))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
# do FM tagging
nlp_FMod = stanza.Pipeline(lang='fr', processings='tokenize,pos,lemma,depparse', tokenize_pretokenized=True, logging_level = "Info")
for h in range(len(theseFiles)):
  file_name = theseFiles.iloc[h,0]
  Stanza_tag_FM(file_name)

# do OF tagging
nlp_OF = stanza.Pipeline(lang='fro', processings='tokenize,pos,lemma,depparse', tokenize_pretokenized=True)
for g in range(len(theseFiles)):
  file_name = theseFiles.iloc[g,0]
  Stanza_tag_OF(file_name)
  
  zipStanzaProcessed(main_directory)


### single line to run all
ProcessStanzaAll(main_directory)
## note to AT : Run in TweetsSession, not ScraperSession
```


#25. HOPS Post
```{python}

def archiveHOPSfiles(main_directory, HOPSfolder, offset):
  '''function to take HOPS files and archive ; takes 3 inputs, last of which is offset in list of WORD + .zip extension. 0 gives raw.zip, 1 gives tidy.zip'''
  HOPS_model_data = pd.read_csv("/Users/username/Desktop/HOPS_models/HOPS_models.csv", sep=";")
  Extensions = ['raw.zip','tidy.zip']
  for m in range(0, len(HOPS_model_data)):
    with ZipFile((main_directory + str(HOPS_model_data.iloc[m,2])+ str(Extensions[offset])), "w", ZIP_DEFLATED, compresslevel=9) as archive:
      for file_path in pathlib.Path(HOPSfolder).rglob("*" + str(HOPS_model_data.iloc[m,2]) + ".csv"):
        archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(HOPSfolder)))

def skippedSentencesChecker(HOPSfolder):
  '''Test if sentences were excluded from HOPS analysis. If so, get file skipped, homogenise, add to analysed data for processing ''' 
  has_skipped = glob.glob(HOPSfolder + '/*211.txt')
  if len(has_skipped) > 0:
    for i in range(len(has_skipped)):
      currentSkippedCode = has_skipped[i][-12:-8]
      SkippedFile = has_skipped[i]
      skippedData = pd.read_csv(SkippedFile, sep="\t", header=None, names = ['hopsID','HOPSforme','UUID'])
      skippedData['HOPS_UPOS'] = "XXXX"
      skippedData['col4'] = "XXXX"
      skippedData['col5'] = "XXXX"
      skippedData['HOPSdepOn'] = "XXXX"
      skippedData['HOPSrole'] = "XXXX"
      skippedData['col8'] = "XXXX"
      skippedData['col9'] = "XXXX"
      HOPSedFiles = list(set(glob.glob(HOPSfolder + f'{currentSkippedCode}*') ) - set(glob.glob(HOPSfolder + f'{currentSkippedCode}-211.txt')) - set(glob.glob(HOPSfolder + f'{currentSkippedCode}-201.txt')))
      for h in range(len(HOPSedFiles)):
        hopsedData = pd.read_csv(HOPSedFiles[h], sep="\t",  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False, header=None, names =['hopsID', 'HOPSforme', 'UUID', 'HOPS_UPOS', 'col4','col5','HOPSdepOn','HOPSrole','col8', 'col9'])
        combinedData = pd.concat([hopsedData, skippedData], axis=0)
        combinedData['sort'] = combinedData['UUID'].str.replace(currentSkippedCode + '-','')
        combinedData = combinedData.sort_values(by='sort')
        combinedData = combinedData.dropna(subset=['UUID'])
        combinedData = combinedData.drop('sort', axis=1)
        # combinedData.reset_index(drop=True)
        combinedData = combinedData[['hopsID', 'HOPSforme', 'UUID', 'HOPS_UPOS', 'col4','col5','HOPSdepOn','HOPSrole','col8', 'col9']]
        combinedData.to_csv(HOPSedFiles[h].replace('.csv','mod.csv'), sep="\t", encoding = 'UTF8', index=False)
        print(f'Reintegrated :: {currentSkippedCode}-{HOPSedFiles[h][-7:-4]}', flush=True  )

def tidyHOPS(HOPSfolder):
  '''function to get HOPS raw output, remove blank columns, add col names, export '''
  source_folder = HOPSfolder
  source_files = set(glob.glob(source_folder +'/*.csv')) -set((glob.glob(source_folder +'/*commands*.csv'))) - set(glob.glob(dossier_temp +'/*raw*.csv'))
  HOPS_files = pd.DataFrame(source_files) # can restrict to specific model here
  HOPS_files = HOPS_files.sort_values(0)
  HOPS_files.reset_index(inplace=True, drop=True)
  HOPS_files.columns=['Path']
  stdColnames =  ['hopsID', 'HOPSforme', 'UUID', 'HOPS_UPOS', 'col4','col5','HOPSdepOn','HOPSrole','col8', 'col9']
  subsetCols = ['hopsID', 'HOPSforme', 'UUID', 'HOPS_UPOS', 'HOPSdepOn','HOPSrole']
  for i in range(len(HOPS_files)):
    rawFileNameIn = HOPS_files.iloc[i,0]  # get name of file
    currentTextCode = rawFileNameIn[-12:-8]  # get code
    currentModel = rawFileNameIn[-7:-4] ## get model
    HOPStidySaveName = rawFileNameIn ## set name of output file
    HOPS_sample = pd.read_csv(rawFileNameIn, sep="\t", nrows = 1, encoding="UTF8", header=None) # read in first row of data to check for headers
    if HOPS_sample.iloc[0,2] == "UUID": ## if colnames in file already
      HOPS_raw = pd.read_csv(rawFileNameIn, sep="\t",  skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False) # read in raw data without specifying headers and col names
    else:  ## if  colnames not in file and no headers present:
      HOPS_raw = pd.read_csv(rawFileNameIn, sep="\t", header = None, skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False, names = stdColnames) # read in raw data specifying that no headers present, listing colbames
    HOPS_tidy = HOPS_raw[subsetCols] # get subset of cols
    HOPS_tidy.to_csv(HOPStidySaveName, sep="\t", header=True, index=False)
    print(currentTextCode + " : " + HOPStidySaveName.replace(dossier_temp,'') + " exporté ", flush=True)

def moveTidyFiles(source_folder, dossier_temp):
  '''function to move tidy output to temp folder after zipping '''
  source_files = set(glob.glob(source_folder +'/*.csv')) -set((glob.glob(source_folder +'/*commands*.csv'))) - set(glob.glob(dossier_temp +'/*raw*.csv'))
  source_files = pd.DataFrame(source_files)
  source_files = source_files.sort_values(0)
  source_files.reset_index(inplace=True, drop=True)
  source_files.columns=['Path']
  for i in range(len(source_files)):
    shutil.move(source_files.iloc[i,0], source_files.iloc[i,0].replace(source_folder, dossier_temp))

def runHOPSpost(main_directory, HOPSfolder, dossier_temp):  
  archiveHOPSfiles(main_directory, HOPSfolder, 0) # 0 to use 'raw.zip' as ending
  skippedSentencesChecker(HOPSfolder)
  tidyHOPS(HOPSfolder)
  archiveHOPSfiles(main_directory, HOPSfolder, 1) # 1 to use 'raw.zip' as ending
  moveTidyFiles(HOPSfolder, dossier_temp)

# 1 line run section
runHOPSpost(main_directory, HOPSfolder, dossier_temp)

```

#26 : TT post
```{python}

def tidy_TTtext(file_name):
  '''to use function, put all 402 and binder texts in same folder. FUnction will bind and output a 403 file '''
  inputFileName = file_name
  binderFileName = file_name.replace('402.txt','400binder.csv')
  outputFileName = file_name.replace('402.txt','403.csv')
  
  inputData = pd.read_csv(file_name, encoding = "UTF8", delimiter = '\t', quotechar = '\"', skip_blank_lines=False, na_filter=False, header=None, names = ['POSttAF','LemmeTTaf']) ## af07,13 needs head false arg
  
  TTbindings = pd.read_csv(binderFileName, encoding = "UTF8", delimiter = '\t', quotechar = '\"', skip_blank_lines=False, na_filter=False, header=None, names = ['FormeTTaf','TokenID']) ## af07,13 needs head  
  
  all_TT = pd.concat([TTbindings,inputData], axis=1)
  all_TT = all_TT[['TokenID', 'FormeTTaf','POSttAF','LemmeTTaf']]
  all_TT['LemmeTTaf'] = ["_" if all_TT.iloc[i,3] == "<unknown>" else all_TT.iloc[i,3] for i in range(len(all_TT)) ]

  all_TT.to_csv(outputFileName, sep="\t", encoding="UTF-8", index=False, header=True)

def runTTpost(dossier_tt):
  '''combine TT output with binder, export to csv then zip all outputs'''  
  ## EN identify files created by TreeTagger placed in the TT folder
  ## FRidentifier les fichiers créés ajoutés au dossier TT
  theseFiles = pd.DataFrame(glob.glob(dossier_tt +'/*402.txt'))
  theseFiles = theseFiles.sort_values(0)
  theseFiles.reset_index(inplace=True, drop=True)
  theseFiles.columns=['Path']

  ## run this bit
  for f in range(len(theseFiles)):
    tidy_TTtext(theseFiles.iloc[f,0])
  
  # zip tt output
  with ZipFile((main_directory + "403.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(dossier_tt+'400/').rglob("*403.csv"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(dossier_tt)))

## 1 line : 
runTTpost(dossier_tt)

```

#27 : DeucPost
```{python}

def correctDeucNames(deucParams, dossier_Deuc):
  '''function to rename Deucalion output files, replacing default 300-pie with code for model used '''
  for j in range(len(deucParams)):
    deucfiles = pd.DataFrame(glob.glob(dossier_Deuc + deucParams.iloc[j,0]  + '/*300-pie*'))
    for i in range(len(deucfiles)):
      os.rename(deucfiles.iloc[i,0], deucfiles.iloc[i,0].replace('300-pie',deucParams.iloc[j,1]).replace(deucParams.iloc[j,0]+'/','') )

def ArchiveDeucOutputs(deucParams, main_directory):
  '''Function to create zips of 310, 320 and 330 files '''
  for i in range(len(deucParams)):
    with ZipFile((main_directory + deucParams.iloc[i,1] + ".zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
      for file_path in pathlib.Path(dossier_Deuc).rglob("*" + deucParams.iloc[i,1] + ".csv"):
        archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(dossier_Deuc)))

def DeucHousekeeping(dossier_Deuc):
  '''Delete 300.csv files from FR,FRO and FREEM folders then  move all output files to dossier_Deuc from subfolders'''
  # delete 300 files from FR and FREEM, move from FRO to 300 folder
  DeleteThese = pd.DataFrame(glob.glob(dossier_Deuc +'*/*300.csv')) 
  for f in range(len(DeleteThese)):
    os.remove(DeleteThese.iloc[f,0])

DeucBinder(dossier_Deuc)
def DeucBinder(dossier_Deuc):
  DeucOutputs = pd.DataFrame(glob.glob(dossier_Deuc + '/*0.csv'))
  DeucOutputs = DeucOutputs.sort_values(0)
  DeucOutputs.reset_index(inplace=True, drop=True)

  for f in range(len(DeucOutputs)):
    deucFile = DeucOutputs.iloc[f,0]
    currentFile = deucFile[-12:-4]
    currentModel = deucFile[-7:-4]
    currentModelFolder = deucFile.replace(dossier_Deuc,'').replace(deucFile[-13:],'')
    binderFilename = deucFile.replace(currentModel,'300').replace('.csv','-binder.csv')
    outputName = deucFile.replace(currentModel, str(int(currentModel)+1)).replace( 
      currentModelFolder + '/'  ,'') 
    
    binderData = pd.read_csv(binderFilename, sep="\t", header=None, names = ['Forme','UUID'])
    deucData = pd.read_csv(deucFile, sep="\t")
    print (currentFile + " :: LenDeucData == " + str(len(deucData)) +  "LenBindData == " +  str(len(binderData)), flush=True)
    if deucData.iloc[(len(deucData)-1),0] is np.nan:
      deucData = deucData.drop((len(deucData)-1))
      print (currentFile + " :: LenDeucData == LenBinder" + str(len(deucData)) +  " == " +  str(len(binderData)), flush=True)
    
      deucAligned = pd.concat([binderData, deucData], axis=1)  
      deucAligned = deucAligned[['Forme', 'lemma', 'POS', 'UUID']]
      deucAligned.to_csv(outputName, index=False, header=True, encoding ='UTF8')
    else:
      print('Error : try using quotechar="\'", doublequote=True')


def DeucPostHousekeeping(deucParamsmain_directory, dossier_Deuc):
  for i in range(len(deucParams)):
    with ZipFile((main_directory +  deucParams.iloc[i,1].replace('0','1asdfd')+ ".zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
           for file_path in pathlib.Path(dossier_Deuc).rglob("*" + deucParams.iloc[i,1].replace('0','1') + ".csv"):
               archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(dossier_Deuc)))
    shutil.rmtree(dossier_Deuc + deucParams.iloc[i,0])

def runDeucPostProcessing(dossier_Deuc, main_directory):
  deucParams = pd.DataFrame([{'model':"fro",'code':"310"},{'model':"freem",'code':"320"},{'model':"fr",'code':"330"}])
  correctDeucNames(deucParams, dossier_Deuc)
  ArchiveDeucOutputs(deucParams, main_directory)
  DeucHousekeeping(dossier_Deuc)
  DeucBinder(dossier_Deuc)
  DeucPostHousekeeping(deucParams, main_directory, dossier_Deuc)

## 1 line
runDeucPostProcessing(dossier_Deuc, main_directory)

```

#28. UD Post - API
```{python}
# moved from UDprocess
dossier_UD = '/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/'
# SaveFile = '/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/531/AF01-531.json'
theseFiles = pd.DataFrame(glob.glob(dossier_UD + '/*/*.json'))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)

for i in range(len(theseFiles)):
    SaveFile = theseFiles.iloc[1,0]
    print(i, flush=True)
    processJSON(SaveFile, dossier_UD)

### funciton to process JSON output from API : dl with conll in name but is json and needs to be parsed, then tidied.   
def processJSON(SaveFile, dossier_UD):
  myInput = open(SaveFile)
  result = json.load(myInput)['result']
  myInput.close()
  inputFolder = SaveFile[-18:-14]
  outputFolder = '/' + str(int(SaveFile[-8:-5])+10)
  if os.path.exists(dossier_UD+outputFolder[1:] + '/')==False:
      os.makedirs(dossier_UD+outputFolder[1:] + '/')
  
with open(dossier_UD + "temp.csv", 'w') as fileA:
  cleanedResult = re.sub('\n# .+\n','\n',result)
  fileA.writelines(cleanedResult)
myLines =  open(dossier_UD + "temp.csv").readlines()[4:-1]
  with open(dossier_UD + "temp.csv", 'w') as fileA:
    fileA.writelines(myLines)

  table = pd.read_csv(dossier_UD + "temp.csv", header=None, sep="\t") # CA16 et 18 might need addition of param: quotechar="\'"
  table.columns = ['UDid', 'UDforme','supp','UDpos', 'UDXPOS','UD_Feats','UD_dep','UD_role','supp','supp']
  table = table.drop("supp", axis=1)
  table = table.dropna(subset=['UD_role'])
  table.to_csv(SaveFile.replace('json','csv').replace(inputFolder, outputFolder).replace(str(int(SaveFile[-8:-5])),str(int(SaveFile[-8:-5])+10)), sep="\t", index=False, encoding="UTF8")
  
  
# bindingFile = '/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/201/MF14-201.txt'
theseFiles = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/54*' + "/*.csv"))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)
# UD_file = '/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/541/MF14-541.csv'
for f in range(58, len(theseFiles)):
  UD_file = theseFiles.iloc[f,0]
  fileCode = UD_file[-12:-4]
  bindingFile =  UD_file.replace('/541','/201').replace('/542','/201').replace('/543','/201').replace('/544','/201').replace('/545','/201').replace('/546','/201').replace('541','201').replace('542','201').replace('543','201').replace('544','201').replace('545','201').replace('546','201').replace('csv','txt')
  
  ## get ud data, remove . as only content on line from retokenisation, reset ind.
  # completed = pd.DataFrame()
  ud_data = pd.read_csv(UD_file, sep="\t")
  if UD_file[-7:-4] in ['542','543','544','545','546']:
    ud_data = ud_data.loc[ud_data['UDforme'] !="."]
    ud_data = ud_data.reset_index(drop=True)
    ud_data['delValue'] = [(1 if ('-' in ud_data.iloc[h-1,0] ==True) else 0) for h in range(len(ud_data))]
    ud_data['delValue'] = [1 if '-' in ud_data.iloc[i-2,0] else ud_data.iloc[i,7] for i in range(len(ud_data))]
    ud_data['UDforme'] = [ud_data.iloc[i-1,1] if '-' in ud_data.iloc[i-1,0] else ud_data.iloc[i,1] for i in range(len(ud_data))]
    ud_data = ud_data.loc[ud_data['delValue'] != 1]
    ud_data['UDid'] = ud_data['UDid'].str.replace("-\d+",'')
    ud_data = ud_data.reset_index(drop=True)
  ## get HOPS data used as input, remove blank and punct, reset
  bindingData = pd.read_csv(bindingFile, sep="\t", header=None)
  bindingData = bindingData.dropna()
  bindingData = bindingData.reset_index(drop=True)
  bindingData.columns = ["ss_intID","Forme","UUID"]
  bindingData = bindingData.loc[bindingData['Forme'] !="."]
  bindingData = bindingData.loc[bindingData['Forme'] !=","]
  bindingData = bindingData.loc[bindingData['Forme'] !="!"]
  bindingData = bindingData.loc[bindingData['Forme'] !="?"]
  bindingData = bindingData.loc[bindingData['Forme'] !="-"]
  bindingData = bindingData.loc[bindingData['Forme'] !=":"]
  bindingData = bindingData.loc[bindingData['Forme'] !=";"]
  bindingData = bindingData.loc[bindingData['Forme'] !="("]
  bindingData = bindingData.loc[bindingData['Forme'] !=")"]
  bindingData = bindingData.loc[bindingData['Forme'] !="\""]
  bindingData = bindingData.loc[bindingData['Forme'] !="\'"]
  bindingData = bindingData.loc[bindingData['Forme'] !="’"]
  bindingData = bindingData.loc[bindingData['Forme'] !="“"]
  bindingData = bindingData.loc[bindingData['Forme'] !="’"]
  bindingData = bindingData.loc[bindingData['Forme'] !="”"]
  bindingData = bindingData.loc[bindingData['Forme'] !="»"]
  bindingData = bindingData.loc[bindingData['Forme'] !="«"]
  bindingData =bindingData.reset_index(drop=True)
  
  results = pd.DataFrame([ { 'fileCode':fileCode,'binderL':len(bindingData),'DataLen':len(ud_data),'delta': len(bindingData) - len(ud_data)} ] )
  allResults = pd.concat([allResults,results], axis=0)
  
  combined = pd.concat([bindingData, ud_data], axis=1)
#   
#   combined.to_csv(UD_file.replace('541.','551.'), sep="\t", index=False)
# 
#   allResults =  pd.DataFrame()
# # combined.columns

combined['ident'] = pd.DataFrame([lev(str(combined.iloc[l,1]), str(combined.iloc[l,4])) for l in range(len(combined))])
  myValue = 10
  SuspectErrLocations= combined['ident'].rolling(3, center=True).sum().loc[combined['ident'].rolling(3, center=True).sum() > int(myValue)].index
  # for s in range(len(SuspectErrLocations)):
    s=0
    View((combined[(SuspectErrLocations[s])-20:(SuspectErrLocations[s])+20]))
from Levenshtein import distance as lev


###### drafting
loc[bindingData['Forme'] != "zzkz"]
bindingData = bindingData[[ 'Forme', 'UUID', 'ss_intID', 'ssid']]

len(ud_data) - len(bindingData) 
ud_data['sentCalc'] = 0 ; currentSentenceValue = 0
for i in range(len(ud_data)):
  if ud_data.iloc[i,0] < ud_data.iloc[i-1,0]:
    currentSentenceValue = currentSentenceValue+1
    ud_data.iloc[i,7] = currentSentenceValue
  else:
    ud_data.iloc[i,7] = currentSentenceValue
    
    i=19
View(ud_data)  
for s in range(100, 2377):
  ud_sentence = ud_data.loc[ud_data['sentCalc'] == s]
  bindingSent = bindingData.loc[bindingData['ssid'] == str(s)]
  
  # len(ud_sentence) == len(bindingSent)
  boundSentence = pd.concat([ud_sentence.reset_index(drop=True), bindingSent.reset_index(drop=True)], axis=1)
  completed = pd.concat([completed, boundSentence], axis=0)
  View(completed)
  
```

#28. UD Post - URL
```{python}
def ConvertUDfromConllu(dossier_UD):
  ''' function to take conllu, remove added lines, convert to csv '''
  UDoutputfiles = glob.glob(dossier_UD + '/*/*.conllu')
  for i in UDoutputfiles:
    UDoutputfile = i
    UDtidyfilename = UDoutputfile.replace('conllu','csv')
    rawConnl = open(UDoutputfile, "rt", encoding="UTF8")
    intConll = rawConnl.read()
    cleanedConnl = re.sub('# .+\n','',intConll)
    rawConnl.close()
    
    fin = open(UDtidyfilename, "wt", encoding="UTF8")
    fin.write(cleanedConnl)
    fin.close()
    print("Processed " + UDoutputfile)
  
def makeUDoutputfolders(dossier_UD):
  for m in range(501, 507):
    foldername = dossier_UD + "/" + str(m + 10)+  "/"
    if os.path.exists(foldername)==False:
      os.makedirs(foldername)
  print('Folders made')
    

  makeUDoutputfolders(dossier_UD) # make folders for output

def ProcessUDcsv(dossier_UD, currentFile):
  ''' function to take tidy UDpipe data, combine with binder, remove and organise columns, export as '''
  ModelCode = currentFile[-7:-4]
  BinderName = currentFile.replace(ModelCode, '500-binder').replace('/500-binder','')
  TextCode = currentFile[-12:-8]
  SaveName = currentFile.replace(ModelCode, str(int(ModelCode) + 10))
  UDColNamesIn = ['UDid','udFORME','udLEMME','udUPOStag','udXPOStag','udFEATS', 'udHEAD','udDEPREL','udDEPS','udMISC']
  
  if TextCode[:2] in ['CA','CM']:
    UDbinderCols = ['udFORMEcheck','UUID']
  else:
    UDbinderCols = ['UUID', 'udFORMEcheck']
  
  # get binder  
  ud_binder = pd.read_csv(BinderName, sep="\t",  skip_blank_lines=False, header=None)
  ud_binder.columns = UDbinderCols

  try:
      UD_data = pd.read_csv(currentFile, sep="\t", header=None, names = UDColNamesIn, skip_blank_lines=False)
  except ValueError:
      try:
          UD_data = pd.read_csv(currentFile, sep="\t", header=None, names = UDColNamesIn, skip_blank_lines=False, quotechar='"', doublequote=False) # 64947
      except ValueError:
          try:
            UD_data = pd.read_csv(currentFile, sep="\t", header=None, names = UDColNamesIn, skip_blank_lines=False, quotechar="'", doublequote=False)# 64945
          except ValueError:
              try:
                UD_data = pd.read_csv(currentFile, sep="\t", header=None, names = UDColNamesIn, skip_blank_lines=False, quotechar='\'', doublequote=True)# 
              except ValueError:
                  try:
                    UD_data = pd.read_csv(currentFile, sep="\t", header=None, names = UDColNamesIn, skip_blank_lines=False, quotechar='"', doublequote=True)#error
                  except ValueError:
                    print("Input problem with UDpred file : tried with single + double apost as quotechar , doublequote as T and F")
  
  # ud_binder['UUID'] =  [str(TextCode) + "-" + str(i).zfill(5)  if ud_binder.iloc[i,0] != "zzkz" else "" for i in range(len(ud_binder)) ]
  if len(ud_binder) ==  len(UD_data):
    print(TextCode, ModelCode, len(ud_binder),' == ', len(UD_data), flush=True)
    ud_aligned = pd.concat([UD_data, ud_binder], axis=1)
    # ud_aligned = pd.merge(ud_preds, ud_binder, how="left", on="UUID")
    ud_aligned = ud_aligned[['UDid', 'udFORME', 'udLEMME', 'udUPOStag', 'udHEAD', 'udDEPREL',  'UUID']]
    ## now aligned UDoutput with binder
    ud_aligned.to_csv(SaveName, index=False, header=True, sep="\t")
  else:
    print(TextCode, ModelCode, len(ud_binder),' != ', len(UD_data), flush=True)



def UDalignedArchiver(UD_modelData, dossier_UD, main_directory):
  for m in range(len(UD_modelData)):
    with ZipFile((main_directory + str(int(UD_modelData.iloc[m,0]) + 10) + '.zip'), "w", ZIP_DEFLATED, compresslevel=9) as archive:
      for file_path in pathlib.Path(dossier_UD).rglob("*" + str(int(UD_modelData.iloc[m,0]) + 10) + ".csv"):
               archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(dossier_UD)))


def UDpostHousekeeping(dossier_UD):
  to_delete = list(glob.glob(dossier_UD + "/*501.csv")) + list(glob.glob(dossier_UD + "/*502.csv")) + list(glob.glob(dossier_UD + "/*503.csv")) +list(glob.glob(dossier_UD + "/*504.csv")) +list(glob.glob(dossier_UD + "/*505.csv")) + list(glob.glob(dossier_UD + "/*506.csv")) +list(glob.glob(dossier_UD + "/*500.csv"))
  for i in to_delete:
      os.remove(i)


UDtidyfiles = set(glob.glob(dossier_UD + '/*/*.csv')) - set(glob.glob(dossier_UD + '/*/*500.csv') )
UDtidyfiles = pd.DataFrame(UDtidyfiles).sort_values(0)
UDtidyfiles = UDtidyfiles.reset_index(drop=True)
for i in range(len(UDtidyfiles)):
  currentFile = UDtidyfiles.iloc[i,0]
  ProcessUDcsv(dossier_UD, currentFile)
  print(str(i)+ " of " + str(len(UDtidyfiles)))

def runUDpostprocessors(dossier_UD, main_directory):
  UD_modelData  = pd.DataFrame([{'Code':'501', 'LangState':'OF','Treebank':'SRCMF'}, {'Code':'502', 'LangState':'MF','Treebank':'GSD'},{'Code':'503', 'LangState':'MF','Treebank':'ParisStories'}, {'Code':'504', 'LangState':'MF','Treebank':'Partut'},{'Code':'505', 'LangState':'MF','Treebank':'Rhapsodie'}, {'Code':'506', 'LangState':'MF','Treebank':'Sequoia'}])
  ConvertUDfromConllu(dossier_UD)
  # ProcessUDcsv(dossier_UD)
  UDalignedArchiver(UD_modelData, dossier_UD, main_directory)
  UDpostHousekeeping(dossier_UD)

# single line
runUDpostprocessors(dossier_UD, main_directory)

```

#29 Stanza Post
```{python}

# get list of files from OF and FMod models
def StanzaPostprocess(dossier_Stanza):
  ''' function to get 610, 620 files, remove and reorder columns'''
  OF_columnsOut = ['StanzaID','StanzaForme','StanzaUPOS_OF','TokenID','StanzaDepOn_OF', 'StanzaRel_OF']
  FMod_columnsOut = ['StanzaID', 'StanzaForme', 'StanzaUPOS_FMod', 'TokenID', 'StanzaDepOn_FMod', 'StanzaRel_FMod']
  StanzaFiles = list(glob.glob( dossier_Stanza +'/*/*620.csv')) + list(glob.glob( dossier_Stanza +'/*/*610.csv'))
  for i in StanzaFiles:
    inputFile = i
    outputFileName = inputFile.replace(inputFile[-7:-4], str(int(inputFile[-7:-4]) +1))
    
    if "610.csv" in inputFile:
      ColumnNamesOut = OF_columnsOut
    else:
      ColumnNamesOut = FMod_columnsOut
  
    inputData = pd.read_csv(inputFile, sep="\t",  skip_blank_lines=False)
    outputData = inputData[ColumnNamesOut]
    outputData.to_csv(outputFileName, header=True, sep="\t", index=False, encoding='UTF8')
  return StanzaFiles

## zip StanzaPreparedFiles
def zipStanzaPostProcessed(main_directory):
  ''' function to zip files prepared for Stanza'''
  with ZipFile((main_directory + "621.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(main_directory + 'PourStanza/').rglob("*621*"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(main_directory + 'PourStanza/')))
  with ZipFile((main_directory + "611.zip"), "w", ZIP_DEFLATED, compresslevel=9) as archive:
       for file_path in pathlib.Path(main_directory + 'PourStanza/').rglob("*611*"):
           archive.write(file_path, arcname=file_path.relative_to(pathlib.Path(main_directory + 'PourStanza/')))

def StanzaPostHousekeeping(StanzaFiles):
  ''' function to delete 610 and 620 files'''
  for i in StanzaFiles:
    os.remove(i)

dossier_Stanza = '/Users/username/PhraseoRoChe/Pipeline/refresh/PourStanza/'
def runStanzaPostProcesses(dossier_Stanza, main_directory):
  ''' function to run Stanza post-processing functions'''
  StanzaFiles = StanzaPostprocess(dossier_Stanza)
  zipStanzaPostProcessed(main_directory)
  StanzaPostHousekeeping(StanzaFiles)

#1 line:
runStanzaPostProcesses(dossier_Stanza, main_directory)

```






#100 : Making output files::  TO DO  ::


# 103. process for HOPS + LGERM only::  TO DO  ::

```{python}
# function 3000
def bindHOPStoLGERM(LGERMfile):
  '''bind HOPS data to LGERM data with standard Colnames, export to file with 'bound' in name, return bound as df '''
  HOPSfile = LGERMfile.replace('103.csv','203.txt')
  LGERMdata = pd.read_csv(LGERMfile, sep="\t", skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  HOPSdata = pd.read_csv(HOPSfile, sep="\t", skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  bound = pd.merge(LGERMdata, HOPSdata, how="right", on="UUID")
  bound_data = bound[[ 'hopsID', 'ssid',  'UUID', 'Forme',  'etiq', 'HOPS_UPOS', 'lemme',   'HOPSdepOn', 'HOPSrole', 'Pp']]
  bound_data.to_csv(LGERMfile.replace('-103.csv','bound.csv'), sep="\t", index=False)
  return bound_data


# function 3001
def calculateEOSvalues(bound_data):
  '''Calculate where sentences end and where to move punctuation to last col, previous row for Lexicoscope; return 2 sets of values '''
  # define what is punct
  PONtypes = {"PUNCT"}
  # create list to store rows to drop
  theseRows = [ ]
  EOS_types = {".", ";" ,":", "!", "?" , "»", "\"", "\'"}
  ## LCs to get EOS values
  EOS_complex = np.array([i+1 for i in range(len(bound_data)-1) if ((bound_data.iloc[i,3] in EOS_types) & (bound_data.iloc[i+1,3] == "»"))])
  # df of values where token is in EOS but i+1 not in EOS_complex; these are EOS_simple. Name column
  
  # EOS_complexAdd = np.array([i+1 for i in range(len(bound_data)-1) if ((bound_data.iloc[i,3] in EOS_types) & (bound_data.iloc[i+1,3] == ","))])
  # 
  # EOS_complex = np.append(EOS_complex, EOS_complexAdd)
  
  EOS_simple = pd.DataFrame([[i ] for i in range(len(bound_data)) if ((bound_data.iloc[i,3] in EOS_types) & ((i +1) not in EOS_complex))])
  EOS_simple.columns = ['value']
  # create set of values in EOS simple, and set of values in EOS_simple_exclude, and subtract EOS_simple_exclude_set from EOS_simple_set
  EOS_simple_set = set(EOS_simple['value'].to_list()  )
  ## convert EOS_complex to set, then get union of simple and complex sets
  EOS_complex_set = set(EOS_complex)
  ## now have lists of values which need to be moved up 1 and to col 11, and up 1 & 2 at same time
  return EOS_simple_set, EOS_complex_set

# function 3002
def processEOSpunct(EOS_simple_set, EOS_complex_set, bound_data):  
  '''Move EOS punctuation to final word of sentence,final column '''
  # add new col on end to store punct
  bound_data['spillover'] = ""
  # move simple EOS punct up and across
  for i in EOS_simple_set:
    bound_data.iloc[i-1,10] = str( bound_data.iloc[i,3] )
  # move complex EOS punct up2 and across and 1 + across
  for i in EOS_complex_set:
    bound_data.iloc[i-2,10] = str( bound_data.iloc[i-1,3]) + str(bound_data.iloc[i,3])
  return bound_data
  
# funciton 3003:
def RemapTidyCols(bound_data):
  '''Remap XPOS to UPOS tagsm remap ROLES, tidy lemmas, add 1 to col posScore IF HOPS and LGERM agree on POS, convert columns with numbers to int64, set column order'''
  POSupos_RemapDict = {"ADJqua":"ADJ", "ADJind":"ADJ", "ADJpos":"ADJ", "PRE":"ADP", "PRE.DETdef":"ADP", "ADVgen":"ADV", "ADVneg":"ADV", "PROadv":"ADV", "ADVint":"ADV", "ADVsub":"ADV", "VERcjg":"AUX", "VERcjg":"VERB", "VERinf":"VERB", "VERppe":"VERB", "CONcoo":"CCONJ", "DETpos":"DET", "DETdef":"DET", "DETind":"DET", "DETndf":"DET", "DETdem":"DET", "DETrel":"DET", "DETint":"DET", "INJ":"INTJ", "NOMcom":"NOUN", "DETcar":"DET", "ADJcar":"ADJ", "PROcar":"PRO", "PROcar":"PRON", "PROdem":"PRON", "PROrel":"PRON", "PROper":"PRON", "PROimp":"PRON", "PROind":"PRON", "PROpos":"PRON", "PROint":"PRON", "PROper.PROper":"PRON", "ADVneg.PROper":"PRON", "PROord":"PRON", "NOMpro":"PROPN", "PONfbl":"PUNCT", "PONfrt":"PUNCT", "PONpga":"PUNCT", "PONpdr":"PUNCT", "CONsub":"SCONJ", "VERppa":"VERB"}
  REL_RemapDict = {"acl:relcl":"acl","aux:caus":"aux","aux:tense":"aux","auxs":"aux","expl:comp":"expl","expl:subj":"expl","flat:name":"flat","nsubj:pass":"nsubj","obl:agent":"obl","obl:arg":"obl","obl:mod":"obl", "aux:pass":"aux", "cc:nc":"cc","obl:advmod":"obl"}
  
  bound_data['LGERM_upos'] = bound_data['etiq'].map(POSupos_RemapDict).fillna(bound_data['etiq'])
  bound_data['HOPSrole'] = bound_data['HOPSrole'].map(REL_RemapDict).fillna(bound_data['HOPSrole'])
  bound_data['lemmeTidy'] = bound_data['lemme'].str.replace('[0-9]','')
    
  bound_data['posScore'] = pd.DataFrame([ 1 if bound_data.iloc[a,5] == bound_data.iloc[a,11] else 0 for a in range(len(bound_data))])

  theseColumns = ['hopsID', 'ssid', 'HOPSdepOn']
  for c in range(len(theseColumns)):
    thisColumn = theseColumns[c]
    bound_data[thisColumn] = bound_data[thisColumn].apply(pd.to_numeric, errors = 'coerce')
    bound_data[thisColumn] = bound_data[thisColumn].fillna(0).astype("int64")

  bound_data['Pp'] = bound_data['Pp'].replace('.0','')

  processed_data = bound_data[['hopsID',  'Forme', 'lemmeTidy', 'HOPS_UPOS','HOPSdepOn', 'HOPSrole','posScore', 'spillover',  'ssid', 'UUID', 'Pp']]
  processed_data['blank'] = "_"       
  return processed_data

temp_processed_data = processed_data =
# function 3004
def buildConllForLexScope(processed_data, TextNamesCodesAF):
  '''Puts data into strings in new cols in df, then calculates necessary strings for page, sentence, changes, etc. Returns df with all required data in final col'''
  ## write output body : put all into string in one cell : 10 cols to be exported
  processed_data['output'] = pd.DataFrame([(str(processed_data.iloc[i,0]) + "\t" + str(processed_data.iloc[i,1]) + "\t" + str(processed_data.iloc[i,2]) + "\t" + str(processed_data.iloc[i,3]) + "\t" + str(processed_data.iloc[i,11]) + "\t" + str(processed_data.iloc[i,11]) + "\t" + str(processed_data.iloc[i,4])+ "\t" + str(processed_data.iloc[i,5]) + "\t" + str(processed_data.iloc[i,6]) + "\t" + str(processed_data.iloc[i,11]) + "\t" + str(processed_data.iloc[i,7]) ) if processed_data.iloc[i,0] > 0 else "" for i in range(len(processed_data)) ])  

  ## drop EOS lines with punct, as this has been moved to previous line
  deleteme = pd.DataFrame([i-1 for i in range(1, len(processed_data)) if processed_data.iloc[i,0] ==0 ])
  deleteme2 = deleteme +1
  processed_data = processed_data.drop( pd.Series(deleteme[0]))
  processed_data = processed_data.drop( pd.Series(deleteme2[0]))
  processed_data = processed_data.reset_index(drop=True)

  ## drop NA rows for hopsID
  processed_data = processed_data.dropna(subset=["UUID"])
  processed_data = processed_data.reset_index(drop=True)
# processed_data.to_csv("/Users/username/Desktop/test_step3.csv", sep="\t", index=False)
## deactivated as over zealous for AF13…
#{
## drop rows with PUNCT 2times at end of sentence : find, drop, reset
  deleteme3 = pd.DataFrame( [ i+1 for i in range(len(processed_data)-1) if ( (processed_data.iloc[i+1,3] == "PUNCT") &  ((processed_data.iloc[i,7] == ".»")|(processed_data.iloc[i,7] == "!»")|(processed_data.iloc[i,7] == "?»")|(processed_data.iloc[i,7] == "»,"))) ])
  # 
  if (deleteme3.shape != (0,0)):
    processed_data = processed_data.drop(pd.Series(deleteme3[0]))
    processed_data = processed_data.reset_index(drop=True)
  #}

  # write dec for start of sentences, with getting sent numbers from column8
  textCode = processed_data.iloc[0,9][0:4]
  textTitle = TextNamesCodesAF.loc[TextNamesCodesAF['Code'] ==textCode].iloc[0,1]
  processed_data['sentDec'] = pd.DataFrame([( "</s><s id=\"s" + str(processed_data.iloc[i,8]) + "\" newdoc_id=\"" + textTitle + "\" " + "sent_id=\"" + str(processed_data.iloc[i,8]) + "\">" ) if processed_data.iloc[i-1,8] > processed_data.iloc[i-2,8] else "" for i in range(1, len(processed_data)) ])

  ## write dec for start of page  
  processed_data['pDec'] = pd.DataFrame([( "<pb id=\"" + str(processed_data.iloc[i,10]).replace('.0','') + "\"/>" ) if (processed_data.iloc[i,10]) != processed_data.iloc[i-1,10] else "" for i in range(1, len(processed_data)) ])
  # # writeDec for last row:
  # processed_data.iloc[len(processed_data)-1, 14] = "</p>"
  # processed_data.iloc[len(processed_data)-1, 13] = "</s>"
  ## write dec for first page, sent
  processed_data.iloc[0,14] =  "<pb id=\"" + str(processed_data.iloc[0,10]).replace('.0','') + "\"/>" ## page dec
  processed_data.iloc[0,13] =  "<s id=\"s" + str(processed_data.iloc[0,8]).replace('.0','') + "\" newdoc_id=\"" + textTitle + "\" " + "sent_id=\"" + str(processed_data.iloc[0,8]) + "\">" ## sent dec
  
  ## paste together the created declarations avoiding NAs::
  # create new col and use this to paste together created elements : 4 LCs to keep comprehensible. 
  ## If page and Sent declarations - cols 14 and 13 - are blank, final declaration is outputbudy = col 12
  processed_data['allOut'] = ""
  processed_data['allOut'] = ( [ processed_data.iloc[i, 12]  if ((processed_data.iloc[i,14] == "") & (processed_data.iloc[i,13] == "")) else processed_data.iloc[i, 15] for i in range(len(processed_data))])
  
  ## if page declaration is blank and sent declaratin is present, final declaration is sentDec + outputbody
  processed_data['allOut'] = ( [ str(processed_data.iloc[i, 13]) + str(processed_data.iloc[i, 12])  if ((processed_data.iloc[i,14] == "") & (processed_data.iloc[i,13] != "")) else processed_data.iloc[i, 15] for i in range(len(processed_data))])
  
  ## if pqge dec is present and sent dec is blank, final declaration is pageDec + outputbody
  processed_data['allOut'] = ( [ str(processed_data.iloc[i, 14]) + str(processed_data.iloc[i, 12])  if ((processed_data.iloc[i,14] != "") & (processed_data.iloc[i,13] == "")) else processed_data.iloc[i, 15] for i in range(len(processed_data))])
  
  ## if page dec is present and sentDec is present, then final output is page dec + sentDec + outputBody
  processed_data['allOut'] = ( [ str(processed_data.iloc[i, 14]) + str(processed_data.iloc[i, 13]) + str(processed_data.iloc[i, 12]) if ((processed_data.iloc[i,14] != "") & (processed_data.iloc[i,13] != "")) else processed_data.iloc[i, 15] for i in range(len(processed_data))])
  
    # writeDec for last row:
    
  processed_data.iloc[len(processed_data)-1, 15]  =  processed_data.iloc[len(processed_data)-1, 15].replace('nannan','') + "</s></p></text></doc></corpus></body>\n</TEI>"

  finaldata = processed_data
  return finaldata


# function 3005
def writetoConll(finaldata, LGERMfile):
  '''Writes required data to conll'''
  Message1 = "Success !"
  Message2 = "Beurk, error !"
  targetFile = LGERMfile.replace('.csv','test.conllu')
  # get XMLheader from file
  # headerFile = LGERMfile.replace('temp','headers').replace('bound.csv','Header.xml')
  # fhin = open(headerFile, 'rt', encoding="UTF8")
  # headerData = fhin.read()
  # fhin.close()
  ## create output file
  fin = open(targetFile, "wt", encoding = 'UTF8')
  # write XMLhead
  # fin.write(headerData)
  # write bits left out of initial draft of xmlhead
  fin.write('<body>\n<corpus>\n<doc>\n<text>\n')
  # iterate over body
  for i in range(len(finaldata)):
    fin.write(str(finaldata.iloc[i, 15] + "\n"))
  # write closing tags for XML
  # fin.write('</s></p></text></doc></corpus></body>\n</TEI>')
  # close file connection
  fin.close()
  try:
    print(Message1)
  except ValueError:
    print(Message2)

############################################################

f_body = open("/Users/username/PhraseoRoChe/Pipeline/AFtextv0/103/AF13-103B.conllu", "wt", encoding="UTF8")
  for i in range(len(finaldata)):
    f_body.write(str(finaldata.iloc[i, 15] + "\n"))
f_body.close()



########################################################################
########################################################################
## run section
# TextNamesCodesAF = pd.read_csv("/Users/username/Desktop/AFtextNames.csv", sep="\t")
TextNamesCodesAF = pd.DataFrame([{'Code':"AF01",'Titre':"ArtusDeBretagne"}, {'Code':"AF03",'Titre':"LancelotT2"}, {'Code':"AF04",'Titre':"MerlinProse"}, {'Code':"AF05",'Titre':"MerlinProse"}, {'Code':"AF06",'Titre':"MerlinProse"}, {'Code':"AF07",'Titre':"PremiersFaitsRoiArtur"}, {'Code':"AF08",'Titre':"TristanMénardT1"}, {'Code':"AF11",'Titre':"TristanCurtisT3"}, {'Code':"AF12",'Titre':"QuêteStGraal"}, {'Code':"AF13",'Titre':"MortArtu"}, {'Code':"AF21",'Titre':"PercevalProse"}, {'Code':"AF22",'Titre':"Perlesvaus"}])

# HOPS_model_data = pd.read_csv("/Users/username/Desktop/HOPS_models/HOPS_models.csv", sep=";")
dossier_temp = '/Users/username//PhraseoRoChe/Pipeline/AFtextv0/temp/'
LGERM_files = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
LGERM_files = LGERM_files.sort_values(0)
LGERM_files.reset_index(inplace=True, drop=True)
LGERM_files.columns=['Path']

## first loop binds HOPS to LGERM. Then can use special routines below (s2000, s2001) to combine parts of AF03, AF04, Af05 which are successive parts of hte same text. 
# second loop takes in bound files, and outputs conll for lexicoscope.

for i in range(len(LGERM_files)):
  LGERMfile = '/Users/username/PhraseoRoChe/Pipeline/AFtextv0/AF/103/AF13-103.csv'
  LGERMfile = LGERM_files.iloc[i,0]
  bound_data = bindHOPStoLGERM(LGERMfile) # function 3000 # 64535


bound_files = pd.DataFrame(glob.glob(dossier_temp +'/*bound.csv'))
bound_files = bound_files.sort_values(0)
bound_files.reset_index(inplace=True, drop=True)
bound_files.columns=['Path']


for i in range( len(bound_files)):
  bound_data = pd.read_csv(bound_files.iloc[i,0], sep="\t", skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False)
  EOS_simple_set, EOS_complex_set = calculateEOSvalues(bound_data) # function 3001
  bound_data = processEOSpunct(EOS_simple_set, EOS_complex_set, bound_data) # funciton 3002
  processed_data =RemapTidyCols(bound_data) # funciton 3003
  finaldata = buildConllForLexScope(processed_data, TextNamesCodesAF) # funciton 3004: 
  writetoConll(finaldata, LGERMfile)
  writetoConll(finaldata, bound_files.iloc[i,0])# funciton 3005
####
finaldata.to_csv(LGERMfile.replace('103.csv','final.csv'), sep="\t", index=False)
## still have cases to remove where POS = punct and is last in sent and len(i,11) == 2 pieces of PUNCT
## still have cases with sent it = float not int

```










::  TO DO  ::
## Create Extension corpus 
Summary :
2001 : prepare from CONLLU
2002 : extract correct tags
2003 : prepare for Taggiing
2004 : Do tagging
2005 : post-processing and scoring




```{text}
These sections take CONLLU input from BFM and SRCMF where some/all annotations have been verified, and uses them to create a GroundTruth corpus. 

Section 2001 uses functions 901-906 to convert the CONNLU files to dataframes
Function 901 reads and parses the conllu data, and outputs a df
Function 902 takes this df if it has blank lines, and numbers sentences and tokens
Function 903 takes the df from 901 if it has no blank lines, and numbers sentences and tokens
Function 904 finds the length of all sentences, and removes any with a length over the prescribed limit
Function 905 takes this dataframe and uses the fileame to lookup the XMLstring declared in the accompanying Noise.csv file, and removes this value from the XMLtokenIDs, and converts these to UUIDs. Further data tiding also done here.
Function 906 adds a line/row 'spacer' at the end of each sentence in the df.
Function 907 is called within Function 906. 907 checks that TokenIDs within each sentence start at 1 and are sequential, and corrects them in case of error.
The RUN section calls these functions, passing the appropriate variables loaded previous to the RUN section, and passes variables from function to function. A CSV is saved as output with all data required for the processors.

```



#2001 Prepare from CONLLU::  TO DO  ::
```{python}
##### definitions
# function 0901
def convertConllDF(inputFile):
  """Read Connl, convert to dataframe, expand and rename columns, remove extraneous data"""
  rawConnl = open(inputFile, "rt", encoding="UTF8")
  intConll = rawConnl.read()
  rawConnl.close()
  sentences = conllu.parse_token_and_metadata(intConll, fields=['id', 'form', 'lemma', 'upos', 'xpos', 'feats', 'head','deprel','deps','misc'])
  connlAsDF = (pd.DataFrame(sentences))
  DictCols = ['misc', 'feats']
  for colname in DictCols:
    extended_df = connlAsDF[colname].apply(pd.Series, dtype="object")
    connlAsDF = connlAsDF.join(extended_df)
    connlAsDF = connlAsDF.drop(colname, axis=1)

  # get required cols 
  connlAsDF = connlAsDF[['id', 'form', 'lemma', 'upos', 'xpos', 'head', 'deprel', 'deps','XmlId']]
  connlAsDF['C_Feats'] = "_"
  connlAsDF.columns = ['C_id','C_FORME','C_LEMME','C_UPOStag','C_XPOStag','C_Feats', 'C_HEAD','C_DEPREL','UUID','C_DEPS']
  return connlAsDF

# function 0902
def sentTokIdGen_wBL(connlAsDF):
  """Generate IDs for tokens and sentences: version for texts with  blank lines between sentences OR where tokens within sentences are not numbered correctly"""
  # create numpy array on values with LC testing if current token is in EOS_types and following token is »; these are EOS_complex, 
  EOS_types = {".", ";" ,":", "!", "?" , "»", "\"", "\'"}
  EOS_complex = np.array([i+1 for i in range(len(connlAsDF)-1) if ((connlAsDF.iloc[i,1] in EOS_types) & (connlAsDF.iloc[i+1,1] in EOS_types))])
  # df of values where token is in EOS but i+1 not in EOS_complex; these are EOS_simple. Name column
  EOS_simple = pd.DataFrame([[i ] for i in range(len(connlAsDF)) if ((connlAsDF.iloc[i,1] in EOS_types) & ((i +1) not in EOS_complex))])
  EOS_simple.columns = ['value']
  # create set of values in EOS simple, and set of values in EOS_simple_exclude, and subtract EOS_simple_exclude_set from EOS_simple_set
  EOS_simple_set = set(EOS_simple['value'].to_list()  )
  ## convert EOS_complex to set, then get union of simple and complex sets
  EOS_complex_set = set(EOS_complex)
  allSentEnds = EOS_complex_set.union(EOS_simple_set)
  # TODO: write code...
  connlAsDF['ss_intID'] =0 
  connlAsDF['ssid'] =1
  SentValueCurrent =1
  TokenValueCurrent = 0

  for k in range(len(connlAsDF)):
    if k in allSentEnds:
      connlAsDF.iloc[k,10] = TokenValueCurrent +1   ### add1 to tokenID
      connlAsDF.iloc[k,11] = SentValueCurrent      ##### set sentID to ucrrentvalue
      SentValueCurrent = SentValueCurrent +1          ### add1 to sentID
      TokenValueCurrent = 0                         #### set toeknID to 0
    else:
      connlAsDF.iloc[k,10] = TokenValueCurrent +1   ### add1 to tokenID
      connlAsDF.iloc[k,11] = SentValueCurrent       ##### keep sentID same
      TokenValueCurrent = TokenValueCurrent +1

  connlAsDF_id = connlAsDF
  return connlAsDF_id, allSentEnds

# function 0903
def sentTokIdGen_noBL(connlAsDF):
  """Generate IDs for tokens and sentences: version for texts with no blank lines between sentences and correctly numbered tokens within sentences"""
  connlAsDF['ss_intID'] =  connlAsDF['C_id']
  # create numpy array on values with LC testing if current token is in EOS_types and following token is »; these are EOS_complex, 
  # df of values where token is in EOS but i+1 not in EOS_complex; these are EOS_simple. Name column
  EOS_simple = pd.DataFrame([[i ] for i in range( len(connlAsDF)-1) if ((connlAsDF.iloc[i+1,10]< connlAsDF.iloc[i,10]))])
  EOS_simple.columns = ['value']
  # create set of values in EOS simple, and set of values in EOS_simple_exclude, and subtract EOS_simple_exclude_set from EOS_simple_set
  allSentEnds = set(EOS_simple['value'].to_list()  )
  connlAsDF['ssid'] =1
  SentValueCurrent =1

  for k in range(len(connlAsDF)):
    if k in allSentEnds:
      connlAsDF.iloc[k,11] = SentValueCurrent      ##### set sentID to ucrrentvalue
      SentValueCurrent = SentValueCurrent +1          ### add1 to sentID
    else:
      connlAsDF.iloc[k,11] = SentValueCurrent       ##### keep sentID same

  connlAsDF_id = connlAsDF
  return connlAsDF_id, allSentEnds

  # function 0904
def removeLongSent(connlAsDF_id, allSentEnds):
  """Find sentences over a specific length and remove from dataframe"""
## find overly long sentences overly long sentences
  testResults = pd.DataFrame([[v,connlAsDF_id.loc[connlAsDF_id['ssid'] == v]['ss_intID'].max()]  for v in set(connlAsDF_id['ssid']) ])
  testResults.columns = ['ssid','len']
  testResults['len'] = testResults['len'].astype("int64") 
  dropSents = list(testResults.loc[testResults['len'] > dropLimit].ssid)
  ## exclude overly long sentences
  c_data = connlAsDF_id.drop(([i  for i in range(len(connlAsDF_id)) if connlAsDF_id.iloc[i,11] in dropSents]))
  return c_data

# function 0905
def tidyData(c_data, inputFile):
  ## remove noise from data cols, add codetext to UUID
  CodeText = inputFile.replace(correctDataFolder,'').replace('.conllu','').replace('-301','').replace('-201','').replace('-200','').replace('-100','')
  noiseValueName1 = customStrings.loc[customStrings['code'] == CodeText].iloc[0,1]
  c_data['UUID'] = c_data['UUID'].str.replace(noiseValueName1,CodeText+"_")
  c_data['C_LEMME'] = c_data['C_LEMME'].str.replace('\t','').replace(' ','')
  c_data['C_FORME'] = c_data['C_FORME'].str.replace(' ', "").replace('\t', "")
  c_data['C_FORME'] = c_data['C_FORME'].str.replace('\"\"\"','\"\"').replace('\"\"\"','\"\"').replace('\"\"\"','\"\"').replace('\"\"\"','\"\"').replace('\"\"\"','\"\"').replace('\"\"\"','\"\"').replace('\"\"\"','\"\"')
  return(c_data)  

# function 0906, contains ## function 0907
def spaceAdder(c_data):
## funciton to add
  c_data_copy = c_data
  sentValues = set(c_data['ssid'].values)
  tidyFrame = pd.DataFrame()
  spacer = pd.DataFrame([{'C_id':"zzkz",'C_FORME':"zzkz",'C_LEMME':"zzkz",'C_UPOStag':"zzkz",'C_XPOStag':"zzkz",'C_Feats':"zzkz",'C_HEAD':"zzkz",'C_DEPREL':"zzkz",'UUID':"zzkz",'C_DEPS':"zzkz",'ss_intID':  "zzkz",'ssid':"zzkz",'C_idOrig':"zzkz"}])
## > changes here
  for s in sentValues:
      currentSentenceraw = c_data.loc[c_data['ssid']== s]
      currentSentence = manageCIDs(currentSentenceraw) 
      currentOut = pd.concat([currentSentence, spacer], axis=0)
      tidyFrame = pd.concat([tidyFrame, currentOut], axis=0)
      c_data = c_data.drop(currentSentence.index, axis=0)
  aligned_id_concat_df= tidyFrame
  return aligned_id_concat_df
  
# function 0907  
def manageCIDs(currentSentence):
  """Takes sentence as df, adds c_idOrig col, and renumbers tokens from 1 if not already so numbered"""
  corrValues = set(list(range(1, len(currentSentence)+1))) # get set of what should be correct values
  Actual_values = set(currentSentence['C_id'].values) # get set of actual values
  currentSentence['C_idOrig'] = currentSentence.C_id
  if Actual_values != corrValues:
    currentSentence['C_id'] = range(1, len(currentSentence)+1)
    
  return currentSentence

##### declarations

inputFile = "/Users/username/PhraseoRoChe/scores/conllu/CA07-200.conllu"
dropLimit = 400
customStrings = pd.read_csv('/Volumes/AlphaThree/Binding/fromScores/customstrings.csv',   sep="\t")
correctDataFolder = "/Users/username/PhraseoRoChe/pipeline/refresh/scores/conllu/"


### run 

conlls100 = pd.DataFrame(glob.glob( correctDataFolder + ('*00.conllu')))
conlls201 = pd.DataFrame(glob.glob( correctDataFolder + ('/*-201.conllu')))
conlls301 = pd.DataFrame(glob.glob( correctDataFolder + ('/*-301.conllu')))
inputConlls = pd.concat([conlls301, conlls201, conlls100], axis=0)
inputConlls = inputConlls.sort_values(0)
inputConlls.reset_index(inplace=True, drop=True)
inputConlls.columns=['Path']
# i=4
for i in range(len(inputConlls)):
  inputFile = inputConlls.iloc[i,0]
  connlAsDF = convertConllDF(inputFile) # function 0901
  
  nan_len = len(connlAsDF.loc[connlAsDF['C_id'].isnull()])
  if nan_len == 0:
    connlAsDF_id, allSentEnds = sentTokIdGen_noBL(connlAsDF) # function 0903
  else:  
    connlAsDF_id, allSentEnds = sentTokIdGen_wBL(connlAsDF) # function 0902
  
  c_data = removeLongSent(connlAsDF_id, allSentEnds) # function 0904
  c_data=  tidyData(c_data, inputFile) # function 0905
  c_fileTidy = inputFile.replace('/conllu', '/correct').replace('conllu', 'csv')
  
  aligned_id_concat_df = spaceAdder(c_data) # function 0906,7
  c_fileTidy = inputFile.replace('/conllu', '/correct').replace('.conllu', 'a.csv')
  aligned_id_concat_df.to_csv(c_fileTidy, sep="\t", index=False, header=True)
  print("Completed " + str(i + 1) + " : " + str(c_fileTidy))



```


#2002 function to extract, export correct tags as csv::  TO DO  ::
```{python}
# function 1000
def newCroiseur(file_name, correctDataFolder):
  """Take a filename as input, and get the 2 source files containing correct data: POSLEM from 201, DEP from 200, and combine to 1 df with all correct data, export df"""
  CodeText = file_name.replace(correctDataFolder,'').replace('-201.csv','')
  
  DEP_filename = file_name
  DEP_data = pd.read_csv(DEP_filename, sep="\t")
  DEP_data.columns = ['dep_C_id', 'dep_C_FORME', 'dep_C_LEMME', 'dep_C_UPOStag', 'dep_C_XPOStag', 'dep_C_HEAD', 'dep_C_DEPREL','dep_C_Feats',  'UUID', 'dep_C_DEPS', 'dep_ss_intID', 'dep_ssid', 'dep_C_idOrig']
  
  DEP_data=DEP_data[['dep_C_id', 'dep_C_FORME', 'dep_C_HEAD', 'dep_C_DEPREL',  'UUID', 'dep_ss_intID', 'dep_ssid', 'dep_C_idOrig','dep_C_Feats','dep_C_DEPS']]
  
  currentCode = CodeText
  ## tidy UUIDs col 8 = sentID, col5 = UUID
  DEP_data['UUID'] = DEP_data['UUID'].str.replace(currentCode + "_",'')
  DEP_data['UUID'] = [currentCode + "EOS" + str(DEP_data.iloc[i-1,6]).zfill(4) if DEP_data.iloc[i,4] == "zzkz" else str(currentCode) + "-" + str(DEP_data.iloc[i,4]).zfill(6) for i in range(len(DEP_data)) ]
  
  POSLEMfile_name = file_name.replace('-201','-200')
  POSLEM_data= pd.read_csv(POSLEMfile_name, sep="\t")
  POSLEM_data.columns = ['pC_id', 'pC_FORME', 'pC_LEMME', 'pC_UPOStag', 'pC_XPOStag', 'pC_Feats', 'pC_HEAD', 'pC_DEPREL', 'UUID', 'pC_DEPS', 'pss_intID', 'pssid', 'pC_idOrig' ]
  
  POSLEM_data= POSLEM_data[['pC_id', 'pC_FORME', 'pC_LEMME', 'pC_UPOStag', 'pC_XPOStag',  'UUID',  'pss_intID', 'pssid', 'pC_idOrig' ]]
  
  # View(POSLEM_data)
  POSLEM_data['UUID'] = POSLEM_data['UUID'].str.replace(currentCode + "_",'')
  POSLEM_data['UUID'] = [currentCode + "EOS" + str(POSLEM_data.iloc[i-1,7]).zfill(4) if POSLEM_data.iloc[i,5] == "zzkz" else str(currentCode) + "-" + str(POSLEM_data.iloc[i,5]).zfill(6) for i in range(len(POSLEM_data)) ]
  
  bound = pd.merge(DEP_data, POSLEM_data, how = 'left', on="UUID")
  # View(bound)
  bound = bound[[  'dep_C_id', 'dep_C_FORME',  'pC_LEMME',  'pC_UPOStag', 'pC_XPOStag' ,'dep_C_HEAD', 'dep_C_DEPREL', 'dep_C_Feats', 'UUID' ,'dep_C_DEPS', 'dep_ss_intID', 'dep_ssid','dep_C_idOrig'  ]]
  bound.columns = [  'C_id', 'C_FORME',  'C_LEMME',  'C_UPOStag', 'C_XPOStag' ,'C_HEAD', 'C_DEPREL', 'C_Feats', 'UUID' ,'C_DEPS', 'ss_intID', 'ssid', 'C_idOrig'  ]
  
  # View(bound)
  bound['C_id'] = bound['C_id'].apply(pd.to_numeric, errors = 'coerce')
  bound['C_id'] = bound['C_id'].fillna(0).astype("int64")
  bound['C_HEAD'] = bound['C_HEAD'].apply(pd.to_numeric, errors = 'coerce')
  bound['C_HEAD'] = bound['C_HEAD'].fillna(0).astype("int64")
  
  bound.to_csv(correctDataFolder + currentCode + ".csv", sep="\t", index=False)
  # return bound, currentCode

## run section, only need to add 201 files as input, others don't need this cross-working

correctDataFolder = "/Users/username/PhraseoRoChe/Pipeline/refresh/scores/correct/"
files_croiser = pd.DataFrame(glob.glob( correctDataFolder +'/*201.csv'))
files_croiser = files_croiser.sort_values(0)
files_croiser.reset_index(inplace=True, drop=True)
files_croiser.columns=['Path']
for f in range(1, len(files_croiser)):
  file_name = files_croiser.iloc[f,0]
  newCroiseur(file_name, correctDataFolder) ## function 1000, gives no return object

## 200 and 201 files have been combined : keep these, delete suffixed versions

## new uuid making goes here:

inputFiles = pd.DataFrame(glob.glob(correctDataFolder.replace('correct','099') + "/*.csv"))
for f in range(len(inputFiles)):
  currentFile = pd.read_csv(inputFiles.iloc[f,0], sep="\t")
  currentFile.columns = ['C_id', 'C_FORME', 'C_LEMME', 'C_UPOStag', 'C_XPOStag', 'C_Feats',
       'C_HEAD', 'C_DEPREL', 'xmlID', 'C_DEPS', 'ss_intID', 'ssid', 'C_idOrig']

  currentFile['UUID'] = [str(inputFiles.iloc[f,0][-12:-8] + "-" + str(i).zfill(6)) for i in range(len(currentFile))]
  currentFile.to_csv(inputFiles.iloc[f,0], sep="\t", index=False, header=True)

```


PREP_GEN

# 2003 generate files for taggers  : functions using saved files ::  TO DO  ::
```{python}
# 1005 : Prepare for HOPS
def PrepareHOPS(file_name):
  '''function to take a set of sentences to process, an initial df for HOPS, a text code and folder to export data to ; no return object. Creates a csv file to be sent to HOPS'''
  ## LC to get df of all cases where the sentID of the current sentence is  fastgroup. [i,4] is Owner, so includes final punct in sents, to preserve blank lines for HOPS
  ## FR compréhension de liste pour obtenir toutes les phrases dont le sentID figure dans fastgroup.  [i,4] est la colonne Owner, pour comprendre la ponctuation de fin de phrase, pour avoir des lignes vides que nécessite HOPS en fin de phrase.
  forHOPS = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)
  forHOPS = forHOPS[['C_id', 'C_FORME','UUID']]

  HOPS_out= pd.DataFrame([(forHOPS.iloc[t,0],forHOPS.iloc[t,1],forHOPS.iloc[t,2]) if forHOPS.iloc[t,1] != "zzkz" else ["","",""] for t in range(len(forHOPS))])
  ## EN export file 
  ## FR exporter le fichier 
  HOPS_out.to_csv(file_name.replace('099','201') , sep="\t", index=False, header=False, encoding='UTF8') # output 





#f2003 : 
    #### TT input is same as DeucInput :)
  inputFile= inputFile.replace('-600','').replace('Stanza','conllu')

#2004:stanza
def prepareStanzaComparing(inputFile, correctDataFolder, StanzaFolder):
  aligned_df = pd.read_csv(inputFile, skip_blank_lines=False, sep="\t")
  # aligned_df = pd.read_csv(inputFile, skip_blank_lines=False, sep="\t", doublequote=True,quotechar='\'')
  aligned_df['ss_intID'] =0 
  aligned_df['ssid'] =1
  SentValueCurrent =1
  TokenValueCurrent = 0
  # aligned_df['C_id'] = aligned_df['C_id'].apply(pd.to_numeric, errors = 'coerce')
  # aligned_df['C_id'] = aligned_df['C_id'].fillna(0).astype("int64")

  ## calc token and sent IDs  
  for k in range(len(aligned_df)):
    if aligned_df.iloc[k,0] == 0:
      aligned_df.iloc[k,10] = TokenValueCurrent +1   ### add1 to tokenID
      aligned_df.iloc[k,11] = SentValueCurrent      ##### set sentID to ucrrentvalue
      SentValueCurrent = SentValueCurrent +1          ### add1 to sentID
      TokenValueCurrent = 0                         #### set toeknID to 0
    else:
      aligned_df.iloc[k,10] = TokenValueCurrent +1   ### add1 to tokenID
      aligned_df.iloc[k,11] = SentValueCurrent       ##### keep sentID same
      TokenValueCurrent = TokenValueCurrent +1
  
  ### exclude punctuation
  challenge_set = [i  for i in range(len(aligned_df)) if ((aligned_df.iloc[i,3] == "PUNCT")  )]
  restricted_df = aligned_df.drop(challenge_set)
  restricted_df = restricted_df.reset_index(drop=True)
  
  ## set EOS chars to blanks
  restricted_df['C_FORME'] = pd.DataFrame([""  if (restricted_df.iloc[i,1] in ["zzkz",".","!","?",":",";"]) else restricted_df.iloc[i,1] for i in range(len(restricted_df))])
  restricted_df = restricted_df.dropna()
  #### set dtypes
  restricted_df['ssid'] = restricted_df['ssid'].apply(pd.to_numeric, errors = 'coerce')
  restricted_df['ssid'] = restricted_df['ssid'].fillna(0).astype("int64")
  ## EN find number of sentences in text, create df to hold processed sentences
  ## FR trouver le nombre de phrases dans le texte, créer une dataframe pour stocker les phrases traitées
  restricted_df = restricted_df[['C_id', 'C_FORME', 'UUID', 'ss_intID', 'ssid']]
  restricted_df = restricted_df.reset_index(drop=True)
  
  ##### make df of sentences to pass to stanza
  corpus = restricted_df
  
  senSet = set(corpus['ssid'].to_list())
  senSet = senSet - {0}
  allOutput = pd.DataFrame()
  
  for s in senSet:
    sentence = corpus.loc[corpus['ssid']== s ]
    thisMin = (sentence.index).min()
    thisMax = (sentence.index).max()+1
    sentenceA = corpus[thisMin:thisMax]
    sentenceUUIDs = list((sentenceA['UUID']))
    uuid_len = len(sentenceUUIDs)
    sentenceUUIDsConverted = str(sentenceUUIDs)
    sentenceA = sentenceA[['C_FORME']]
    
    currentSentence = ""
    for w in range(len(sentenceA)):
      thisWord = str(sentenceA.iloc[w,0] )
      if thisWord == '\x85':
        thisWord = html.unescape('&hellip;')
      currentSentence = str(currentSentence) + " " + thisWord
    
    currentOutput = {'sentID':s, 'text':currentSentence, 'sMin':thisMin, 'sMax':thisMax,'s':sentenceUUIDsConverted}#, 'uuid_len':uuid_len, 'delta':thisMax-thisMin}
    output = pd.DataFrame(currentOutput, index = [s])
    allOutput = pd.concat([allOutput, output], axis=0)
  
  allStanzaOutputname = inputFile.replace(correctDataFolder, StanzaFolder).replace('.csv','-600b.csv')
  allOutput.to_csv(allStanzaOutputname, sep="\t", index=False)

  # allOutput['check'] = pd.DataFrame(['True' if 0 == (int(allOutput.iloc[i,6]) - int(allOutput.iloc[i,5])) else "false" for i in range(len(allOutput)) ])
# allOutput['check'].value_counts()


  # now have file of sid, sent, start, end

#f 2005 : deucFileMaking
#get input file, prepare for DEUC; same file, binder as TT will use for scoring
def prepareDeuc_comparing(file_name, correctDataFolder, DeucFolder):
  if file_name[-12:-10] in ['CA','CM']:
    usecols = [1,13]
  else:
    usecols = [1,9]
  DeucMaking = pd.read_csv(file_name, sep="\t", usecols =usecols, encoding = 'UTF8')
  DeucBinderName = file_name.replace('099.csv','300-binder.csv').replace(correctDataFolder, DeucFolder)
  DeucFileName = DeucBinderName.replace('-binder','')
  DeucMaking = DeucMaking.dropna()
  DeucMaking.to_csv(DeucBinderName, index=False, sep="\t", header=None)
  DeucMaking.drop(['UUID'], axis=1).to_csv(DeucFileName, index=False, sep="\t", header=None)

#f 2006 : UD
def prepare_UD_comparing(file_name, correctDataFolder, UDFolder):
  inputFile = pd.read_csv(file_name, sep="\t", encoding = 'UTF8', skip_blank_lines=False)
  inputFile = inputFile.reset_index(drop=True)

  outputFile = inputFile[['C_FORME','UUID']]
  UDBinderName = file_name.replace('.csv','-500-binder.csv').replace(correctDataFolder, UDFolder)
  outputFile.to_csv(UDBinderName, index=False, sep="\t", header=None)
  output_filename = file_name.replace('.csv','-500.txt').replace(correctDataFolder, UDFolder)
  outputFile.drop(['UUID'], axis=1).to_csv(output_filename, index=False, sep="\t", header=None)

  fin = open(output_filename, "rt", encoding = "UTF-8")
  read_data = fin.read().replace('\n""','\n') 
  fin.close()
  # export binder final version
  fin = open(output_filename, "w", encoding = "UTF-8")
  fin.write(read_data)
  fin.close()

####### end of function defs

```

# 2003b. generate files for taggers  : run ::  TO DO  ::
```{python}
# declarations 
/Users/username/PhraseoRoChe/scores/conllu
##folders, paths
DeucFolder = "/Users/username/PhraseoRoChe/Pipeline/refresh/PourDeucalion/"
UDFolder = "/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/"
correctDataFolder = "/Users/username/PhraseoRoChe/Pipeline/refresh/scores/correct/"
HOPSfolder = "/Users/username/PhraseoRoChe/Pipeline/refresh/newlyLGERMedTexts/HOPS"
StanzaFolder = "/Users/username/PhraseoRoChe/Pipeline/refresh/PourStanza/"
inputFiles = pd.DataFrame(glob.glob(correctDataFolder +'/C[AM][0-9][0-9]-099.csv'))
inputFiles = inputFiles.sort_values(0)
inputFiles.reset_index(inplace=True, drop=True)
inputFiles.columns=['Path']
sLengthLimit = 350
StanzaFiles = pd.DataFrame(glob.glob(correctDataFolder +'/C[AM][0-1][0-9]-099.csv'))
StanzaFiles = StanzaFiles.sort_values(0)
StanzaFiles.reset_index(inplace=True, drop=True)
StanzaFiles.columns=['Path']

## run from gen_files
for f in range( len(inputFiles)):
# for f in [2,3,4,5]:
  file_name = inputFiles.iloc[f,0] 
  # UUID_preparer(file_name)
  # prepareDeuc_comparing(file_name, correctDataFolder, DeucFolder)
  prepare_UD_comparing(file_name, correctDataFolder, UDFolder)
  # PrepareHOPS(file_name)
  # prepareHOPScomp2(file_name, sLengthLimit, correctDataFolder, HOPSfolder)

for v in range(len(StanzaFiles)):
  inputFile= StanzaFiles.iloc[v,0] 
  prepareStanzaComparing(inputFile, correctDataFolder, StanzaFolder)


file_name = '/Users/username/PhraseoRoChe/scores/conllu/CA16.csv'

```

PREP_specific

# 2003.5a. HOPS prep::  TO DO  ::
```{python}
## make commands for HOPS
def prepareHOPScommands(inputFile, HOPS_model_data):
  
  commands = pd.DataFrame([str("hopsparser parse " + HOPS_model_data.iloc[i,5] + "/ " + inputFile +  " "+ inputFile.replace('-201.csv','') + str(HOPS_model_data.iloc[i,5]).replace('/Volumes/AlphaThree/HOPS_models/','-') + '.csv') for i in range(0, len(HOPS_model_data))]) ## leave as 1 to omit oldhopsModel
  #os.system(commands.iloc[0,0])
  return commands

commands = prepareHOPScommands(thisTextName, HOPS_model_data)

allCommands.to_csv(HOPSfolder + "/more_commands.csv", index=False, header=False)
########
HOPS_model_data = pd.read_csv("/Users/username/Desktop/HOPS_models/HOPS_models.csv", sep=";")

# HOPSfolder = "/Users/username/PhraseoRoChe/pipeline/refresh/HOPS/"
HOPSFiles = pd.DataFrame(glob.glob(HOPSfolder +'/*201.csv'))
HOPSFiles = HOPSFiles.sort_values(0)
HOPSFiles.reset_index(inplace=True, drop=True)
HOPSFiles.columns=['Path']
allCommands = pd.DataFrame()
for h in range(len(HOPSFiles)):
  inputFile = HOPSFiles.iloc[h,0]
  commands = prepareHOPScommands(inputFile, HOPS_model_data)
  allCommands = pd.concat([allCommands, commands], axis=0)  

```

# 2003.7a Deuc prepare::  TO DO  ::
```{python}
def do_deucPrep(dossier_temp, deucParams):
  ##EN  make subfolders 
  ## FR créer les sous-dossiers de stockage
  for m in range(len(deucParams)):
    currentPath = dossier_temp + 'deuc' + deucParams.iloc[m,0] + "/"
    if os.path.exists(currentPath)==False:
      os.makedirs(currentPath)
    if os.path.exists(dossier_temp + '/deucdone/')==False:
      os.makedirs(dossier_temp + '/deucdone/')
```

RUN

# 2004.7a Deuc run::  TO DO  ::
```{python}
def do_deucProcessing(inputFile, deucParams, pieHeader):
  for m in range(0, len(deucParams)):
    path_terminalFile = inputFile.replace(" ",'\ ')
    thisModel = deucParams.iloc[m,0]
    thisCommand = f'{pieHeader} {thisModel} {path_terminalFile}'
    os.system(thisCommand)
    shutil.move(inputFile.replace('-300.csv','-300-pie.csv'), inputFile.replace(dossier_temp,dossier_temp + f'/deuc{thisModel}/').replace('300',deucParams.iloc[m,1]))
    # time.time() -starttime 
    # madefile = inputFile.replace('300.csv','300-pie.csv')
  # shutil.move(inputFile, inputFile.replace(dossier_temp,dossier_temp + f'/deucdone/'))

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
                  ## note : need to copy 300 files to subfolders, then launch all 3                      models simultaneously in 3 diff shells    <<<<<<<<<<<<<
|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#### end definitions

import shutil
deucParams = pd.DataFrame([{'model':"fro",'code':"310"},{'model':"freem",'code':"320"},{'model':"fr",'code':"330"}])
pieHeader = f'pie-extended tag --no-tokenizer '
dossier_temp = "/Users/username/Downloads/srcmf-ud-master/corpus/Deuc"
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*300.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']

## EN set variables for files, models, paths…
## FR déclarer les variables pour construire les URL

do_deucPrep(dossier_temp, deucParams)
# import time
for f in range(len(theseFiles)):
  # starttime = time.time()
  inputFile = theseFiles.iloc[f,0]
  do_deucProcessing(inputFile, deucParams, pieHeader)


```

# 2004.6a TT run ::  TO DO  ::
```{python}
dossier_temp = '/Users/username//PhraseoRoChe/scores/Deuc/'

def process_TT(file_name, tt_AbsPath):
  file_name_forTerminal = file_name.replace(' ','\ ')
  tt_header = 'cat '
  tt_chunk = f' | {tt_AbsPath}/tree-tagger-old-french > '  
  file_name_out = file_name_forTerminal.replace('300','402')
  command = f'{tt_header}{file_name_forTerminal}{tt_chunk}{file_name_out}'
  os.system(command)

tt_AbsPath = "/Users/username/Dropbox/models/tree-tagger-MacOSX-Intel-3.2.3/cmd" # declare absolute path to folder containing tree-tagger-old-french; don't added final slash
## EN loop to process each file individually
## boucle pour traiter chaque fichier  individuellement
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*300.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']

for f in range(len(theseFiles)):
  # startTime = time.time()
  file_name = theseFiles.iloc[f,0] 
  process_TT(file_name, tt_AbsPath)



```

# 2004.9a stanza run::  TO DO  ::
```{python}
## functions to take allOutput files and process
def Stanza_tag_FM(file_name):
  allOutput = pd.read_csv(file_name, sep="\t")
  allOutput['s'] = allOutput['s'].apply(lambda x: ast.literal_eval(x))
  processed_FM = pd.DataFrame()
  for s in range(len(allOutput)):
    currentSentence = allOutput.iloc[s,1]
    currentDict = allOutput.iloc[s,4]
    doc_FMod = nlp_FMod(currentSentence)
    dict_FMod = doc_FMod.to_dict()
    conll_FMod = CoNLL.convert_dict(dict_FMod)
    FMod_out  = pd.DataFrame(conll_FMod[0]) 
    FMod_out.columns = StanzaColNamesFMod
    FMod_out = FMod_out.drop(['blank1', 'blank3', 'start_end_char'], axis=1)
    FMod_out = FMod_out.astype(myDtypesFMod)
    FMod_out['TokenID'] = currentDict
    
    ##EN add the processed sentence to the dataframe of processed sentences and save to disk with correct name + path. Loop continues with next sentence then next text.
    ##FR ajouter la phrase traitée en bas de la dataframe contenant les phrases traitées puis exporter ce document vers csv, avec le nom + chemin crées plus haut. La boucle se poursuivra avec les phrases suivantes, puis les textes suivants.
    processed_FM = pd.concat([processed_FM, FMod_out])

  StanzaOutputNameFMod = file_name.replace('600','620')
  processed_FM.to_csv(StanzaOutputNameFMod, sep="\t", index=False, header=True)
file_name = '/Users/username/PhraseoRoChe/scores/Stanza/CA16-600.csv'
def Stanza_tag_OF(file_name):
  allOutput = pd.read_csv(file_name, sep="\t")
  allOutput['s'] = allOutput['s'].apply(lambda x: ast.literal_eval(x))
  processed_OF = pd.DataFrame()
  for s in range( len(allOutput)):
    currentSentence = allOutput.iloc[s,1]
    currentDict = allOutput.iloc[s,4]
    doc_OF = nlp_OF(currentSentence)
    dicts_OF = doc_OF.to_dict() 
    conll_OF = CoNLL.convert_dict(dicts_OF)
    OF_out  = pd.DataFrame(conll_OF[0]) 
    # len(OF_out)
    # len(currentDict)
    OF_out.columns =StanzaColNamesOF
    OF_out = OF_out.drop(['blank1', 'blank3', 'start_end_char'], axis=1)
    OF_out = OF_out.astype(myDtypesOF)
    OF_out['TokenID'] = currentDict
    
    ##EN add the processed sentence to the dataframe of processed sentences and save to disk with correct name + path. Loop continues with next sentence then next text.
    ##FR ajouter la phrase traitée en bas de la dataframe contenant les phrases traitées puis exporter ce document vers csv, avec le nom + chemin crées plus haut. La boucle se poursuivra avec les phrases suivantes, puis les textes suivants.
    processed_OF = pd.concat([processed_OF, OF_out])

  StanzaOutputNameOF = file_name.replace('600','610')
  processed_OF.to_csv(StanzaOutputNameOF, sep="\t", index=False, header=True)


### filelists
stanzaFolder = "/Users/username/PhraseoRoChe/scores/Stanza/"
StanzaFiles = pd.DataFrame(glob.glob( stanzaFolder +'/*600.csv'))
StanzaFiles = StanzaFiles.sort_values(0)
StanzaFiles.reset_index(inplace=True, drop=True)
StanzaFiles.columns=['Path']

# dictionaries
myDtypesOF = {"StanzaID": int, "StanzaForme": object, "StanzaUPOS_OF": object, "StanzaXPOS_OF": object, "StanzaFeats_OF": object, "StanzaDepOn_OF": int, "StanzaRel_OF": object}
myDtypesFMod = {"StanzaID": int, "StanzaForme": object, "StanzaUPOS_FMod": object, "StanzaXPOS_FMod": object, "StanzaFeats_FMod": object, "StanzaDepOn_FMod": int, "StanzaRel_FMod": object}

## colnames
StanzaColNamesOF =['StanzaID', 'StanzaForme', 'blank1', 'StanzaUPOS_OF', 'StanzaXPOS_OF','StanzaFeats_OF', 'StanzaDepOn_OF','StanzaRel_OF','blank3','start_end_char']
StanzaColNamesFMod =['StanzaID', 'StanzaForme', 'blank1','StanzaUPOS_FMod','StanzaXPOS_FMod','StanzaFeats_FMod','StanzaDepOn_FMod','StanzaRel_FMod','blank3','start_end_char']

# pipelines
nlp_OF = stanza.Pipeline(lang='fro', processings='tokenize,pos,lemma,depparse', tokenize_pretokenized=True)
nlp_FMod = stanza.Pipeline(lang='fr', processings='tokenize,pos,lemma,depparse', tokenize_pretokenized=True, logging_level = "Info")

time.time()
for h in range( len(StanzaFiles)):
  file_name = StanzaFiles.iloc[h+1,0]
  Stanza_tag_OF(file_name)
time.time()
for g in range( len(StanzaFiles)):
  file_name = StanzaFiles.iloc[g,0]
  Stanza_tag_FM(file_name)
time.time()

file_name = "/Users/username/Downloads/srcmf-ud-master/corpus/stanza/CA08-600.csv"
Stanza_tag_FM(file_name)

```

POST
# 2005.5a HOPS-post1::  TO DO  ::
```{python}

def align_scorer(scoring):
  '''check alignment between correct data and data to check '''
  align_test = scoring[[ 'C_FORME',  'HOPSforme', 'UUID']]
  align_test['result'] = pd.DataFrame([1 if align_test.iloc[i,0] == align_test.iloc[i,1] else 0 for i in range(len(align_test))])
  n_items = align_test.shape[0]
  alignmentScore = round(align_test['result'].sum()/n_items,4)
  return  alignmentScore, n_items
  
def POS_scorer(scoring):
  '''calculate POS accuracy scores'''
  POS_data = scoring[['C_FORME',  'C_UPOStag',   'HOPS_UPOS', 'UUID']]
  punctToDrop = POS_data.loc[POS_data['C_UPOStag'] == "PUNCT"]
  POS_data = POS_data.drop(punctToDrop.index)
  POS_data = POS_data.reset_index(drop=True)
  POS_data['score'] = pd.DataFrame([1 if POS_data.iloc[i,1] == POS_data.iloc[i,2] else 0 for i in range(len(POS_data))])
  n_items = POS_data.shape[0]
  POS_score = round(POS_data['score'].sum()/n_items,4)
  return  POS_score

def score_attachments(scoring):
  '''calculate scores for simultaneous correct ID of head and relationship '''
  ATT_data = scoring[['C_FORME',  'C_HEAD', 'HOPSdepOn',  'C_DEPREL',   'HOPSrole', 'UUID']]
  ATT_data['scoreHead'] = pd.DataFrame([1 if ((ATT_data.iloc[i,1] == ATT_data.iloc[i,2])) else 0 for i in range(len(ATT_data))])
  ATT_data['scoreREL'] = pd.DataFrame([1 if ((ATT_data.iloc[i,3] == ATT_data.iloc[i,4]) ) else 0 for i in range(len(ATT_data))])
  ATT_data['scoreLAS'] = pd.DataFrame([1 if ((1 == ATT_data.iloc[i,6]) & (ATT_data.iloc[i,6] == ATT_data.iloc[i,7]) ) else 0 for i in range(len(ATT_data))])
  HEAD_score = round(ATT_data['scoreHead'].sum()/ATT_data.shape[0],4)
  REL_score = round(ATT_data['scoreREL'].sum()/ATT_data.shape[0],4)
  LAS_score = round(ATT_data['scoreLAS'].sum()/ATT_data.shape[0],4)
  # HEAD_score = ATT_data['scoreHead'].sum()
  # REL_score = ATT_data['scoreREL'].sum()
  # LAS_score = ATT_data['scoreLAS'].sum()
  return  HEAD_score, REL_score, LAS_score

def get_model_data(HOPS_model_data, currentParser):
  '''Use code number to get model data from df'''
  currentTreebank = HOPS_model_data[HOPS_model_data.iloc[:,2] == int(currentParser) ].iloc[0,1]
  currentBertLike = HOPS_model_data[HOPS_model_data.iloc[:,2] == int(currentParser) ].iloc[0,4]
  LangStateTool = HOPS_model_data[HOPS_model_data.iloc[:,2] == int(currentParser) ].iloc[0,3]
  return(currentTreebank, currentBertLike, LangStateTool)

def import_align_remap_data(currentFile, HOPS_columns, Compare_dataFile, currentTextCode):
  '''Read csv, name cols, remap with dicts, merge, save df'''
  inputData = pd.read_csv(currentFile, sep="\t", encoding="UTF8", names = HOPS_columns)
  inputData = inputData[['hopsID', 'HOPSforme', 'UUID', 'HOPS_UPOS', 'HOPSdepOn','HOPSrole'] ]
  ## original version saved df to same name as input file, this gives tidy df, but overwrites raw HOPS output ; future runs won't overwrite
  
  # remap UPOS, role
  POS_RemapDict = {"AUX" : 'VERB'}
  # REL_RemapDict = {"aux:caus":"aux","aux:tense":"aux","auxs":"aux","expl:comp":"expl","expl:subj":"expl","flat:name":"flat","nsubj:pass":"nsubj","obl:agent":"obl","obl:arg":"obl","obl:mod":"obl"}
  inputData['HOPS_UPOS'] = inputData['HOPS_UPOS'].map(POS_RemapDict).fillna(inputData['HOPS_UPOS'])
  # inputData['HOPSrole'] = inputData['HOPSrole'].map(REL_RemapDict).fillna(inputData['HOPSrole'])
  
  # Correct_data = pd.read_csv(Compare_dataFile, sep="\t", encoding="UTF8", quotechar="\'") # might need 
  Correct_data = pd.read_csv(Compare_dataFile, sep="\t", encoding="UTF8") # might need
  # Correct_data = Correct_data.drop(0)
  # conditon1 : concat if same len
  # Correct_data = Correct_data.reset_index(drop=True)
  holding = pd.merge(inputData, Correct_data, how='right', on="UUID")
  
  aligned = holding[['C_id', 'C_FORME', 'HOPSforme', 'C_LEMME', 'C_UPOStag', 'HOPS_UPOS','C_HEAD', 'HOPSdepOn',
  'C_DEPREL', 'HOPSrole', 'UUID','ss_intID', 'ssid', 'C_idOrig' ]]
  saveName = currentFile.replace('.csv','Aligned.csv')
  aligned.to_csv(saveName, index=False, header=True, sep="\t")

  if currentTextCode in ['CA02','CA05','CA06']:
    targetCol = 'C_DEPREL'
    targetValue = "punct"
  else:
    targetCol = 'C_UPOStag'
    targetValue = "PUNCT"
  
  punctToDrop = aligned.loc[aligned[targetCol] == targetValue]
  aligned = aligned.drop(punctToDrop.index)
  aligned = aligned.reset_index(drop=True)
  aligned = aligned.dropna(subset= ['HOPSforme'])
  scoring = aligned.reset_index(drop=True)
  return scoring, saveName

## get files output by hops
HOPS_folder = "/Users/username/PhraseoRoChe/scores/correct/301s/HOPS/"
HOPS_model_data = pd.read_csv("/Users/username/Desktop/HOPS_models/HOPS_models.csv", sep=";")

HOPS_Files = pd.DataFrame(glob.glob(HOPS_folder +'*.csv' ))
HOPS_Files = HOPS_Files.sort_values(0)
HOPS_Files.reset_index(inplace=True, drop=True)
HOPS_Files.columns=['Path']
# import, get col names, restrict
HOPS_columns =['hopsID', 'HOPSforme', 'UUID', 'HOPS_UPOS', 'col4','col5','HOPSdepOn','HOPSrole','col8', 'col9'] 
corrCols = ['C_UDid','C_FORME','C_LEMME','C_UPOStag','C_XPOStag','C_FEATS', 'C_HEAD','C_DEPREL','C_DEPS','UUID']
i=0
all_results = pd.DataFrame()
for i in range(34, 100): #120): # 34-100 need quote statement disables
  currentFile = HOPS_Files.iloc[i,0]
  # print(currentFile)
# for i in range(100, len(HOPS_Files)): #120): #
# currentFile = "/Users/username/PhraseoRoChe/scores/correct/301s/HOPS/CA02-222.csv"
  # currentParser = currentFile.replace("/Users/username/Downloads/srcmf-ud-master/corpus/graalForHOPS", '').replace(".txt","")
  currentParser = currentFile[-7:-4]
  Compare_dataFile =  currentFile.replace(currentParser,'').replace('-.csv','.csv').replace('301s/HOPS/','')
  if ("CA" in currentFile) ==True:
    LangStateText ="OF" 
  else:
    LangStateText ="FM" 
  LemmeScorePER = "XXX"
  currentTextCode = currentFile.replace(HOPS_folder,'').replace(currentParser,'').replace('-.csv','')
  
  currentTreebank, currentBertLike, LangStateTool= get_model_data(HOPS_model_data, currentParser)
  
  scoring,  saveName = import_align_remap_data(currentFile, HOPS_columns, Compare_dataFile, currentTextCode)
  # View(scoring)
  scoring['C_HEAD'] = scoring['C_HEAD'].apply(pd.to_numeric, errors = 'coerce')
  scoring['HOPSdepOn'] = scoring['HOPSdepOn'].apply(pd.to_numeric, errors = 'coerce')
  
  alignmentScorePER, n_items = align_scorer(scoring)
  POS_scorePER = POS_scorer(scoring)
  HEAD_score, REL_score, LAS_score = score_attachments(scoring)
  
  resultdf  = pd.DataFrame([{'tool':currentParser, 'Treebank':currentTreebank,'BERT_like':currentBertLike,'LangStateTool':LangStateTool,'LangStateText':LangStateText,'NameText':currentTextCode,'n':n_items,'alignmentscore_PER':alignmentScorePER,'POS_scorePER':POS_scorePER,'Lemme_scorePER':LemmeScorePER,'HEAD_scorePER': HEAD_score, 'REL_scorePER':REL_score, 'ATT_scorePER':LAS_score}])
  all_results = pd.concat([all_results, resultdf])

# View(resultdf)

all_results.to_csv(HOPS_folder+ "/AllResultsHOPS_AF.csv", index=False)
# all_results = pd.read_csv("/Users/username/Desktop/resultsouot.csv", sep=",")
View(all_results)

```

#2005.6a TT post::  TO DO  ::
```{python}
# function
def align_scoreTTfiles(tt_predFile, all_results):
  tt_preds = pd.read_csv(tt_predFile, sep="\t", header=None, names = TTpredColnames, quotechar="\'", doublequote=True)
  binderName = tt_predFile.replace('402','300-binder')
  tt_binder = pd.read_csv(binderName, sep="\t", header=None, names = TTbinderColnames)
  tt_aligned = pd.concat([tt_preds, tt_binder], axis=1)
  tt_aligned['P_pos_tx'] = tt_aligned['P_pos'].map(TTpos_RemapDict).fillna(tt_aligned['P_pos'])
    
  CorrectFilename = tt_predFile.replace(dossier_temp,CorrectFileDirectory).replace('-402','') 
  TextCode = tt_predFile.replace(dossier_temp,'').replace('/Deuc/deucdone/','').replace('-402.csv','')

  correct = pd.read_csv(CorrectFilename, encoding="UTF8",   sep="\t", usecols = [1,2,3,9], names = TTcorrColnames)
  correct = correct.drop([0])
  correct = correct.reset_index(drop=True)

  bound = pd.merge(correct, tt_aligned, how="left", on="UUID")
  TT_data = bound.dropna()
  TT_data = TT_data.reset_index(drop=True)
  TT_data['align'] = pd.DataFrame([1 if TT_data.iloc[i,0] == TT_data.iloc[i,6] else 0 for i in range(len(TT_data))]) ## if forme = forme 0 == 6
  TT_data['scorePOS'] = pd.DataFrame([1 if TT_data.iloc[i,2] == TT_data.iloc[i,7] else 0 for i in range(len(TT_data))]) ## if POS = POS == 2 = 4
  TT_data['scoreLemme'] = pd.DataFrame([1 if TT_data.iloc[i,1] == TT_data.iloc[i,5] else 0 for i in range(len(TT_data))])
  TT_dataSaveName = CorrectFilename.replace(".csv","TT_data.csv")
  TT_data.to_csv(TT_dataSaveName, index=False, header=True, sep="\t")
  alignmentScoreABS = TT_data['align'].sum()
  POS_scoreABS = TT_data['scorePOS'].sum()
  n_items = TT_data.shape[0]
  alignmentScorePER = round(alignmentScoreABS/n_items,4)
  POS_scorePER = round(POS_scoreABS/n_items,4)
  LemmeScorePER = round(TT_data['scoreLemme'].sum()/n_items,4)

  resultdf  = pd.DataFrame([{'tool':currentParser, 'Treebank':currentTreebank,'BERT_like':currentBertLike,'LangStateTool':LangStateTool,'LangStateText':LangStateText,'NameText':TextCode,'n':n_items,'alignmentscore_PER':alignmentScorePER,'POS_scorePER':POS_scorePER,'Lemme_scorePER':LemmeScorePER,'HEAD_scorePER': HEAD_scorePER, 'REL_scorePER':REL_scorePER, 'ATT_scorePER':ATT_scorePER}])

  all_results = pd.concat([all_results, resultdf], axis=0)
  return all_results
## declarations
dossier_temp = '/Users/username/Downloads/srcmf-ud-master/corpus/Deuc/'
CorrectFileDirectory = "/Users/username/Downloads/srcmf-ud-master/corpus/conluu/"
TTbinderColnames = ['P_forme','UUID']
TTpredColnames = ['P_pos', 'P_lemme']
TTcorrColnames = ['C_forme','C_lemme','C_pos','UUID']

TTpos_RemapDict = {"ADJqua":"ADJ", "ADJind":"ADJ", "ADJpos":"ADJ", "PRE":"ADP", "PRE.DETdef":"ADP", "ADVgen":"ADV", "ADVneg":"ADV", "PROadv":"ADV", "ADVint":"ADV", "ADVsub":"ADV", "VERcjg":"AUX", "VERcjg":"VERB", "VERinf":"VERB", "VERppe":"VERB", "CONcoo":"CCONJ", "DETpos":"DET", "DETdef":"DET", "DETind":"DET", "DETndf":"DET", "DETdem":"DET", "DETrel":"DET", "DETint":"DET", "INJ":"INTJ", "NOMcom":"NOUN", "DETcar":"DET", "ADJcar":"ADJ", "PROcar":"PRO", "PROcar":"PRON", "PROdem":"PRON", "PROrel":"PRON", "PROper":"PRON", "PROimp":"PRON", "PROind":"PRON", "PROpos":"PRON", "PROint":"PRON", "PROper.PROper":"PRON", "ADVneg.PROper":"PRON", "PROord":"PRON", "NOMpro":"PROPN", "PONfbl":"PUNCT", "PONfrt":"PUNCT", "PONpga":"PUNCT", "PONpdr":"PUNCT", "CONsub":"SCONJ", "VERppa":"VERB"}

HEAD_scorePER= "XXX"
REL_scorePER = "XXX"
ATT_scorePER = "XXX"
currentParser = "TT_OF"
currentTreebank = "XXX"
currentBertLike = "XXX"
LangStateTool = "OF"
LangStateText = "OF"

tt_Files = pd.DataFrame(glob.glob(dossier_temp +'*402.csv'))
tt_Files = tt_Files.sort_values(0)
tt_Files.reset_index(inplace=True, drop=True)
tt_Files.columns=['Path']
all_results = pd.DataFrame()

for i in range(len(tt_Files)):
  tt_predFile = tt_Files.iloc[i,0]
  all_results =  align_scoreTTfiles(tt_predFile, all_results)
  
all_results2.to_csv("/Users/username/Desktop/" + "allresults2.csv", sep="\t", header=True, index=False)
# all_results = pd.read_csv(CorrectFileDirectory + "allresults.csv", sep="\t" )

```

# 2005.7a deuc post::  TO DO  ::
```{python}
## functions
def getNamesForDeuc(DeucDossier, Deuc_models):
  deucfiles = pd.DataFrame()
  for f in range(len(Deuc_models)):
    path = DeucDossier + Deuc_models.iloc[f,2]
    deucfilesCurr = pd.DataFrame(glob.glob( path + '*.csv'))
    deucfilesCurr.columns=['Path']
    deucfilesCurr['subpath'] = Deuc_models.iloc[f,2]
    deucfilesCurr['model'] = Deuc_models.iloc[f,1]
    deucfilesCurr['code'] = Deuc_models.iloc[f,0]
    
    deucfiles = pd.concat([deucfiles, deucfilesCurr], axis=0 )
    deucfiles = deucfiles.sort_values('Path')
    deucfiles.reset_index(inplace=True, drop=True)
  return deucfiles

deucfiles = getNamesForDeuc(DeucDossier, Deuc_models)


deuc_predFile = "/Users/username/Downloads/srcmf-ud-master/corpus/Deuc/deucfreem/CA05-320.csv"

def align_scoreDeucfiles(deuc_predFile, deuc_subpath, deuc_model, deuc_code, all_results):
  Deuc_preds = pd.read_csv(deuc_predFile, sep="\t")
  Deuc_preds.columns = DeucPredsColnames
  binderName = deuc_predFile.replace(deuc_subpath,'').replace(deuc_code,'').replace('.csv','300-binder.csv') 
  
  Deuc_binder = pd.read_csv(binderName, sep="\t", header=None, names = DeucBinderColnames)
  
  Deuc_aligned = pd.concat([Deuc_preds, Deuc_binder], axis=1)
  Deuc_aligned = Deuc_aligned[['P.Forme', 'P.Lemma', 'P.POS', 'UUID']]
  Deuc_aligned =Deuc_aligned.dropna()
  Deuc_aligned['P_pos_tx'] = Deuc_aligned['P.POS'].map(Deucpos_RemapDict).fillna(Deuc_aligned['P.POS'])
  Deuc_aligned['P_lem_tx'] = Deuc_aligned['P.Lemma'].str.replace('[0-9]','')
  
  CorrectFilename = deuc_predFile.replace(deuc_subpath,'').replace('Deuc','conluu').replace(deuc_code, '').replace('-.csv','.csv')
  TextCode = CorrectFilename.replace(CorrectFileDirectory,'').replace('.csv','')

  correct = pd.read_csv(CorrectFilename, encoding="UTF8",   sep="\t", usecols = [1,2,3,9], names = DeuccorrColnames)
  correct = correct.drop([0])
  correct = correct.reset_index(drop=True)

  bound = pd.merge(correct, Deuc_aligned, how="left", on="UUID")
  Deuc_data = bound.dropna()
  Deuc_data = Deuc_data.reset_index(drop=True)

  Deuc_data['align'] = pd.DataFrame([1 if Deuc_data.iloc[i,0] == Deuc_data.iloc[i,4] else 0 for i in range(len(Deuc_data))]) ## if forme = forme 0 == 6
  Deuc_data['scorePOS'] = pd.DataFrame([1 if Deuc_data.iloc[i,2] == Deuc_data.iloc[i,7] else 0 for i in range(len(Deuc_data))]) ## if POS = POS == 2 = 4
  Deuc_data['scoreLemme'] = pd.DataFrame([1 if Deuc_data.iloc[i,1] == Deuc_data.iloc[i,8] else 0 for i in range(len(Deuc_data))])

  Deuc_dataSaveName = CorrectFilename.replace(".csv","Deuc_data-") + deuc_code + ".csv"
  Deuc_data.to_csv(Deuc_dataSaveName, index=False, header=True, sep="\t")
  n_items = Deuc_data.shape[0]
  alignmentScorePER = round(Deuc_data['align'].sum()/n_items,4)
  POS_scorePER = round(Deuc_data['scorePOS'].sum()/n_items,4)
  LemmeScorePER = round(Deuc_data['scoreLemme'].sum()/n_items,4)

  resultdf  = pd.DataFrame([{'tool':currentParser, 'Treebank':currentTreebank,'BERT_like':currentBertLike,'LangStateTool':deuc_model,'LangStateText':LangStateText,'NameText':TextCode,'n':n_items,'alignmentscore_PER':alignmentScorePER,'POS_scorePER':POS_scorePER,'Lemme_scorePER':LemmeScorePER,'HEAD_scorePER': HEAD_scorePER, 'REL_scorePER':REL_scorePER, 'ATT_scorePER':ATT_scorePER}])


  all_results = pd.concat([all_results, resultdf], axis=0)
  return all_results

### end of def

## dictionaries, dfs
Deucpos_RemapDict = {"ADJqua":"ADJ", "ADJind":"ADJ", "ADJpos":"ADJ", "PRE":"ADP", "PRE.DETdef":"ADP", "ADVgen":"ADV", "ADVneg":"ADV", "PROadv":"ADV", "ADVint":"ADV", "ADVsub":"ADV", "VERcjg":"AUX", "VERcjg":"VERB", "VERinf":"VERB", "VERppe":"VERB", "CONcoo":"CCONJ", "DETpos":"DET", "DETdef":"DET", "DETind":"DET", "DETndf":"DET", "DETdem":"DET", "DETrel":"DET", "DETint":"DET", "INJ":"INTJ", "NOMcom":"NOUN", "DETcar":"DET", "ADJcar":"ADJ", "PROcar":"PRO", "PROcar":"PRON", "PROdem":"PRON", "PROrel":"PRON", "PROper":"PRON", "PROimp":"PRON", "PROind":"PRON", "PROpos":"PRON", "PROint":"PRON", "PROper.PROper":"PRON", "ADVneg.PROper":"PRON", "PROord":"PRON", "NOMpro":"PROPN", "PONfbl":"PUNCT", "PONfrt":"PUNCT", "PONpga":"PUNCT", "PONpdr":"PUNCT", "CONsub":"SCONJ", "VERppa":"VERB"}

Deuc_models  = pd.DataFrame([{'Code':'310', 'LangState':'OF','Path':'deucfro/'}, {'Code':'320', 'LangState':'FPC','Path':'deucfreem/'},{'Code':'330', 'LangState':'FMod','Path':'deucfr/'}])


currentParser = "Deuc"
currentTreebank = "XXX"
currentBertLike = "XXX"
HEAD_scorePER= "XXX"
REL_scorePER= "XXX"
ATT_scorePER = "XXX"
DeucBinderColnames = ['P.Forme_binder','UUID']
DeucPredsColnames = ['P.Forme', 'P.Lemma', 'P.POS', 'morph', 'treated']
DeuccorrColnames = ['C_forme','C_lemme','C_pos','UUID']

DeucDossier = "/Users/username/Downloads/srcmf-ud-master/corpus/Deuc/"
CorrectFileDirectory  = "/Users/username/Downloads/srcmf-ud-master/corpus/conluu/"

### run 

deucFiles = set(range(len(deucfiles))) ## get as set, remove 12 as has issues
# deucFiles = deucFiles - {12}
for a in deucFiles:
  deuc_predFile = deucfiles.iloc[a,0]
  deuc_subpath = deucfiles.iloc[a,1]
  deuc_model = deucfiles.iloc[a,2]
  deuc_code = deucfiles.iloc[a,3]

  all_results = align_scoreDeucfiles(deuc_predFile, deuc_subpath, deuc_model, deuc_code, all_results)



## solving script to help when wierd EOF errors : load with special params, then hl, run all lines of function from def
 a = 12 # problemValues
  deuc_predFile = deucfiles.iloc[a,0]
  deuc_subpath = deucfiles.iloc[a,1]
  deuc_model = deucfiles.iloc[a,2]
  deuc_code = deucfiles.iloc[a,3]
  Deuc_preds = pd.read_csv(deuc_predFile, sep="\t", quotechar="\'", doublequote=False)




```

# 2005.8a  UDPipeoutput convert conllu to csv::  TO DO  ::
```{python}
# convert conllu files to csv
def ud_ScorePrep(rawUDpredFile):
  tidyUDpredFile = rawUDpredFile.replace('conllu','csv').replace('predictions','tidy')
  rawConnl = open(rawUDpredFile, "rt", encoding="UTF8")
  intConll = rawConnl.read()
  cleanedConnl = re.sub('# .+\n','',intConll)
  rawConnl.close()
  
  fin = open(tidyUDpredFile, "wt", encoding="UTF8")
  fin.write(cleanedConnl)
  fin.close()

## declarations
rawUDpredFile = "/Users/username/Downloads/srcmf-ud-master/corpus/UD/CA01-501.conllu"
UDFolder = "/Users/username/PhraseoRoChe/scores/correct/301s/UD_scoring/predictions/"

rawUD_preds = pd.DataFrame(glob.glob(UDFolder +'*conllu'))
rawUD_preds = rawUD_preds.sort_values(0)
rawUD_preds.reset_index(inplace=True, drop=True)
rawUD_preds.columns=['Path']

## run
for i in range(len(rawUD_preds)):
  rawUDpredFile = rawUD_preds.iloc[i,0]
  ud_ScorePrep(rawUDpredFile)

```

# 2005.8b : align, score UDpipe::  TO DO  ::
```{python}
# fun to combine, align, score UDmodels
def align_UDfiles(ud_predFile, UD_models):
  """docstring to add:"""
  UDpredCols = ['P_UDid','P_udFORME','P_udLEMME','P_udUPOStag','P_udXPOStag','P_udFEATS', 'P_udHEAD','P_udDEPREL','P_udDEPS','P_udMISC']
  UDbinderCols = ['P_udFORME', 'UUID']
  try:
      ud_preds = pd.read_csv(ud_predFile, sep="\t", header=None, names = UDpredCols, skip_blank_lines=False)
  except ValueError:
      try:
          ud_preds = pd.read_csv(ud_predFile, sep="\t", header=None, names = UDpredCols, skip_blank_lines=False, quotechar='"', doublequote=False) # 64947
      except ValueError:
          try:
            ud_preds = pd.read_csv(ud_predFile, sep="\t", header=None, names = UDpredCols, skip_blank_lines=False, quotechar="'", doublequote=False)# 64945
          except ValueError:
              try:
                ud_preds = pd.read_csv(ud_predFile, sep="\t", header=None, names = UDpredCols, skip_blank_lines=False, quotechar='\'', doublequote=True)# 
              except ValueError:
                  try:
                    ud_preds = pd.read_csv(ud_predFile, sep="\t", header=None, names = UDpredCols, skip_blank_lines=False, quotechar='"', doublequote=True)#error
                  except ValueError:
                    print("Input problem with UDpred file : tried with single + double apost as quotechar , doublequote as T and F")
  
  ud_preds = ud_preds[['P_UDid','P_udFORME','P_udLEMME','P_udUPOStag', 'P_udHEAD','P_udDEPREL']]
  
  CodeText = ud_predFile[-12:-8]
  CodeModel = ud_predFile[-7:-4]
  currentTreebank = UD_models[UD_models.iloc[:,0] == CodeModel ].iloc[0,2]
  LangStateTool = UD_models[UD_models.iloc[:,0] == CodeModel ].iloc[0,1]
  currentBertLike = "XXX"
  if ("CA" in CodeText) ==True:
    LangStateText ="OF" 
  else:
    LangStateText ="FM" 
  
  currentModelData = [CodeText, CodeModel, currentTreebank, currentBertLike, LangStateTool, LangStateText]
  
  ud_BinderFile = (ud_predFile[:-8] + "UDbinder.csv").replace('tidy','binders') 
  ud_binder = pd.read_csv(ud_BinderFile, sep="\t", header=None, names = UDbinderCols, skip_blank_lines=False)
  ud_aligned = pd.concat([ud_preds, ud_binder], axis=1)
  # ud_aligned = pd.merge(ud_preds, ud_binder, how="left", on="UUID")
  ud_aligned.columns = ['P_UDid', 'P_udFORME', 'P_udLEMME', 'P_udUPOStag', 'P_udHEAD', 'P_udDEPREL', 'del', 'UUID']
  ## now aligned UDoutput with binder
  ud_aligned = ud_aligned.drop((ud_aligned.columns[[6]]), axis=1)
  
  CorrectFilename = ud_predFile.replace('301s/UD_scoring/tidy/','').replace('-501','').replace('-502','').replace('-503','').replace('-504','').replace('-505','').replace('-506','')
  
  correct = pd.read_csv(CorrectFilename, encoding="UTF8",   sep="\t")
  # remappings : DEP and POS  
  DEP_RemapDict = {"aux:caus":"aux","aux:tense":"aux","auxs":"aux","expl:comp":"expl","expl:subj":"expl","flat:name":"flat","nsubj:pass":"nsubj","obl:agent":"obl","obl:arg":"obl","obl:mod":"obl", "aux:pass":"aux", "cc:nc":"cc","obl:advmod":"obl"}
  correct['C_DEPREL'] = correct['C_DEPREL'].map(DEP_RemapDict).fillna(correct['C_DEPREL'])
  
  aligned = pd.merge(correct, ud_aligned, how="left", on="UUID")
  aligned = aligned[[ 'C_FORME', 'C_LEMME', 'C_UPOStag',  'C_HEAD', 'C_DEPREL', 'UUID', 'P_UDid',  'P_udFORME', 'P_udLEMME', 'P_udUPOStag', 'P_udHEAD', 'P_udDEPREL']]
  
  # remap REL predictions
  aligned['P_udDEPREL'] = aligned['P_udDEPREL'].map(DEP_RemapDict).fillna(aligned['P_udDEPREL'])
  # remap POS predictions
  POS_RemapDict = {"AUX" : 'VERB'}
  aligned['P_udUPOStag'] = aligned['P_udUPOStag'].map(POS_RemapDict).fillna(aligned['P_udUPOStag'])
  
  
  saveName = CorrectFilename.replace("correct/","correct/301s/UD_scoring/tidy/").replace('.csv','') + ud_predFile[-8:-4] + 'Aligned.csv'
  aligned.to_csv(saveName, index=False, header=True, sep="\t")
  
  punctToDrop = aligned.loc[aligned['C_FORME'] == "zzkz"]
  aligned = aligned.drop(punctToDrop.index)
  scoring = aligned.reset_index(drop=True)
  
  return scoring,  currentModelData
  
def getUDscores(scoring, currentModelData):
  '''calculate POS accuracy scores'''
  scoringDF = scoring[['C_FORME', 'P_udFORME', 'C_UPOStag', 'P_udUPOStag', 'C_LEMME','P_udLEMME','UUID','C_HEAD','P_udHEAD', 'C_DEPREL', 'P_udDEPREL',  'P_UDid']]
  punctToDrop = scoringDF.loc[scoringDF['C_UPOStag'] == "PUNCT"]
  scoringDF = scoringDF.drop(punctToDrop.index)
  scoringDF = scoringDF.reset_index(drop=True)
  scoringDF['AlignScore'] = pd.DataFrame([1 if scoringDF.iloc[i,0] == scoringDF.iloc[i,1] else 0 for i in range(len(scoringDF))])
  scoringDF['POS_Score'] = pd.DataFrame([1 if scoringDF.iloc[i,2] == scoringDF.iloc[i,3] else 0 for i in range(len(scoringDF))])
  scoringDF['LemmeScore'] = pd.DataFrame([1 if scoringDF.iloc[i,5] == scoringDF.iloc[i,4] else 0 for i in range(len(scoringDF))])
  scoringDF['HEADscore'] = pd.DataFrame([1 if scoringDF.iloc[i,7] == scoringDF.iloc[i,8] else 0 for i in range(len(scoringDF))])
  scoringDF['RELscore'] = pd.DataFrame([1 if scoringDF.iloc[i,9] == scoringDF.iloc[i,10] else 0 for i in range(len(scoringDF))])
  scoringDF['LAS'] = pd.DataFrame([1 if ((1 == scoringDF.iloc[i,16]) & (scoringDF.iloc[i,8] == scoringDF.iloc[i,7]) ) else 0 for i in range(len(scoringDF))])

  ALIGNscore = round(scoringDF['AlignScore'].sum()/scoringDF.shape[0],4)
  POS_score = round(scoringDF['POS_Score'].sum()/scoringDF.shape[0],4)
  LEMME_score = round(scoringDF['LemmeScore'].sum()/scoringDF.shape[0],4)
  HEAD_score = round(scoringDF['HEADscore'].sum()/scoringDF.shape[0],4)
  REL_score = round(scoringDF['RELscore'].sum()/scoringDF.shape[0],4)
  LAS_score = round(scoringDF['LAS'].sum()/scoringDF.shape[0],4)
  outputScores = [ALIGNscore, POS_score, LEMME_score, HEAD_score, REL_score, LAS_score, scoringDF.shape[0]]
  return  outputScores

def compileScores(outputScores, currentModelData, all_results):
  resultdf  = pd.DataFrame([{'tool':currentModelData[1], 'Treebank':currentModelData[2],'BERT_like':currentModelData[3],'LangStateTool':currentModelData[4],'LangStateText':currentModelData[5],'NameText':currentModelData[0],'n':outputScores[6],'alignmentscore_PER':outputScores[0],'POS_scorePER':outputScores[1],'Lemme_scorePER':outputScores[2],'HEAD_scorePER': outputScores[3], 'REL_scorePER':outputScores[4], 'ATT_scorePER':outputScores[5]}])

  all_results = pd.concat([all_results, resultdf], axis=0)
  return all_results

## declarations
UD_modelData  = pd.DataFrame([{'Code':'501', 'LangState':'OF','Treebank':'SRCMF'}, {'Code':'502', 'LangState':'MF','Treebank':'GSD'},{'Code':'503', 'LangState':'MF','Treebank':'ParisStories'}, {'Code':'504', 'LangState':'MF','Treebank':'Partut'},{'Code':'505', 'LangState':'MF','Treebank':'Rhapsodie'}, {'Code':'506', 'LangState':'MF','Treebank':'Sequoia'}])

tidyUD_preds = pd.DataFrame(glob.glob(UDFolder.replace('predictions', 'tidy') + '*[1-6].csv'))
tidyUD_preds = tidyUD_preds.sort_values(0)
tidyUD_preds.reset_index(inplace=True, drop=True)
tidyUD_preds.columns=['Path']
## run
all_results = pd.DataFrame()
for i in range(0, len(tidyUD_preds)):
# for i in range(92, 83):
  ud_predFile = tidyUD_preds.iloc[i,0]
  # print(ud_predFile)
  scoring,  currentModelData = align_UDfiles(ud_predFile, UD_models)
  outputScores=  getUDscores(scoring, currentModelData)
  all_results =  compileScores(outputScores, currentModelData, all_results)
  
  ## 78, 90, 102
# range(78,84) + range(90,96,102,108)

all_results.to_csv("/Users/username/Desktop/UDresultsnewest.csv", sep="\t", header=True, index=False)

```

#2005.9a. stanza align score::  TO DO  ::
```{python}
# funct def
def align_scorestanzafiles(stanzaPredFile, all_results):
  if "610" in stanzaPredFile:
    stanzaState = "OF"
  else:
    stanzaState = "FMod"
  stanza_preds = pd.read_csv(stanzaPredFile, sep="\t",  skip_blank_lines=False)
  stanza_preds.columns = stanzaColNames
  stanza_preds = stanza_preds[['P.StanzaID', 'P.Forme', 'P.UPOS', 'P.HEAD', 'P.Rel', 'UUID']]
  TextCode = stanzaPredFile.replace(StanzaDirectory,'').replace('-610.csv','').replace('-620.csv','')
  currentParser=  stanzaPredFile.replace(StanzaDirectory,'').replace(TextCode,'').replace('.csv','')
  currentParser = currentParser.replace('-','')
  CorrectFilename = stanzaPredFile.replace(StanzaDirectory,CorrectFileDirectory).replace('-610','').replace('-620','')

  correct = pd.read_csv(CorrectFilename, encoding="UTF8",   sep="\t")
  correct.columns = correctColNames
  bound = pd.merge(correct, stanza_preds, how="left", on="UUID")

  Stanza_data = bound.dropna()
  Stanza_data = Stanza_data.reset_index(drop=True)
  Stanza_data = Stanza_data[[ 'C.FORME', 'C.UPOStag',  'C.HEAD', 'C.DEPREL', 'UUID',  'P.Forme', 'P.UPOS', 'P.HEAD', 'P.Rel']]

  Stanza_data['C.HEAD'] = Stanza_data['C.HEAD'].apply(pd.to_numeric, errors = 'coerce')
  Stanza_data['P.HEAD'] = Stanza_data['P.HEAD'].apply(pd.to_numeric, errors = 'coerce')

  Stanza_data['align'] = pd.DataFrame([1 if Stanza_data.iloc[i,0] == Stanza_data.iloc[i,5] else 0 for i in range(len(Stanza_data))]) ## if forme = forme 0 == 6
  Stanza_data['scorePOS'] = pd.DataFrame([1 if Stanza_data.iloc[i,1] == Stanza_data.iloc[i,6] else 0 for i in range(len(Stanza_data))]) ## if POS = POS == 2 = 4
  Stanza_data['ScoreHEAD'] = pd.DataFrame([1 if Stanza_data.iloc[i,2] == Stanza_data.iloc[i,7] else 0 for i in range(len(Stanza_data))]) ## if forme = forme 0 == 6
  Stanza_data['ScoreREL'] = pd.DataFrame([1 if Stanza_data.iloc[i,3] == Stanza_data.iloc[i,8] else 0 for i in range(len(Stanza_data))]) ## if forme = forme 0 == 6
  Stanza_data['ScoreATT'] = pd.DataFrame([1 if ((Stanza_data.iloc[i,2] == Stanza_data.iloc[i,7]) & (Stanza_data.iloc[i,3] == Stanza_data.iloc[i,8])) else 0 for i in range(len(Stanza_data))]) ## if forme = forme 0 == 6

  Stanza_dataSaveName = (CorrectFilename).replace(".csv","-" + stanzaState + "Stanza.csv")
  Stanza_data.to_csv(Stanza_dataSaveName, index=False, header=True, sep="\t")


  n_items = Stanza_data.shape[0]
  alignmentScorePER = round(Stanza_data['align'].sum()/n_items,4)
  POS_scorePER = round(Stanza_data['scorePOS'].sum()/n_items,4)
  HEAD_scorePER = round(Stanza_data['ScoreHEAD'].sum()/n_items,4)
  REL_scorePER = round(Stanza_data['ScoreREL'].sum()/n_items,4)
  ATT_scorePER = round(Stanza_data['ScoreATT'].sum()/n_items,4)
  

  
  resultdf  = pd.DataFrame([{'tool':currentParser, 'Treebank':currentTreebank,'BERT_like':currentBertLike,'LangStateTool':stanzaState,'LangStateText':LangStateText,'NameText':TextCode,'n':n_items,'alignmentscore_PER':alignmentScorePER,'POS_scorePER':POS_scorePER,'Lemme_scorePER':LemmeScorePER,'HEAD_scorePER': HEAD_scorePER, 'REL_scorePER':REL_scorePER, 'ATT_scorePER':ATT_scorePER}])

  all_results = pd.concat([all_results, resultdf], axis=0)
  return all_results


## declarations
stanzaColNames = ['P.StanzaID','P.Forme','P.UPOS','P.XPOS','p.Feats','P.HEAD','P.Rel','UUID']
correctColNames = ['C.id', 'C.FORME', 'C.LEMME', 'C.UPOStag', 'C.XPOStag', 'C.udFEATS', 'C.HEAD', 'C.DEPREL', 'C.DEPS', 'UUID']
LemmeScorePER = "XXX"
currentTreebank = "XXX"
currentBertLike = "XXX"
CorrectFileDirectory = "/Users/username/Downloads/srcmf-ud-master/corpus/conluu/"
StanzaDirectory = "/Users/username/Downloads/srcmf-ud-master/corpus/stanza/"

StanzaPredFilesOF = pd.DataFrame(glob.glob( stanzaFolder +'/*610.csv'))
StanzaPredFilesFM = pd.DataFrame(glob.glob( stanzaFolder +'/*620.csv'))
StanzaPredFiles = pd.concat([StanzaPredFilesOF, StanzaPredFilesFM])
StanzaPredFiles = StanzaPredFiles.sort_values(0)
StanzaPredFiles.reset_index(inplace=True, drop=True)
StanzaPredFiles.columns=['Path']

for i in range(len(StanzaPredFiles)):
  stanzaPredFile = StanzaPredFiles.iloc[i,0]
  all_results = align_scorestanzafiles(stanzaPredFile, all_results)

```

#### Gold Corpus::  TO DO  ::
#3000Import gold::  TO DO  ::
#3001prepare output::  TO DO  ::
#3002run::  TO DO  ::
#3003post::  TO DO  ::


### 540s: after € replacement:
FP08 : quote = '"', double = F
FP06 : quote = '"', double = F



```{python}

theseFiles = pd.DataFrame(glob.glob("/Volumes/AlphaThree/Binding/zzProcessed/zzReady/232" + "/*.csv"))
results = pd.DataFrame()
for i in range(len(theseFiles)):
  currentFile = theseFiles.iloc[i,0]
  currentData = pd.read_csv(currentFile, usecols=[2], sep="\t")
  finalValue = currentData.iloc[len(currentData)-1,0]
  fileName = currentFile[-12:-8]
  result = pd.DataFrame([{'fileName':fileName,'finalValue':finalValue}])
  results = pd.concat([results, result], axis=0)
results.to_csv("/Users/username/Desktop/finalValues.csv", sep="\t", index=False)

```


#4000 UD API usage
## 4001 Create files for API
```{python}
# function 3300
def prepare_udpipe_api_files(fileList):
  '''Prepare files for API :: for each file, get col1, remove np.nan cases and write destinationfile == write input as vertical text'''
  allResults = pd.DataFrame()
  for f in range(len(fileList)):
    newFileforJSON = fileList.iloc[f,0]
  ## file prep                                             
    dataforJSONraw = pd.read_csv(newFileforJSON, sep="\t", usecols= [1], header=None, names=['forme'], encoding="UTF8", quotechar="\'")
    dataforJSONraw['formeOut'] = ["" if dataforJSONraw.iloc[i,0] is np.nan else dataforJSONraw.iloc[i,0] for i in range(len(dataforJSONraw))]
    fileOut = newFileforJSON.replace('/201/','/').replace('201.txt', '550.conll')
    dataout = open(fileOut, 'wt', encoding='UTF8')
    dataforJSONraw = dataforJSONraw.reset_index(drop=True)
    for i in range(len(dataforJSONraw)):
      dataout.write( str(str(dataforJSONraw.iloc[i,1])+'\n'))
    dataout.close()
    
def prepare_udpipe_api_calls(dossier_UD, ModelsData):
  ## prepare commands for each model for each file, sending commands to df
  theseFiles = glob.glob("/" + dossier_UD + "/530/*conll")
  allResults =  pd.DataFrame()
  for f in theseFiles:
    fileOut = f
    if " " in fileOut:
      fileOut = fileOut.replace(' ','\ ')
    for m in range(len(ModelsData)):
      thisModelName = ModelsData.iloc[m,0]
      thisModelCode = ModelsData.iloc[m,1]
      outputName = fileOut.replace('550',str(thisModelCode))
      command = f'curl -F data=@{fileOut} -F model={thisModelName} -F input=vertical -F tagger= -F parser= http://lindat.mff.cuni.cz/services/udpipe/api/process > {outputName}'
    
      result = pd.DataFrame([{'f':f,'m':m,'command':command}])
      # print(command, flush=True)
      allResults = pd.concat([allResults,result], axis=0)
  
  return allResults
    # write all commands to df
  
# funciton 3301 : make folders where necessary
def makeUDoutputfolders(dossier_UD, a, b):
  for m in range(a, b):
    foldername = dossier_UD + "/" + str(m + 10)+  "/"
    if os.path.exists(foldername)==False:
      os.makedirs(foldername)
  print('Folders made')

## run function 3300 :: generate list of files to process to send to function 3300
# list models, and list files to take as input : for vertical input, use 201.txt as input
ModelsData =  pd.DataFrame([{'name':'old_french','code':'531'}, {'name':'french-gsd','code':'532'}, {'name':'french-parisstories','code':'533'}, {'name':'french-partut','code':'534'}, {'name':'french-rhapsodie','code':'535'}, {'name':'french-sequoia','code':'536'}])


import glob
file_list = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/newlyLGERMedTexts/temp/' + '*201.txt'))
file_list = file_list.sort_values(0)
file_list = file_list.reset_index(drop=True)
allResults =  prepare_udpipe_api_calls(dossier_UD, ModelsData)
commands_file = '/'+dossier_UD + "udpipecommands.csv"
allResults['command'].to_csv('/'+dossier_UD + "udpipecommands.csv", sep="\t", index=False, header=None)

#######

## run section for 3301-3003
dossier_UD = 'Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/'
makeUDoutputfolders(dossier_UD, 551, 557)

## desktop script newpyscript.py looks for desktop/udpipecommands.csv and gets, so send contents to desktopcsv file

```





after run:
```{python}
# function 3302: get files output by API calls
def getFileList_API(dossier_UD):  
  UDoutputfiles = pd.Series()
  for i in range(541,547):
    # get files output each model, put into list
    inputFiles = pd.Series(glob.glob(dossier_UD + str(i) + "/*.conll"))
    UDoutputfiles = pd.concat( [inputFiles, UDoutputfiles])
  
  UDoutputfiles = pd.DataFrame(UDoutputfiles)
  UDoutputfiles = UDoutputfiles.sort_values(0)
  UDoutputfiles = UDoutputfiles.reset_index(drop=True)
  return(UDoutputfiles)



```


# 4100 sem : create binder
```{python}

dossier_SEM = 
inputFile = '/Users/username/Dropbox/__doodling/inputForR/103/AF03-103.csv' 
theseFiles = pd.DataFrame(glob.glob(dossier_temp + "/*103.csv"))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)
for i in range(len(theseFiles)):
# for i in range(36,len(theseFiles)):
  inputFile = theseFiles.iloc[i,0]
  prepareSEM(inputFile)

## to to : if code = CA, CM, need to use cols 1,8

def prepareSEM(inputFile):
  outputBinderName = inputFile.replace('103.csv','700.csv')
  outputName= outputBinderName.replace('700.csv','701.txt')
  
  # get data from 103 file, get col with no punct in forms and UUID : 12,13 if normal corpus, 1,8 if Extensioon text
  if inputFile[-12:-10] in ['CA','CM']:
    myColumns = [1,3,8]
    formColumn = 'C_FORME'
  else:
    myColumns = [12,13]
    formColumn = 'FormeNoP'

  inputData = pd.read_csv(inputFile, sep="\t", usecols=myColumns)
  
  ## drop punct as is NA
  inputData = inputData.dropna(subset=[formColumn])
  if inputFile[-12:-10] in ['CA','CM']:
    punct_rows = inputData.loc[inputData['C_UPOStag'] == "PUNCT"]
    inputData = inputData.drop(punct_rows.index)
    inputData = inputData[['C_FORME', 'UUID']]
  
  ## drop  punct
  output = pd.DataFrame( [[inputData.iloc[i,0],inputData.iloc[i,1]] for i in range(len(inputData)) if inputData.iloc[i,0] != 'zzblank'    ] )
  
  ## put blanks at EOS markers
  output = pd.DataFrame( [["",""]  if output.iloc[i,0] == 'zzkz' else [output.iloc[i,0],output.iloc[i,1]]    for i in range(len(output))  ] )
  
  #name cols
  output.columns = ['Forme','UUID']
    ## remove apostrophes to avoid retokenisation
  output['FormeOut'] = pd.DataFrame([output.iloc[i,0].replace("\'","") if '\'' in output.iloc[i,0] else output.iloc[i,0] for i in range(len(output))])
  
  # remove spaces to avoid retokenisaiton
  output['FormeOut'] = pd.DataFrame([output.iloc[i,2].replace(" ","") if ' ' in output.iloc[i,2] else output.iloc[i,2] for i in range(len(output))])
  
  # remove . to avoid retokenisaiton
  output['FormeOut'] = pd.DataFrame([output.iloc[i,2].replace(".","") if '.' in output.iloc[i,2] else output.iloc[i,2] for i in range(len(output))])
  # output['check'].value_counts()
 
  # remove - to avoid retokenisaiton
  output['FormeOut'] = pd.DataFrame([output.iloc[i,2].replace("-","") if '-' in output.iloc[i,2] else output.iloc[i,2] for i in range(len(output))])
  # output['check'].value_counts()
 
 # add helpful indicator to show which lines have been changed
  output['check'] = ["MOD" if output.iloc[i,0] != output.iloc[i,2] else "" for i in range(len(output))  ]
  
  
  output.to_csv(outputBinderName, sep="\t", index=False, encoding='UTF8', header=True)
  output['FormeOut'].to_csv(outputName, sep="\t", index=False, encoding='UTF8', header=False)
  
  fin = open(outputName, "rt", encoding = "UTF-8")
  binder_data = fin.read().replace('\n""','\n') ## need to modify to replace triple \n\n\n with \n\n and\n\t\t\n\t\t\n\t\t… with double
  fin.close()
  # export binder final version
  fin = open(outputName, "w", encoding = "UTF-8")
  fin.write(binder_data)
  fin.close()
  print(inputFile + " Done", flush=True )
  
  # View(output)
  # View(inputData)



```

# 4101 bind SEM web output
```{python}
from Levenshtein import distance as lev

theseFiles = glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/SEM/702' + '/*conll')
for f in theseFiles:
  inputFile = theseFiles[0]
  binderFile = inputFile.replace('702.conll','700.csv')
  
  # binderFile = '/Users/username/PhraseoRoChe/Pipeline/refresh/SEM/700/MF89-700.csv'
  binderData = pd.read_csv(binderFile, sep="\t")
  binderData = binderData.dropna(subset=['UUID'])
  binderData = binderData.reset_index(drop=True)
  # inputFile = '/Users/username/PhraseoRoChe/Pipeline/refresh/SEM/702/MF89.conll'
  taggedData = pd.read_csv(inputFile, sep="\t", header=None, names = ['FormeD','POS'])
  
  if (len(taggedData) == len(binderData)):
    SEMaligned = pd.concat([binderData, taggedData], axis=1)
    SEMaligned = SEMaligned[['Forme',  'UUID','POS']]
    SEMaligned.to_csv(inputFile.replace('702.conll','703.csv'), sep="\t", header=True, index=False, encoding= "UTF8")
    print(inputFile,' :: success ')
  else:
    print(inputFile, " ", str(len(taggedData)), " vs ", str(len(binderData)))
    # find errors:
    errorFinder = pd.concat([binderData, taggedData], axis=1)
    
    errorFinder['ident'] = pd.DataFrame([lev(str(errorFinder.iloc[l,0]), str(errorFinder.iloc[l,4])) for l in range(len(errorFinder))])
  ## get get rolling sum as df, then subset df to get cases where rolling sum is greater than myValue
  myValue = 10
  SuspectErrLocations= errorFinder['ident'].rolling(3, center=True).sum().loc[errorFinder['ident'].rolling(3, center=True).sum() > int(myValue)].index
  # for s in range(len(SuspectErrLocations)):
    s=0
    View((errorFinder[(SuspectErrLocations[s])-20:(SuspectErrLocations[s])+20]))
      
      
## note that if forme is - only, need to grep to place in front of next item, remove line      
  # View(SEMaligned)
```


#4200 prepaer RNN

```{python}
inputFiles = pd.DataFrame(glob.glob(main_directory + "/temp/"+ "*103.csv"))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)
i=i-1
inputFile = inputFiles.iloc[i,0]
outputBinder = inputFile.replace('103.csv','900.csv')
outputData = inputFile.replace('103.csv','901.csv')
inputData = pd.read_csv(inputFile, sep="\t")
output = inputData[['FormeNoP', 'UUID']]
output = output.loc[output['FormeNoP'] != 'zzblank']
output = output.loc[output['FormeNoP'] != 'zzkz']
output = output.reset_index(drop=True)
output.to_csv(outputBinder, index=False, header=None, sep="\t")
output[['FormeNoP']].to_csv(outputData, index=False, header=None, sep="\t")

```

# 4300 do RNN
```{text}
# RNN tagger : 
terminal : cd /Users/username/Downloads/RNNTagger/
to tag wiht OF : shell script to run + input + output :
cmd/rnn-tagger-old-french.sh /Users/username/Dropbox/__doodling/temp/MF99-900.csv > /Users/username/Dropbox/__doodling/temp/MF99-911n.csv
move RNN folder to path wiht no spaces:

```

# 4400 bind RNN post

```{python}
inputFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/RNN/*-900.csv"))
inputFiles =inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)
for i in range(len(inputFiles)):
  rnn_binder_file = inputFiles.iloc[i,0]
  rnn_OF_file = rnn_binder_file.replace('900','911')
  rnn_OF_outname = rnn_OF_file.replace('911','912')
  rnn_FM_file = rnn_binder_file.replace('900','921')
  rnn_FM_outname = rnn_FM_file.replace('921','922')
  rnn_binder = pd.read_csv(rnn_binder_file, sep="\t", header=None)
  
  rnn_binder.columns = ['F_900','UUID']
  ## OF section
  rnn_OF = pd.read_csv(rnn_OF_file, sep="\t", header=None)
  rnn_OF.columns = ['F_911','P_911','L_911']
  if len(rnn_OF) == len(rnn_binder):
    rnn_OF_bound = pd.concat([rnn_binder, rnn_OF], axis=1)
    rnn_OF_out = rnn_OF_bound.drop(['F_911'], axis=1).dropna(subset=["F_900"])
    rnn_OF_out.to_csv(rnn_OF_outname, sep="\t", index=False)
  ##FM section
  rnn_FM = pd.read_csv(rnn_FM_file, sep="\t", header=None)
  rnn_FM.columns = ['F_921','P_921','L_921']
  if len(rnn_FM) == len(rnn_binder):
    rnn_FM_bound = pd.concat([rnn_binder, rnn_FM], axis=1)
    rnn_FM_out = rnn_FM_bound.drop(['F_921'], axis=1).dropna(subset=["F_900"])
    rnn_FM_out.to_csv(rnn_FM_outname, sep="\t", index=False)

View(rnn_OF_bound)

```

#4500 CLTK

```{python}
import platform
import glob
import pandas as pd
import time
from cltk import NLP
import cltk
OFcltk_nlp = NLP(language='fro')
from cltk.languages.pipelines import OldFrenchPipeline
from cltk.languages import pipelines
# udModelOF = udpipe_load_model(file="/Users/username/Dropbox/models/old_french-srcmf-ud-2.5-191206.udpipe")
# filesForCLTKa = glob.glob('/Users/username/Dropbox/__doodling/inputForR/601' + "/*-601.csv")
filesForCLTK = pd.DataFrame(filesForCLTK)
filesForCLTK = filesForCLTK.sort_values(0)
filesForCLTK = filesForCLTK.reset_index(drop=True)
# def doCLTKprocessing(filesForCLTK):

for i in range(len(filesForCLTK)):
  currentFile = filesForCLTK.iloc[i,0]
  outputfilename = currentFile.replace('-601','-801')
  inputfile = pd.read_csv(currentFile, sep="\t")
  allInfo = pd.DataFrame()
  # start =time.time()
  sentencevalues = inputfile['sentID'].apply(pd.to_numeric, errors = 'coerce')
  sentencevalues = sentencevalues.fillna(0).astype("int64")
  sentencevalues = list(set(sentencevalues) - {0})
  inputfile['UUID'] = inputfile['s'].apply(lambda x: ast.literal_eval(x))
for s in range(len(sentencevalues)):
# s= len(inputfile)-1
  SentenceInfo = pd.DataFrame()
  currentSentnce = inputfile.iloc[s,1]
  UUIDs = pd.Series(inputfile.iloc[s,5])
  OFcltk_doc = OFcltk_nlp.analyze(text=currentSentnce)
  for token in OFcltk_doc:
    tokenInfo = pd.DataFrame([{'Token':token.string, 'Lemma':token.lemma, 'POS':token.upos, 'i':token.index_token, 'REL':token.dependency_relation , 'HeadIndex':token.governor, 'ssid':str('s')}])
    SentenceInfo = pd.concat([SentenceInfo, tokenInfo], axis=0)
    SentenceInfo = SentenceInfo.reset_index(drop=True)    
  AllSentenceInfo = pd.concat([SentenceInfo, UUIDs], axis=1)  
  allInfo = pd.concat([allInfo, AllSentenceInfo], axis=0)
# end = time.time()
  allInfo.to_csv(outputfilename, sep="\t", header=True, index=False)


```

#4600 spark
```{python}
# import json
import pandas as pd
import numpy as np

import sparknlp
import pyspark.sql.functions as F
from pyspark.sql.functions import explode, arrays_zip, expr

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.types import StringType, IntegerType
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score

# start spark run
spark = sparknlp.start()
print ("Spark NLP Version :", sparknlp.version())


# define pipeline : assembler
documentAssembler = DocumentAssembler()\
        .setInputCol("text")\
        .setOutputCol("document")

# define pipeline : tokenizer
tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")

regexTokenizer = RegexTokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token") \
    .setPattern("\\s+")

posA = PerceptronModel.pretrained("pos_srcmf", "fro").setInputCols(["document", "token"]).setOutputCol("posA")

sequenceClassifier_loadedB = CamemBertForTokenClassification.pretrained("camembert_classifier_poet","fr") \
    .setInputCols(["document", "token"]) \
    .setOutputCol("posB")


sequenceClassifier_loadedC = CamemBertForTokenClassification.pretrained("camembert_classifier_pos_french","fr").setInputCols(["document", "token"]).setOutputCol("posC")

tokenClassifierD = BertForTokenClassification.pretrained("bert_pos_french_postag_model","fr").setInputCols(["document", "token"]).setOutputCol("posD")

posE = PerceptronModel.pretrained("pos_rhapsodie", "fr").setInputCols(["document", "token"]).setOutputCol("posE")

posF = PerceptronModel.pretrained("pos_sequoia", "fr").setInputCols(["document", "token"]).setOutputCol("posF")

posG = PerceptronModel.pretrained("pos_gsd", "fr").setInputCols(["document", "token"]).setOutputCol("posG")

posH = PerceptronModel.pretrained("pos_parisstories", "fr").setInputCols(["document", "token"]).setOutputCol("posH")

posI = PerceptronModel.pretrained("pos_partut", "fr").setInputCols(["document", "token"]).setOutputCol("posI")

posJ = PerceptronModel.pretrained("pos_ud_gsd", "fr").setInputCols(["document", "token"]).setOutputCol("posJ")


# define pipeline : lemmatizer
lemma = LemmatizerModel.pretrained("lemma_srcmf", "fro").setInputCols(["token"]).setOutputCol("lemma")

dependencyParserApproach = DependencyParserApproach() \
    .setInputCols("sentence", "pos", "token") \
    .setOutputCol("dependency") \
    .setDependencyTreeBank("src/test/resources/parser/unlabeled/dependency_treebank")
    
# define pipeline : dep parser
dep_parser = DependencyParserModel.pretrained('dependency_conllu')\
.setInputCols(["document", "pos", "token"])\
.setOutputCol("dependency")

# define pipeline : typed dep parser
typed_dep_parser = TypedDependencyParserModel.pretrained('dependency_typed_conllu')\
        .setInputCols(["token", "pos", "dependency"])\
        .setOutputCol("dependency_type")

# define pipeline : order of processes
nlpPipeline_A = Pipeline(stages = [documentAssembler, regexTokenizer ,lemma, posA, sequenceClassifier_loadedB, sequenceClassifier_loadedC, tokenClassifierD, posE, posF, posG, posH, posI, posJ])

theseFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/PourStanza/" + "*.csv"))
##### secodn run : take 106 data
for i in range(len(theseFiles)):
  start = time.time()
  inputname = theseFiles.iloc[i,0]
  outputname = inputname.replace('601.csv', '770.csv')
  inputData = pd.read_csv(inputname, sep="\t", encoding = "UTF8")
  # send to spark df
  
  text_list = list(inputData['text'])
  
  # when using HOPS input, gives correct rows of output
  targetDF = spark.createDataFrame(text_list, StringType()).toDF("text")
  
  # define where to send output
  result = nlpPipeline_A.fit(targetDF).transform(targetDF)
  
  # do the processing and send to pd
  basicOutput = result.select(F.explode(F.arrays_zip(result.token.result,
  result.token.begin,
  result.token.end,
  result.lemma.result,
  result.posA.result,
  result.posB.result,
  result.posC.result,
  result.posD.result,
  result.posE.result,
  result.posF.result,
  result.posG.result,
  result.posH.result,
  result.posI.result,
  result.posJ.result)).alias("cols")).select(F.expr("cols['0']").alias("chunk"), F.expr("cols['3']").alias("lem"),F.expr("cols['4']").alias("posA"),F.expr("cols['5']").alias("posB"),F.expr("cols['6']").alias("posC"),F.expr("cols['7']").alias("posD"),F.expr("cols['8']").alias("posE"),F.expr("cols['9']").alias("posF"),F.expr("cols['10']").alias("posG"),F.expr("cols['11']").alias("posH"),F.expr("cols['12']").alias("posI"),F.expr("cols['13']").alias("posJ")).toPandas()
  end = time.time()
    # result.dependencyParserApproach.result,
  # result.dep_parser.result,
  # result.typed_dep_parser.result,

  #,F.expr("cols['14']").alias("dependencyParserApproach")F.expr("cols['15']").alias("dep_parser"),F.expr("cols['16']").alias("typed_dep_parser")
  basicOutput.to_csv(outputname, sep="\t", index=False, encoding="UTF8")
  print("done file " + outputname, "@ ", len(basicOutput)/(end - start), "wps", flush = True)
del(spark)

```

# 4700 ## normal spacy in python

```{python}
# doc = nlp(data)
# nlp.pipe_names
# output_path = '/Users/username/PhraseoRoChe/Pipeline/refresh/temp/'
# if os.path.exists(output_path)==False:
#   os.makedirs(output_path)

# with nlp.disable_pipes('ner'):
  # doc = nlp(data)
# theseFiles = theseFiles[1:]
theseFiles = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/temp/' + "*103.csv"))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)

import spacy
from spacy.lang.fr import French
import time
from spacy.tokens import Doc
nlp = French()
nlp = spacy.load('fr_core_news_lg')
f=0
for f in range(len(theseFiles)):
  start =time.time()
  currentFile = theseFiles.iloc[f,0]
  inputfile = pd.read_csv(currentFile, sep="\t")
  if currentFile[-12:-10] in ['CA','CM']:
    FormeColName = "C_FORME"
  else:
    FormeColName = "Forme"
  
  outputfilename = output_path+ currentFile[-12:-7] + '850.csv'
  allInfo = pd.DataFrame()
  sentencevalues = inputfile['ssid'].apply(pd.to_numeric, errors = 'coerce')
  sentencevalues = sentencevalues.fillna(0).astype("int64")
  sentencevalues = list(set(sentencevalues) - {0} )
  # sentencevalues = [i for i in sentencevalues if i >1779]
# sentencevalues test= set(sentencevalues) - set(list(range(1,1779))) - {0}
  # for s in range(1,int(inputfile.iloc[len(inputfile)-2,11])):
for s in range(0,len(sentencevalues)):
  currentSentnce = inputfile.loc[inputfile['ssid']== str(s)]
  words = currentSentnce[FormeColName]
  currentSentnce['spaces'] = True
  UUIDs = currentSentnce['UUID']
  sentenceInfo =  pd.DataFrame()
  doc = Doc(nlp.vocab, words = currentSentnce[FormeColName], spaces = currentSentnce['spaces'])
  for token in nlp(doc):
    tokenInfo = pd.DataFrame([{'Token':token.text, 'Lemma':token.lemma_, 'POS':token.pos_, 'tag':token.tag_, 'i':token.i, 'REL':token.dep_ , 'HeadIndex':token.head.i, 'ssid':str(s)}])
    sentenceInfo = pd.concat([sentenceInfo, tokenInfo], axis=0)
  sentenceInfo = sentenceInfo.reset_index(drop=True)
  sentenceInfoFull = pd.concat([sentenceInfo, UUIDs.reset_index(drop=True)], axis=1)
  allInfo         = pd.concat([allInfo, sentenceInfoFull], axis=0)
  allInfo.to_csv(outputfilename, sep="\t", header=True, index=False)
  print(str(currentFile[-12:-8]) + " :: " + str(time.time() - start), flush=True)

test =  set(sentencevalues) - set(list((range((1779)))))

```

# 4800 : UDpipe in R
### ud pipe R local output
```{r} 

## import libraries
library(rjson) ; library(jsonlite) ; library(tibble) ; library(beepr); library(tm) ; library(textclean) ; library(dplyr) ; library(stringr) ; library(rvest) ; library(tidyr); library(tidyverse); library(tidytext) ; library(purrr) ; library(tidyr) ; library(lubridate) ; library(tictoc) ; library(xml2); library(udpipe)

theseFiles <- list.files(path = "/Users/username/PhraseoRoChe/Pipeline/refresh/temp/", pattern = "103.csv", full.names = T)

results <-  tibble()

outputPath <- "/Users/username/Dropbox/Pipeline/refresh/temp/"

# load models to use
udModelFModGSD <- udpipe_load_model(file="/Users/username/Dropbox/models/french-gsd-ud-2.5-191206.udpipe") 
udModelSeq <- udpipe_load_model(file="/Users/username/Dropbox/models/french-sequoia-ud-2.5-191206.udpipe")
udModelPartut <- udpipe_load_model(file="/Users/username/Dropbox/models/french-partut-ud-2.5-191206.udpipe")
udModelParStor <- udpipe_load_model(file="/Users/username/Dropbox/models/french-spoken-ud-2.5-191206.udpipe")
udModelOF <- udpipe_load_model(file="/Users/username/Dropbox/models/old_french-srcmf-ud-2.5-191206.udpipe")
f=1
# for (f in 37:length(theseFiles)){
for (f in 1:length(theseFiles)){
  # start <- Sys.time()
  currentFileIn <- theseFiles[f]
  TextCode <- substr(currentFileIn, 63, 66)
  inputData <- read.csv(currentFileIn, sep="\t")
  
  if ((substr(TextCode,1,2) == "CA") |  (substr(TextCode,1,2) == "CM"))
    {myColumns <- c(2,9,11,12)}  else{    myColumns <- c( 2,11,12,14)  }

  inputData <- inputData[myColumns]
  outputOF <- tibble()
  outputGSD <- tibble()
  outputPStor<- tibble()
  outputSeq<- tibble()
  outputPartut<- tibble()
  pathHead <- '/Users/username/PhraseoRoChe/Pipeline/refresh/temp/'
  
  OFname<- paste0(pathHead, 581, "/",TextCode, "-",581, ".csv")
  GSDName<- paste0(pathHead, 582, "/",TextCode, "-",582, ".csv")
  PStorName<- paste0(pathHead, 583, "/",TextCode, "-",583, ".csv")
  PartutName<- paste0(pathHead, 584, "/",TextCode, "-",584, ".csv")
  SeqName<- paste0(pathHead, 585, "/",TextCode, "-",585, ".csv")
  # OFname<- paste0(pathHead, 'GG01', "-",581, ".csv")
  # GSDName<- paste0(pathHead, 'GG01', "-",582, ".csv")
  # PStorName<- paste0(pathHead, 'GG01', "-",583, ".csv")
  # PartutName<- paste0(pathHead, 'GG01', "-",584, ".csv")
  # SeqName<- paste0(pathHead, 'GG01', "-",585, ".csv")
  SentenceValues <- unique(inputData$ssid)
  SentenceValues <- SentenceValues[-2] #- "zzkz"
  
  # for (i in 155:158){
  for (i in 1:length(SentenceValues)){
    i = SentenceValues[i]
    currentSentence = inputData[which(inputData$ssid ==i),]
    UUIDvalues <- currentSentence$UUID
    if ((substr(TextCode,1,2) == "CA") |  (substr(TextCode,1,2) == "CM"))
    {input = list(c(currentSentence$C_FORME))}  else{input = list(c(currentSentence$Forme))}
    txt <- sapply(input, FUN=function(x) paste(x, collapse = "\n"))

    # tag sentence with each model
    taggedSentenceOF <- as.data.frame(udpipe_annotate(udModelOF, x = txt, tokenizer = "vertical", tagger = 'default', parser = 'default', lemmatiser = 'default'))
    taggedSentenceGSD <- as.data.frame(udpipe_annotate(udModelFModGSD, x = txt, tokenizer = "vertical", tagger = 'default', parser = 'default', lemmatiser = 'default'))
    taggedSentencePStor <- as.data.frame(udpipe_annotate(udModelParStor, x = txt, tokenizer = "vertical", tagger = 'default', parser = 'default', lemmatiser = 'default'))
    taggedSentenceSeq <- as.data.frame(udpipe_annotate(udModelSeq, x = txt, tokenizer = "vertical", tagger = 'default', parser = 'default', lemmatiser = 'default'))
    taggedSentencePartut <- as.data.frame(udpipe_annotate(udModelPartut, x = txt, tokenizer = "vertical", tagger = 'default', parser = 'default', lemmatiser = 'default'))

    
  # add UUIDs
    taggedSentenceGSD$UUID <-UUIDvalues
    taggedSentenceOF$UUID <-UUIDvalues  
    taggedSentencePStor$UUID <-UUIDvalues
    taggedSentenceSeq$UUID <-UUIDvalues
    taggedSentencePartut$UUID <-UUIDvalues
    taggedSentenceGSD$ssid <-i
    taggedSentenceOF$ssid <-i
    taggedSentencePStor$ssid <-i
    taggedSentenceSeq$ssid <-i
    taggedSentencePartut$ssid <-i
  
    # restrict cols
    currentOutputGSD <- taggedSentenceGSD[c(5:12,15,16)]
    currentOutputOF <- taggedSentenceOF[c(5:12,15,16)]
    currentOutputPStor <- taggedSentencePStor[c(5:12,15,16)]
    currentOutputSeq <- taggedSentenceSeq[c(5:12,15,16)]
    currentOutputPartut <- taggedSentencePartut[c(5:12,15,16)]
    ## bind
# View(outputOF)
    outputOF <- rbind(outputOF, currentOutputOF)
    outputGSD <- rbind(outputGSD, currentOutputGSD)
    outputPStor <- rbind(outputPStor, currentOutputPStor)
    outputSeq <- rbind(outputSeq, currentOutputSeq)
    outputPartut <- rbind(outputPartut, currentOutputPartut)
taggedSentenceOF
    # cat('i==', i, " of " ,length(SentenceValues), ":: Texte ", f, ' of ', length(theseFiles) ,'\n')
      }
  ## write when finished parsing file
  
  write_csv(outputOF, OFname)
  write_csv(outputGSD, GSDName)
  write_csv(outputPStor, PStorName)
  write_csv(outputPartut, PartutName)
  write_csv(outputSeq, SeqName)

}
  

```


# 5000 combining everything
# 5020 combine RNNs
```{python}




## first run with RNN has some old versions of binders in CA 7,8,9,10,11,14,15,16,17,18,19,CM03,CM04, but files are correctly aligned wiht eachother
RNN_files = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/RNN/912/' + '*.csv'))
r=0
allResults = pd.DataFrame()

# for r in range(len(RNN_files)):
r=r+1
  RNN_OFfile = RNN_files.iloc[r,0]
  RNN_FMfile = RNN_OFfile.replace('912','922')
  saveName = RNN_OFfile.replace('912/','').replace('-912','-9XX')
  RNN_OF_data = pd.read_csv(RNN_OFfile, sep="\t")
  RNN_OF_data.columns = ['Forme', 'UUID', 'P_912','L_912']
  RNN_FM_data = pd.read_csv(RNN_FMfile, sep="\t")
  RNN_FM_data.columns = ['Forme', 'UUID', 'P_922','L_922']
  currentFile = RNN_OFfile[-12:-8]
  # RNN_all = pd.merge(RNN_OF_data, RNN_FM_data, how='inner', on=["UUID"])
  if RNN_OF_data.shape == RNN_FM_data.shape:
    RNN_OF_data.shape == RNN_FM_data.shape
    RNN_data = pd.merge(RNN_OF_data, RNN_FM_data, how="left", on=["UUID", "Forme"])
    RNN_data.to_csv(saveName, sep="\t", index=False)
    saveName
    
    message1 = 'correct'
  else:
    message1 = "ERR"

  # RNN_all = pd.concat([RNN_OF_data, RNN_FM_data], axis=1)
  # RNN_all.columns = ['Forme', 'UUID', 'P_912', 'L_912', 'FormeD', 'UUIDD', 'P_922', 'L_922']
  # RNN_all = RNN_all.drop(['FormeD','UUIDD'], axis=1)

  if RNN_OF_data.shape[0] == RNN_FM_data.shape[0] == RNN_all.shape[0]:
    message2 = 'correct'
  else:
    message2 = "ERR"
  if RNN_OF_data.shape[1] == RNN_FM_data.shape[1] == (RNN_all.shape[1]-2):
    message3 = 'correct'
  else:
    message3 = "ERR"
  
  result = pd.DataFrame([{'File':currentFile,'Input':message1,'OutLength':message2,'OutWidth':message3 }])
  allResults = pd.concat([allResults,result], axis=0 )
  RNN_all.to_csv(saveName, sep="\t", index=False)

#regenerateNewBinders if errror in binder
# for f in glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/temp/103/*103.csv'):
#   inputFile =  f
#   outputBinder = inputFile.replace('103.csv','900.csv')
#   inputData = pd.read_csv(inputFile, sep="\t")
#   output = inputData[['C_FORME', 'UUID']]
#   output = output.loc[output['C_FORME'] != 'zzkz']
#   output.to_csv(outputBinder, index=False, header=None, sep="\t")

#process with new binders
RNN_files = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/RNN/912/' + '*XX.csv'))
f=10
datafile = RNN_files.iloc[f,0]
binderFile = datafile.replace('XX','00')

# data = pd.read_csv(datafile, sep="\t")
# binder = pd.read_csv(binderFile, sep="\t", names = ['FormeB','UUID_b'])
# data.shape[0] == binder.shape[0]
# updated = pd.concat([data, binder], axis=1)
# updated
# updated = updated.drop(['UUID','FormeB'], axis=1)
# updated.columns = ['Forme', 'P_912', 'L_912', 'P_922', 'L_922', 'UUID']
# updated
# updated.to_csv(datafile, sep="\t", index=False)
# 


```


#5024 glue deuc together
```{python}
deucFiles = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/PourDeucalion/' + '*311.csv'))
deucFiles = deucFiles.sort_values(0)
deucFiles = deucFiles.reset_index(drop=True)
for d in range(len(deucFiles)):
  OF_file = deucFiles.iloc[d,0]
  EMF_file = OF_file.replace('311','321')
  FM_file = OF_file.replace('311','331')
  outputfile = OF_file.replace('/311','').replace('311','3XX')
  OF_data = pd.read_csv(OF_file, sep=",")
  OF_data.columns = ['F_311','L_311','P_311','UUID']
  EMF_data = pd.read_csv(EMF_file, sep=",")
  EMF_data.columns = ['F_321','L_321','P_321','UUID']
  FM_data = pd.read_csv(FM_file, sep=",")
  FM_data.columns = ['F_331','L_331','P_331','UUID']

  if (   (OF_data.shape ==  EMF_data.shape)):
    deuc_int = pd.merge(OF_data, EMF_data,  how="inner", on="UUID")
    if(FM_data.shape == EMF_data.shape) &    (deuc_int.shape[0] == FM_data.shape[0]):
      deuc_all = pd.merge(deuc_int, FM_data,  how="inner", on="UUID")
      deuc_all = deuc_all.drop(['F_321','F_331'], axis=1)
      deuc_all.columns = ['Forme', 'L_311', 'P_311', 'UUID', 'L_321', 'P_321', 'L_331', 'P_331']
      deuc_all.to_csv(outputfile, sep="\t", index=False)
      print(outputfile, flush=True)
      
```

#5021 : glue UD58X together
```{python}
UDfiles = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/581/' + '*581.csv'))
UDfiles = UDfiles.sort_values(0)
UDfiles = UDfiles.reset_index(drop=True)
for d in range(len(deucFiles)):
  file581 = UDfiles.iloc[d,0]
  file582 = file581.replace('581','582')
  file583 = file581.replace('581','583')
  file584 = file581.replace('581','584')
  file585 = file581.replace('581','585')
  saveName = file581.replace('/581','').replace('581.','58X.')
  UDcols = [0, 1, 2, 3, 6, 7, 8, 9]
  data581 = pd.read_csv(file581, sep=",", usecols=UDcols)
  data581.columns = ['int581', 'F_581', 'L_581', 'P_581', 'H_581', 'R_581','UUID', 'ssid581']

  data582 = pd.read_csv(file582, sep=",", usecols=UDcols)
  data582.columns = ['int582', 'F_582', 'L_582', 'P_582', 'H_582', 'R_582','UUID', 'ssid582']

  UD_data = pd.merge(data581, data582, how="inner", on="UUID")

  data583 = pd.read_csv(file583, sep=",", usecols=UDcols)
  data583.columns = ['int583', 'F_583', 'L_583', 'P_583', 'H_583', 'R_583','UUID', 'ssid583']

  UD_data = pd.merge(UD_data, data583, how="inner", on="UUID")

  data584 = pd.read_csv(file584, sep=",", usecols=UDcols)
  data584.columns = ['int584', 'F_584', 'L_584', 'P_584', 'H_584', 'R_584','UUID', 'ssid584']

  UD_data = pd.merge(UD_data, data584, how="inner", on="UUID")

  data585 = pd.read_csv(file585, sep=",", usecols=UDcols)
  data585.columns = ['int585', 'F_585', 'L_585', 'P_585', 'H_585', 'R_585','UUID', 'ssid585']

  UD_data = pd.merge(UD_data, data585, how="inner", on="UUID")
  
  if (UD_data.shape[0] == data581.shape[0]):
    print('success!')
    UD_data = UD_data.drop(['ssid582','ssid584','ssid583','ssid585','F_582','F_583','F_584','F_585'], axis=1)
    UD_data.columns = ['int581', 'Forme', 'L_581', 'P_581', 'H_581', 'R_581', 'UUID', 'ssid', 'int582', 'L_582', 'P_582', 'H_582', 'R_582', 'int583', 'L_583', 'P_583', 'H_583', 'R_583', 'int584', 'L_584', 'P_584', 'H_584', 'R_584', 'int585', 'L_585', 'P_585', 'H_585', 'R_585']
    UD_data.to_csv(saveName, sep="\t", index=False)
    print(saveName, flush=True)
      
```

# 5022 : combine all HOPS outputs
```{python}
## funcitons ot help:
def checkINTids(HOPS_combined):
  '''function to check if all int_ids are identical ; returns message to this effect '''
  intChecker = HOPS_combined[['int_232', 'int_241', 'int_242', 'int_244', 'int_245', 'int_261', 'int_262', 'int_263', 'int_271', 'int_272', 'int_273', 'int_281', 'int_282', 'int_283', 'int_291', 'int_292', 'int_293']]
  
  a = intChecker.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("IDs match error")
    id_match_state = 0
  else:
    print("IDs match pass")
    id_match_state = 1
  return HOPS_combined, id_match_state

# cgeck Forme agree in all cases
def checkFvalues(HOPS_combined):
  '''function to check if all F_ids are identical ; returns message to this effect '''
  F_Checker = HOPS_combined[['F_232', 'F_241', 'F_242', 'F_244', 'F_245', 'F_261', 'F_262', 'F_263', 'F_271', 'F_272', 'F_273', 'F_281', 'F_282', 'F_283', 'F_291', 'F_292', 'F_293']]
  
  a = F_Checker.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("F_value match error")
    F_match_state = 0
  else:
    print("F_value match pass")
    F_match_state = 1
  return HOPS_combined, F_match_state
# find errors:: errors = np.where(b==False), then view these rows in HOPS_combined
# errorList = pd.DataFrame([errors[0][i] for i in range(len(errors[0]))])
# 
## tidy id values if pass check
def id_tidier(HOPS_combined, id_match_state):
  if id_match_state ==1:
    # when IDmatch passed, remove all but 1 col, saveout
    width_initial = HOPS_combined.shape[1]
    HOPS_combined = HOPS_combined.drop(['int_241', 'int_242', 'int_244', 'int_245', 'int_261', 'int_262', 'int_263', 'int_271', 'int_272', 'int_273', 'int_281', 'int_282', 'int_283', 'int_291', 'int_292', 'int_293'], axis=1)
    # HOPS_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", HOPS_combined.shape[1])
  return HOPS_combined

## tidy F values if pass check
def F_tidier(HOPS_combined, F_match_state):
  if F_match_state ==1:
    # when Fmatch passed, remove all but 1 col, saveout
    width_initial = HOPS_combined.shape[1]
    HOPS_combined = HOPS_combined.drop([ 'F_241', 'F_242', 'F_244', 'F_245', 'F_261', 'F_262', 'F_263', 'F_271', 'F_272', 'F_273', 'F_281', 'F_282', 'F_283', 'F_291', 'F_292', 'F_293'], axis=1)
    # HOPS_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", HOPS_combined.shape[1])
    return HOPS_combined

def tidyColumns(HOPS_combined):
  ## tidy final colnames :
  # change names to generic
  HOPS_combined.columns = ['ss_intID', 'Forme', 'UUID', 'P_232', 'H_232', 'R_232', 'P_241', 'H_241', 'R_241', 'P_242', 'H_242', 'R_242', 'P_244', 'H_244', 'R_244', 'P_245', 'H_245', 'R_245', 'P_261', 'H_261', 'R_261', 'P_262', 'H_262', 'R_262', 'P_263', 'H_263', 'R_263', 'P_271', 'H_271', 'R_271', 'P_272', 'H_272', 'R_272', 'P_273', 'H_273', 'R_273', 'P_281', 'H_281', 'R_281', 'P_282', 'H_282', 'R_282', 'P_283', 'H_283', 'R_283', 'P_291', 'H_291', 'R_291', 'P_292', 'H_292', 'R_292', 'P_293', 'H_293', 'R_293']
  
  # put in order
  HOPS_combined = HOPS_combined[['ss_intID', 'Forme', 'UUID', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293']]
  return HOPS_combined

def prepareVariables(currentText, HOPS_codes):
  ''' get empty df to store data, get list of HOPS files'''
  HOPS_combined = pd.DataFrame() # storage for output
  # get list of all HOPS files for current text
  HOPS_files = pd.DataFrame([[('/Users/username/PhraseoRoChe/Pipeline/refresh/HOPS/' + currentCode + "/" + currentText + "-" + currentCode + ".csv"),currentCode] for currentCode in HOPS_codes])
  HOPS_files.columns = ['Path','currentCode']
  return HOPS_combined, HOPS_files, currentText

def do_binding(HOPS_combined, HOPS_files, currentText, sample_folder, colnames_base):
  '''do merges on UUID cols in HOPS outouts '''
  for f in range(len(HOPS_files)):
    inputFileName = HOPS_files.iloc[f,0]
    intermedSaveName = sample_folder[:-4] +currentText + "-29X.csv"
    FinalSaveName = sample_folder[:-4] +currentText + "-2XX.csv"
    custom_colnames = [colnames_base[c] + HOPS_files.iloc[f,1] if colnames_base[c] != "UUID" else "UUID" for c in range(len(colnames_base)  )] # add code to colnames except for UUID
    inputData = pd.read_csv(inputFileName, sep="\t", names=custom_colnames, usecols = HOPScols)#, quotechar="\'", doublequote=True)
    if f ==0:
      HOPS_combined = inputData # no merge if f==0
      print("Success: added ", currentText, HOPS_files.iloc[f,1], flush=True)
    else:
      if len(inputData) == len(HOPS_combined):
        HOPS_combined = pd.merge(HOPS_combined, inputData, how="inner", on="UUID")
        print(f+1, " :: Success: added ", currentText, HOPS_files.iloc[f,1], flush=True)
      else:
        print("Haluting, Length error @ ", currentText, HOPS_files.iloc[f,1], flush=True)
  HOPS_combined.to_csv(intermedSaveName, sep="\t", index=False)
  return HOPS_combined, FinalSaveName

###

# copy from list of models used
HOPS_codes = ['232', '241', '242', '244', '245', '261', '262', '263', '271', '272', '273', '281', '282', '283', '291', '292', '293']

#colnames: bases
colnames_base = ['int_','F_','UUID','P_','H_','R_']
HOPScols = [0,1,2,3,6,7]
## take example folder, use contents to get list of text codes
sample_folder = '/Users/username/PhraseoRoChe/Pipeline/refresh/HOPS/232/'
filenames = pd.DataFrame(glob.glob(sample_folder + "*.csv"))
filenames = filenames.sort_values(0)
filenames = filenames[0].str.replace(sample_folder,'')
TextCodes = [filenames.iloc[i][:4] for i in range(len(filenames))]

#### end of pre-run declarations
# Haluting, Length error @  FP08 282, t=38, MF13, t=51
# Fmatch error t =45, MF05
# set current text
# currentText = 

t=1

HOPS_combined, HOPS_files, currentText=  prepareVariables(TextCodes[t], HOPS_codes)

HOPS_combined, FinalSaveName = do_binding(HOPS_combined, HOPS_files, currentText, sample_folder, colnames_base)

## int_id redundancy check:
HOPS_combined, id_match_state = checkINTids(HOPS_combined)
HOPS_combined = id_tidier(HOPS_combined, id_match_state)

## F_value check:
HOPS_combined, F_match_state = checkFvalues(HOPS_combined)
HOPS_combined = F_tidier(HOPS_combined, F_match_state)

# when passed
HOPS_combined = tidyColumns(HOPS_combined)
HOPS_combined.to_csv(FinalSaveName, sep="\t", index=False)

# others : no specifyq, double
# CA16 q=', double=T
# FP08 : q=", double=F
#MF05-293 has somemissing , where present in other texts, leading to Fvalue fail ; 
#verify with : F_Checker[errorList.iloc[14,0]:], errorList coming from commented lines in function

```


# 5023 combine UD 51X
```{python}
## funcitons ot help:
def checkINTids(UD_combined):
  '''function to check if all int_ids are identical ; returns message to this effect '''
  intChecker = UD_combined[['int_511', 'int_512', 'int_513', 'int_514','int_515', 'int_516']]
  intCheckerD = intChecker.dropna()
  a = intCheckerD.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("IDs match error")
    id_match_state = 0
  else:
    print("IDs match pass")
    id_match_state = 1
  return UD_combined, id_match_state

# cgeck Forme agree in all cases
def checkFvalues(UD_combined):
  '''function to check if all F_ids are identical ; returns message to this effect '''
  F_Checker = UD_combined[['F_511', 'F_512', 'F_513', 'F_514','F_515', 'F_516']]
  F_CheckerD = F_Checker.dropna()
  F_CheckerD = F_CheckerD.reset_index(drop=True)
  a = F_CheckerD.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("F_value match error")
    F_match_state = 0
  else:
    print("F_value match pass")
    F_match_state = 1
  return UD_combined, F_match_state
# find errors:: , then view these rows in UD_combined
# errors = np.where(b==False)
# errorList = pd.DataFrame([errors[0][i] for i in range(len(errors[0]))])
# errors = pd.DataFrame(dtype="object")
# for i in range(len(errorList)):
#   thisError = pd.DataFrame(F_CheckerD.iloc[errorList.iloc[i,0]:(errorList.iloc[i,0] +1)], dtype="object")
#   errors = pd.concat([errors, thisError], axis=0)


def prepareVariables(currentText, UD_codes):
  ''' get empty df to store data, get list of HOPS files'''
  UD_combined = pd.DataFrame(dtype="object") # storage for output
  # get list of all HOPS files for current text
  UD_files = pd.DataFrame([[('/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/' + currentCode + "/" + currentText + "-" + currentCode + ".csv"),currentCode] for currentCode in UD_codes])
  UD_files.columns = ['Path','currentCode']
  return UD_combined, UD_files, currentText

def do_binding(UD_combined, UD_files, currentText, sample_folder, colnames_base):
  '''do merges on UUID cols in HOPS outouts '''
  for f in range(len(UD_files)):
    inputFileName = UD_files.iloc[f,0]
    intermedSaveName = sample_folder[:-4] +currentText + "-51Y.csv"
    FinalSaveName = sample_folder[:-4] +currentText + "-51X.csv"
    custom_colnames = [colnames_base[c] + UD_files.iloc[f,1] if colnames_base[c] != "UUID" else "UUID" for c in range(len(colnames_base)  )] # add code to colnames except for UUID
    inputData = pd.read_csv(inputFileName, sep="\t")#,  quotechar="\'", doublequote=True)
    inputData.columns = custom_colnames
    if f ==0:
      UD_combined = inputData # no merge if f==0
      print(f+1, " :: Success: added ", currentText, UD_files.iloc[f,1], flush=True)
    else:
      if len(inputData) == len(UD_combined):
        UD_combined = pd.merge(UD_combined, inputData, how="inner", on="UUID")
        print(f+1, " :: Success: added ", currentText, UD_files.iloc[f,1], flush=True)
      else:
        print("Haluting, Length error @ ", currentText, UD_files.iloc[f,1], flush=True)
  UD_combined.to_csv(intermedSaveName, sep="\t", index=False)
  return UD_combined, FinalSaveName

def F_tidier(UD_combined, F_match_state):
  if F_match_state ==1:
    # when Fmatch passed, remove all but 1 col, saveout
    width_initial = UD_combined.shape[1]
    UD_combined = UD_combined.drop([ 'F_512', 'F_513', 'F_514','F_515', 'F_516'], axis=1)
    # UD_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", UD_combined.shape[1])
    return UD_combined

def id_tidier(UD_combined, id_match_state):
  if id_match_state ==1:
    # when IDmatch passed, remove all but 1 col, saveout
    width_initial = UD_combined.shape[1]
    UD_combined = UD_combined.drop([ 'int_512', 'int_513', 'int_514','int_515', 'int_516'], axis=1)
    UD_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", UD_combined.shape[1])
  return UD_combined


def tidyColumns(UD_combined):
  ## tidy final colnames :
  # change names to generic
  UD_combined.columns = ['ss_intID', 'Forme', 'L_511', 'P_511', 'H_511', 'R_511', 'UUID', 'L_512', 'P_512', 'H_512', 'R_512', 'L_513', 'P_513', 'H_513', 'R_513', 'L_514', 'P_514', 'H_514', 'R_514', 'L_515', 'P_515', 'H_515', 'R_515', 'L_516', 'P_516', 'H_516', 'R_516']
  
  # put in order
  UD_combined = UD_combined[['ss_intID','Forme','UUID','P_511','P_512','P_513','P_514','P_515','P_516','L_511','L_512','L_513','L_514','L_515','L_516','H_511','H_512','H_513','H_514','H_515','H_516','R_511','R_512','R_513','R_514','R_515','R_516']]
  return UD_combined


## tidy F values if pass check


# copy from list of models used
UD_codes = ['511', '512', '513', '514', '515', '516']
	udFORME	udLEMME	udUPOStag	udHEAD	udDEPREL	UUID

#colnames: bases
colnames_base = ['int_','F_','L_','P_','H_','R_','UUID']

## take example folder, use contents to get list of text codes
sample_folder = '/Users/username/PhraseoRoChe/Pipeline/refresh/PourUD/511/'
filenames = pd.DataFrame(glob.glob(sample_folder + "*.csv"))
filenames = filenames.sort_values(0)
filenames = filenames[0].str.replace(sample_folder,'')
TextCodes = [filenames.iloc[i][:4] for i in range(len(filenames))]
t=t-1
UD_combined, UD_files, currentText=  prepareVariables(TextCodes[t], UD_codes)

UD_combined, FinalSaveName = do_binding(UD_combined, UD_files, currentText, sample_folder, colnames_base)
intermedSaveName = "intermedSaveName"
## int_id redundancy check:
UD_combined, id_match_state = checkINTids(UD_combined)
UD_combined = id_tidier(UD_combined, id_match_state)

## F_value check:
UD_combined, F_match_state = checkFvalues(UD_combined)
UD_combined = F_tidier(UD_combined, F_match_state)

UD_combined = tidyColumns(UD_combined)
UD_combined.to_csv(FinalSaveName, sep="\t", index=False)


#err  CA16,18
```

# 5025 combine Stanza
```{python}
## funcitons ot help:
def prepareVariables(currentText, stanza_codes):
  ''' get empty df to store data, get list of HOPS files'''
  stanza_combined = pd.DataFrame(dtype="object") # storage for output
  # get list of all HOPS files for current text
  stanza_files = pd.DataFrame([[('/Users/username/PhraseoRoChe/Pipeline/refresh/PourStanza/' + currentCode + "/" + currentText + "-" + currentCode + ".csv"),currentCode] for currentCode in stanza_codes])
  stanza_files.columns = ['Path','currentCode']
  return stanza_combined, stanza_files, currentText

def do_binding(stanza_combined, stanza_files, currentText, sample_folder, colnames_base):
  '''do merges on UUID cols in Stanza outouts '''
  for f in range(len(stanza_files)):
    inputFileName = stanza_files.iloc[f,0]
    # intermedSaveName = sample_folder[:-4] +currentText + "-51Y.csv"
    FinalSaveName = sample_folder[:-4] +currentText + "-6XX.csv"
    custom_colnames = [colnames_base[c] + stanza_files.iloc[f,1] if colnames_base[c] != "UUID" else "UUID" for c in range(len(colnames_base)  )] # add code to colnames except for UUID
    inputData = pd.read_csv(inputFileName, sep="\t")#,  quotechar="\'", doublequote=True)
    inputData.columns = custom_colnames
    if f ==0:
      stanza_combined = inputData # no merge if f==0
      print(f+1, " :: Success: added ", currentText, stanza_files.iloc[f,1], flush=True)
    else:
      if len(inputData) == len(stanza_combined):
        stanza_combined = pd.merge(stanza_combined, inputData, how="inner", on="UUID")
        print(f+1, " :: Success: added ", currentText, stanza_files.iloc[f,1], flush=True)
      else:
        print("Haluting, Length error @ ", currentText, stanza_files.iloc[f,1], flush=True)
  stanza_combined.to_csv(intermedSaveName, sep="\t", index=False)
  return stanza_combined, FinalSaveName

def checkINTids(stanza_combined):
  '''function to check if all int_ids are identical ; returns message to this effect '''
  intChecker = stanza_combined[['int_611', 'int_621']]
  intCheckerD = intChecker.dropna()
  a = intCheckerD.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("IDs match error")
    id_match_state = 0
  else:
    print("IDs match pass")
    id_match_state = 1
  return stanza_combined, id_match_state


# cgeck Forme agree in all cases
def checkFvalues(stanza_combined):
  '''function to check if all F_ids are identical ; returns message to this effect '''
  F_Checker = stanza_combined[['F_611', 'F_621', ]]
  F_CheckerD = F_Checker.dropna()
  F_CheckerD = F_CheckerD.reset_index(drop=True)
  a = F_CheckerD.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("F_value match error")
    F_match_state = 0
  else:
    print("F_value match pass")
    F_match_state = 1
  return stanza_combined, F_match_state
# find errors:: , then view these rows in stanza_combined
# errors = np.where(b==False)
# errorList = pd.DataFrame([errors[0][i] for i in range(len(errors[0]))])
# errors = pd.DataFrame(dtype="object")
# for i in range(len(errorList)):
#   thisError = pd.DataFrame(F_CheckerD.iloc[errorList.iloc[i,0]:(errorList.iloc[i,0] +1)], dtype="object")
#   errors = pd.concat([errors, thisError], axis=0)



def F_tidier(stanza_combined, F_match_state):
  if F_match_state ==1:
    # when Fmatch passed, remove all but 1 col, saveout
    width_initial = stanza_combined.shape[1]
    stanza_combined = stanza_combined.drop([ 'F_621'], axis=1)
    # UD_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", stanza_combined.shape[1])
    return stanza_combined

def id_tidier(stanza_combined, id_match_state):
  if id_match_state ==1:
    # when IDmatch passed, remove all but 1 col, saveout
    width_initial = stanza_combined.shape[1]
    stanza_combined = stanza_combined.drop(['int_621'], axis=1)
    # stanza_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", stanza_combined.shape[1])
  return stanza_combined


def tidyColumns(stanza_combined):
  ## tidy final colnames :
  # change names to generic
  stanza_combined.columns = ['ss_intID', 'Forme', 'P_611', 'UUID', 'H_611', 'R_611', 'P_621', 'H_621', 'R_621']

  # put in order
  stanza_combined = stanza_combined[['ss_intID', 'Forme', 'P_611', 'P_621', 'UUID', 'H_611', 'H_621', 'R_611',  'R_621']]
  return stanza_combined


#
# 
# ## tidy F values if pass check
# 
# 
# # copy from list of models used
stanza_codes = ['611', '621']
# 	udFORME	udLEMME	udUPOStag	udHEAD	udDEPREL	UUID
StanzaID	StanzaForme	StanzaUPOS_OF	TokenID	StanzaDepOn_OF	StanzaRel_OF
# #colnames: bases
colnames_base = ['int_','F_','P_','UUID','H_','R_']
# 
# ## take example folder, use contents to get list of text codes
sample_folder = '/Users/username/PhraseoRoChe/Pipeline/refresh/PourStanza/611/'
filenames = pd.DataFrame(glob.glob(sample_folder + "*.csv"))
filenames = filenames.sort_values(0)
filenames = filenames[0].str.replace(sample_folder,'')
TextCodes = [filenames.iloc[i][:4] for i in range(len(filenames))]
# t=t+1
stanza_combined, stanza_files, currentText=  prepareVariables(TextCodes[t], stanza_codes)

stanza_combined, FinalSaveName = do_binding(stanza_combined, stanza_files, currentText, sample_folder, colnames_base)

## int_id redundancy check:
stanza_combined, id_match_state = checkINTids(stanza_combined)
stanza_combined = id_tidier(stanza_combined, id_match_state)

# ## F_value check:
stanza_combined, F_match_state = checkFvalues(stanza_combined)
stanza_combined = F_tidier(stanza_combined, F_match_state)

stanza_combined = tidyColumns(stanza_combined)
stanza_combined.to_csv(FinalSaveName, sep="\t", index=False)
FinalSaveName

# 
#err  CA08,CM02
```


Now have all outputs from diff models of same source glued together.
Time to glue these outputs to the 103 file

#5030 LGERM + HOPS : need to redo with ssid col in output!

```{python}
def checkFvalues(TwoState):
  '''function to check if f_values from HOPS and LGERM are identical ; returns message to this effect '''
  F_Checker = TwoState[['F_103', 'Forme']]
  F_Checker['F_103'] = ["" if F_Checker.iloc[i,0] == "zzkz" else F_Checker.iloc[i,0]  for i in range(len(F_Checker))]
  F_Checker['Forme'] = ["" if F_Checker.iloc[i,1] is np.nan  else F_Checker.iloc[i,1]  for i in range(len(F_Checker))]
  
  # F_CheckerD = F_Checker.dropna()
  # F_CheckerD = F_CheckerD.reset_index(drop=True)
  a = F_Checker.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("F_value match error : see output to consider manual override")
    F_match_state = 0
  else:
    print("F_value match success")
    F_match_state = 1
  return TwoState, F_match_state

def F_tidier(TwoState, F_match_state):
  if F_match_state ==1:
    # when Fmatch passed, remove all but 1 col, saveout
    width_initial = TwoState.shape[1]
    TwoState = TwoState.drop([ 'Forme'], axis=1)
    # UD_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", TwoState.shape[1])
    return TwoState
  else:
    return TwoState

def id_tidier(TwoState, id_match_state):
  if id_match_state ==1:
    # when IDmatch passed, remove all but 1 col, saveout
    width_initial = TwoState.shape[1]
    TwoState = TwoState.drop(['ss_intID'], axis=1)
    print("Width ::", width_initial, "==> ", TwoState.shape[1])
  return TwoState

def checkINTids(TwoState):
  '''function to check if all int_ids are identical ; returns message to this effect '''
  intChecker = TwoState[['ss_intID_L', 'ss_intID']]
  for colName in ['ss_intID_L', 'ss_intID']:
    intChecker[colName] = intChecker[colName].apply(pd.to_numeric, errors = 'coerce')
    intChecker[colName] = intChecker[colName].fillna(0).astype("int64")  

  intCheckerD = intChecker.dropna()
  a = intCheckerD.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("ID match error")
    id_match_state = 0
  else:
    print("ID match success")
    id_match_state = 1
  return TwoState, id_match_state

def ID_value_errorGetter(TwoState, id_match_state):
  if id_match_state !=1:
    intChecker = TwoState[['ss_intID_L', 'ss_intID']]
    for colName in ['ss_intID_L', 'ss_intID']:
      intChecker[colName] = intChecker[colName].apply(pd.to_numeric, errors = 'coerce')
      intChecker[colName] = intChecker[colName].fillna(0).astype("int64")  
    intCheckerD = intChecker.dropna()
    a = intCheckerD.values
    b = (a == a[:, [0]]).all(axis=1)
    # find errors:: , then view these rows in stanza_combined
    AllErrors = np.where(b==False)
    errorList = pd.DataFrame([AllErrors[0][i] for i in range(len(AllErrors[0]))])
    ID_Errors = pd.DataFrame(dtype="object")
    for i in range(len(errorList)):
      thisError = pd.DataFrame(intCheckerD.iloc[errorList.iloc[i,0]:(errorList.iloc[i,0] +1)], dtype="object")
      ID_Errors = pd.concat([ID_Errors, thisError], axis=0)
    return ID_Errors 


def f_value_errorGetter(TwoState, F_match_state):
  if F_match_state !=1:
    F_Checker = TwoState[['F_103', 'Forme', ]]
    F_Checker['F_103'] = ["" if F_Checker.iloc[i,0] == "zzkz" else F_Checker.iloc[i,0]  for i in range(len(F_Checker))]
    F_Checker['Forme'] = ["" if F_Checker.iloc[i,1] is np.nan  else F_Checker.iloc[i,1]  for i in range(len(F_Checker))]
    
    # F_CheckerD = F_Checker.dropna()
    # F_CheckerD = F_CheckerD.reset_index(drop=True)
    a = F_Checker.values
    b = (a == a[:, [0]]).all(axis=1)
    F_match_state = 0
    # find errors:: , then view these rows in stanza_combined
    AllErrors = np.where(b==False)
    errorList = pd.DataFrame([AllErrors[0][i] for i in range(len(AllErrors[0]))])
    F_Errors = pd.DataFrame(dtype="object")
    for i in range(len(errorList)):
      thisError = pd.DataFrame(F_Checker.iloc[errorList.iloc[i,0]:(errorList.iloc[i,0] +1)], dtype="object")
      F_Errors = pd.concat([F_Errors, thisError], axis=0)
    return F_Errors 

def tidyColumns(TwoState):
  ## tidy final colnames :
  # change names to generic : colset1 = cols WITH lgerm ; colset2 - WITHOUT lgerm
  colSet1 = ['F_103',  'ss_intID_L', 'UUID', 'ssid', 'P_103','P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'L_103', 'H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282',  'H_283', 'H_291', 'H_292', 'H_293', 'R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293',  'Page']
  colSet2 = ['F_103',  'ss_intID_L', 'UUID', 'ssid', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282',  'H_283', 'H_291', 'H_292', 'H_293', 'R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293']
  if  'Page' in TwoState.columns:
    colSet = colSet1
  else:
    colSet = colSet2
  TwoState = TwoState[colSet]
  return TwoState

#### end of defintiions

# general declarations : L = live == LGERM texts ; C = textesde contrôle SRCMF/Democrat
LGERM_columNamesL = ['TokenID', 'F_103', 'P_103', 'L_103', 'code_103', 'known', 'wordid', 'Page', 'Para',   'TokenChap', 'ss_intID_L', 'ssid', 'FormeNoP', 'UUID']
LGERM_col_orderL = ['F_103', 'P_103', 'L_103', 'code_103',  'Page', 'ss_intID_L', 'ssid', 'UUID']
## note : C_FORME changed to F_103 to use existing functions
LGERM_columNamesC = ['C_id', 'F_103', 'C_LEMME', 'C_UPOStag', 'C_XPOStag', 'C_HEAD',  'C_DEPREL', 'C_Feats', 'UUID', 'C_DEPS', 'ss_intID_L', 'ssid', 'C_idOrig']

LGERM_col_orderC = ['F_103', 'C_LEMME', 'C_UPOStag',  'C_HEAD',  'C_DEPREL', 'UUID',  'ss_intID_L', 'ssid','C_id',  'C_idOrig']

# file declarations
inputFiles = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/temp/103/*103.csv'))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)

# missing : AF07, CA09, CA16, CP02
for i in range(len(inputFiles)):
i=0 ;
LGERM_file = inputFiles.iloc[i,0]
#"/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/103/CA08-099.csv"
# if '103.csv' in LGERM_file:
LGERM_columNames = LGERM_columNamesL
LGERM_col_order = LGERM_col_orderL
HOPS_file = LGERM_file.replace('103','2XX')
FinalSaveName = LGERM_file.replace('/103','').replace('-103','-state2')
# else:
#   LGERM_columNames = LGERM_columNamesC
#   LGERM_col_order = LGERM_col_orderC
#   HOPS_file = LGERM_file.replace('103','2XX').replace('099.','2XX.')
#   FinalSaveName = LGERM_file.replace('/103','').replace('-099','-state2')


  View(LGERM_data)
## get, tidy LGERM data
LGERM_data = pd.read_csv(LGERM_file, sep="\t", low_memory=False)
LGERM_data.columns = LGERM_columNames
LGERM_data = LGERM_data[LGERM_col_order]

LGERM_dataSample = LGERM_data[['F_103', 'UUID']]
# HOPS_file = "/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/2XX/CA08-2XX.csv"
# load 2XX file, merge on UUID, left to preserve sent boundaries, etc
HOPS_data = pd.read_csv(HOPS_file, sep="\t", low_memory=False)
HOPS_dataSample = HOPS_data[['Forme','UUID']]
#####
LGERM_dataSample = LGERM_dataSample.dropna(subset=['UUID'])
theseRows = [i for i in range(len(LGERM_dataSample)) if LGERM_dataSample.iloc[i,0] == "zzkz"]
LGERM_dataSample = LGERM_dataSample.drop(400)
LGERM_dataSample = LGERM_dataSample.reset_index(drop=True)
glued = pd.concat([HOPS_dataSample, LGERM_dataSample], axis=1)
glued.columns = ['F1','UUIDa','F2','UUIDb']
glued = glued[['UUIDa','F2','UUIDb']]
# equivTable = pd.read_csv("/Users/username/PhraseoRoChe/Pipeline/refresh/zzPreviousStates/used_to_make_readyState/C_equivTables/CA08-equiv.csv", sep="\t")
# 
# worker0 = pd.merge( LGERM_data, equivTable, how="left", left_on="UUID", right_on="UUID_L")
# worker1 = pd.merge( equivTable, HOPS_data, how="left", left_on="UUID", right_on="UUID")
# worker3 = pd.merge(worker0, worker1, how="left", on="UUID_L")
# worker3.columns
# 
# HOPS_data = pd.merge(HOPS_data, equivTable, how="right", left_on="UUID", right_on="UUID_L")
# HOPS_data = HOPS_data.drop(['UUID_y'], axis=1)
# HOPS_data.columns = ['ss_intID', 'Forme', 'UUID_r', 'P_232', 'P_241', 'P_242', 'P_244',
#        'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281',
#        'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'H_232', 'H_241', 'H_242',
#        'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273',
#        'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'R_232', 'R_241',
#        'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272',
#        'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293',
#        'UUID']
# ####
View(LGERM_data) = pd.merge(LGERM_data, glued, how="left", left_on = "UUID", right_on='UUIDb')
TwoState = pd.merge(LGERM_data, HOPS_data, how="left", left_on="UUIDa", right_on="UUID")
TwoState = worker3
# test ss_intID values
TwoState, id_match_state = checkINTids(TwoState)
##  option to send values from ss_intID to ss_intID_L
# TwoState['ss_intID_L'] = TwoState['ss_intID']
if id_match_state==0:
  ID_errors = ID_value_errorGetter(TwoState, id_match_state)
  print(F_Errors)
# F_match_state = 1
else:
  TwoState = id_tidier(TwoState, id_match_state)
  View(TwoState)
  
  ## test forme, F-103
  TwoState, F_match_state,  = checkFvalues(TwoState)
  if F_match_state==0:
    F_Errors = f_value_errorGetter(TwoState, F_match_state)
    print(F_Errors) # this will show HOPS excluded sents as errors
  # F_match_state = 1 
  else:
    # tidy, order, export
    TwoState = F_tidier(TwoState, F_match_state)
    TwoState = tidyColumns(TwoState)
    TwoState.to_csv(FinalSaveName, sep="\t", index=False)
    FinalSaveName

```

Variant of HOPS+LGERM combiner for files CA, CM other than CA01
```{python}
## LGERM + HOPS for C data : other than CA01

def checkFvalues(TwoState):
  '''function to check if f_values from HOPS and LGERM are identical ; returns message to this effect '''
  F_Checker = TwoState[['F_103', 'Forme', ]]
  F_Checker['F_103'] = ["" if F_Checker.iloc[i,0] == "zzkz" else F_Checker.iloc[i,0]  for i in range(len(F_Checker))]
  F_Checker['Forme'] = ["" if F_Checker.iloc[i,1] is np.nan  else F_Checker.iloc[i,1]  for i in range(len(F_Checker))]
  
  # F_CheckerD = F_Checker.dropna()
  # F_CheckerD = F_CheckerD.reset_index(drop=True)
  a = F_Checker.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("F_value match error : see output to consider manual override")
    F_match_state = 0
  else:
    print("F_value match success")
    F_match_state = 1
  return TwoState, F_match_state

def F_tidier(TwoState, F_match_state):
  if F_match_state ==1:
    # when Fmatch passed, remove all but 1 col, saveout
    width_initial = TwoState.shape[1]
    TwoState = TwoState.drop(['Forme'], axis=1)
    # UD_combined.to_csv(intermedSaveName, sep="\t", index=False)
    print("Width ::", width_initial, "==> ", TwoState.shape[1])
    return TwoState
  else:
    return TwoState

def id_tidier(TwoState, id_match_state):
  if id_match_state ==1:
    # when IDmatch passed, remove all but 1 col, saveout
    width_initial = TwoState.shape[1]
    TwoState = TwoState.drop(['ss_intID','ss_intID_L'], axis=1)
    print("Width ::", width_initial, "==> ", TwoState.shape[1])
  return TwoState

def checkINTids(TwoState):
  '''function to check if all int_ids are identical ; returns message to this effect '''
  intChecker = TwoState[['C_id', 'ss_intID']]
  for colName in ['C_id', 'ss_intID']:
    intChecker[colName] = intChecker[colName].apply(pd.to_numeric, errors = 'coerce')
    intChecker[colName] = intChecker[colName].fillna(0).astype("int64")  

  intCheckerD = intChecker.dropna()
  a = intCheckerD.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("ID match error")
    id_match_state = 0
  else:
    print("ID match success")
    id_match_state = 1
  return TwoState, id_match_state

def Id_value_errorGetter(TwoState, id_match_state):
  if id_match_state !=1:
    intChecker = TwoState[['C_id', 'ss_intID']]
    for colName in ['C_id', 'ss_intID']:
      intChecker[colName] = intChecker[colName].apply(pd.to_numeric, errors = 'coerce')
      intChecker[colName] = intChecker[colName].fillna(0).astype("int64")  

    # F_CheckerD = F_Checker.dropna()
    # F_CheckerD = F_CheckerD.reset_index(drop=True)
    a = intChecker.values
    b = (a == a[:, [0]]).all(axis=1)
    AllErrors = np.where(b==False)
    errorList = pd.DataFrame([AllErrors[0][i] for i in range(len(AllErrors[0]))])
    ID_Errors = pd.DataFrame(dtype="object")
    for i in range(len(errorList)):
      thisErrorIndex = errorList.iloc[i,0]
      thisError = pd.DataFrame(TwoState.iloc[thisErrorIndex,:], dtype="object")
      thisError = thisError.T 
      ID_Errors = pd.concat([ID_Errors, thisError], axis=0)
    return ID_Errors 
    
def f_value_errorGetter(TwoState, F_match_state):
  if F_match_state !=1:
    F_Checker = TwoState[['F_103', 'Forme', ]]
    F_Checker['F_103'] = ["" if F_Checker.iloc[i,0] == "zzkz" else F_Checker.iloc[i,0]  for i in range(len(F_Checker))]
    F_Checker['Forme'] = ["" if F_Checker.iloc[i,1] is np.nan  else F_Checker.iloc[i,1]  for i in range(len(F_Checker))]
    
    # F_CheckerD = F_Checker.dropna()
    # F_CheckerD = F_CheckerD.reset_index(drop=True)
    a = F_CheckerD.values
    b = (a == a[:, [0]]).all(axis=1)
    F_match_state = 0
    # find errors:: , then view these rows in stanza_combined
    AllErrors = np.where(b==False)
    errorList = pd.DataFrame([AllErrors[0][i] for i in range(len(AllErrors[0]))])
    F_Errors = pd.DataFrame(dtype="object")
    for i in range(len(errorList)):
      thisError = pd.DataFrame(F_CheckerD.iloc[errorList.iloc[i,0]:(errorList.iloc[i,0] +1)], dtype="object")
      F_Errors = pd.concat([F_Errors, thisError], axis=0)
    return F_Errors 

def tidyColumns(TwoState):
  ## tidy final colnames :
  # change names to generic : colset1 = cols WITH lgerm ; colset2 - WITHOUT lgerm
  colSet = ['F_103','C_id','C_idOrig', 'ssid', 'UUID', 'C_UPOStag', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'C_LEMME','C_HEAD', 'H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293',  'C_DEPREL','R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293']
  TwoState = TwoState[colSet]
  return TwoState

#### end of defintiions

# general declarations : L = live == LGERM texts ; C = textesde contrôle SRCMF/Democrat
LGERM_columNamesL = ['TokenID', 'F_103', 'P_103', 'L_103', 'code_103', 'known', 'wordid', 'Page', 'Para',   'TokenChap', 'ss_intID_L', 'ssid', 'FormeNoP', 'UUID_L']
LGERM_col_orderL = ['F_103', 'P_103', 'L_103', 'code_103',  'Page', 'ss_intID_L', 'ssid', 'UUID_L']
## note : C_FORME changed to F_103 to use existing functions
LGERM_columNamesC = ['C_id', 'F_103', 'C_LEMME', 'C_UPOStag', 'C_XPOStag', 'C_HEAD',  'C_DEPREL', 'C_Feats', 'UUID_L', 'C_DEPS', 'ss_intID_L', 'ssid', 'C_idOrig']

LGERM_col_orderC = ['F_103', 'C_LEMME', 'C_UPOStag',  'C_HEAD',  'C_DEPREL', 'UUID_L',  'ss_intID_L', 'ssid','C_id',  'C_idOrig']

# file declarations
inputFiles = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/temp/103/*099.csv'))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)

errors: AF07XX file error : recreate with correct data
FP02, CA02 id error, CA03,CA04
i=8-CA, i=15
for i in range(len(inputFiles)):

i=i+1
LGERM_file = inputFiles.iloc[i,0]
LGERM_columNames = LGERM_columNamesC
LGERM_col_order = LGERM_col_orderC
HOPS_file = LGERM_file.replace('103','2XX').replace('099.','2XX.')
FinalSaveName = LGERM_file.replace('/103','').replace('-099','-state2')
  # LGERM_data.to_csv(LGERM_file, sep="\t", index=False)
## get, tidy LGERM data
LGERM_data = pd.read_csv(LGERM_file, sep="\t", low_memory=False)
LGERM_data.columns = LGERM_columNames
LGERM_data = LGERM_data[LGERM_col_order]
dropMe = [i  for i in range(len(LGERM_data)) if LGERM_data.iloc[i,0]=="zzkz" ]
LGERM_data = LGERM_data.drop(dropMe)
LGERM_data = LGERM_data.reset_index(drop=True)
# load 2XX file, merge on UUID, left to preserve sent boundaries, etc
HOPS_data = pd.read_csv(HOPS_file, sep="\t", low_memory=False)
## dealing with diff len UUIDs

if len(HOPS_data) == len(LGERM_data):
  TwoState = pd.concat([LGERM_data, HOPS_data], axis=1)
  print("Successfully bound")
# test ss_intID values
TwoState, id_match_state = checkINTids(TwoState)
##  option to send values from ss_intID to ss_intID_L
# TwoState['ss_intID_L'] = TwoState['ss_intID']
TwoState = id_tidier(TwoState, id_match_state)
# id_errors=  Id_value_errorGetter(TwoState, id_match_state)
## test forme, F-103
TwoState, F_match_state,  = checkFvalues(TwoState)
if F_match_state==0:
  F_Errors = f_value_errorGetter(TwoState, F_match_state)
  print(F_Errors)
  # break
  # F_match_state = 1
else:
  # tidy, order, export
  TwoState = F_tidier(TwoState, F_match_state)
# TwoState.columns = ['F_103', 'C_LEMME', 'C_UPOStag', 'C_HEAD', 'C_DEPREL', 'UUID_L',
#        'ss_intID_L', 'ssid', 'C_id', 'C_idOrig', 'UUID', 'P_232', 'P_241',
#        'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272',
#        'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'H_232',
#        'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271',
#        'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293',
#        'R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263',
#        'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292',
#        'R_293']
  ## UUID_processing
  UUID_equivTable = TwoState[['UUID', 'UUID_L']]
  UUID_equivTable.to_csv(FinalSaveName.replace('state2','equiv'), sep="\t", index=False)
  TwoState = TwoState.drop(['UUID_L'], axis=1)
  
  TwoState = tidyColumns(TwoState)
  TwoState.to_csv(FinalSaveName, sep="\t", index=False)
  FinalSaveName

```

TwoState -> Three state :: add data using DEUC binder
```{python}

## CA9,16 give errors but can still be left bound
inputFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/_TwoState/*.csv"))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)
## works for Live files, not CA/CM files
# i=1
two_state_file = inputFiles.iloc[i,0]
three_state_file =  two_state_file.replace('state2','state3').replace('_Two',"_Three")
RNN_intputFile = two_state_file.replace('_TwoState','9XX').replace('state2','9XX')
DeucInputFile= two_state_file.replace('_TwoState','3XX').replace('state2','3XX')
two_state_data = pd.read_csv(two_state_file, sep="\t", low_memory=False)
DeucData = pd.read_csv(DeucInputFile, sep="\t", low_memory=False)
if len(DeucData) == len(two_state_data.loc[two_state_data['F_103']!= 'zzkz']):
  print(two_state_file[-15:-11]+ ":: Lengths match, adding DEUC via method1", flush=True)
  holding = pd.merge(two_state_data, DeucData, how='left', on='UUID')
  holding = holding.drop(['Forme'], axis=1)
  RNN_data = pd.read_csv(RNN_intputFile, sep="\t", low_memory=False)
  threeStateData = pd.merge(holding, RNN_data, how="left", on="UUID")
  print(two_state_file[-15:-11]+ ":: Lengths match, adding RNN via method1", flush=True)
  threeStateData.to_csv(three_state_file, sep="\t", index=False)
else:
  ## CA01 is special case
  if (two_state_file[-15:-13] in ['CA','CM']): 
    if (len(DeucData) == len(two_state_data)):
      print(two_state_file[-15:-11]+ ":: Lengths match, binding via method2", flush=True)
      DeucData.columns = ['Forme_D', 'L_311', 'P_311', 'UUID_D', 'L_321', 'P_321', 'L_331', 'P_331']
      holding = pd.concat([two_state_data,DeucData], axis=1)  
      holding = holding.drop(['Forme_D','UUID_D'], axis=1)
      RNN_data = pd.read_csv(RNN_intputFile, sep="\t", low_memory=False)
      threeStateData = pd.merge(holding, RNN_data, how="left", on="UUID")
      # RNN_data['UUIDr'] = RNN_data['UUID'].str.replace('_','-0')
      # threeStateData = pd.merge(holding, RNN_data, how="left", left_on="UUID", right_on="UUIDr")
    # threeStateData = pd.merge(holding, RNN_data, how="left", left_on="UUID_x", right_on="UUID")
      threeStateData.to_csv(three_state_file, sep="\t", index=False)
      print(two_state_file[-15:-11]+ ":: Lengths match, adding RNN via method2",flush=True) ## for CA, CM other than CA01
    if (len(DeucData.loc[DeucData['Forme']!= 'zzkz']) == len(two_state_data)):
      print(two_state_file[-15:-11]+ ":: Lengths match, binding via method3", flush=True)
      holding = pd.merge(two_state_data, DeucData, how="left", on='UUID')
      holding = holding.drop(['Forme'], axis=1)
      RNN_data = pd.read_csv(RNN_intputFile, sep="\t", low_memory=False)
      threeStateData = pd.merge(holding, RNN_data, how="left", on="UUID")
      print(two_state_file[-15:-11]+ ":: Lengths match, adding RNN via method3", flush=True)
      threeStateData.to_csv(three_state_file, sep="\t", index=False)
    else:
      print("ERROR : " + two_state_file[-15:], flush=True)

  else:
    print("ERROR : " + two_state_file[-15:], flush=True)

View(threeStateData)
holding.columns.
###
# holdingSample = holding[['F_103', 'UUID_x', 'UUID_y', 'UUID_L', 'UUID', 'ss_intID']]
###
all_results = pd.DataFrame(dtype="object")
for i in range(len(inputFiles)):
  TwoStateFile = inputFiles.iloc[i,0]
  LeftLengthIn = len(pd.read_csv(TwoStateFile, sep="\t", low_memory=False))
  LeftLengthOut = len(pd.read_csv(TwoStateFile.replace('state2','state3').replace("Two","Three"), sep="\t", low_memory=False))
  FileCode = TwoStateFile[-15:-11]
  results = pd.DataFrame( [ {'File':FileCode,'LeftLengthIn':LeftLengthIn, 'LeftLengthOut':LeftLengthOut }])
  all_results = pd.concat([all_results, results], axis=0)
all_results['status'] = [1 if all_results.iloc[i,2] == all_results.iloc[i,1]  else 0 for i in range(len(all_results))]
all_results['status'].sum()

```

Three state -> Four state :: add files using Stanza input : 6XX, 77X, 703, 801

last sent missing a few words from Stanza output for some texts::
MF03
:: do ignoring correspondence between internalIDs, can do 1 homogenise later


### variant for Live Texts : has ssid col
```{python}
ChallengeData = StanzaData
def CheckFormsAlign(sample, ChallengeData, specialList, k):
  '''Function to check if F_103 is equal to form in challenge set, col k '''
  sample_bound = pd.merge(sample, ChallengeData, how="left", on="UUID")
  sample_bound['check'] = [1 if sample_bound.iloc[i,0] == sample_bound.iloc[i,k] else (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
  FormAlignmentErrors = sample_bound.loc[sample_bound['check'] == 0]
  print(len(FormAlignmentErrors))
  return sample_bound, FormAlignmentErrors

specialList = ['zzkz','.', ',', ';', ':', '»', '«', '\"', '\'', '“', '’', '”', '‘', '-', '!', '?','(',')']
###
inputFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/_ThreeState/*.csv"))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)

f=f+1
three_state_file = ''inputFiles.iloc[f,0]
four_state_file =  three_state_file.replace('state3','state4').replace('_Three',"_Four")

StanzaInputFile = three_state_file.replace('_ThreeState','6XX').replace('state3','6XX')
StanzaData = pd.read_csv(StanzaInputFile, sep="\t")
StanzaData.columns = ['ss_intID_6XX', 'Forme', 'P_611',  'P_621', 'UUIDb', 'H_611', 'H_621', 'R_611', 'R_621']
StanzaSample = StanzaData[['ss_intID_6XX', 'Forme',  'UUID']]

SEMinputfile = StanzaInputFile.replace('6XX','703')
SEMData = pd.read_csv(SEMinputfile, sep="\t")
SEMData.columns = ['F_703','UUID','P_703']

CLTKinputfile = StanzaInputFile.replace('6XX','801')
CLTKData = pd.read_csv(CLTKinputfile, sep="\t")
CLTKData.columns = ['F_801', 'L_801', 'P_801', 'i_801', 'R_801', 'H_801', 'ssid_801', 'UUID']

SparkInputFile = StanzaInputFile.replace('6XX','772')
SparkData = pd.read_csv(SparkInputFile, sep="\t")
## if BCD active:
# SparkData = SparkData[['ss_intID', 'Forme', 'UUID', 'chunk', 'lem', 'posA',  'posE', 'posF', 'posG', 'posH', 'posI', 'posJ']]
#        
SparkData.columns = ['ss_intID_77X', 'F_772', 'UUIDb', 'chunk_772', 'L_772', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779']

three_state_data = pd.read_csv(three_state_file, sep="\t", low_memory=False)
sample = three_state_data[['F_103', 'UUID', 'ssid', 'ss_intID_L']]



StanzaSample
## check form alignments
sample_bound, alignmentErrors  = CheckFormsAlign(sample, StanzaData, specialList, 4)
sample_bound, alignmentErrors = CheckFormsAlign(sample, SEMData, specialList, 3)
sample_bound, alignmentErrors = CheckFormsAlign(sample, CLTKData, specialList, 3)
sample_bound, alignmentErrors = CheckFormsAlign(sample, SparkData, specialList, 4)
View(alignmentErrors)

## if correct, or if only punct, bind to real data
three_Stanza = pd.merge(three_state_data, StanzaData, on="UUIDb", how="left")
three_stanza_SEM = pd.merge(three_Stanza, SEMData, how="left", on="UUIDb")
three_stanza_SEM_CLTK = pd.merge(three_stanza_SEM, CLTKData, how="left", on="UUIDb")
three_stanza_SEM_CLTK_Spark = pd.merge(three_stanza_SEM_CLTK, SparkData, how="left", on="UUID")
three_stanza_SEM_CLTK_Spark.to_csv(four_state_file, sep="\t", index=False)

```

# 5040 :  3state to 4 state :: latest
```{python}

def checkStanzaAlignment(StanzaInputFile, specialList):
  StanzaData = pd.read_csv(StanzaInputFile, sep="\t")
  StanzaData.columns = ['ss_intID_6XX', 'Forme', 'P_611',  'P_621', 'UUID', 'H_611', 'H_621', 'R_611', 'R_621']
  StanzaSample = StanzaData[['ss_intID_6XX', 'Forme',  'UUID']]
  StanzaSample = StanzaSample.reset_index(drop=True)
  # get list of rows to remove as they have punct, EOS, spaces…
  myRows = [i for i in range(len(sample)) if sample.iloc[i,0]  in specialList ]
  # remove rows to create sampleReduced
  sampleRed = sample.drop(myRows, axis=0)
  sampleRed = sampleRed.reset_index(drop=True)
  # shapes are either identical, or nigh-on
  print("SampleShape: " + str(sampleRed.shape), flush=True)
  print("StanzaShape: "+ str(StanzaSample.shape), flush=True)
  
  ## find errors in UUID and FORME cols by doing concat
  test1 = pd.concat([sampleRed, StanzaSample], axis=1 )
  # test1.columns
  test1['uuidcheck'] = [1 if test1.iloc[i,1] == test1.iloc[i,6] else 0 for i in range(len(test1))]
  test1['Forme_check'] = [1 if test1.iloc[i,0] == test1.iloc[i,5] else 0 for i in range(len(test1))]
  # test1['IntID_check'] = [1 if int(test1.iloc[i,4]) == int(test1.iloc[i,3]) else 0 for i in range(len(test1))]
  
  print("UUID score = " + str(test1['uuidcheck'].sum() / len(test1)), flush=True)
  print("Forme score = " + str(test1['Forme_check'].sum() / len(test1)), flush=True)
  # print("IntID score = " + str(test1['IntID_check'].sum() / len(test1)), flush=True)
  return test1, sampleRed, StanzaData


def checkSEMalignment(SEMinputfile):
  SEMData = pd.read_csv(SEMinputfile, sep="\t")
  SEMData.columns = ['F_703','UUID','P_703']
  print("SEMshape: " + str(SEMData.shape), flush=True)
  test1 = pd.concat([sampleRed, SEMData], axis=1 )
  test1['uuidcheck'] = [1 if test1.iloc[i,1] == test1.iloc[i,5] else 0 for i in range(len(test1))]
  test1['Forme_check'] = [1 if test1.iloc[i,0] == test1.iloc[i,4] else 0 for i in range(len(test1))]
  print("UUID score = " + str(test1['uuidcheck'].sum() / len(test1)), flush=True)
  print("Forme score = " + str(test1['Forme_check'].sum() / len(test1)), flush=True)
  return test1, SEMData

def  checkCLTKalignment(CLTKinputfile):
  CLTKData = pd.read_csv(CLTKinputfile, sep="\t")
  CLTKData.columns = ['F_801', 'L_801', 'P_801', 'i_801', 'R_801', 'H_801', 'ssid_801', 'UUID']
  print("CLTKshape " + str(CLTKData.shape), flush=True)
  
  test3 = pd.concat([sampleRed, CLTKData], axis=1)
  test3['uuidcheck'] = [1 if test3.iloc[i,1] == test3.iloc[i,11] else 0 for i in range(len(test3))]
  test3['Forme_check'] = [1 if test3.iloc[i,0] == test3.iloc[i,4] else 0 for i in range(len(test3))]
  test3['IntID_check'] = [1 if test3.iloc[i,3] == test3.iloc[i,7] else 0 for i in range(len(test3))]
  
  print("UUID score = " + str(test3['uuidcheck'].sum() / len(test3)), flush=True)
  print("Forme score = " + str(test3['Forme_check'].sum() / len(test3)), flush=True)
  print("IntID score = " + str(test3['IntID_check'].sum() / len(test3)), flush=True)
  return test3, CLTKData

def checkSparkAlignment(SparkInputFile):
  sample = three_state_data[['F_103', 'UUID', 'ssid', 'ss_intID_L']]
  print("SampleShape: " + str(sample.shape), flush=True)
  
  SparkData = pd.read_csv(SparkInputFile, sep="\t")
  
  if SparkData.shape[1]==15:
    SparkData.columns = ['ss_intID', 'Forme', 'UUID', 'chunk', 'lem', 'posA', 'posB', 'posC', 'posD', 'posE', 'posF', 'posG', 'posH', 'posI', 'posJ']
    SparkData = SparkData.drop(['posB', 'posC','posD'], axis=1)
    
  # if n columns = 12
  SparkData.columns = ['ss_intID_77X', 'F_772', 'UUID', 'chunk_772', 'L_772', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779']
  print("SparkShape: " + str(SparkData.shape), flush=True)
  
  SparkDataSample = SparkData[['ss_intID_77X', 'F_772', 'UUID']]
  test4 = pd.concat([sampleRed, SparkDataSample], axis=1)
  # test4.columns
  test4['uuidcheck'] = [1 if test4.iloc[i,1] == test4.iloc[i,6] else 0 for i in range(len(test4))]
  test4['Forme_check'] = [1 if test4.iloc[i,0] == test4.iloc[i,5] else 0 for i in range(len(test4))]
  test4['IntID_check'] = [1 if test4.iloc[i,3] == test4.iloc[i,4] else 0 for i in range(len(test4))]
  
  print("UUID score = " + str(test4['uuidcheck'].sum() / len(test4)), flush=True)
  print("Forme score = " + str(test4['Forme_check'].sum() / len(test4)), flush=True)
  print("IntID score = " + str(test4['IntID_check'].sum() / len(test4)), flush=True)
  return test4, SparkData

#finding errors in 700, 801, 77X , 6XX files == latest version

inputFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/_ThreeState/*.csv"))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)
specialList = ['zzkz','.', ',', ';', ':', '»', '«', '\"', '\'', '“', '’', '”', '‘', '-', '!', '?','(',')']
f=0
f=f+1
three_state_file = inputFiles.iloc[f,0]
three_state_data = pd.read_csv(three_state_file, sep="\t", low_memory=False)
four_state_file =  three_state_file.replace('state3','state4').replace('_Three',"_Four")
StanzaInputFile = three_state_file.replace('_ThreeState','6XX').replace('state3','6XX')
SEMinputfile = StanzaInputFile.replace('6XX','703')
CLTKinputfile = StanzaInputFile.replace('6XX','801')
SparkInputFile = StanzaInputFile.replace('6XX','772')

######## doodle

# 1 get sample from three state data
sample = three_state_data[['F_103', 'UUID_y', 'ssid', 'ss_intID_L']]
three_state_data.columns
## Part A : Check Stanza
test1, sampleRed, StanzaData = checkStanzaAlignment(StanzaInputFile, specialList)
View(test1)
# if correct, can bind on UUIDs
worker1 = pd.merge(sampleRed, StanzaData, how="right", left_on="UUID_x",right_on="UUID")
# StanzaData[['UUID']]
# worker1 = pd.concat([sampleRed, StanzaData], axis=1)
## Part B: Check SEM
test1, SEMData = checkSEMalignment(SEMinputfile)
#Bind SEM to combination of realdatasample + Stanza : with merge
worker2 = pd.merge(worker1, SEMData, how="left", on="UUID")
# worker2 = pd.concat([worker1, SEMData.drop(['UUID'], axis=1)], axis=1)
# View(test1)

## Part C: Check CLTK
test3, CLTKData = checkCLTKalignment(CLTKinputfile)
# on error:
targets = test3.loc[test3['uuidcheck']==0]

View(test3)
## if all correct, can bind
worker3 = pd.merge(worker2, CLTKData, how="left", on="UUID")
# worker3 = pd.concat([worker2, CLTKData.drop(['UUID'], axis=1)], axis=1)


## Part D: Check Spark

test4, SparkData = checkSparkAlignment(SparkInputFile)

View(test4)
worker4 = pd.merge(worker3, SparkData, how="left", left_on="UUID")
worker4 = pd.concat([worker3, SparkData], axis=1)
worker4.iloc[0,1] = "CA08-000002"
## if all correct, can bind
### this gives threestate sample cols and newly aligned data : tidy then add to rest of threestate data
# threeState = threeState.drop(['UUID_y'], axis=1)
worker4.columns = ['F_103', 'UUID', 'ssid', 'ss_intID_L', 'ss_intID_6XX', 'F_6XX', 'P_611',
       'P_621', 'H_611', 'H_621', 'R_611', 'R_621', 'F_703', 'P_703', 'F_801',
       'L_801', 'P_801', 'i_801', 'R_801', 'H_801', 'ssid_801', 'ss_intID_77X',
       'F_772', 'chunk_772', 'L_772', 'P_773', 'P_774', 'P_775', 'P_776',
       'P_777', 'P_778', 'P_779']

four_state_data = pd.merge(three_state_data, worker4, how="left", left_on = ["UUID_y", 'F_103'], right_on = ["UUID_y", 'F_103'])
four_state_data.to_csv(four_state_file, sep="\t", index=False)
## end doodle
View(four_state_data)

three_state_data.columns

```

AF01-703 error @ 47215: skipped word
AF12-801 @ 8258 : ???
FP02-801 @ 2926 P - gentilzhommes
FP11-801 @ 33763 :: -arrièrechambre

MF03 : sic and last sent in 703, 801, 
MF08-801 @ 23717 preudhomme split
MF10-801 @ 03736 & 53647 : gentilz-hommes
MF14-703, MF19 have additional columns
MF19-703, 803 @ 08391, 00914
MF26-801 has - errors at 00969, 1568…

CA01 UUIDs are out by 1 in 6XX
#5041: 3state to 4state for COMP
has no ssid col adn CONCATs SEM while MERGE others
```{python}

def CheckFormsAlign(sample, ChallengeData, specialList, k):
  '''Function to check if F_103 is equal to form in challenge set, col k '''
  sample_bound = pd.merge(sample, ChallengeData, how="left", on="UUID")
  sample_bound['check'] = [1 if sample_bound.iloc[i,0] == sample_bound.iloc[i,k] else (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
  FormAlignmentErrors = sample_bound.loc[sample_bound['check'] == 0]
  print(len(FormAlignmentErrors))
  return sample_bound, FormAlignmentErrors

specialList = ['zzkz','.', ',', ';', ':', '»', '«', '\"', '\'', '“', '’', '”', '‘', '-', '!', '?','(',')']
###
inputFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/have_703_801errors/_ThreeState/*.csv"))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)

f=f+1
three_state_file = inputFiles.iloc[f,0]
four_state_file =  three_state_file.replace('state3','state4').replace('_Three',"_Four")

StanzaInputFile = three_state_file.replace('_ThreeState','6XX').replace('state3','6XX')
StanzaData = pd.read_csv(StanzaInputFile, sep="\t")
StanzaData.columns = ['ss_intID_6XX', 'Forme', 'P_611',  'P_621', 'UUID', 'H_611', 'H_621', 'R_611', 'R_621']

SEMinputfile = StanzaInputFile.replace('6XX','703')
SEMData = pd.read_csv(SEMinputfile, sep="\t")
SEMData.columns = ['F_703','UUID','P_703']

CLTKinputfile = StanzaInputFile.replace('6XX','801')
CLTKData = pd.read_csv(CLTKinputfile, sep="\t")
CLTKData.columns = ['F_801', 'L_801', 'P_801', 'i_801', 'R_801', 'H_801', 'ssid_801', 'UUID']

SparkInputFile = StanzaInputFile.replace('6XX','772')
SparkData = pd.read_csv(SparkInputFile, sep="\t")
SparkData.columns = ['ss_intID_77X', 'F_772', 'UUID', 'chunk_772', 'L_772', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779']

three_state_data = pd.read_csv(three_state_file, sep="\t", low_memory=False)
sample = three_state_data[['F_103', 'UUID']]

StanzaData.shape[0]
SEMData.shape[0]
CLTKData.shape[0]
SparkData.shape[0]

sample_bound, alignmentErrors  = CheckFormsAlign(sample, StanzaData, specialList, 3)
sample_bound, alignmentErrors = CheckFormsAlign(sample, SEMData, specialList, 2)
sample_bound, alignmentErrors = CheckFormsAlign(sample, CLTKData, specialList, 2)
sample_bound, alignmentErrors = CheckFormsAlign(sample, SparkData, specialList, 3)

## if correct, or if only punct, bind to real data
StanzaSEM = pd.concat([StanzaData, SEMData.drop(['UUID'], axis=1)], axis=1)
StanzaSEM_CLTK = pd.merge(StanzaSEM, CLTKData, how="left", on="UUID")
StanzaSEM_CLTK_Spark = pd.merge(StanzaSEM_CLTK, SparkData, how="left", on="UUID")
four_state_data = pd.merge(three_state_data, StanzaSEM_CLTK_Spark, on="UUID", how="left")
four_state_data.to_csv(four_state_file, sep="\t", index=False)

## test relative alignment of stanza and SEM

StanzaSEM['checkS'] = [1 if StanzaSEM.iloc[a,1] == StanzaSEM.iloc[a,9] else 0 for a in range(len(StanzaSEM))]
StanzaSEM.loc[StanzaSEM['checkS'] == 0]
StanzaSEM.columns

```


not 4 to 5
```{python}
def combineTestCheckAlign(sample, ChallengeData, specialList, k):
  '''Function to check if F_103 is equal to form in challenge set, col k '''
  sample_bound = pd.merge(sample, ChallengeData, how="left", on="UUID")
  sample_bound['check'] = [1 if sample_bound.iloc[i,0] == sample_bound.iloc[i,k] else (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
  FormAlignmentErrors = sample_bound.loc[sample_bound['check'] == 0]
  print(len(FormAlignmentErrors))
  return sample_bound, FormAlignmentErrors

specialList = ['zzkz','.', ',', ';', ':', '»', '«', '\"', '\'', '“', '’', '”', '‘', '-', '!', '?','(',')']


# def testIntIDalignment(sample_bound, targetColumn=):
#   ''' ''' 
#   sample_bound['ss_intID_L'] = sample_bound['ss_intID_L'].apply(pd.to_numeric, errors = 'coerce')
#   sample_bound['ss_intID_L'] = sample_bound['ss_intID_L'].fillna(0).astype("int64")
#   sample_bound[targetColumn] = sample_bound[targetColumn].apply(pd.to_numeric, errors = 'coerce')
#   sample_bound[targetColumn] = sample_bound[targetColumn].fillna(0).astype("int64")
#   sample_bound['idcheck'] = [1 if int(sample_bound.iloc[i,3]) == int(sample_bound.iloc[i,7]) else  (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
#   IDalignmentErrors = sample_bound.loc[sample_bound['idcheck'] == 0]
#   print(len(IDalignmentErrors))
#   return sample_bound, IDalignmentErrors

def prepareFourStateExport(three_state_data, StanzaData, CLTKData, SparkData, SEMData, four_state_file):
## load real data
  live_data = pd.merge(three_state_data, StanzaData, how="left", on="UUID")
  live_data = pd.merge(live_data, CLTKData, how="left", on="UUID")
  live_data = pd.merge(live_data, SparkData, how="left", on="UUID")
  live_data = pd.merge(live_data, SEMData, how="left", on="UUID")
  live_data = live_data.drop(['ssid_x','Forme_x','ss_intID','Forme_y','ssid_y', 'ss_intID_772','F_703','chunk_772', 'F_801', 'i_801','F_772'], axis=1)
  live_data = live_data[['F_103', 'ss_intID_L', 'UUID', 'P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293','P_311','P_321','P_331', 'P_801', 'P_912', 'P_922', 'P_611', 'P_621','P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'L_103','L_311', 'L_321', 'L_331', 'L_772', 'L_801', 'L_912', 'L_922', 'H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_621','H_801', 'R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_611', 'R_621', 'R_801', 'Page' ]]
  live_data.to_csv(four_state_file, sep="\t", index=False)
### if length of errors for all is 0, then all correctly aligned, so can bind to read data and export.
  return live_data


###
inputFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/_ThreeState/*.csv"))
inputFiles = inputFiles.sort_values(0)
inputFiles = inputFiles.reset_index(drop=True)

f=f-1
three_state_file = inputFiles.iloc[f,0]
four_state_file =  three_state_file.replace('state3','state4').replace('_Three',"_Four")
StanzaInputFile = three_state_file.replace('_ThreeState','6XX').replace('state3','6XX')
SEMinputfile = StanzaInputFile.replace('6XX','703')
CLTKinputfile = StanzaInputFile.replace('6XX','801')
SparkInputFile = StanzaInputFile.replace('6XX','772')

# get useful columns to make comparisons easy
three_state_data = pd.read_csv(three_state_file, sep="\t", low_memory=False)
sample = three_state_data[['F_103', 'UUID', 'ssid', 'ss_intID_L']]

## add Stanza,  Stanza FORME alignment
StanzaData = pd.read_csv(StanzaInputFile, sep="\t")
# chck Form alignment,# chck ID alignment
sample_bound, alignmentErrors  = combineTestCheckAlign(sample, StanzaData, specialList, 5)
sample_bound, IDalignmentErrors = testIntIDalignment(sample_bound, 'ss_intID')

## loop of ifs to see if all agree : then run auto, else, run one at a time with special options as below
if ((len(IDalignmentErrors) ==0) & (len(alignmentErrors) ==0)):
# add SEM, check Form alignment
  SEMData = pd.read_csv(SEMinputfile, sep="\t")
  SEMData.columns = ['F_703','UUID','P_703']
  alignmentErrors, sample_bound = combineTestCheckAlign(sample, SEMData, specialList, 4)
  if len(IDalignmentErrors) ==0:
    ## add CLTK, check alignments
    CLTKData = pd.read_csv(CLTKinputfile, sep="\t")
    CLTKData.columns = ['F_801', 'L_801', 'P_801', 'i_801', 'R_801', 'H_801', 'ssid', 'UUID']
    sample_bound, alignmentErrors,  = combineTestCheckAlign(sample, CLTKData, specialList, 4)
    sample_bound, IDalignmentErrors = testIntIDalignment(sample_bound, 'i_801')

    if ((len(IDalignmentErrors) ==0) & (len(alignmentErrors) ==0)):
      ## add Spark, check
      SparkData = pd.read_csv(SparkInputFile, sep="\t")
      SparkData.columns = ['ss_intID_772', 'F_772', 'UUID', 'chunk_772', 'L_772', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779']
      sample_bound, alignmentErrors = combineTestCheckAlign(sample, SparkData, specialList, 5)
      if len(alignmentErrors) ==0:
        live_data = prepareFourStateExport(three_state_data, StanzaData, CLTKData, SparkData, SEMData, four_state_file)
        print("Printing ",four_state_file, flush=True)

del(three_state_data, SparkData, CLTKData, live_data, sample_bound)

###### if the above loop doesn't autoprocess the texts: bind one at a time conserving necesary info:

StanzaData.columns = ['ss_intID_6XX', 'Forme', 'P_611', 'P_621', 'UUID', 'H_611', 'H_621', 'R_611', 'R_621']
three_stanza = pd.merge(three_state_data, StanzaData, how="left", on="UUID")

three_stanza_SEM = pd.merge(three_stanza, SEMData, how="left", on="UUID")
three_stanza_SEM_CLTK = pd.merge(three_stanza_SEM, CLTKData, how="left", on="UUID")
three_stanza_SEM_CLTK_Spark = pd.merge(three_stanza_SEM_CLTK, SparkData, how="left", on="UUID")

three_stanza_SEM_CLTK_Spark = three_stanza_SEM_CLTK_Spark[['F_103', 'ss_intID_L', 'UUID', 'ssid_x', 'P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293','P_311', 'P_321','P_331','P_611', 'P_621','P_912',  'P_922', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801',  'L_103','L_311','L_321',  'L_331',   'L_772', 'L_801','L_912','L_922' ,'H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_621','H_801', 'R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293',  'R_611', 'R_621','R_801', 
 'Page',  'ss_intID_6XX', 'Forme_x','Forme_y', 'F_703', 'F_801', 'i_801','ssid_y', 'F_772', 'chunk_772','ss_intID_772']]

four_state = three_stanza_SEM_CLTK_Spark.drop(['Forme_x','Forme_y', 'F_703', 'F_801', 'i_801','ssid_y', 'F_772', 'chunk_772','ss_intID_772'], axis=1) 
four_state.to_csv(four_state_file, sep="\t", index=False)

```


```{python}
StanzaData = pd.read_csv(StanzaInputFile, sep="\t", low_memory=False)
StanzaData.columns

len(StanzaData) == len(three_state_data.loc[three_state_data['F_103']!="zzkz"])
## check ss_intID, Formes match
holding = pd.merge(three_state_data, StanzaData, how="left", on="UUID")
holding.shape

three_state_data.shape
StanzaData.shape

CLTKdata = pd.read_csv(CLTKinputfile, sep="\t")
CLTKdata.columns = ['F_801', 'L_801', 'P_801', 'i', 'R_801', 'H_801', 'ssid', 'UUID']

holding = pd.merge(holding, CLTKdata, how="left", on="UUID")
holding.shape
three_state_data.shape
CLTKdata.shape

sparkdata = pd.read_csv(SparkInputFile, sep="\t")
holding = pd.merge(holding, sparkdata, how="left", on="UUID")


holding.columns

holding.shape
three_state_data.shape
CLTKdata.shape
FormesCols = ['F_103', 'ss_intID_L', 'UUID', 'ssid_x',  'Forme_x','Forme_y', 'F_801',  'Forme', 'chunk', 'F_703']

POS_cols = ['F_103', 'ss_intID_L', 'UUID', 'ssid_x', 'P_103', 'P_232', 'P_241',  'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_331', 'P_912', 'P_922',  'P_611', 'P_621',  'P_801',  'posA', 'posE', 'posF', 'posG', 'posH', 'posI', 'posJ',  'P_703']

LEM_cols = ['F_103', 'ss_intID_L', 'UUID',  'L_103', 'L_311',   'L_321',   'L_331',  'L_912', 'L_922', 'L_801', 'lem']
       
HEADcols = ['F_103', 'ss_intID_L', 'UUID', 'ssid_x',  'H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_621',  'H_801']

RELcols = ['F_103', 'ss_intID_L', 'UUID', 'ssid_x',  'R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293',  'R_611', 'R_621',  'R_801']

IDcols = ['F_103', 'ss_intID_L', 'UUID', 'ssid_x', 'ss_intID_x',
       'i',  'ssid_y', 'ss_intID_y']

Forme_data = holding[FormesCols]
LEM_data = holding[LEM_cols]
POS_data = holding[POS_cols]
HEAD_data = holding[HEADcols]
REL_data = holding[RELcols]
IDdata = holding[IDcols]

```

missingSentAdded

```{python}

inputFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/801/*801.csv"))
i=i+1
currentFile = inputFiles.iloc[i,0]
extraFile = currentFile.replace('801.csv', '801s1.csv')

Data = pd.read_csv(currentFile, sep="\t", low_memory=False)
firstSentence = pd.read_csv(extraFile, sep="\t", low_memory=False)
print(Data.head())

correctData = pd.concat([firstSentence, Data], axis=0)
correctData =  correctData.reset_index(drop=True)
correctData.to_csv(currentFile, sep="\t", index=False)

```


# 5050 4state to 5 state : livedata
add 4XX, 51X, 58X to complete binds for LIVE data
-- last sentence missing AF05, 
```{python}
def combineTestCheckAlign(sample, ChallengeData, specialList, k):
  '''Function to check if F_103 is equal to form in challenge set, col k '''
  sample_bound = pd.merge(sample, ChallengeData, how="left", left_on="UUIDb", right_on="UUIDb")
  sample_bound['check'] = [1 if sample_bound.iloc[i,0] == sample_bound.iloc[i,k] else (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
  FormAlignmentErrors = sample_bound.loc[sample_bound['check'] == 0]
  print(len(FormAlignmentErrors))
  return sample_bound, FormAlignmentErrors

specialList = ['zzkz','.', ',', ';', ':', '»', '«', '\"', '\'', '“', '’', '”', '‘', '-', '!', '?','(',')']
theseFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/_FourState/*.csv"))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)
i=i+1
## get names of files
fourStateFile = four_state_file =
# fourStateFile = theseFiles.iloc[i,0]
TT_file = fourStateFile.replace('_FourState','403').replace('state4','403')
UDpipe_R_file = fourStateFile.replace('_FourState','58X').replace('state4','58X')
UDpipe_URL_file = fourStateFile.replace('_FourState','51X').replace('state4','51X')
five_state_name = fourStateFile.replace('Four','Five').replace('4.csv', '5.csv')

fourStateData = pd.read_csv(fourStateFile, sep="\t", low_memory=False)
sampleData = fourStateData[['F_103', 'ss_intID_L', 'UUIDb']]
# fourStateData.columns[40:40]
TT_data = pd.read_csv(TT_file, sep="\t")
TT_data.columns = ['UUIDb', 'F_402', 'P_402', 'L_402']
sample_bound, FormAlignmentErrors = combineTestCheckAlign(sampleData, TT_data, specialList, 3)
# prints number of alignment errors
View(sample_bound[['F_103','UUIDb']].to_csv("/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/UUIDmatche2r.csv", sep="\t", index=False))
####
# worker1 = pd.merge(fourStateData, TT_data, how="left", left_on="UUID_x", right_on="UUID_x")
matche2r = pd.read_csv('/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/UUIDmatche2r.csv', sep="\t")
matche2r.dropna()
test2 = pd.concat([matcher,], axis=1)
intermediate = pd.merge(UDpipeR_data, matcher, how="left", left_on="UUIDb", right_on="UUIDb")
View(intermediate[['Forme','UUIDb','CorrUUIDc']])
test = pd.concat([matche2r, UDpipeRsample], axis=1)
, how="inner", left_on="CorrUUIDc", right_on="UUIDb")
View(test)

test.columns = ['F_103', 'UUID_103', 'F_581', 'UUID_56789']
test.to_csv("/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/real_matcher.csv", sep="\t", index=False)
View(UDpipeRsample) = UDpipeRsample.drop(914) # 400, 915
UDpipeRsample = UDpipeRsample.reset_index(drop=True)

UDpipeRsample = UDpipeR_data[['Forme','UUIDb']]
UDpipeRsample.columns = ['F_581','581_UUIDb']
intermediate = pd.merge(test, UDpipeR_data, how="left", left_on="UUID_56789", right_on="UUIDb")
View(successquestion)
# need col callen uuidb with 103ids
intermediate.columns
# fourStateData_TT_UDpipeR = pd.merge(fourStateData_TT,intermediate, how="right", left_on="UUIDb", right_on="UUID_103")
##
get UDR data
####
UDpipeR_data = pd.read_csv(UDpipe_R_file, sep="\t", low_memory=False)
sample_bound, FormAlignmentErrors = combineTestCheckAlign(sampleData, UDpipeR_data, specialList, 4)
# worker2 = pd.merge(worker1 , UDpipeR_data, how="left", left_on="UUID_x", right_on="UUID")
UDpipeR_data.columns


UDpipeURL_data = pd.read_csv(UDpipe_URL_file, sep="\t", low_memory=False)
sample_bound, FormAlignmentErrors = combineTestCheckAlign(sampleData, UDpipeURL_data, specialList, 4)
UDpipeURL_data.columns = ['ss_intID', 'F_51X', 'UUID', 'P_511', 'P_512', 'P_513', 'P_514',
       'P_515', 'P_516', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516',
       'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'R_511', 'R_512',
       'R_513', 'R_514', 'R_515', 'R_516']

worker3 = pd.concat([worker2, UDpipeURL_data], axis=1)
worker3['test'] = [1 if worker3.iloc[i,0] == worker3.iloc[i,141] else 0 for i in range(len(worker3))]
# worker3.columns[-26:-25]

# result = worker3[['test','Forme', 'F_51X']]
# View(result)
# r=0
# UDpipeURL_data['UUIDc'] = ['AF06-'+ str(int(UDpipeURL_data.iloc[r,2][-5:]) - 53651).zfill(5) for r in range(len(UDpipeURL_data))]

# sample_bound = pd.merge(sampleData, UDpipeURL_data, how="left", left_on="UUID", right_on='UUIDc')
# View(sample_bound)
# sample_bound['check'] = [1 if sample_bound.iloc[i,0] == sample_bound.iloc[i,k] else (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
# if sample_bound['check'].sum() ==  len(sample_bound['check']), then can export

fourStateData_TT = pd.merge(fourStateData, TT_data.drop(['F_402'], axis=1), how="left", on="UUIDb") 
fourStateData_TT_UDpipeR = pd.merge(fourStateData_TT, UDpipeR_data.drop(['Forme'], axis=1), how="left", on="UUID") 
five_state = pd.merge(fourStateData_TT_UDpipeR, UDpipeURL_data.drop(['Forme'], axis=1), how="left", on="UUID")
worker3.to_csv(five_state_name, sep="\t", index=False)

fivecols = list(five_state.columns)

######
  myRows = [i for i in range(len(sampleData)) if sampleData.iloc[i,0]  in specialList ]
  # remove rows to create sampleReduced
  sampleDataRed = sampleData.drop(myRows, axis=0)
  sampleDataRed = sampleDataRed.reset_index(drop=True)

  TT_rows =[i for i in range(len(TT_data)) if TT_data.iloc[i,1] in specialList]
  TT_red = TT_data.drop(TT_rows, axis=0)
  TT_red = TT_red.reset_index(drop=True)

worker1 = pd.concat([sampleDataRed, TT_red.drop(['UUID'], axis=1)], axis=1)

worker1['formetest'] = [1 if worker1.iloc[i,0] == worker1.iloc[i,3] else 0 for i in range(len(worker1))]
View(worker1)
worker1 = worker1.drop(['F_402', 'formetest'], axis=1)
worker1['formetest'].sum()
# worker1 done : bind using UUID

UDpipeR_data = UDpipeR_data.drop(['Forme','UUID'], axis=1)
worker2 = pd.concat([sampleData, UDpipeR_data], axis=1)
worker2['formetest'] = [1 if worker2.iloc[i,0] == worker2.iloc[i,3] else 0 for i in range(len(worker2))]
worker2['formetest'].sum()/ len(worker2['formetest'])
worker2.columns
View(worker2)
worker2 = worker2.drop(['F_402', 'formetest'], axis=1)

# worker2 done
# 58X + 403
worker3 = pd.merge(worker2, worker1, how="left", on=["UUID", 'F_103', 'ss_intID_L']) 
# 581+403 + 51X
worker4 = pd.merge(worker3, UDpipeURL_data, how="left", on=["UUID"])
worker4

fiveStateData= pd.merge(fourStateData, worker4, how="left", on=["UUID", "F_103"])
fiveStateData.to_csv(five_state_name, sep="\t", index=False)


```
wahoo, have all data aligned for ≈ half corpus


## add 4XX, 51X, 58X to complete binds for COMPARISONcorpus data
-- last sentence missing AF05, 
```{python}
def combineTestCheckAlign(sample, ChallengeData, specialList, k):
  '''Function to check if F_103 is equal to form in challenge set, col k '''
  sample_bound = pd.merge(sample, ChallengeData, how="left", on="UUID")
  sample_bound['check'] = [1 if sample_bound.iloc[i,0] == sample_bound.iloc[i,k] else (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
  FormAlignmentErrors = sample_bound.loc[sample_bound['check'] == 0]
  print(len(FormAlignmentErrors))
  return sample_bound, FormAlignmentErrors

specialList = ['zzkz','.', ',', ';', ':', '»', '«', '\"', '\'', '“', '’', '”', '‘', '-', '!', '?','(',')']
theseFiles = pd.DataFrame(glob.glob("/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/_FourState/*.csv"))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)

i=i+1
fourStateFile = theseFiles.iloc[1,0]
TT_file = fourStateFile.replace('_FourState','403').replace('state4','403')
UDpipe_R_file = fourStateFile.replace('_FourState','58X').replace('state4','58X')
UDpipe_URL_file = fourStateFile.replace('_FourState','51X').replace('state4','51X')
five_state_name = fourStateFile.replace('Four','Five').replace('4.csv', '5.csv')

fourStateData = pd.read_csv(fourStateFile, sep="\t", low_memory=False)
sampleData = fourStateData[['F_103', 'C_id', 'UUID']]

## get UDPipeURL, drop
UDpipeURL_data = pd.read_csv(UDpipe_URL_file, sep="\t", low_memory=False)
UDpipeURL_data = UDpipeURL_data.dropna(subset=["Forme"])

UDpipeURL_data = UDpipeURL_data.reset_index(drop=True)
## get UDpipeR, specify subtype of UUID
UDpipeR_data = pd.read_csv(UDpipe_R_file, sep="\t", low_memory=False)
UDpipeR_data.columns= [ 'int581', 'Forme_581', 'L_581', 'P_581', 'H_581', 'R_581', 'UUID_r', 'ssid', 'int582', 'L_582', 'P_582', 'H_582', 'R_582', 'int583', 'L_583', 'P_583', 'H_583', 'R_583', 'int584', 'L_584', 'P_584', 'H_584', 'R_584',   'int585', 'L_585', 'P_585', 'H_585', 'R_585']

TT_data = pd.read_csv(TT_file, sep="\t")
TT_data.columns = ['UUID_r', 'F_402', 'P_402', 'L_402']


if UDpipeR_data.shape[0] == UDpipeURL_data.shape[0] == sampleData.shape[0]:
  PipeData = pd.concat([UDpipeURL_data, UDpipeR_data], axis=1)
  PipeDataTT = pd.merge(PipeData, TT_data, how="left", on="UUID_r")
  PipeDataTTchecker = PipeDataTT[['Forme', 'F_402', 'Forme_581','UUID','UUID_r']]
  
  PipeDataTTchecker['check'] = [1 if ((((PipeDataTTchecker.iloc[a,0]) == (PipeDataTTchecker.iloc[a,1]))) & (((PipeDataTTchecker.iloc[a,0]) == (PipeDataTTchecker.iloc[a,2])) )) else 0 for a in range(len(PipeDataTTchecker))]
  
  possibleErrors = PipeDataTTchecker[PipeDataTTchecker['check'] == 0].dropna(subset=["F_402"])
  # if length of possErrors is 0, forms align, so all good
  if len(possibleErrors)==0:
    five_state = pd.merge(fourStateData, PipeDataTT, how="left", left_on="UUID", right_on='UUID')
    five_state.to_csv(five_state_name, sep="\t", index=False)
    print(five_state_name, flush=True)




CA16 putaside as 50x discrepancies in length… & no CID

View(sample_bound)
sample_bound['check'] = [1 if sample_bound.iloc[i,0] == sample_bound.iloc[i,k] else (1 if sample_bound.iloc[i,0] in specialList else 0) for i in range(len(sample_bound))]
if sample_bound['check'].sum() ==  len(sample_bound['check']), then can export

## tt has same problem SEM does
TT_data = pd.read_csv(TT_file, sep="\t")
TT_data.columns = ['UUIDr', 'F_402', 'P_402', 'L_402']
# homogenise UUIDs
TT_data['UUID'] = [TT_data.iloc[r,0].replace('_','-0') for r in range(len(TT_data))]
sample_bound, FormAlignmentErrors = combineTestCheckAlign(sampleData, TT_data, specialList, 2)





fourStateData_TT = pd.merge(fourStateData, TT_data.drop(['F_402'], axis=1), how="left", on="UUID") 
fourStateData_TT_UDpipeR = pd.merge(fourStateData_TT, UDpipeR_data.drop(['Forme'], axis=1), how="left", on="UUID") 
five_state = pd.merge(fourStateData_TT_UDpipeR, UDpipeURL_data.drop(['Forme'], axis=1), how="left", left_on="UUID", right_on='UUIDc')
five_state.to_csv(five_state_name, sep="\t", index=False)

five_state.columns


#####
fourStateData.columns

## 4state sample + udpipeURL sample
fourStateData = fourStateData [['F_103', 'ss_intID_L', 'UUID']]
UDpipeURL_dataSample = UDpipeURL_data[['ss_intID', 'Forme', 'UUID']]
UDpipeURL_data.columns = ['ss_intID', 'Forme', 'UUIDr', 'P_511', 'P_512', 'P_513', 'P_514',
       'P_515', 'P_516', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516',
       'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'R_511', 'R_512',
       'R_513', 'R_514', 'R_515', 'R_516']
worker1 = pd.concat([fourStateData, UDpipeURL_data], axis=1)
# worker1['formecheck'] = [1 if worker1.iloc[i,0] == worker1.iloc[i,4] else 0 for i in range(len(worker1)) ]
# worker1['formecheck'].sum() /len(worker1['formecheck'])
worker1.columns = ['F_103', 'ss_intID_L', 'UUID', 'ss_intID', 'Forme', 'UUIDr',
       'formecheck']
View(worker2)


UDpipeR_dataSample = UDpipeR_data[['int581', 'Forme_581', 'UUID_r']]
worker3 = pd.merge(worker1, UDpipeR_data, how="left", left_on="UUID", right_on="UUID_r")
"UUID_r" in worker3.columns

worker3['formecheck'] = [1 if worker3.iloc[i,0] == worker3.iloc[i,8] else 0 for i in range(len(worker3)) ]
worker3['formecheck'].sum()/len(worker3['formecheck'])

worker5 = pd.merge(worker3, TT_data, how="left", left_on="UUID", right_on="UUIDr")
worker5['formecheck'] = [1 if worker5.iloc[i,0] == worker5.iloc[i,11] else 0 for i in range(len(worker5)) ]
worker5['formecheck'].sum() /len(worker5['formecheck'])

View(worker5)

```


## 5060 add 850file

```{python}
thisCode = "CA09"
udspacyfile = f'/Users/username/PhraseoRoChe/Pipeline/refresh/zzReadyArchive/850/{thisCode}-850.csv'
five_state_file = f'/Volumes/AlphaThree/Binding/newest/_FiveState/{thisCode}-state5.csv'

udspacy_data = pd.read_csv(udspacyfile, sep="\t")
udspacy_data.columns = ['UUID', 'F_850', 'L_850', 'P_850', 'supp', 'ss_intID_850', 'R_850', 'H_850', 'ssid']
udspacy_data.columns = [ 'F_850', 'L_850', 'P_850', 'supp', 'ss_intID_850', 'R_850', 'H_850', 'ssid','UUID']

udspacy_data = udspacy_data.drop(['supp'], axis=1)
five_state_Sample = pd.read_csv(five_state_file, sep="\t", usecols=[0,126])
# five_state_Sample.columns[120:]
bound_data = pd.merge(five_state_Sample, udspacy_data, how="left", left_on="UUID_r", right_on="UUID")

result = sum([1  if bound_data.iloc[i,0] == bound_data.iloc[i,2]  else (1 if bound_data.iloc[i,0] == "zzkz" else 0) for i in range(len(bound_data)) ])/len(bound_data)
print(result)
if result> 0.99:
# View(bound_data)

# if works, bind to alldata
  six_state_name = five_state_file.replace('state5','state6').replace('FiveState','SixState')
  five_state_data = pd.read_csv(five_state_file, sep="\t", low_memory=False)
  six_state_data = pd.merge(five_state_data, udspacy_data, how="left", left_on="UUID_r", right_on="UUID")
  six_state_data.to_csv(six_state_name, sep="\t", index=False)
  del(five_state_data, six_state_data)
  



```

GG tidying
```{python}
six_state
six_statecols = list(six_state.columns)
f_checker = six_state[['F_103_x', 'F_103_y', 'F_581', 'F_850', 'Forme_x',  'Forme_y']]
f_checker = f_checker.drop(['F_103_x', 'F_103_y', 'F_581', 'F_850',  'Forme_y'], axis=1)
f_checker.columns = ['F_103',  'drop']
f_checker = f_checker.drop(['drop'], axis=1)
'Forme_y'])
## f cols tidy
ssid_colchecl = six_state[['ssid_x', 'ssid_y', 'ssid']]
ssid_colchecl = ssid_colchecl.drop(['ssid_x', 'ssid_y'], axis=1)
# ssid cols tidy

intIDcols = six_state[['i_801',  'ss_intID_6XX', 'ss_intID_77X', 'ss_intID_850',  'ss_intID_y']]
intIDcols.columns = ['intID_80X', 'intID_6XX' ,'intID_77X',   'intID_85X', 'intID']
View(intIDcols)

[['ss_intID_L', 'ss_intID_6XX', 'ss_intID_77X', 'i_801', 'ss_intID_850']]
[[, '', 'intID_80X', ']


tidy_formData = f_checker
tidy_ssidData = ssid_colchecl
tidy_idcolData = intIDcols
tidyHeadData = six_state[['H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'H_581', 'H_582', 'H_583', 'H_584', 'H_585', 'H_611', 'H_621', 'H_801', 'H_850']] # cols match
tidyLemmeData = six_state[ ['L_103', 'L_311', 'L_321', 'L_331', 'L_402', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516', 'L_581', 'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_912', 'L_922']]

tidyPOSdata = six_state[['P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_402', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_912', 'P_922']]

tidyRelData = six_state[['R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']]

tidy_Pagedata = six_state[['Page']]

View(UUID_data)
UUID_dataTidy = six_state[['UUIDa',  'UUID_103', 'UUID_56789', 'UUID_y']]
UUID_dataTidy.columns = ['UUIDa',  'UUIDb', 'UUIDc', 'UUIDd']
UUID_dataTidy_Out = UUID_dataTidy[['UUIDd']]
UUID_dataTidy_Out.columns = ['UUID']
tidySixState = pd.concat([UUID_dataTidy_Out, tidy_formData, tidyPOSdata, tidy_Pagedata, tidyRelData, tidyHeadData, tidy_idcolData, tidy_ssidData,  tidyLemmeData], axis=1)
tidySixState.to_csv('/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/GG01-state7.csv', sep="\t", index=False)

#UUID_dataTidy, , , , , , , ,  
View(GG_UUIDexplain)
GG_UUIDexplain = pd.concat([UUID_dataTidy, tidy_formData], axis=1)
GG_UUIDexplain.to_csv("/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/UUID_match_master.csv", sep="\t", index=True)
# cols match : 
# tidyHeadData, tidyLemmeData, tidyPOSdata, tidy_Pagedata, tidyRelData, tidy_ssidData, tidy_formData, tidy_idcolData

tidySixState = tidySixState.drop([])

```


# get list of colnames for each file
```{python}
results = pd.DataFrame(dtype="object")
these_files = pd.DataFrame(glob.glob('/Volumes/AlphaThree/Binding/newest/_SixState/*-state6.csv'))

def test_fmatch(currentForme_Data):
  form_data_testing = currentForme_Data
  form_data_testing = form_data_testing.dropna()
  a = form_data_testing.values
  b = (a == a[:, [0]]).all(axis=1)
  if False in b:
    print("F_value match error")
    F_match_state = 0
  else:
    print("F_value match pass")
    F_match_state = 1
    
    

HEADcolumns = ['H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'H_581', 'H_582', 'H_583', 'H_584', 'H_585', 'H_621', 'H_801', 'H_850']
LEMMEcolumns = ['L_103', 'L_311', 'L_321', 'L_331', 'L_402', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516', 'L_581', 'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_912', 'L_922']
POScolumns =['P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_402', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_912', 'P_922']
RELcolumns= ['R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']
INTIDcolumns =['int581', 'int582', 'int583', 'int584', 'int585', 'ss_intID_6XX', 'ss_intID_77X', 'i_801','ss_intID_850', 'ss_intID_L', 'ss_intID']
FORMEcolumns = ['F_103', 'F_703', 'chunk_772', 'F_772', 'F_801', 'F_850', 'Forme_x', 'Forme_y']
SENTidcolumns = ['ssid', 'ssid_801', 'ssid_x', 'ssid_y']
PAGEcolumns = ['Page']
UUIDcolumns = ['UUID']


## divide up
currentFile = these_files.iloc[0,0]
inputData = pd.read_csv(currentFile, sep="\t", low_memory=False)
inputData.columns
currentHEAD_data = inputData[HEADcolumns]
inputData = inputData.drop(list(HEADcolumns), axis=1)
currentLEMME_data = inputData[LEMMEcolumns]
inputData = inputData.drop(list(LEMMEcolumns), axis=1)
currentPOS_data = inputData[POScolumns]
inputData = inputData.drop(list(POScolumns), axis=1)
currentREL_data = inputData[RELcolumns]
inputData = inputData.drop(list(RELcolumns), axis=1)

currentINTID_data = inputData[INTIDcolumns]
inputData = inputData.drop(list(INTIDcolumns), axis=1)

currentForme_Data = inputData[ FORMEcolumns]
inputData = inputData.drop(list(FORMEcolumns), axis=1)

currentSent_Data = inputData[SENTidcolumns]
inputData = inputData.drop(list(SENTidcolumns), axis=1)
currentPAGE_data = inputData[PAGEcolumns]
inputData = inputData.drop(list(PAGEcolumns), axis=1)
currentUUID_data = inputData[UUIDcolumns]
inputData = inputData.drop(list(UUIDcolumns), axis=1)

# form test
test_fmatch(currentForme_Data)
#if pass:
finalFormData = currentForme_Data[['F_103']]

##  ssids : reduce to ssid_x
currentSent_Data = currentSent_Data[['ssid_x']]
currentSent_Data.columns = ['ssid']
## intIDs
test_intid_data = currentINTID_data
test_intid_data = test_intid_data.dropna(subset=["ss_intID"])
useful_intID_columns = currentINTID_data[['ss_intID_L', 'ss_intID_6XX', 'ss_intID_77X', 'i_801', 'ss_intID_850']]
useful_intID_columns.columns = ['intID', 'intID_6XX', 'intID_77X', 'intID_80X', 'intID_85X']
# drop 581-6, keep 581as 58X # test_intid_data.columns 
# View(test_intid_data)
## glue back together

inputData = pd.concat([inputData, currentUUID_data, finalFormData, currentPOS_data, currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data,  currentPAGE_data,  useful_intID_columns], axis=1)

outputname = currentFile.replace('Six','Seven').replace('state6', 'state7')
inputData.to_csv(outputname, sep="\t", index=False)
del(inputData, currentUUID_data, finalFormData, currentPOS_data,  currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data, currentPAGE_data, useful_intID_columns)


for i in range(len(these_files)):
  
  currentCode = currentFile[-15:-11]
  these_names = inputData.columns[120:]
  
  
  
  
```


# variant for live difficult cases
```{python}
HEADcolumns = ['H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'H_581', 'H_582', 'H_583', 'H_584', 'H_585', 'H_621', 'H_801', 'H_850']
LEMMEcolumns = ['L_103', 'L_311', 'L_321', 'L_331', 'L_402', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516', 'L_581', 'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_912', 'L_922']
POScolumns =['P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_402', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_912', 'P_922']
RELcolumns= ['R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']
INTIDcolumns =['int581', 'int582', 'int583', 'int584', 'int585', 'ss_intID_6XX', 'ss_intID_77X', 'i_801','ss_intID_850',  'ss_intID','ss_intID_L_y', 'ss_intID_L_x']
FORMEcolumns = ['F_103', 'F_703', 'chunk_772', 'F_772', 'F_801', 'F_850', 'F_6XX', 'F_703', 'F_801','Forme']
SENTidcolumns = [ 'ssid_801', 'ssid_x', 'ssid_y','ssid_y.1','ssid_x.1']
PAGEcolumns = ['Page']
UUIDcolumns = ['UUID']
# for MF04
INTIDcolumns =['ss_intID_L', 'int581', 'int582', 'int583', 'int584','int585','ss_intID', 'ss_intID_850', ]
FORMEcolumns = ['F_103',  'F_850',]
SENTidcolumns = [  'ssid_x',         'ssid_y' ]


## divide up
currentFile = these_files.iloc[12,0]
inputData = pd.read_csv(currentFile, sep="\t", low_memory=False)
inputData.columns
currentHEAD_data = inputData[HEADcolumns]
inputData = inputData.drop(list(HEADcolumns), axis=1)
currentLEMME_data = inputData[LEMMEcolumns]
inputData = inputData.drop(list(LEMMEcolumns), axis=1)
currentPOS_data = inputData[POScolumns]
inputData = inputData.drop(list(POScolumns), axis=1)
currentREL_data = inputData[RELcolumns]
inputData = inputData.drop(list(RELcolumns), axis=1)

currentINTID_data = inputData[INTIDcolumns]
inputData = inputData.drop(list(INTIDcolumns), axis=1)

currentForme_Data = inputData[ FORMEcolumns]
inputData = inputData.drop(list(FORMEcolumns), axis=1)

currentSent_Data = inputData[SENTidcolumns]
inputData = inputData.drop(list(SENTidcolumns), axis=1)
currentPAGE_data = inputData[PAGEcolumns]
inputData = inputData.drop(list(PAGEcolumns), axis=1)
currentUUID_data = inputData[UUIDcolumns]
inputData = inputData.drop(list(UUIDcolumns), axis=1)

# form test
test_fmatch(currentForme_Data)
#if pass:
finalFormData = currentForme_Data[['F_103']]

##  ssids : reduce to ssid_x
currentSent_Data = currentSent_Data[['ssid_x']]
currentSent_Data.columns = ['ssid']
## intIDs
test_intid_data = currentINTID_data.columns
test_intid_data = test_intid_data.dropna(subset=["ss_intID"])
useful_intID_columns = currentINTID_data[['ss_intID_L_x', 'ss_intID_6XX', 'ss_intID_77X', 'i_801', 'ss_intID_850']]
## special for MF04
# useful_intID_columns = currentINTID_data[['ss_intID_L',  'ss_intID_850']]
# useful_intID_columns['intID_6XX'] = [useful_intID_columns.iloc[i,0] for i in range(len(useful_intID_columns))]
# useful_intID_columns['intID_77X'] = [useful_intID_columns.iloc[i,0] for i in range(len(useful_intID_columns))]       
# useful_intID_columns['intID_80X'] = [useful_intID_columns.iloc[i,0] for i in range(len(useful_intID_columns))]       
# useful_intID_columns = useful_intID_columns[['ss_intID_L','intID_6XX','intID_77X','intID_80X','ss_intID_850']]

useful_intID_columns.columns = ['intID', 'intID_6XX', 'intID_77X', 'intID_80X', 'intID_85X']
# drop 581-6, keep 581as 58X # test_intid_data.columns 
# View(test_intid_data)
## glue back together

inputData = pd.concat([inputData, currentUUID_data, finalFormData, currentPOS_data, currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data,  currentPAGE_data,  useful_intID_columns], axis=1)

outputname = currentFile.replace('Six','Seven').replace('state6', 'state7')
inputData.to_csv(outputname, sep="\t", index=False)
del(inputData, currentUUID_data, finalFormData, currentPOS_data,  currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data, currentPAGE_data, useful_intID_columns, currentForme_Data)


for i in range(len(these_files)):
  
  currentCode = currentFile[-15:-11]
  these_names = inputData.columns[120:]
  
  


```


# variant for Comparison Corpora : normal
```{python}

cHEADcolumns = ['C_HEAD','H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'H_581', 'H_582', 'H_583', 'H_584', 'H_585', 'H_621', 'H_801', 'H_850']
cLEMMEcolumns = ['C_LEMME','L_311', 'L_321', 'L_331', 'L_402', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516', 'L_581', 'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_912', 'L_922']
cPOScolumns =['C_UPOStag', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_402', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_912', 'P_922']
cRELcolumns= ['C_DEPREL','R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']
cINTIDcolumns =['int581', 'int582', 'int583', 'int584', 'int585', 'ss_intID_6XX', 'ss_intID_77X', 'i_801','ss_intID_850', 'ss_intID','C_id','C_idOrig']
cFORMEcolumns = ['F_103', 'F_703', 'chunk_772', 'F_772', 'F_801', 'F_850', 'Forme_x', 'Forme_y','Forme', 'Forme_581', 'F_402']
SENTidcolumns = ['ssid', 'ssid_801', 'ssid_x', 'ssid_y']
PAGEcolumns = ['Page']
cUUIDcolumns = ['UUID', 'UUID_r']


## divide up
currentFile = these_files.iloc[i-3,0]
inputData = pd.read_csv(currentFile, sep="\t", low_memory=False)
# inputData.columns[40:80]
currentHEAD_data = inputData[cHEADcolumns]
inputData = inputData.drop(list(cHEADcolumns), axis=1)
currentLEMME_data = inputData[cLEMMEcolumns]
inputData = inputData.drop(list(cLEMMEcolumns), axis=1)
currentPOS_data = inputData[cPOScolumns]
inputData = inputData.drop(list(cPOScolumns), axis=1)
currentREL_data = inputData[cRELcolumns]
inputData = inputData.drop(list(cRELcolumns), axis=1)
currentINTID_data = inputData[cINTIDcolumns]
inputData = inputData.drop(list(cINTIDcolumns), axis=1)
currentForme_Data = inputData[ cFORMEcolumns]
inputData = inputData.drop(list(cFORMEcolumns), axis=1)
currentSent_Data = inputData[SENTidcolumns]
inputData = inputData.drop(list(SENTidcolumns), axis=1)
# currentPAGE_data = inputData[PAGEcolumns]
# inputData = inputData.drop(list(PAGEcolumns), axis=1)
currentUUID_data = inputData[cUUIDcolumns]
inputData = inputData.drop(list(cUUIDcolumns), axis=1)

test_fmatch(currentForme_Data)
#if pass:
finalFormData = currentForme_Data[['F_103']]

##  ssids : reduce to ssid_x
currentSent_Data = currentSent_Data[['ssid_x']]
currentSent_Data.columns = ['ssid']
## intIDs
test_intid_data = currentINTID_data
test_intid_data = test_intid_data.dropna(subset=["ss_intID"])
useful_intID_columns = currentINTID_data[['ss_intID', 'ss_intID_6XX', 'ss_intID_77X', 'i_801', 'ss_intID_850','C_id', 'C_idOrig']]
useful_intID_columns.columns = ['intID', 'intID_6XX', 'intID_77X', 'intID_80X', 'intID_85X','C_id', 'C_idOrig']
# drop 581-6, keep 581as 58X # test_intid_data.columns 
# View(test_intid_data)
## glue back together

inputData = pd.concat([inputData, currentUUID_data, finalFormData, currentPOS_data, currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data,  useful_intID_columns], axis=1)

outputname = currentFile.replace('Six','Seven').replace('state6', 'state7')
inputData.to_csv(outputname, sep="\t", index=False)
del(inputData, currentUUID_data, finalFormData, currentPOS_data,  currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data,  useful_intID_columns)


```

# variant for Comparison Corpora : special cases
```{python}

cHEADcolumns = ['C_HEAD','H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'H_581', 'H_582', 'H_583', 'H_584', 'H_585', 'H_621', 'H_801', 'H_850']
cLEMMEcolumns = ['C_LEMME','L_311', 'L_321', 'L_331', 'L_402', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516', 'L_581', 'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_912', 'L_922']
cPOScolumns =['C_UPOStag', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_402', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_912', 'P_922']
cRELcolumns= ['C_DEPREL','R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']

cINTIDcolumns =['intID_80X','intID_85X','C_id','C_idOrig','ss_intID','intID','intID_6XX','intID_77X']
cFORMEcolumns = ['F_103','C_FORME']
SENTidcolumns = [ 'ssid']
PAGEcolumns = ['Page']
cUUIDcolumns =  ['UUID']


['UUID', 'F_103', 'ssid', 'intID', 'intID_6XX', 'intID_77X', 'intID_80X',
       'intID_85X', 'C_id', 'C_FORME', 'C_XPOStag', 'C_Feats', 'UUID',
       'C_DEPS', 'ss_intID', 'ssid', 'C_idOrig',
       
dropcols =[  'C_XPOStag', 'C_Feats', 'C_DEPS' ]
## divide up
currentFile = these_files.iloc[5,0]
inputData = pd.read_csv(currentFile, sep="\t", low_memory=False)
# inputData.columns[160:]

inputData = sevenstateA
currentHEAD_data = inputData[cHEADcolumns]
inputData = inputData.drop(list(cHEADcolumns), axis=1)
currentLEMME_data = inputData[cLEMMEcolumns]
inputData = inputData.drop(list(cLEMMEcolumns), axis=1)
currentPOS_data = inputData[cPOScolumns]
inputData = inputData.drop(list(cPOScolumns), axis=1)
currentREL_data = inputData[cRELcolumns]
inputData = inputData.drop(list(cRELcolumns), axis=1)

currentForme_Data = inputData[ cFORMEcolumns]
inputData = inputData.drop(list(cFORMEcolumns), axis=1)
currentINTID_data = inputData[cINTIDcolumns]
inputData = inputData.drop(list(cINTIDcolumns), axis=1)

currentSent_Data = inputData[SENTidcolumns]
inputData = inputData.drop(list(SENTidcolumns), axis=1)
# currentPAGE_data = inputData[PAGEcolumns]
# inputData = inputData.drop(list(PAGEcolumns), axis=1)
currentUUID_data = inputData[cUUIDcolumns]
inputData = inputData.drop(list(cUUIDcolumns), axis=1)
# inputData = inputData.drop(list(dropcols), axis=1)
test_fmatch(currentForme_Data)
#if pass:
finalFormData = currentForme_Data[['F_103']]

##  ssids : reduce to ssid_x
currentSent_Data = currentSent_Data[['ssid']]
currentSent_Data.columns = ['ssid', 'ssidasd']
## intIDs
test_intid_data = currentINTID_data
View(test_intid_data)
test_intid_data = test_intid_data.dropna(subset=["ss_intID"])
useful_intID_columns = currentINTID_data[['ss_intID', 'ss_intID_6XX', 'ss_intID_77X', 'i_801', 'ss_intID_850','C_id', 'C_idOrig']]
useful_intID_columns.columns = ['intID', 'intID_6XX', 'intID_77X', 'intID_80X', 'intID_85X','C_id', 'C_idOrig']
# drop 581-6, keep 581as 58X # test_intid_data.columns 
# 
## glue back together
['UUID_x', 'UUID_y', 'UUID_L', '', '', 'UUID_x.2',
       'UUID.1', '', '', '']
currentUUID_data = currentUUID_data.drop(['UUID_y'], axis=1)
View(currentUUID_data)
currentUUID_data = currentUUID_data[['UUID']]
currentUUID_data.columns = ['UUID','UUID_r']

inputData = pd.concat([inputData, currentUUID_data, finalFormData, currentPOS_data, currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data,  useful_intID_columns], axis=1)

outputname = '/Volumes/AlphaThree/Binding/newest/_SevenState/CA16new.csv'
currentFile.replace('Six','Seven').replace('state6', 'state7')
outoutnew.to_csv(outputname, sep="\t", index=False)
outoutnew = inputData
inputData.shape

del(inputData, currentUUID_data, finalFormData, currentPOS_data,  currentLEMME_data, currentHEAD_data, currentREL_data, currentSent_Data,  useful_intID_columns)


```

CA1,9,16 need to be rebound with C_tags
```{python}
CA09 : sevenstate, 99state, left_on="UUID_y", right_on="UUID"
CA16 : concat as same l

CA01,  sevenstate, 99state, left_on="UUID_x" ,right_on="UUID"
sixatate= pd.read_csv('/Volumes/AlphaThree/Binding/newest/_SixState/CA09-state6.csv', sep="\t", low_memory=False)
nine9state = pd.read_csv('/Volumes/AlphaThree/Binding/newest/_SixState/099/CA16-099.csv', sep="\t", low_memory=False)

test = pd.merge(sixatate, nine9state , how="left", left_on="UUID_y" ,right_on="UUID")

six_state = pd.read_csv('/Volumes/AlphaThree/Binding/newest/_FiveState/CA16-state5.csv', sep="\t", low_memory=False)

droprows = [i for i in range(len(nine9state)) if nine9state.iloc[i,8] == "zzkz"]
specialList = ['CA16_19333_1', 'CA16_19334','CA16_20982_1', 'CA16_20983']

specialDrop = [i for i in range(len(nine9state)) if nine9state.iloc[i,8] in specialList]
six_state.columns[30:100]
nine_nineTidy = nine9state.drop(droprows, axis=0).drop(specialDrop, axis=0)
nine_nineTidy = nine_nineTidy.reset_index(drop=True)
six_state[['F_103','UUID_r']]
View(nine_nineTidy)
test = pd.concat([nine_nineTidy, six_state[['F_103','UUID']]], axis=1)
test.columns = [ 'C_id', 'C_FORME', 'C_LEMME', 'C_UPOStag', 'C_HEAD', 'C_DEPREL', 'UUID_r','ss_intID', 'C_idOrig', 'UUID']
       
test = test.drop(['ssid'], axis=1)

sevenstate = sevenstate.drop(50279, axis=1)
i=50277
theseRows = [i for i in range(len(sevenstate)) if sevenstate.iloc[i-1,:] is np.nan]
sevenstate = sevenstate.drop(theseRows, axis=0)

sevenstateA = pd.concat([sevenstate, nine_nineTidy], axis=1)

```

#6000 update a sentence of a given text
```{python}
theseFiles = pd.DataFrame(glob.glob('/Users/username/PhraseoRoChe/Pipeline/refresh/temp/*850.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)

:: AF06 all NA for 850, same for AF12
done for i=2-11 incl

i=i-7
currentFile = theseFiles.iloc[i,0]

currentColOrder, targetSetnence, myChunk, currentColOrder, live_data, live_file, CLTKnewdata ,trimmed_live=   loadLiveForChunking(currentFile)
chunkRebuilt = updateChunk(targetSetnence, myChunk, CLTKnewdata)

  Live_data_updated = pd.merge(trimmed_live, chunkRebuilt, how="left", left_on="UUID", right_on="UUID")
  colnameslist = list(Live_data_updated.columns)
  colnameslist[130]='ssid'
  Live_data_updated.columns = colnameslist
  Live_data_updated = Live_data_updated.drop(['ssid_y'], axis=1)
  Live_data_updated = Live_data_updated.reindex(columns=currentColOrder)
  Live_data_updated.to_csv(live_file, sep="\t", index=False)




exportUpdatedFile(trimmed_live, chunkRebuilt, currentColOrder, live_file)

def loadLiveForChunking(currentFile):
  CLTKnewdata = pd.read_csv(currentFile, sep="\t", usecols = [1,2,4,5,6,8])
  CLTKnewdata.columns = ['L_850','P_850','intID_85X','R_850','H_850','UUID']
  ## get real data
  thisCode = currentFile[-12:-8]
  # thisCode = 'FP02'
  live_file = f'/Volumes/AlphaThree/Binding/newest/_SevenState/_SevenState/{thisCode}-state7.csv'
  live_data = pd.read_csv(live_file, sep="\t", low_memory=False)
  currentColOrder = live_data.columns
  myChunk = live_data[['L_850','P_850','intID_85X','R_850','H_850','UUID','ssid']]
  trimmed_live = live_data.drop(['L_850','P_850','intID_85X','R_850','H_850'], axis=1)
  targetSetnence = myChunk.loc[myChunk['ssid'] == (myChunk.ssid.values)[-2]]
  print(myChunk)
  return currentColOrder, targetSetnence, myChunk, currentColOrder, live_data, live_file, CLTKnewdata, trimmed_live

def updateChunk(targetSetnence, myChunk, CLTKnewdata):
  this_index = targetSetnence.index
  myChunkTrimmed = myChunk.drop(targetSetnence.index)
  NewSentence = CLTKnewdata
  NewSentence['ssid'] = (myChunk.ssid.values)[-2]
  NewSentence.index = this_index
  chunkRebuilt = pd.concat([myChunkTrimmed, NewSentence], axis=0)
  print(chunkRebuilt)
  return chunkRebuilt

def exportUpdatedFile(trimmed_live, chunkRebuilt, currentColOrder, live_file):
  # Live_data_updated = pd.merge(trimmed_live, chunkRebuilt, how="left", left_on="UUID", right_on="UUID")
  # colnameslist = list(Live_data_updated.columns)
  # colnameslist[130]='ssid'
  # Live_data_updated.columns = colnameslist
  # Live_data_updated.drop(['ssid_y'], axis=1)
  # Live_data_updated = Live_data_updated.reindex(columns=currentColOrder)
  # Live_data_updated.to_csv(live_file, sep="\t", index=False)
# 
#   colnameslist[0] = "UUID" ; 
#   colnameslist[1] = "del1" ; 
#   colnameslist[2] = "del5" ; 
#   colnameslist[3] = "del4" ; 
#   colnameslist[144] = "del2"
#   colnameslist[145] = "del3"
  


```

# update the analysys for a gibven tool for a whole text
```{python}
inputdata = pd.read_csv('/Volumes/AlphaThree/Binding/newest/_SevenState/850 ==011698 on/CA01-state7.csv', sep="\t", low_memory=False)

View(inputdata)
# get list of cols
col_list = list(inputdata.columns)  
#make subset of cols to update : 00 to 76
myChunk = inputdata[['UUID', 'F_103', 'P_850', 'L_850', 'H_850', 'R_850',  'ssid', 'intID_85X']]
View(myChunk)

# drop cols to be updated except ssid and uuid and F103
inputTrimmed = inputdata.drop(['P_850', 'L_850', 'H_850', 'R_850',  'intID_85X'], axis=1)

## make binder if UUID values use different values: see 6 statefile
# get sixstate file
sixstate = pd.read_csv('/Volumes/AlphaThree/Binding/newest/_SixState/CA01-state6.csv', sep="\t", low_memory=False)
# get sixstate columns as list
sixstatecols = list(sixstate.columns)

# get UUID values and F_103 from col list
custombinder = sixstate[['F_103', 'UUID_x', 'UUID_y', 'UUIDr', 'UUID_x.1', 'UUID.1', 'UUID.2', 'UUID_y.1', 'UUID']]
# unload sixstate data
del(sixstate)

# remove duplicate cols from custombinder, leaving F_103, one col with UUID series frmo myChunk and one col with series from newly created data from tool XXX. 
custombinder = custombinder.drop(['UUID.1'], axis=1)
custombinder.columns = ["F_103","UUIDa","UUIDb"]
# merge custom binder with inputdata from tool : 01 to 96
newcontent = pd.read_csv('/Users/username/PhraseoRoChe/Pipeline/refresh/temp/850/CA01-850.csv', sep="\t", low_memory=False)
custombinder['UUIDa'] = custombinder['UUIDa'].str.replace("_","-0")
updatedContent = pd.merge(newcontent, custombinder, how="left", left_on="UUID", right_on="UUIDa")

## check match for forms, then:
# remove form cols
updatedContent = updatedContent.drop(['F_103', 'Token','UUIDa','tag'], axis=1)

# remove noise cols of data
updatedContent = updatedContent.drop(['UUID'], axis=1)

#rename cols
updatedContent.columns = ['L_850', 'P_850', 'intID_85X', 'R_850', 'H_850', 'ssid', 'UUID']
# remove noise cols of repeated UUID if any (_x, _y…)
updatedContent = updatedContent.drop(['UUID_a' ], axis=1)

# rename correct UUID col with same name as UUID col in sevenstatefile by renaming all cols in updatedContent
updatedContent.columns = ['L_850', 'P_850', 'intID_85X', 'R_850', 'H_850', 'UUID']

# print head, tail of updatedContent in console, then head tail of sevenstate to check all good
updatedContent
inputTrimmed = inputTrimmed.drop(['ssid'], axis=1)

# when checked, bind on uuid

full_update = pd.merge(inputTrimmed, updatedContent, how="left", on="UUID")

# set col order to original
full_update = full_update[col_list]

# export
outputfilename = '/Volumes/AlphaThree/Binding/newest/_SevenState/_SevenState/CA01-state7.csv'
full_update.to_csv(outputfilename, sep="\t", index=False)

# clear temp data:
del(all_update, inputdata, myChunk, inputTrimmed, custombinder, newcontent, updatedContent)


```

## seeing if anything's amiss : 

```{python}

all_results = pd.DataFrame(dtype="object")
results = pd.DataFrame(dtype="object")
theseFiles = pd.DataFrame(glob.glob('/Volumes/AlphaThree/Binding/newest/_SevenState/SevenCorrected/*state7.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles = theseFiles.reset_index(drop=True)
for f in range(len(theseFiles)):  
  testfile = theseFiles.iloc[f,0]
  test_data = pd.read_csv(testfile, sep="\t", low_memory=False)
  # View(test_data)
  columnlist = list(test_data.columns)
  
  file = testfile[-15:-11]
  results = pd.DataFrame(dtype='object')
  for c in range(len(columnlist)):
    score = test_data[columnlist[c]].isnull().sum()
    result = pd.DataFrame([{columnlist[c]:score, 'index':file}])
    result = result.set_index('index')
    results = pd.concat([results, result], axis=1)
  all_results = pd.concat([all_results, results], axis=0)
all_results.to_csv("/Users/username/Desktop/results.csv", sep="\t", index=True)

View(all_results)

chunk = test_data[['F_103','P_331' ,'P_402', 'P_511', 'P_611']]

```

#7000 : remap values

```{python}
#7001UPOS tidy

def tidyUPOStags(test_data, UPOSunique):
  '''function to remap AUX to VERB in all columns with UPOS tags ''' 
  UPOStidy_RemapDict = {"AUX":"VERB", "SYM":"NOUN","PART":"ADP"}
  UPOScolumns = ['P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293',
  'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779','P_801' ]
  ## note all should be in CA data
  for col in UPOScolumns:
    test_data[col] = test_data[col].map(UPOStidy_RemapDict).fillna(test_data[col])
    # uniquevalues = list(test_data[col].unique())
    # UPOSunique = uniquevalues + UPOSunique
  return test_data, UPOSunique

#7002 UPOS tidy
def tidyXPOStags(test_data, XPOScolumns, XPOSunique):
  '''function to remap XPOS to UPOS tags in all XPOS columns ''' 
  POSupos_RemapDict = {"ADJqua":"ADJ", "ADJind":"ADJ", "ADJpos":"ADJ", "PRE":"ADP", "PRE.DETdef":"ADP", "ADVgen":"ADV", "ADVneg":"ADV", "PROadv":"ADV", "ADVint":"ADV", "ADVsub":"ADV", "VERcjg":"AUX", "VERcjg":"VERB", "VERinf":"VERB", "VERppe":"VERB", "CONcoo":"CCONJ", "DETpos":"DET", "DETdef":"DET", "DETind":"DET", "DETndf":"DET", "DETdem":"DET", "DETrel":"DET", "DETint":"DET", "INJ":"INTJ", "NOMcom":"NOUN", "DETcar":"DET", "ADJcar":"ADJ", "PROcar":"PRO", "PROcar":"PRON", "PROdem":"PRON", "PROrel":"PRON", "PROper":"PRON", "PROimp":"PRON", "PROind":"PRON", "PROpos":"PRON", "PROint":"PRON", "PROper.PROper":"PRON", "ADVneg.PROper":"PRON", "PROord":"PRON", "NOMpro":"PROPN", "PONfbl":"PUNCT", "PONfrt":"PUNCT", "PONpga":"PUNCT", "PONpdr":"PUNCT", "CONsub":"SCONJ", "VERppa":"VERB","ADJord":"ADJ", "ADVing":"ADJ", "OUT":"ETR", "PRE.DETcom":"ADP", "PONpxx":"PUNCT", "PROrel.PROper":"PRON", "ADVgen.PROper":"ADV","PRE.PROper":"ADP", "ADJnum":"ADJ", "ADJrel":"ADJ", "adv.":"ADV", "ADVgen.VERcjg":"ADV", "ARTcon":"DET", "ARTdef":"DET", "ARTind":"DET", "ARTpar":"DET", "AUX":"VERB", "CON":"CONJ", "CONsub.DETdef":"SCONJ", "CONsub.PROper":"SCONJ", "DET.ndf":"DET", "DETcom":"DET", "DETint_exc":"DET", "DETord":"DET", "ETR":"ETR", "INTJ":"INTJ", "Nomcom":"NOUN", "NOMCom":"NOUN", "PON":"PUNCT", "PONfbl%":"PUNCT", "PRE.ADVgen":"ADP", "PRE.DETdef|NOMB.=p|GENRE=m|CAS=r":"ADP", "PRE.DETdef|NOMB.=s|GENRE=m|CAS=r":"ADP", "PRE.DETrel":"ADP", "PRE.NOMcom":"ADP", "PRE.PROdem":"ADP", "PRE.PROrel":"ADP", "PRO":"PRON", "PROper.ADVgen":"PRON", "PROPN":"PROPN", "PUNCT":"PUNCT", "RED":"X", "SPACE":"X", "subst. fém.":"NOUN", "subst. masc.":"NOUN", "SYM":"X", "verbe":"VERB", "zzkz":"X"}
  
  ## all but P_103 in CA data cols
  for col in XPOScolumns:
    test_data[col] = test_data[col].map(POSupos_RemapDict).fillna(test_data[col])
    # uniquevalues = list(test_data[col].unique())
    # XPOSunique = uniquevalues + XPOSunique
  return test_data, XPOSunique


#7003 RNN-UPOS
def remapRNNtoUPOS(test_data, RNNunique):
  RNN_remapDict = {"A.card.f.p":"ADJ", "A.card.f.s":"ADJ", "A.card.m.p":"ADJ", "A.card.m.s":"ADJ", "A.ind.f.p":"ADJ", "A.ind.f.s":"ADJ", "A.ind.m.p":"ADJ", "A.ind.m.s":"ADJ", "A.int.f.s":"ADJ", "A.int.m.s":"ADJ", "A.ord.f.s":"ADJ", "A.ord.m.p":"ADJ", "A.ord.m.s":"ADJ", "A.qual.f.p":"ADJ", "A.qual.f.s":"ADJ", "A.qual.m.p":"ADJ", "A.qual.m.s":"ADJ", "ADV.-":"ADV", "ADV.excl":"ADV", "ADV.int":"ADV", "ADV.neg":"ADV", "C.C":"CCONJ", "D.def.f.p":"DET", "D.def.f.s":"DET", "D.def.m.p":"DET", "D.def.m.s":"DET", "D.dem.f.p":"DET", "D.dem.f.s":"DET", "D.dem.m.p":"DET", "D.dem.m.s":"DET", "D.excl.m.s":"DET", "D.ind.f.p":"DET", "D.ind.f.s":"DET", "D.ind.m.p":"DET", "D.ind.m.s":"DET", "D.int.f.p":"DET", "D.int.f.s":"DET", "D.int.m.p":"DET", "N.C.f.p":"NOUN", "N.C.f.s":"NOUN", "N.C.m.p":"NOUN", "N.C.m.s":"NOUN", "N.P.f.p":"PROPN", "N.P.f.s":"PROPN", "N.P.m.p":"PROPN", "N.P.m.s":"PROPN", "N.P.x.s":"PROPN", "CL.obj.1.f.s":"PRON", "CL.obj.1.m.p":"PRON", "CL.obj.1.m.s":"PRON", "CL.obj.2.m.p":"PRON", "CL.obj.3.f.p":"PRON", "CL.obj.3.f.s":"PRON", "CL.obj.3.m.p":"PRON", "CL.obj.3.m.s":"PRON", "CL.refl.1.f.s":"PRON", "CL.refl.1.m.p":"PRON", "CL.refl.1.m.s":"PRON", "CL.refl.3.f.p":"PRON", "CL.refl.3.f.s":"PRON", "CL.refl.3.m.p":"PRON", "CL.refl.3.m.s":"PRON", "CL.suj.1.f.s":"PRON", "CL.suj.1.m.p":"PRON", "CL.suj.1.m.s":"PRON", "CL.suj.2.f.p":"PRON", "CL.suj.2.m.p":"PRON", "CL.suj.2.m.s":"PRON", "CL.suj.3.f.p":"PRON", "CL.suj.3.f.s":"PRON", "CL.suj.3.m.p":"PRON", "CL.suj.3.m.s":"PRON", "PPRO.1.f.s":"PRON", "PPRO.1.m.p":"PRON", "PPRO.1.m.s":"PRON", "PPRO.2.m.p":"PRON", "PPRO.3.f.p":"PRON", "PPRO.3.f.s":"PRON", "PPRO.3.m.p":"PRON", "PPRO.3.m.s":"PRON", "PRO.ind.f.p":"PRON", "PRO.ind.f.s":"PRON", "PRO.ind.m.p":"PRON", "PRO.ind.m.s":"PRON", "PRO.int.m.p":"PRON", "PRO.int.m.s":"PRON", "PRO.neg.m.p":"PRON", "PRO.neg.m.s":"PRON", "PRO.poss.f.s":"PRON", "PRO.poss.m.p":"PRON", "PRO.poss.m.s":"PRON", "PRO.refl.f.s":"PRON", "PRO.refl.m.s":"PRON", "PRO.rel.f.p":"PRON", "PRO.rel.f.s":"PRON", "PRO.rel.m.p":"PRON", "PRO.rel.m.s":"PRON", "PONCT.S":"PUNCT", "PONCT.W":"PUNCT", "C.S":"SCONJ", "V.C.1.p":"VERB", "V.C.1.s":"VERB", "V.C.2.p":"VERB", "V.C.3.p":"VERB", "V.C.3.s":"VERB", "V.F.1.p":"VERB", "V.F.1.s":"VERB", "V.F.2.p":"VERB", "V.F.3.p":"VERB", "V.F.3.s":"VERB", "V.I.1.p":"VERB", "V.I.1.s":"VERB", "V.I.2.p":"VERB", "V.I.3.p":"VERB", "V.I.3.s":"VERB", "V.J.3.p":"VERB", "V.J.3.s":"VERB", "V.P.1.p":"VERB", "V.P.1.s":"VERB", "V.P.2.p":"VERB", "V.P.2.s":"VERB", "V.P.3.p":"VERB", "V.P.3.s":"VERB", "V.S.1.p":"VERB", "V.S.3.p":"VERB", "V.S.3.s":"VERB", "V.T.3.s":"VERB", "V.Y.1.p":"VERB", "V.Y.2.p":"VERB", "VK.f.p":"VERB", "VK.f.s":"VERB", "VK.m.p":"VERB", "VK.m.s":"VERB", "D.card.f.p":"DET", "D.card.m.p":"DET", "D.card.m.s":"DET", "D.neg.f.s":"DET", "D.neg.m.s":"DET", "D.part.f.s":"DET", "D.part.m.s":"DET", "D.poss.f.p":"DET", "D.poss.f.s":"DET", "D.poss.m.p":"DET", "D.poss.m.s":"DET", "N.card.f.s":"NOUN", "N.card.m.s":"NOUN", "P+D.def.f.p":"ADP", "P+D.def.m.p":"ADP", "P+D.def.m.s":"ADP", "P+PRO.rel.3.f.p":"ADP", "P+PRO.rel.3.m.s":"ADP", "PRO.card.f.p":"PRON", "PRO.card.m.p":"PRON", "PRO.card.m.s":"PRON", "PRO.dem.f.p":"PRON", "PRO.dem.f.s":"PRON", "PRO.dem.m.p":"PRON", "PRO.dem.m.s":"PRON", "P":"ADP", "VW":"VERB", "VG":"VERB", "I":"INTJ", "ET":'ETR', "_":"X", "A.int.f.p":"ADJ", "A.int.m.p":"ADJ", "A.ord.f.p":"ADJ", "ABR":"X", "ADJcar":"ADJ", "ADJind":"ADJ", "ADJord":"ADJ", "ADJpos":"ADJ", "ADJqua":"ADJ", "ADVgen":"ADV", "ADVgen.PROper":"ADV", "ADVing":"ADV", "ADVint":"ADV", "ADVneg":"ADV", "ADVneg.PROper":"ADV", "ADVsub":"ADV", "CL.obj.1.f.p":"PRON", "CL.suj.1.f.p":"PRON", "CONcoo":"CCONJ", "CONsub":"SCONJ", "CONsub.PROper":"SCONJ", "D.card.f.s":"DET", "D.excl.f.s":"DET", "D.excl.m.p":"DET", "DETcar":"DET", "DETcom":"DET", "DETdef":"DET", "DETdem":"DET", "DETind":"DET", "DETint":"DET", "DETndf":"DET", "DETpos":"DET", "DETrel":"DET", "INJ]":"INTJ", "N.card.f.p":"NOUN", "N.card.m.p":"NOUN", "NOMcom":"NOUN", "NOMpro":"PROPN", "OUT":"ETR", "P+A.qual.m.s":"ADP", "P+PRO.rel.3.m.p":"ADP", "PONfbl":"PUNCT", "PONfrt":"PUNCT", "PONpdr":"PUNCT", "PONpga":"PUNCT", "PONpxx":"PUNCT", "PPRO.2.f.s":"PRON", "PRE":"ADP", "PRE.DETcom":"ADP", "PRE.DETdef":"ADP", "PRE.DETrel":"ADP", "PRE.PROper":"ADP", "PRE.PROrel":"ADP", "PRO.neg.f.s":"PRON", "PRO.refl.m.p":"PRON", "PROadv":"PRON", "PROcar":"PRON", "PROcom":"PRON", "PROdem":"PRON", "PROimp":"PRON", "PROind":"PRON", "PROint":"PRON", "PROord":"PRON", "PROper":"PRON", "PROper.PROper":"PRON", "PROPN":"PROPN", "PROpos":"PRON", "PROrel":"PRON", "PROrel.PROper":"PRON", "PUNCT":"PUNCT", "RED":"X", "VERcjg":"VERB", "VERinf":"VERB", "VERppa":"VERB", "VERppe":"VERB"}

  RNN_tagcolumns = ['P_911','P_92']
  for col in RNN_tagcolumns:
    test_data[col] = test_data[col].map(RNN_remapDict).fillna(test_data[col])
    # uniquevalues = list(test_data[col].unique())
    # RNNunique = uniquevalues + RNNunique
  return test_data, RNNunique


#7004 SEMUPOS
def remapSEMtoUPOS(test_data, SEMunique):

  SEM_remapDict = {"_":"X", "_ADJ":"ADJ", "_ADV":"ADV", "_CC":"CCONJ", "_CS":"SCONJ", "_DET":"DET", "_NC":"NOUN", "_NPP":"PROPN", "_P":"ADP","P":"ADP", "_PRO":"PRON", "_V":"VERB", "_VINF":"VERB", "_VPP":"VERB", "ADJ":"ADJ", "ADJWH":"ADJ", "ADP":"ADP", "ADV":"ADV", "ADVWH":"ADV", "CCONJ":"CCONJ", "CLO":"PRON", "CLR":"PRON", "CLS":"PRON", "DET":"DET", "ETR":"ETR", "NC":"NOUN", "NPP":"PROPN", "P+D":"ADP", "PRO":"PRON", "PROREL":"PRON", "PROWH":"PRON", "SCONJ":"SCONJ", "V":"VERB", "VIMP":"VERB", "VINF":"VERB", "VPP":"VERB", "VPR":"VERB", "VS":"VERB","CC":"CCONJ","CS":"SCONJ","_VPR":"VERB", "P+PRO":"ADP",  "I":"INTJ",  "ET":"ETR", "_PROREL":"PRON",   "_CLS":"PRON",   "DETWH":"DET"
}
  SEM_tagcolumns = ['P_703']
  for col in SEM_tagcolumns:
    test_data[col] = test_data[col].map(SEM_remapDict).fillna(test_data[col])
    # uniquevalues = list(test_data[col].unique())
    # SEMunique = uniquevalues + SEMunique
  return test_data, SEMunique

# 7005 REL remap
# collist = list(test_data.columns)
def remapRels(test_data, RELunique):
  REL_RemapDict = {"acl:relcl":"acl","aux:caus":"aux","aux:tense":"aux","auxs":"aux","expl:comp":"expl","expl:subj":"expl","flat:name":"flat","nsubj:pass":"nsubj","obl:agent":"obl","obl:arg":"obl","obl:mod":"obl", "aux:pass":"aux", "cc:nc":"cc","obl:advmod":"obl",  "nsubj:obj":"nsubj", "case:det":"det", "mark:advmod":"mark", "obj:advneg":"obj", "flat:foreign":"flat", "expl:pass":"expl", "nsubj:caus":"nsubj" , "advcl:cleft":"advcl" ,"obj:agent ":"obj" , "obj:lvc":"obj","expl:pv":"expl", "dep:comp":"dep", "advmod:obl":"advmod", "parataxis:insert":"parataxis", "obj:advmod":"obj", "parataxis:parenth":"parataxis", "obj:agent":"obj", "nsubj:advmod":"iobj", "iobj:agent":"iobj", "nmod:appos":"nmod", "csubj:pass":"csubj" }
  RELcolumns = ['R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']
  for col in RELcolumns:
    test_data[col] = test_data[col].map(REL_RemapDict).fillna(test_data[col])
    # uniquevalues = list(test_data[col].unique())
    # RELunique = uniquevalues + RELunique
  return test_data, RELunique

```


```{python}
test_data[['R_582']].value_counts(), '']] = remapRels(test_data)


collist = list(test_data.columns)
# 7006 lemme tidy
def tidyLemmes(test_data):
  Lemme_cols = ['L_103', 'L_311', 'L_321', 'L_331', 'L_403', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516',  'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_911', 'L_921']
  for col in Lemme_cols:
    test_data[col] = test_data[col].str.replace('[0-9]','').str.lower()
    test_data[col] = test_data[col].str.replace('\|.*','').str.lower()
    
  return test_data
## note no L_581  


test_data[['L_103t','L_103']]

```

need to post apply rule that NUM followed by N/PROPN = DET, DET + NUM not followedby N/PROPN == PRON.

#7001.2 POS tidiers
```{python}
def prepareFileDF(path, suffix):
  theseFiles = pd.DataFrame(glob.glob(path + '*'+ suffix))
  theseFiles = theseFiles.sort_values(0)
  theseFiles = theseFiles.reset_index(drop=True)
  return theseFiles
test_data = gold_aligned
import pandas as pd 
theseFiles = prepareFileDF('/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/GG01', "-state7.csv")
CA_Files = prepareFileDF('/Volumes/AlphaThree/Binding/newest/_SevenState/SevenCorrected/[C]', "-state7.csv")
## dev : find missing values from dicts
UPOSunique = []
XPOSunique = []
RNNunique = []
SEMunique= []
RELunique = []
for f in range( len(theseFiles)):
# for f in range( len(CA_Files)):
  # testfile = CA_Files.iloc[f,0]
  testfile = theseFiles.iloc[f,0]
  outputfile = testfile.replace('SevenState/SevenCorrected','st8').replace('state7','st8')
  # set list of XPOScolumns depending on data type
  if testfile[-15:-13] in ['CA','CM']:
    XPOScolumns = ['P_311', 'P_321', 'P_331',  'P_402','P_850' ]
  else:
    XPOScolumns = ['P_103','P_311', 'P_321', 'P_331',  'P_403','P_850' ]
  
  test_data = pd.read_csv(testfile, sep="\t", low_memory=False)
  test_data, UPOSunique = tidyUPOStags(test_data, UPOSunique)
  test_data, XPOSunique = tidyXPOStags(test_data, XPOScolumns, XPOSunique)
  test_data, RNNunique = remapRNNtoUPOS(test_data, RNNunique)
  test_data, SEMunique = remapSEMtoUPOS(test_data, SEMunique)
  test_data, RELunique = remapRels(test_data, RELunique)
  ## dev : find missing values from dicts
  # UPOSunique = list(set(UPOSunique))
  # XPOSunique = list(set(XPOSunique))
  # RNNunique = list(set(RNNunique))
  # SEMunique= list(set(SEMunique))
  # RELunique = list(set(RELunique))
  test_data.to_csv(outputfile, sep="\t", index=False)
  print("Processed " + testfile[-15:-11], flush=True)

  # outputfile = testfile.replace('state7','st8')
  # test_data.to_csv(outputfile, sep="\t", index=False)


test_dataLemmMod=  tidyLemmes(test_data)
test_dataLemmMod = test_dataLemmMod.drop(['L_103t'], axis=1)

```
new problem wtih col names in CA01 : eg P922 : décalage

```{python}
SEM_tagcolumns ; RNN_tagcolumns ; XPOScolumns ; UPOScolumns
POScolumns ; RELcolumns ; Lemme_cols ; HEADcolumns
BaseCols = ['UUID', 'F_103']
ViewerCols =   BaseCols + HEADcolumns
head_data = test_data[ViewerCols]


```

## add GG correct tags to DF
```{python}
GGfile = '/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/GG01-GoldTags.csv'
GG_correct= pd.read_csv(GGfile, sep="\t")
test = pd.concat([test_data[['F_103','UUID']],GG_correct], axis=1)
test.to_csv(GGfile.replace('Tags','alignmentKey'), sep="\t", index=False)
View(test)
gold_aligned = pd.concat([test_data,GG_correct], axis=1)
gold_aligned.to_csv(GGfile.replace('Tags','Aligned'), sep="\t", index=False)


```

## option to convert head cols to int, set NA as -99
```{python}
for col in Head_cols:
  results[col] = results[col].apply(pd.to_numeric, errors = 'coerce')
  results[col] = results[col].fillna("-99").astype("int64")
```


```{python}
# tidying gold v3

gold_aligned = pd.read_csv("/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/GG01-allV3.csv", sep=";")

col_list = list(gold_aligned.columns )

corrColumns = ['C_DEPREL', 'C_HEAD', 'C_LEMME', 'C_UPOStag', 'UUID']
POScols = ['P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_403', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_911', 'P_921']
HEADcols = ['H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'H_581', 'H_582', 'H_583', 'H_584', 'H_585', 'H_611', 'H_621', 'H_801', 'H_850'] 
RELcols = ['R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']


LEMcols = ['L_103', 'L_311', 'L_321', 'L_331', 'L_403', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516', 'L_581', 'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_911', 'L_921']

id_cols = [ 'F_103',  'StanzaID','intID_80X', 'intID_85X',  'intID', 
  'ssid']

dropcols = ['Unnamed: 2', 'Unnamed: 57', 'Unnamed: 64', 'Unnamed: 68', 'UUID_oldA', 'UUID_oldB', 'UUID_oldC', 'intID.1','ssid.1','ss_intID']

chunk = gold_aligned[LEMcols]

gold_aligned = gold_aligned.drop(dropcols, axis=1)
gold_aligned.to_csv("/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/GG01-allV31.csv", sep="\t", index=False)

```

```{python}
colOrder = list(test_data.columns)
POScols = ['P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_402', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_912', 'P_922']


theseValues = test_data['P_703'].value_counts()
theseValues.to_csv("/Users/username/Desktop/RNN_values.csv", sep="\t", index=True)
SEM_tagcolumns = ['P_703']
View(test_data[POScols])


```


## test scoring v2
```{python}
## definitions
def get_posScores(results, textCode, colscores_all, pos_specific_results, live_state=1):
  '''calculate POS scores. results= df with standardised colnames ; live_state= int,  1 for livestate files, 0 for CA/CM/GG files'''
  col_names_list = []
  # empty list to hold col names, empty df to hold results
  scores = pd.DataFrame(dtype="object")
  POScols = [ 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_403', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_911', 'P_921']
  if live_state ==1:
    POScols= ["P_103"] +   POScols
  # for each col, run the lambda function, putting 1 if row,col == upostag else 0, then add col to scores df, add colname to list
  for col in POScols:
    run_scorer = results.apply(lambda row: 1 if row[col] == row['C_UPOStag'] else  ( 1  if ((row['C_UPOStag'] == "ADP.DET") & (row['F_103'] in list1) & (row[col] in list2)) else 0) , axis=1)
    scores = pd.concat([scores, run_scorer], axis=1)
    col_names_list.append(col)
  # set names of cols
  scores.columns = col_names_list
  # sul pos scores for each tool
  pos_sums = [scores[col].sum() for col in scores.columns]
  # zip together col names and sums, exploding the zip into a df with cols for test name and name of score
  output = pd.DataFrame([*zip(scores.columns, pos_sums)], columns =['tool','p_score'])
  # add col calculating % score
  output['p_perc'] =  output.p_score /len(run_scorer)*100
  # add col to specify text
  output['text'] = textCode
  # move scores to specific df
  pos_scores_detail = pd.DataFrame(scores)
  output['tool'] = output['tool'].str[2:]
  holding = output
  pos_scores_detail['row_sum'] = pos_scores_detail.sum(axis=1)
  text_col_scores = pd.DataFrame(pos_scores_detail.row_sum.value_counts())
  text_col_scores.columns = [ textCode]
  text_col_scores['score'] = text_col_scores.index.values
  
  colscores_all = pd.merge(colscores_all, text_col_scores, how="right",  on="score")

  ## get scores for each POS in the text and send to df
  # get list of correct tags, for for each correct tag:
  corr_tags = list(set(results.C_UPOStag.values))
  for corr_tag in corr_tags:
    # get index of values with correct TAG, use index with iloc to get all rows
    current_pos_data = pos_scores_detail.iloc[results['C_UPOStag'].loc[lambda s: s == corr_tag].index]
    ## create df zipping together sums of cols, % accuracy and names of cols, which will be rows
    currentResult = pd.DataFrame([*zip((current_pos_data).sum(), (current_pos_data).sum()/len(current_pos_data))], index=current_pos_data.columns, columns = [str(corr_tag) + str(textCode), str(corr_tag) + str(textCode) + "%"])
    currentResult = currentResult.drop('row_sum') # drop summ row
    pos_specific_results= pd.merge(pos_specific_results, currentResult, how="right", left_index=True, right_index=True) # merge on index

  return( holding, output, pos_scores_detail, colscores_all, pos_specific_results)

def get_LemmeScores(results, holding, textCode, live_state=1):
  '''calculate POS scores 
  inputs: results is a df with standardised colnames
          live_state is 1 or 0, 1 for livestate files, 0 for CA/CM/GG files, setting to 0 excludes LGERM column from this analysis
          ; todo '''
          
  Lemme_cols = [ 'L_311', 'L_321', 'L_331', 'L_403', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516',  'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_911', 'L_921']
  if live_state ==1:
    Lemme_cols= ["L_103"] +   Lemme_cols

  col_names_list = []
  scores = pd.DataFrame(dtype="object")
  # for each col, run the lambda function, putting 1 if row,col == upostag else 0, then add col to scores df, add colname to list
  for col in Lemme_cols:
    run_scorer = results.apply(lambda row: 1 if row[col] == row['C_LEMME'] else 0, axis=1)
    scores = pd.concat([scores, run_scorer], axis=1)
    col_names_list.append(col)
  # set names of cols
  scores.columns = col_names_list
  # sul pos scores for each tool
  LEM_sums = [scores[col].sum() for col in scores.columns]
  # zip together col names and sums, exploding the zip into a df with cols for test name and name of score
  output = pd.DataFrame([*zip(scores.columns, LEM_sums)], columns =['tool','L_score'])
  # add col calculating % score
  output['L_perc'] =  output.L_score /len(run_scorer)*100
  # add col to specify text
  output['text'] = textCode
  # move scores to specific df
  LEM_scores_detail = pd.DataFrame(scores)
  output['tool'] = output['tool'].str[2:]
  
  holding = pd.merge(holding, output, how="left", on=["tool",'text'])

  return( holding, output, LEM_scores_detail)


def get_HeadScores(results, holding, textCode):
  '''calculate Head scores 
  inputs: results is a df with standardised colnames
          live_state is 1 or 0, 1 for livestate files, 0 for CA/CM/GG files, setting to 0 excludes LGERM column from this analysis
          ; todo '''
          
  Head_cols =  ['H_232', 'H_241', 'H_242', 'H_244', 'H_245', 'H_261', 'H_262', 'H_263', 'H_271', 'H_272', 'H_273', 'H_281', 'H_282', 'H_283', 'H_291', 'H_292', 'H_293', 'H_611', 'H_511', 'H_512', 'H_513', 'H_514', 'H_515', 'H_516', 'H_581', 'H_582', 'H_583', 'H_584', 'H_585', 'H_621', 'H_801', 'H_850']

  col_names_list = []
  scores = pd.DataFrame(dtype="object")
  # for each col, run the lambda function, putting 1 if row,col == upostag else 0, then add col to scores df, add colname to list
  results['H_801'] = [results.iloc[i,112] +1 if  results.iloc[i,112] == -1 else results.iloc[i,112] for i in range(len(results))]
  for col in Head_cols:
    results[col] = results[col].apply(pd.to_numeric, errors = 'coerce')
    results[col] = results[col].fillna("-99").astype("int64")
    results['C_HEAD'] = results['C_HEAD'].apply(pd.to_numeric, errors = 'coerce')
    results['C_HEAD'] = results['C_HEAD'].fillna("-99").astype("int64")
    run_scorer = results.apply(lambda row: 1 if row[col] == row['C_HEAD'] else 0, axis=1)
    scores = pd.concat([scores, run_scorer], axis=1)
    col_names_list.append(col)
  # set names of cols
  scores.columns = col_names_list
  # sul pos scores for each tool
  Head_sums = [scores[col].sum() for col in scores.columns]
  # zip together col names and sums, exploding the zip into a df with cols for test name and name of score
  output = pd.DataFrame([*zip(scores.columns, Head_sums)], columns =['tool','H_score'])
  # add col calculating % score
  output['H_perc'] =  output.H_score /len(run_scorer)*100
  # add col to specify text
  output['text'] = textCode
  # move scores to specific df
  Head_scores_detail = pd.DataFrame(scores)
  output['tool'] = output['tool'].str[2:]
  
  holding = pd.merge(holding, output, how="left", on=["tool",'text'])

  return( holding, output, Head_scores_detail)


def get_RelScores(results, holding, textCode):
  '''calculate Head scores 
  inputs: results is a df with standardised colnames
          live_state is 1 or 0, 1 for livestate files, 0 for CA/CM/GG files, setting to 0 excludes LGERM column from this analysis
          ; todo '''
          
  Rel_cols =  ['R_232', 'R_241', 'R_242', 'R_244', 'R_245', 'R_261', 'R_262', 'R_263', 'R_271', 'R_272', 'R_273', 'R_281', 'R_282', 'R_283', 'R_291', 'R_292', 'R_293', 'R_511', 'R_512', 'R_513', 'R_514', 'R_515', 'R_516', 'R_581', 'R_582', 'R_583', 'R_584', 'R_585', 'R_611', 'R_621', 'R_801', 'R_850']

  col_names_list = []
  scores = pd.DataFrame(dtype="object")
  # for each col, run the lambda function, putting 1 if row,col == upostag else 0, then add col to scores df, add colname to list
  for col in Rel_cols:
    run_scorer = results.apply(lambda row: 1 if row[col] == row['C_DEPREL'] else 0, axis=1)
    scores = pd.concat([scores, run_scorer], axis=1)
    col_names_list.append(col)
  # set names of cols
  scores.columns = col_names_list
  # sul pos scores for each tool
  Relsums = [scores[col].sum() for col in scores.columns]
  # zip together col names and sums, exploding the zip into a df with cols for test name and name of score
  output = pd.DataFrame([*zip(scores.columns, Relsums)], columns =['tool','R_score'])
  # add col calculating % score
  output['R_perc'] =  output.R_score /len(run_scorer)*100
  # add col to specify text
  output['text'] = textCode
  # move scores to specific df
  Rel_scores_detail = pd.DataFrame(scores)
  output['tool'] = output['tool'].str[2:]
  
  holding = pd.merge(holding, output, how="left", on=["tool",'text'])

  return( holding, output, Rel_scores_detail)



### end definitions

## dev of functions

####
# ## 1. POS SCORES
# # empty list to hold col names, empty df to hold results
# col_names_list = []
# scores = pd.DataFrame(dtype="object")
# POScols = ['P_103', 'P_232', 'P_241', 'P_242', 'P_244', 'P_245', 'P_261', 'P_262', 'P_263', 'P_271', 'P_272', 'P_273', 'P_281', 'P_282', 'P_283', 'P_291', 'P_292', 'P_293', 'P_311', 'P_321', 'P_331', 'P_402', 'P_511', 'P_512', 'P_513', 'P_514', 'P_515', 'P_516', 'P_581', 'P_582', 'P_583', 'P_584', 'P_585', 'P_611', 'P_621', 'P_703', 'P_773', 'P_774', 'P_775', 'P_776', 'P_777', 'P_778', 'P_779', 'P_801', 'P_850', 'P_912', 'P_922']
# 
# # for each col, run the lambda function, putting 1 if row,col == upostag else 0, then add col to scores df, add colname to list
# for col in POScols:
#   list1 = ["du","des", "au", "aux"]
#   list2 = ["DET","ADP"]
#   run_scorer = results.apply(lambda row: 1 if row[col] == row['C_UPOStag'] else  ( 1  if ((row['C_UPOStag'] == "ADP.DET") & (row['F_103'] in list1) & (row[col] in list2)) else 0) , axis=1)
#   scores = pd.concat([scores, run_scorer], axis=1)
#   col_names_list.append(col)
# # set names of cols
# scores.columns = col_names_list
# # sul pos scores for each tool
# pos_sums = [scores[col].sum() for col in scores.columns]
# # zip together col names and sums, exploding the zip into a df with cols for test name and name of score
# output = pd.DataFrame([*zip(scores.columns, pos_sums)], columns =['tool','p_score'])
# # add col calculating % score
# output['p_perc'] =  output.p_score /len(run_scorer)*100
# # add col to specify text
# output['text'] = "CA19"
# # move scores to specific df
# pos_scores_detail = pd.DataFrame(scores)
# output['tool'] = output['tool'].str[2:]
# holding = output
# pos_scores_detail['row_sum'] = pos_scores_detail.sum(axis=1)
# 
# 
# all_detail= pd.concat([results[['UUID','F_103']], pos_scores_detail], axis=1)
# all_detail['row_sum%'] = all_detail['row_sum'].values/46
# all_detail['row_sum'].value_counts()
# 
# text_col_scores = pd.DataFrame(pos_scores_detail.row_sum.value_counts())
# text_col_scores.reset_index(drop=False)
# text_col_scores.columns = [ textCode]
# 
# text_col_scores2 = text_col_scores.sort_values(['CM05'])
# colscores_all['score'] = colscores_all.index.values
# colscores_all = pd.DataFrame(dtype="object")
# colscores_all = pd.merge(colscores_all, text_col_scores, how="right",  left_index=True, right_index=True)
# )
# pd.merge()
# # get POS score by POS category
# special = ["r_" + col for col in pos_scores_detail.columns]
# pos_scores_detail.columns = special
# extended_data = pd.concat([results, pos_scores_detail], axis=1)
# targetList = ["que", "qu\'", "k", "q" ]
# keepRows = [i for i in range(len(extended_data)) if extended_data.iloc[i,2] not in targetList]
# restricted = extended_data.drop(keepRows, axis=0)
# rest_sums = [restricted[col].sum() for col in special]
# detailed_output = pd.DataFrame([*zip(special, rest_sums, restricted.r_row_sum.values)], columns = ['tool','score', 'ttl'])
# detailed_output['acc'] = detailed_output.score/534
# 
# combos = extended_data[['UUID','F_103', 'r_row_sum']]
# View(extended_data)
# ## 2. C_LEMME SCORES
# # empty list to hold col names, empty df to hold results
# Lemme_cols = [ 'L_311', 'L_321', 'L_331', 'L_402', 'L_511', 'L_512', 'L_513', 'L_514', 'L_515', 'L_516',  'L_582', 'L_583', 'L_584', 'L_585', 'L_772', 'L_801', 'L_850', 'L_912', 'L_922']
# 
# col_names_list = []
# scores = pd.DataFrame(dtype="object")
# # for each col, run the lambda function, putting 1 if row,col == upostag else 0, then add col to scores df, add colname to list
# for col in Lemme_cols:
#   run_scorer = results.apply(lambda row: 1 if row[col] == row['C_LEMME'] else 0, axis=1)
#   scores = pd.concat([scores, run_scorer], axis=1)
#   col_names_list.append(col)
# # set names of cols
# scores.columns = col_names_list
# # sul pos scores for each tool
# LEM_sums = [scores[col].sum() for col in scores.columns]
# # zip together col names and sums, exploding the zip into a df with cols for test name and name of score
# output = pd.DataFrame([*zip(scores.columns, LEM_sums)], columns =['tool','L_score'])
# # add col calculating % score
# output['L_perc'] =  output.L_score /len(run_scorer)*100
# # add col to specify text
# output['text'] = textCode
# # move scores to specific df
# LEM_scores_detail = pd.DataFrame(scores)
# output['tool'] = output['tool'].str[2:]
# 
# holding = pd.merge(holding, output, how="left", on=["tool",'text'])

## end dev funcitons
all_holding = pd.DataFrame()
colscores_all = pd.DataFrame()
pos_specific_results =pd.DataFrame()
colscores_all['score'] = range(0, 47)
  list1 = ["du","des", "au", "aux"]
  list2 = ["DET","ADP"]
results=  test_dataLemmMod
## run 
for i in range(10,20):
  name_input_file = "/Volumes/AlphaThree/Binding/newest/_st8/CA" + str(i) + "-st8.csv"
  results = pd.read_csv(GGfile.replace('Tags','Aligned'), sep="\t", low_memory=False)
  textCode = name_input_file[-12:-8]
  # all_cols_list = list(results.columns)
  # run_scorer  = results.apply(lambda row: 1 if row['c'] == row['d'] else 0, axis=1)
  
  holding, output, pos_scores_detail, colscores_all, pos_specific_results = get_posScores(results, textCode, colscores_all, pos_specific_results, 1)
  # holding, output, pos_scores_detail, colscores_all = get_posScores(results, textCode,colscores_all, 1)
  holding, output, LEM_scores_detail = get_LemmeScores(results, holding, textCode, 1 )
  holding, output, Head_scores_detail = get_HeadScores(results, holding, textCode)
  holding, output, Rel_scores_detail = get_RelScores(results, holding, textCode)
  holding_name = name_input_file.replace('-st8','results2')
  details_name = name_input_file.replace('-st8','results-details2')
  holding.to_csv(holding_name, sep="\t", index=False)
  details = pd.concat([results['UUID'], results['ssid'], pos_scores_detail, LEM_scores_detail, Head_scores_detail, Rel_scores_detail ], axis=1)
  # details.to_csv(details_name, sep="\t", index=False)

  all_holding = pd.concat([all_holding,holding], axis=0)
  print("Done " + str(textCode), flush=True)
View(all_holding)


all_holding = all_holding.reset_index(drop=True)
all_holding.to_csv("/Users/username/Desktop/allholding2.csv", sep="\t", index=False)
colscores_all.to_csv("/Users/username/Desktop/colscoresall.csv", sep="\t", index=False)
pos_specific_resultsT.to_csv("/Users/username/Desktop/posspecificresults.csv", sep="\t", index=True)
pos_specific_resultsT = pos_specific_results.T

details_name = "/Users/username/PhraseoRoChe/Pipeline/GoldAnalyses/Gold/NewTest/GoldAll/GG01-allV310_details_results.csv"


```

```{python}

del(GG_MF, remainderB, inputData, twoVoneVone, twoVoneVonePart1, results, details, two2v2, GG_results, two0v1v3)

```


```{python}

sent_scores = details.groupby('ssid').sum()
sent_lens[:] = details.groupby('ssid').agg({'count'})
sent_scores/sent_lens
sent_scores.to_clipboard()
a/b

for col in sent_scores.columns:
  result_col = sent_scores[sent_scores.columns[0]]/sent_lens.values
  resultsFrame = pd.concat([resultsFrame, pd.DataFrame(result_col)], axis=0)

resultsFrame = pd.DataFrame()

```


```{python}
POSbest = ['P_103', 'P_241', 'P_516', 'P_912']
LEMbest = ['L_103', 'L_321', 'L_512', 'L_912']
HEADbest = ['H_242', 'H_271', 'H_272', 'H_512']
RELbest = ['R_242', 'R_512', 'R_516','R_516']
baseSet = ['F_103', 'C_UPOStag', 'C_LEMME','C_HEAD', 'C_DEPREL', 'UUID']

subsample = results[baseSet + POSbest + LEMbest + HEADbest + RELbest]
subsample = subsample[['UUID', 'F_103', 'C_UPOStag', 'P_103', 'P_241', 'P_516', 'P_912', 'C_LEMME','L_103', 'L_321', 'L_512', 'L_912',  'C_HEAD', 'H_242', 'H_271', 'H_272', 'H_512', 'C_DEPREL', 'R_242', 'R_512', 'R_516', 'R_516']]

subsample.to_csv("/Users/username/Desktop/subsampleGG.csv", sep="\t", index=False)

```

# test calculations
```{python}

subsample1 = subsample.dropna(subset=["F_103"])
  col_names_list = []
  scores = pd.DataFrame(dtype="object")

  for col in POSbest:
    run_scorer = subsample1.apply(lambda row: 1 if row[col] == row['C_UPOStag'] else  ( 1  if ((row['C_UPOStag'] == "ADP.DET") & (row['F_103'] in list1) & (row[col] in list2)) else 0) , axis=1)
    scores = pd.concat([scores, run_scorer], axis=1)
    col_names_list.append(col)
  # set names of cols
  scores.columns = col_names_list
  # sul pos scores for each tool
  pos_sums = [scores[col].sum() for col in scores.columns]
  # zip together col names and sums, exploding the zip into a df with cols for test name and name of score
  output = pd.DataFrame([*zip(scores.columns, pos_sums)], columns =['tool','p_score'])
  # add col calculating % score
  output['p_perc'] =  output.p_score /len(run_scorer)*100
  # add col to specify text
  output['text'] = textCode
  # move scores to specific df
  pos_scores_detail = pd.DataFrame(scores)
  output['tool'] = output['tool'].str[2:]
  holding = output
  pos_scores_detail['row_sum'] = pos_scores_detail.sum(axis=1)
  text_col_scores = pd.DataFrame(pos_scores_detail.row_sum.value_counts())
  text_col_scores.columns = [ textCode]
  text_col_scores['score'] = text_col_scores.index.values
  
  ## get POS predictions and POS scores, bind
  pos_data_detail = pd.concat([subsample[POSbest],pos_scores_detail], axis=1)
  ## add on UUID and forme cols
  pos_data_detail = pd.concat([subsample[['UUID','F_103','C_UPOStag']], pos_data_detail ], axis=1)

# now have df of Forme, UUID, POS predicitons and scores
## remove PUNCT

View(pos_data_detail)
pos_data_detail = pos_data_detail.dropna(subset=["F_103"])
pos_data_detail = pos_data_detail.reset_index(drop=True)

# numbe rof correct preductions == row sum already present
## number of predicted candidates

pos_data_detail['candidates'] = [len(list(set(pos_data_detail.iloc[i,3:7]))) for i in range(len(pos_data_detail))]

[corr_found]
pos_data_detail['group'] = ""
# Group1 = 4 votes, all for correct prediction
pos_data_detail['group'] = ['Group1' if ((pos_data_detail.iloc[i,12] ==1) & (pos_data_detail.iloc[i,11] ==4)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

# Group2 = 4 votes, all for wrong prediction
pos_data_detail['group'] = ['Group2' if ((pos_data_detail.iloc[i,12] ==1) & (pos_data_detail.iloc[i,11] ==0)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

# group3 : 3 votes for correct pred., 2 candidates
pos_data_detail['group'] = ['Group3' if ((pos_data_detail.iloc[i,12] ==2) & (pos_data_detail.iloc[i,11] ==3)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

## idxmax to get i value of max, this is i in output.iloc[i,0] which gets tool name
# target_col = "P_" + str(output.iloc[output['p_score'].idxmax(),0])
# index
# pos_data_detail.columns.get_loc(target_col)


a if a & b & c else( d if e and f else g) for i in i
pos_data_detail['group'] = ['Group4' if ((pos_data_detail.iloc[i,12] ==2) & (pos_data_detail.iloc[i,11] ==2) & (pos_data_detail.iloc[i,8] ==1)) else ('Group4e' if ((pos_data_detail.iloc[i,12] ==2) & (pos_data_detail.iloc[i,11] ==2)) else pos_data_detail.iloc[i,13])  for i in range(len(pos_data_detail))]

# group5 : 1 votes for correct pred., 2 candidates
pos_data_detail['group'] = ['Group5' if ((pos_data_detail.iloc[i,12] ==2) & (pos_data_detail.iloc[i,11] ==1)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

# group6 : 0 votes for correct pred., 2 candidates
pos_data_detail['group'] = ['Group6' if ((pos_data_detail.iloc[i,12] ==2) & (pos_data_detail.iloc[i,11] ==0)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

# group7 : 2 votes for correct pred., 3 candidates
pos_data_detail['group'] = ['Group7' if ((pos_data_detail.iloc[i,12] ==3) & (pos_data_detail.iloc[i,11] ==2)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

# group 8 : 3 candidates, 1 correct vote
pos_data_detail['group'] = ['Group8' if ((pos_data_detail.iloc[i,12] ==3) & (pos_data_detail.iloc[i,11] ==1)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

# group 9 : 4 candidates, 1 correct vote
pos_data_detail['group'] = ['Group9' if ((pos_data_detail.iloc[i,12] ==4) & (pos_data_detail.iloc[i,11] ==1)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

# group 10 : 4 candidates, 0 correct vote
pos_data_detail['group'] = ['Group10' if ((pos_data_detail.iloc[i,12] ==4) & (pos_data_detail.iloc[i,11] ==0)) else pos_data_detail.iloc[i,13]  for i in range(len(pos_data_detail))]

pos_data_detail['group'].value_counts().sum()
internal_state = pos_data_detail['group'].value_counts()
internal_state = internal_state.reset_index(drop=False)
internal_state.columns = ['group','count']

# get 4-0 and 0-4 data
grouplist1 = ['Group1','Group2']
grouplist2 = ['Group3','Group4','Group5','Group6']
grouplist3 = ['Group7','Group8']
grouplist4 = ['Group9','Group10']
maj_list = ['Group1','Group3','Group7']
internal_state['cand'] = [1 if internal_state.iloc[j,0] in grouplist1 else (2 if internal_state.iloc[j,0] in grouplist2 else (3 if internal_state.iloc[j,0] in grouplist3 else (4 if internal_state.iloc[j,0] in grouplist4 else 0))) for j in range(len(internal_state))]


corr_score =  int(internal_state.loc[internal_state['group'] == "Group1"].iloc[0,1]) +  int( internal_state.loc[internal_state['group'] == "Group3"].iloc[0,1]) + int( internal_state.loc[internal_state['group'] == "Group4"].iloc[0,1]) +  int( internal_state.loc[internal_state['group'] == "Group7"].iloc[0,1]) 
corr_score/internal_state['count'].sum() *100 # = 93.94760614272809 %, == 0,7 % improvement in GG, but need to consider present errors in gold labels

output_current = output_state

# get 3-1 and 0-4 data
```

