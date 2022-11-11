# en dev actif ; prend csv of 9state

##  Dicts
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

###### end of declatations, start of loop to import files
## define directories and input files
NineState = pd.read_csv(f'{PathHead}'temp/AF01-909.csv", encoding = "UTF8", sep="\t", skip_blank_lines=False, na_filter=True, low_memory=False)
NineState.columns  

  ## remap AUX to VERB pour UD, Stanza, HOPS pour ne pas les distinguer, using list comprehension and map
for i in range(len(RemapAUXverbColumns)):
    currentColumn =RemapAUXverbColumns[i]
    NineState[currentColumn] =  NineState[currentColumn].map(HOPS_StanzaRemapDict).fillna(NineState[currentColumn])

  # remap XPOS to UPOS with list comprehension and map
for i in range(len(XPOSremapColumns)):
    currentColumn =XPOSremapColumns[i]
    NineState[currentColumn] =  NineState[XPOSremapColumnSources[i]].map(XposUposRemapDict).fillna(NineState[XPOSremapColumnSources[i]])


test = NineState[['TokenID', 'LGERM_Forme','LGERM_lemme','LGERM_UPOS', 'Pp','hopsID_x', 'sentID','col4', 'col5', 'HOPSdepOn', 'HOPSrole', 'col8', 'col9']]

# ## get 100sentences, move punct to previous word
# i=2594

## restrict test to test range of 100 sentences
test = test.loc[test["TokenID"] < 2595] ## get first 100 sentences from token ids, ponct has no sent id

## choose columns
fordict = test[['TokenID','hopsID_x','LGERM_Forme','LGERM_lemme','LGERM_UPOS','col4', 'col5', 'HOPSdepOn', 'HOPSrole', 'col8', 'col9', 'sentID','Pp']] ## get 10 cols for conll and TokenIDcol

# define list of what is punctuation
PONtypes = {"PON", "PONfbl", "PONfbl%", "PONpdr", "PONpga", "PONpxx", "PONfrt"}

# create list to store rows to drop
theseRows = [ ]

# add new col on end to store punct
fordict['spillover'] = ""

# for row i if next forms is punct but subsequent is not, put punct in col11, and add row number to delete list
## second if : for each row i, if next and following forms are punct, send both forms to the punct col for i, and add i+1 and i+2 to rows to be deleted
## AF01 has random square bracket at n=45173 with na as token
for i in range(len(fordict)-2):
  if (fordict.iloc[i+1,4] in PONtypes) &((fordict.iloc[i+2,4] not in PONtypes)) :
    fordict.iloc[i,13] = str( fordict.iloc[i+1,2] )
    theseRows.append(i+1)
  if (fordict.iloc[i+1,4] in PONtypes) &((fordict.iloc[i+2,4] in PONtypes)) :
    fordict.iloc[i,13] = str( fordict.iloc[i+1,2]) + str(fordict.iloc[i+2,2])
    theseRows.append(i+1)
    theseRows.append(i+2)

## process end of text : to add final punct to final word
f = len(fordict)
  if fordict.iloc[f-1,4] in PONtypes:
    fordict.iloc[f-2,13] =    fordict.iloc[(f-1),2]
    theseRows.append(f-1)

# drop rows from delete list
fordict2 = fordict.drop(set(theseRows))


## convert floats to int in names cols, all na values should be dropped in lines with punct
theseColumns = ['hopsID_x', 'HOPSdepOn', 'sentID']

for c in range(len(theseColumns)):
  thisColumn = theseColumns[c]
  fordict2[thisColumn] = fordict2[thisColumn].astype('int')

## reset index as lines have been dropped and send to new df
newdf = fordict2.reset_index(drop=True)


# write outputbody : put all text in cell
newdf['output'] = pd.DataFrame([( str(newdf.iloc[i,1]) + "\t" +  str(newdf.iloc[i,2]) + "\t" + str(newdf.iloc[i,3]) + "\t" + str(newdf.iloc[i,4]) + "\t" + str(newdf.iloc[i,5]) + "\t" + str(newdf.iloc[i,6]) + str(newdf.iloc[i,7]) + "\t" + str(newdf.iloc[i,8]) + "\t" + str(newdf.iloc[i,9]) + "\t" +  str(newdf.iloc[i,10]) ) if i > 0 else ""  for i in range(len(newdf)) ])

# write dec for start of sentences
newdf['sentDec'] = pd.DataFrame([( "</s><s id=\"s" + str(newdf.iloc[i,11]) + "\" newdoc_id=\"ArtusDeBretagne\" " + "sent_id=\"" + str(newdf.iloc[i,11]) + "\">" ) if newdf.iloc[i,11] >newdf.iloc[i-1,11] else "" for i in range(1, len(newdf)) ])

## write dec for start of page  
newdf['pDec'] = pd.DataFrame([( "</p><p id=\"p" + str(newdf.iloc[i,12]) + "\">" ) if newdf.iloc[i,12] > newdf.iloc[i-1,12] else "" for i in range(1, len(newdf)) ])

## write dec for first page, sent
newdf.iloc[0,16] =  "<p id=\"p" + str(newdf.iloc[0,12]) + "\">" ## page dec
newdf.iloc[0,15] =  "<s id=\"s" + str(newdf.iloc[0,11]) + "\" newdoc_id=\"ArtusDeBretagne\" " + "sent_id=\"" + str(newdf.iloc[0,11]) + "\">" ## sent dec

## write dec for end body
EndDec = "\n</s></p></text></doc></corpus>"
t = len(newdf)-1
newdf.iloc[t,14] = ([( str(newdf.iloc[t,1]) + "\t" +  str(newdf.iloc[t,2]) + "\t" + str(newdf.iloc[t,3]) + "\t" + str(newdf.iloc[t,4]) + "\t" + str(newdf.iloc[t,5]) + "\t" + str(newdf.iloc[t,6]) + str(newdf.iloc[t,7]) + "\t" + str(newdf.iloc[t,8]) + "\t" + str(newdf.iloc[t,9]) + "\t" + str(newdf.iloc[t,10]) + EndDec )  ])

## dec for end page
newdf.iloc[t,16] = ([( "</p><p id=\"p" + str(newdf.iloc[i,12]) + "\">" ) if newdf.iloc[t,12] > newdf.iloc[t-1,12] else ""  ])

## dec for end sent
newdf.iloc[t, 15] = ([( "</s><s id=\"s" + str(newdf.iloc[t,11]) + "\" newdoc_id=\"ArtusDeBretagne\" " + "sent_id=\"" + str(newdf.iloc[t,11]) + "\">" ) if newdf.iloc[t,11] >newdf.iloc[t-1,11] else ""  ])


# newdf.to_csv("f'{PathHead}/testconll.txt", sep="\t", index=False)
# newdf['allOut'] = ""
## v slow with 4x ifs, need to put into 4 LCs, or fewer if poss  

## paste together the created declarations avoiding NAs::
# create new col and use this to paste together created elements : 4 LCs to keep comprehensible. If page and Sent declarations are blank, final declaration is outputbudy
newdf['allOut'] = ( [ newdf.iloc[i, 14]  if ((newdf.iloc[i,16] == "") & (newdf.iloc[i,15] == "")) else newdf.iloc[i, 17] for i in range(len(newdf))])

## if page declaration is blank and sent declaratin is present, final declaration is sentDec + outputbody
newdf['allOut'] = ( [ str(newdf.iloc[i, 15]) + str(newdf.iloc[i, 14])  if ((newdf.iloc[i,16] == "") & (newdf.iloc[i,15] != "")) else newdf.iloc[i, 17] for i in range(len(newdf))])

## if pqge dec is present and sent dec is blank, final declaration is pageDec + outputbody
newdf['allOut'] = ( [ str(newdf.iloc[i, 16]) + str(newdf.iloc[i, 14])  if ((newdf.iloc[i,16] != "") & (newdf.iloc[i,15] == "")) else newdf.iloc[i, 17] for i in range(len(newdf))])

## if page dec is present and sentDec is present, then final output is page dec + sentDec + outputBody
newdf['allOut'] = ( [ str(newdf.iloc[i, 16]) + str(newdf.iloc[i, 15]) + str(newdf.iloc[i, 14]) if ((newdf.iloc[i,16] != "") & (newdf.iloc[i,15] != "")) else newdf.iloc[i, 17] for i in range(len(newdf))])

# path to XML file with XLMLheader
xmlheaderPath = 'C:/Users/renwicka/Dropbox/IRGA Phraseo 13-18/Pipeline/TEIHeaderModele.xml'
# open, read, close XMLhead
xmlhead = open(xmlheaderPath, "rt", encoding = 'UTF8')
xmlhead_data = xmlhead.read()
xmlhead.close()

## open outputfile
fin = open(f'{PathHead}/testconll.conll', "wt", encoding = 'UTF8')
# write XMLhead
fin.write(xmlhead_data)
# write bits left out of initial draft of xmlhead
fin.write('<body>\n<corpus>\n<doc>\n<text>\n')
# iterate over body
for i in range(len(newdf)):
  fin.write(str(newdf.iloc[i, 17] + "\n"))
# write closing tags for XML
fin.write('</body>\n</TEI>')
# close file connection
fin.close()

