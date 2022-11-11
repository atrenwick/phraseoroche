#8. Traiter w Stanza
###### Fonction qui prendra la sortie de Stanza et l'ajoutera à la df EXISTING, qui contient toutes les autres données, 
#### importer les libraries nécessaires

main_directory = f'{PathHead}'/refresh/test2/'
dossier_temp = main_directory + "temp/"
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
StanzaSpeedResults = pd.DataFrame()
myDtypes = {"StanzaID": int, "StanzaForme": object, "StanzaUPOS": object, "StanzaXPOS": object, "StanzaFeats": object, "StanzaDepOn": int}
nlp = stanza.Pipeline(lang='fro', processings='tokenize,pos,lemma,depparse', tokenize_pretokenized=True)

CorpusColNames = ['TokenID', 'LGERM_Forme', 'LGERM_XPOS', 'LGERM_lemme', 'LGERM_code','d','f','Pp','Para', 'TokenChap', 'FormeOut','hopsID','sentID'] 
StanzaColNames =['StanzaID', 'StanzaForme', 'blank1','StanzaUPOS','StanzaXPOS','StanzaFeats','StanzaDepOn','blank2','blank3','start_end_char']
  # f=1
# for f in range(1,2):
for f in range(len(theseFiles)):
  startTime = time.time()
  file_name = theseFiles.iloc[f,0]
  #### réduire le nom du fichier au code de l'ouvrage en supprimant le suffixe et le chemin
  currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
  ## importer les données du fichier ; laisser pd décider des types de données dans ces gros df avec l'argument low_memory
  # StanzaOutputName = file_name.replace('-104.csv', '-600.csv')
  corpus = pd.read_csv(file_name, low_memory=False, sep="\t")#, header=None
  #### ajouter une colonne qui contient le code du texte
  corpus.columns = CorpusColNames
  corpus['texte'] = currentCode
#### on va maintenant extraire le texte, une phrase à la fois
## déterminer le nombre de phrases dans le texte : trouver la valeur maximale en forcant pd à comprendre les entrées comme des entiers
  SentenceCount = corpus[['sentID']].apply(pd.to_numeric, errors='coerce').max()[0]
  #### créer un df vide dans lequel stocker les phrases analysées
  processed = pd.DataFrame()
  ### pour chaque phrase entre le début et la valeur maximale, fais le suivant ; ajouter 1 à la dernière valeur, car range va jusqu'à cette valeur sans la comprendre
  for s in range(1, int(SentenceCount)+1):
  # for s in range(1,10):
    # get words from sentence : la phrase est constituée de tous les rangs dont la valeur de la colonne HOPSsentID est égale à s, qui est un entier
    sentence = corpus.loc[corpus['sentID']== s]
    # get index of start of sentence. Déterminer l'index de la valeur minimale = du 1er mot de la phrase.
    thisMin = (sentence.index).min()
    # get index of end of sent. Déterminer l'index de la valeur maximale de la phrase = de la ponctuation se trouvant en fin de phrase, et pour inclure cette ponctuation, ajouter 1 à cette valuer
    thisMax = (sentence.index).max() +1
    # get all rows between min, max+1. Les mots de la phrase sont donc les rangs entre ces limites, donc on extrait les rangs en question.
    sentenceA = corpus[thisMin:thisMax]
    ## get indices of words in sentence, as these are TokenIDs to match on later. On extrait aussi l'index de ces rangs, pour associer l'index, qui est le TokenID, et le mot/ponct.
    sentenceIndex = pd.Series(corpus[thisMin:thisMax].TokenID)
    ## get only word column. On s'intéresse uniquement aux mots, donc on ne prend que la colonne LGERM_Forme. On a une série de mots qui est notre phrase.
    sentenceA = sentenceA[['LGERM_Forme']]
    
    ### on crée un string pour stocker la suite de formes qui constituent la phrase ; au début, il est vide.
    currentSentence = ""
    ## pour chaque rang, et donc forme, on extrait la forme en tant que string, ajoute une espace puis la colle après les formes précedentes jusqu'à ce que la phrase soit un string complet.
    for w in range(len(sentenceA)):
      thisWord = str(sentenceA.iloc[w,0] )
      if thisWord == '\x85':
        thisWord = html.unescape('&hellip;')
      currentSentence = str(currentSentence) + " " + thisWord
    
    ### une fois toute la phrase est dans un string, on traite le string avec le pipeline de stanza pour créer un doc    
    doc = nlp(currentSentence)
    #### on transforme le doc en dictionnaire avec la méthode to_dict() qui prendra les paires cle:valeur et les transformera en dictionnaire
    dicts = doc.to_dict() ## convert doc to dicts
    #### on convertit ce dictionnaire en format conll
    
    conll = CoNLL.convert_dict(dicts) ## convert dicts to conll 
    ## on extrait les données conll de notre phrase en les mettant dans un df
    testingout  = pd.DataFrame(conll[0]) ## extract conll data
     ##### on donne des noms aux colonnes
    
    testingout.columns =StanzaColNames
   #### on se limite aux colonnes avec des données qui sont pertinentes et en changeant l'ordre si souhaité
    testingout = testingout.drop(['blank1','blank2', 'blank3', 'start_end_char'], axis=1)
    ### on définit le type de données de chaque colonne : on laisse le texte en objet, mais on force les données chiffrées en entiers, et pas des float
    testingout = testingout.astype(myDtypes)
     ### on ajoute une colonne qui comprend l'index des mots extraits ; ces index sont des TokenID
    testingout['TokenID'] = sentenceIndex.values
    
      #### après ces étapes, on met les données pour la phrase actuelle en bas de notre df de stockage, et la boucle recommence
    processed = pd.concat([processed, testingout])
    ### enregistrer la df construit, qui contient mtnt toutes les phrases, avec séparateur comme indiqué, et sans index
    StanzaOutputName = file_name.replace('-103.csv', '-600.csv')
    processed.to_csv(StanzaOutputName, sep="\t", index=False)
  # os.system('say "J\'ai fini avec Stanza"')


# print(time.time())  
## stanza seems to run about 1700 wpm on iMac, whilst simultaneously running HOPS but with misplaced say command running too………
## 49s for 18000w without HOPS running = 18000/49*60 ≈ 22k wpm
## HOPS runs faster : 5800 wpm on imac whilst simultaneoysly running Stanza

# del(conll, corpus, currentCode, currentSentence, dicts, doc, dossier_temp, f, file_name, main_directory, nlp, processed, s, sentence, sentenceA, SentenceCount , sentenceIndex, StanzaOutputName, testingout, theseFiles, thisMax, thisMin, thisWord, w)

