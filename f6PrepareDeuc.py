#6. Export Deuc
##### make file for DeucX defining directory and files
main_directory = f'{PathHead}'/refresh/test2/'
dossier_Deuc = main_directory + 'PourDeucalion/'
dossier_temp = main_directory + "temp/"

# identifier les fichiers créés par f1 dans le dossier_temp
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*103.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
DeucMakerResults = pd.DataFrame()
for f in range(len(theseFiles)):
  # file_name = LGERMtidyName
# startTime = time.time()
# get input file, and get code from same
file_name = theseFiles.iloc[f,0] 
currentCode = file_name.replace(dossier_temp,'').replace('-103.csv','')
# read csv as df, apply col names
step2 = pd.read_csv(file_name, sep="\t", encoding = "UTF8", low_memory=False)
## step2 ==  is the tidy df, standardise col names
step2.columns =['TokenID', 'LGERM_Forme', 'LGERM_XPOS', 'LGERM_lemme', 'LGERM_code','d','f','Pp','Para', 'TokenChap', 'FormeOut','hopsID','sentID'] 

# declare pontypes and suffixes
Deuc_PONtypes = {"ponctuation"}
mySuffixes = ["-EMF-310.csv","-OF-320.csv", "-FMod-330.csv"]

### restrict to text only lines of LGERM to generate Deucalion Input by removing punctiation
DeucNoStops =step2[~step2['LGERM_code'].isin(Deuc_PONtypes)] 
toDeucOutput = DeucNoStops[['TokenID','LGERM_Forme']]

## LC to tidy deucalion to avoid line additions : replace apostrophe, fullstop; dash and median point with blank for each row of df
forDeucalion = pd.DataFrame([toDeucOutput.iloc[h,1].replace("'","").replace(".","").replace("-","").replace("·","")   for h in range(len(toDeucOutput)) ])

## write files - text file raw for deucalion, and binder, which is same but with TokenIDs for matching later
for s in range(len(mySuffixes)):
  currentName = dossier_Deuc +  currentCode + mySuffixes[s]  
  currentNameBinder = currentName.replace('.csv','binder.csv').replace(dossier_Deuc,dossier_temp)
  forDeucalion.to_csv(currentName, sep="\t", index=False, encoding="UTF8", header=None)
  toDeucOutput.to_csv(currentNameBinder, sep="\t", index=False, encoding="UTF8", header=None)

## results
# endTime = time.time()
# result = {'code':[currentCode], 'wCount':[len(forDeucalion)], 'start':[startTime],'end':[endTime], 'delta':[endTime - startTime], 'speed':[len(forDeucalion)/(endTime - startTime) ]}
# result = pd.DataFrame(result)
# DeucMakerResults = DeucMakerResults.append(result)
# print(DeucMakerResults)
## runs at about 36k w/s

# del(currentCode, currentName, currentNameBinder, Deuc_PONtypes, DeucNoStops, dossier_Deuc, f, file_name, forDeucalion, h, mySuffixes, s, step2, theseFiles, toDeucOutput)
# define URL for deucalionhome, and links to specific models
baseURL = "https://dh.chartes.psl.eu/deucalion/"
EMFLink = baseURL + "freem"
OFLink = baseURL + "fro"
FModLink = baseURL + "fr"

## open deucalion link in new tab ; need to look at open txt file, copy to clipboard
webbrowser.open(OFLink, new=2)
## display message:
## open folder, and file, copy all, paste into box, hit tag
## when done, SaveAs to DeucalionSubDirectory, use same name, add OUT and put CSV as extension.
## 


del(working, ToRemove, resultFrame)
