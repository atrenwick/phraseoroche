
#9. TraiterHOPS -- txt in, csv out
## tag with HOPS

main_directory = f'{PathHead}'/refresh/test2/'
dossier_temp = main_directory + "temp/"
hopsHeader = "hopsparser parse "
parserLocation = f'{PathHead}/UD_Old_French-SRCMF-flaubert+fro/'

# identifier les fichiers créés par f2 dans le dossier_temp
theseFiles = pd.DataFrame(glob.glob(dossier_temp +'/*201.txt'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']

for f in range(len(theseFiles)):
# for f in range(0,1):
  file_name = theseFiles.iloc[f,0] 
  HOPSinputFilename= file_name.replace(' ','\ ')
  ### need line here to check for dropped sentences : if dropped sentences, output name = 02, otherwise, outputname = 03

  if os.path.exists(file_name.replace('201','211'))==True:
        SaveValue = '202'
  else:
    SaveValue='203'

  HOPSoutputFilename = HOPSinputFilename.replace('201',SaveValue).replace('.txt', '.csv')
  command = f'{hopsHeader}{parserLocation} {HOPSinputFilename } {HOPSoutputFilename}'
  os.system(command)
  # os.system('say "J\'ai fini"')
  # print(SaveValue)
  
#≈ 6k w/min
## throws error if text contains point mediane
## AF10 has prob char at 19864 ; … char not recog in XML, correct in csv, wrong in Tidy

# del(command, dossier_temp, f, file_name, hopsHeader, HOPSinputFilename, HOPSoutputFilename, main_directory, parserLocation, theseFiles)
