# fonction optionnelle pour compresser les fichiers 100.csv :  tidier, to move processed 100 files to custom_folder, zip, delete

theseFiles = pd.DataFrame(glob.glob(main_directory +'/*100.csv'))
theseFiles = theseFiles.sort_values(0)
theseFiles.reset_index(inplace=True, drop=True)
theseFiles.columns=['Path']
Zippath = main_directory + "100/"
directory = pathlib.Path(Zippath)
outputName = Zippath.replace("/100/","/100") + ".zip"
if os.path.exists(Zippath)==False:
    os.makedirs(Zippath)

for i in range(len(theseFiles)):
  shutil.move(theseFiles.iloc[i,0], theseFiles.iloc[i,0].replace(main_directory,Zippath))  #
  
with ZipFile(outputName, "w", ZIP_DEFLATED, compresslevel=9) as archive:
     for file_path in directory.rglob("*"):
         archive.write(file_path, arcname=file_path.relative_to(directory))

shutil.rmtree(directory)
