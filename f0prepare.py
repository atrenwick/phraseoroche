#f0Prepare

import pandas as pd
import platform
import glob
import html
import webbrowser
import stanza
import shutil
import pathlib
from zipfile import ZipFile, ZIP_DEFLATED
import time
import numpy as np
import os



#  EN declarepaths to home and main directory folders, replacing userName with user name
# FRDéclarations pour système d'exploitation, chemin et fichiers d'entrée
if platform.system() == "Darwin":
  PathHead = "/Users/userName/"
elif platform.system() == "Windows":
  PathHead = "C:/Users/userName/"
elif platform.system() == "Linux":
  PathHead = "C:/Users/userName/"## insérer chemin linux ici
main_directory = f'{PathHead}/Dropbox/IRGA Phraseo 13-18/Pipeline/refresh/test2'
