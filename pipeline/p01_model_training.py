# Libraries -----------------------------------------------------------------------------------
import os
import sys
sys.path.append(os.getcwd())

import params as params

# Defining executable file extensions -----------------------------------------------------------------------------------

if params.operating_system == "Windows":
    binary_extensions =".exe"
else:
    binary_extensions = ""

# Info preprocessing -----------------------------------------------------------------------------------

print("-----------------------------------------------------------\nPreprocessing Initialization...\n-----------------------------------------------------------")

# Preprocessing -----------------------------------------------------------------------------------
os.system(f"python{binary_extensions} preprocessing/a01_preprocessing.py")