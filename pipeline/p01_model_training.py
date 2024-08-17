# Libraries -----------------------------------------------------------------------------------
import os
import sys

import params as params

sys.path.append(os.getcwd())

# Defining executable file extensions -----------------------------------------------------------------------------------

if params.operating_system == "Windows":
    binary_extensions =".exe"
else:
    binary_extensions = ""

# Info preprocessing -----------------------------------------------------------------------------------

print("-----------------------------------------------------------\nPreprocessing Initialization...\n-----------------------------------------------------------")

# Preprocessing -----------------------------------------------------------------------------------
os.system(f"python{binary_extensions} preprocessing/a01_preprocessing.py")