# Libraries -----------------------------------------------------------------------------------
import os
import sys

import params as params
# Append current working directory
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

# Model Training and Determining the Optimal Number of Clusters for Dataset Compression -----------------------------------------------------------------------------------
os.system(f"python{binary_extensions} models/b01_model_creation.py")