# This file makes sure that modules in the 'src' directory in the root of this
# project can be imported in Jupyter notbooks in the directory that contains 
# this file.

import sys
import os

module_path = os.path.abspath(os.path.join(os.pardir))
if module_path not in sys.path:
    sys.path.append(module_path)