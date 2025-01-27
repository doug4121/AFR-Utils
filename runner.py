import sys

sys.path.insert(1, '/AFR-Core')

from fawkes.protection import Fawkes
import os

mode = sys.argv[2]
extension = sys.arg[3]
image_paths = {sys.argv[1]}
my_fawkes = Fawkes("extractor_2", '0', 1, mode)
my_fawkes.run_protection(image_paths, debug=True, format=extension)