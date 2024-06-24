import sys
import pandas as pd

print(sys.argv)

# day = sys.argv[0] file name
if len(sys.argv) > 1:
    day = sys.argv[1]
else:
    day = '2020-01-01' 

# some fancy stuff with pandas 
print(f'job finished successfully for day = {day}')