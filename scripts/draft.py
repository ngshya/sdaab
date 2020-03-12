import io
import pandas as pd
import pickle
from sdaab.disk.storage_disk import StorageDisk
s = StorageDisk("/home/ngshya/projects/sdaab/tmp/")
s.ls()

dtf_tmp = pd.DataFrame({"A": [1,2,3], "B": [4,5,6]})
s.upload_from_memory(dtf_tmp, "dtf_tmp")



'''
s.upload_from_memory(dtf_tmp, file="dtf_tmp")
dtf_tmp_2 = s.download_to_memory(file="dtf_tmp")



f = io.StringIO()
dtf_tmp.to_csv(f)
s.upload(f, "nome_che_voglio")


f = s.download("nome_che_voglio")
s.load(f)
'''