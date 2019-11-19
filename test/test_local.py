
import PIL, PIL.Image
import os
from mapdb import MapDB

db = MapDB('test_local.db')

root = r'C:\Users\kimok\OneDrive\Documents\GitHub\AutoMap\tests\testmaps'
for fil in os.listdir(root):
    if fil.endswith(('.png','.jpg')) and 'georeferenced' not in fil and 'debug' not in fil:
        print fil

        ###
        img = PIL.Image.open(root+"/"+fil)
        if img.size[0] > 3000:
            continue
        db.process(fil, img)

