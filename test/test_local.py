
import PIL, PIL.Image
import os
from mapdb import MapDB

# params
BUILD = False

# init
db = MapDB('testdata/test_local.db')

# build
if BUILD:
    root = r'C:\Users\kimok\OneDrive\Documents\GitHub\AutoMap\tests\testmaps'
    for fil in os.listdir(root):
        if fil.endswith(('.png','.jpg')) and 'georeferenced' not in fil and 'debug' not in fil:
            print fil

            if fil < 'ierland-toeristische-attracties-kaart.jpg':
                continue

            ###
            pth = root+"/"+fil
            img = PIL.Image.open(pth)
            if img.size[0] > 2000 or img.size[1] > 2000:
                continue
            db.process(pth, img, textcolor=(0,0,0))

# explore db
for row in db.query('select link,width,height,transform from maps'):
    print list(row)
db.view_image(2)
db.view_georef(2)


