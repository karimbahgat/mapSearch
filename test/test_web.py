
from mapdb import MapDB

import PIL, PIL.Image
import os
import urllib
import io

# params
SCRAPE = False
ROOT = "https://legacy.lib.utexas.edu/maps/thailand.html" # should be part of the perry cestanada website

# init
db = MapDB('testdata/test_web.db')

if SCRAPE:
    # define images to process
    raw = urllib.urlopen(ROOT).read()
    elems = raw.replace('>', '<').split('<')

    # loop image links, process, and insert
    for elem in elems:
        if elem.startswith('a href='):
            url = elem.replace('a href=', '').strip('"')

            if url.endswith(('.png','.jpg','.gif')):
                if not url.startswith('http'):
                    url = 'https://legacy.lib.utexas.edu/maps/' + url

                if db.get('select 1 from maps where link = ?', (url,) ):
                    # skip if already exists
                    continue

                print 'loading', url
                fobj = io.BytesIO(urllib.urlopen(url).read())
                img = PIL.Image.open(fobj)

                if img.size[0] > 3000:
                    continue

                print 'processing'
                db.process(url, img)





# test inspect maps
for row in db.query('select link,width,height,transform from maps'):
    print list(row)
db.view_image(2)
db.view_georef(2)




# explore footprints (hmmm...)
##import pythongis as pg
##import json
##d = pg.VectorData()
##for row in db.query('select link,mapregion from maps'):
##    d.add_feature([], json.loads(row['mapregion']))
##d.view()

# easier way
#db.view_footprints((r['map'] for r in db.search_text('%indus%')))



# MAJOR NEXT STEP FOR GEOREF
# 1:
# USE COLOR EDGES TO DETERMINE IMAGE OBJECTS
# CALC AVG COLOR OF EACH OBJECT
# RUN TEXT DETECTION ON EACH COLOR?
# 2:
# COMPARE W LITERATURE APPROACH OF MORPH DILATION TO DETECT TEXT BLOBS

# TODO FOR MAPDB
# Maybe create separate class for Map, MapText, and TiePoint
# return these from queries instead of sqlite row instances
# search_text() returns MapText instances, whose .map attr references the Map instance
# view_image() can take list of MapText instances to display (eg from a search), otherwise show all
# view_footprints can take list of Map instances



