
import automap as mapfit
#from automap.main import quantize, mask_text, image_segments, threshold, detect_data, process_text, detect_text_points, connect_text, find_matches, warp
#from automap.rmse import polynomial, predict

import PIL, PIL.Image

import sqlite3
import json
import os
import warnings

warnings.simplefilter('always')




class MapDB(object):
    def __init__(self, path):
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.cur = self.db.cursor()
        self.cur.execute('create table if not exists maps (link text, added DATETIME DEFAULT CURRENT_TIMESTAMP, width int, height int, image blob, transform text, xmin real, ymin real, xmax real, ymax real, title text, mapregion text, legend text)')
        self.cur.execute('create table if not exists maptext (map int, text text, conf real, fontheight int, top int, left int, width int, height int, function text)')
        self.cur.execute('create table if not exists maptiepoints (map int, col real, row real, x real, y real)')

    # ingesting

    def process(self, link, img, **kwargs):
        print 'processing img'
        #result = process_image(img)
        params = dict(db=r"C:\Users\kimok\Desktop\BIGDATA\gazetteer data\optim\gazetteers.db", source='best')
        params.update(kwargs) # override with user input
        params.update(dict(warp=False)) # override with hardcoded defaults
        result = mapfit.automap(img, **params)
        
        print 'inserting info'
        
        # map info
        # image
        w,h = result['image']['width'], result['image']['height']
        # transform
        transform = result.get('transform_estimation', None) # json storage of the estimated forward and backward transforms
        # bbox
        if 0: #'bbox' in result:
            xmin,ymin,xmax,ymax = result['bbox']
        else:
            xmin=ymin=xmax=ymax = None
        # segmentation
        mapregion = next((f['geometry'] for f in result['segmentation']['features'] if f['properties']['type']=='Map'), None)
        # insert
        vals = (link, w, h, json.dumps(transform), xmin,ymin,xmax,ymax, json.dumps(mapregion) )
        self.cur.execute('insert into maps (link, width, height, transform, xmin, ymin, xmax, ymax, mapregion) values (?, ?, ?, ?, ?, ?, ?, ?, ?)', vals)
        mapID = self.cur.lastrowid

        # text info
        for f in result['text_recognition']['features']:
            vals = [mapID] + [f['properties'][k] for k in 'text_clean, conf, fontheight, top, left, width, height'.split(', ')]
            self.cur.execute('insert into maptext (map, text, conf, fontheight, top, left, width, height) values (?, ?, ?, ?, ?, ?, ?, ?)', vals)

        # gcps info
        if 'gcps_final' in result:
            gcps = result['gcps_final']
            for f in result['gcps_final']['features']:
                col,row,x,y = [f['properties'][k] for k in 'origx origy matchx matchy'.split()]
                vals = [mapID] + [col,row,x,y]
                self.cur.execute('insert into maptiepoints values (?,?,?,?,?)', vals)

        else:
            pass

        self.db.commit()

    # inspecting

    def query(self, sql, params=None):
        params = tuple(params) if params else tuple()
        return self.cur.execute(sql, params)

    def get(self, sql, params=None):
        return self.query(sql, params).fetchone()

    def search_text(self, text):
        return self.query('''select maps.link, maptext.*
                            from maps,maptext
                            where maptext.map = maps.oid
                            and maptext.text like ? ''', (text,) )

    # viewing

    def view_image(self, mapID):
        import pythongis as pg
        import urllib
        import io

        # get map info
        link,width,height,mapregion = self.get('select link,width,height,mapregion from maps where oid=?', (mapID,) )

        # init renderer
        render = pg.renderer.Map(width,height)
        render._create_drawer()
        render.drawer.pixel_space()

        # load image
        print 'loading', link
        if link.startswith('http'):
            fobj = io.BytesIO(urllib.urlopen(link).read())
            img = PIL.Image.open(fobj)
        else:
            img = PIL.Image.open(link)
        rast = pg.RasterData(image=img) 
        render.add_layer(rast)

        # add image regions
        bounds = pg.VectorData()
        bounds.add_feature([], json.loads(mapregion))
        render.add_layer(bounds, fillcolor=None, outlinecolor='red', outlinewidth=0.2)

        # add text
        texts = pg.VectorData(fields=['text','conf'])
        textquery = self.query('select text,conf,left,top,width,height from maptext where maptext.map = ?', (mapID,) )
        for text in textquery:
            x1,y1 = text['left'], text['top']
            x2,y2 = x1+text['width'], y1+text['height']
            box = [x1,y1,x2,y2]
            row = [text['text'], text['conf']]
            geoj = {'type':'Polygon', 'coordinates':[[(x1,y1),(x2,y1),(x2,y2),(x1,y2)]]}
            texts.add_feature(row, geoj)
        render.add_layer(texts, fillcolor=None, outlinecolor='green', outlinewidth=0.5)

        # add tiepoints
        tiepoints = pg.VectorData()
        tiepointquery = self.query('select col,row from maptiepoints where maptiepoints.map = ?', (mapID,) )
        for col,row in tiepointquery:
            geoj = {'type':'Point', 'coordinates':(col,row)}
            tiepoints.add_feature([], geoj)
        if len(tiepoints):
            render.add_layer(tiepoints, fillcolor=None, outlinecolor='red', outlinewidth=0.3)

        # view
        render.zoom_auto()
        render.view()

    def view_georef(self, mapID):
        import pythongis as pg
        import urllib
        import io

        # get map info
        link,width,height,transform = self.get('select link,width,height,transform from maps where oid=?', (mapID,) )
        transform = json.loads(transform) if transform else None

        if transform:
            # init renderer
            render = pg.renderer.Map()

            # load image
            print 'loading', link
            if link.startswith('http'):
                fobj = io.BytesIO(urllib.urlopen(link).read())
                img = PIL.Image.open(fobj)
            else:
                img = PIL.Image.open(link)

            # get tiepoints
            tiepoints = []
            for col,row,x,y in self.query('select col,row,x,y from maptiepoints where map = ?', (mapID,) ):
                px = (col,row)
                xy = (x,y)
                pair = (px,xy)
                tiepoints.append(pair)

            # georeference image
            # TODO: warp currently doesnt reuse the transform, it reestimates them from tiepoints
            if transform['type'] == 'polynomial':
                order = transform['order']
                georef = warp(img, None, tiepoints, order)
            render.add_layer(georef)

            # add tiepoints
            tiepoints = pg.VectorData()
            tiepointquery = self.query('select x,y from maptiepoints where maptiepoints.map = ?', (mapID,) )
            for x,y in tiepointquery:
                geoj = {'type':'Point', 'coordinates':(x,y)}
                tiepoints.add_feature([], geoj)
            render.add_layer(tiepoints, fillcolor=None, outlinecolor='red', outlinewidth=0.3)

            # view
            render.zoom_auto()
            render.view()

        else:
            raise Exception('Map ID {} is not able to be georeferenced: no transform function has been estimated'.format(mapID))

    def view_footprints(self, mapIDs=None):
        import pythongis as pg
        render = pg.renderer.Map()

        render.add_layer(r"C:\Users\kimok\Downloads\cshapes\cshapes.shp",
                         fillcolor=(222,222,222))
        
        d = pg.VectorData(fields=['mapID', 'link'])
        sql = 'select oid,link,xmin,ymin,xmax,ymax from maps'
        if mapIDs is not None:
            sql += ' where oid in ({})'.format(','.join(map(str,mapIDs)))
        for oid,link,x1,y1,x2,y2 in self.query(sql):
            if x1 is None: continue
            row = [oid, link]
            geoj = {'type':'Polygon', 'coordinates':[[(x1,y1),(x2,y1),(x2,y2),(x1,y2)]]}
            d.add_feature(row, geoj)
        if len(d):
            render.add_layer(d, fillcolor=(0,200,0,100), outlinewidth=0.2)

        render.zoom_bbox(*d.bbox)
        render.view()












            

    
