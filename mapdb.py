
import automap
from automap.main import quantize, mask_text, image_segments, threshold, detect_data, process_text, detect_text_points, connect_text, find_matches, warp
from automap.rmse import polynomial, predict

import PIL, PIL.Image

import sqlite3
import json
import os
import warnings

warnings.simplefilter('always')


# --------------------------------------------------


def prep_ocr(img):
    '''prepare for ocr'''
    im_prep = img

    # upscale for better ocr
    print 'upscaling'
    im_prep = im_prep.resize((im_prep.size[0]*2, im_prep.size[1]*2), PIL.Image.LANCZOS)

    # precalc color differences (MAYBE move to inside threshold?)
    print 'quantize'
    im_prep = quantize(im_prep)

    return im_prep

def mark_placenames(textdata, mapp_poly, box_polys):
    # decide which labels to consider placenames
    # only labels inside map region
    filt = mask_text(textdata, mapp_poly)
    # excluding labels inside any boxes
    for box in box_polys:
        filt = mask_text(filt, box, invert=True)
    # only nonnumeric, first uppercased, rest lowercased
    filt = [r for r in filt
            if r['numeric'] is False and r['text_clean'][0].isupper() and not r['uppercase']]
    # mark as placename
    for r in filt:
        r['function'] = 'placename'



# --------------------------------------------------------------------



def extract_text(img, textcolor=(0,0,0), colorthresh=25, textconf=60):
    # prep
    im_prep = prep_ocr(img)

    data = []
    textcolors = [textcolor] # input color
    for color in textcolors:
        # threshold
        print 'thresholding', color
        im_prep_thresh,mask = threshold(im_prep, color, colorthresh)
        
        # ocr
        print 'detecting text'
        subdata = detect_data(im_prep_thresh)
        print 'processing text'
        subdata = process_text(subdata, textconf)

        # assign text characteristics
        for dct in subdata:
            dct['color'] = color

        # filter out duplicates from previous loop, in case includes some of the same pixels
        print '(skip duplicates)',len(subdata),len(data)
        subdata = [r for r in subdata
                   if (r['top'],r['left'],r['width'],r['height'])
                   not in [(dr['top'],dr['left'],dr['width'],dr['height']) for dr in data]
                   ]

        # connect text data
        print '(connecting texts)'
        subdata = connect_text(subdata)

        # detect text coordinates
        print 'determening text anchors'
        subdata = detect_text_points(im_prep_thresh, subdata)

        data.extend(subdata)
        print 'text data size', len(subdata), len(data)

    # downscale the data coordinates of the upscaled image back to original coordinates
    for r in data: 
        for k in 'top left width height'.split():
            r[k] = int(r[k]) / 2
        # same for points
        if 'anchor' in r:
            x,y = r['anchor']
            r['anchor'] = (x/2, y/2)

    return data

def get_placenames(textdata, img):
    # final placenames with anchor points
    mapp_poly,box_polys = image_segments(img)
    mark_placenames(textdata, mapp_poly, box_polys)
    points = [(r['text_clean'], r['anchor']) for r in textdata if r['function']=='placename' and 'anchor' in r]
    return points

def match_placenames(points, matchthresh=0.1, **kwargs):
    origs,matches = find_matches(points, matchthresh, **kwargs)
    orignames,origcoords = zip(*origs)
    matchnames,matchcoords = zip(*matches)
    tiepoints = zip(origcoords, matchcoords)
    return tiepoints

def process_image(img):
    result = {}

    # basic
    result['width'] = img.size[0]
    result['height'] = img.size[1]

    # meta
    mapp_poly,box_polys = image_segments(img)
    result['mapregion'] = {'type':'Polygon', 'coordinates':[[tuple(c[0]) for c in mapp_poly]]}

    # text
    text = result['text'] = extract_text(img)
    placenames = result['placenames'] = get_placenames(text, img)
    try:
        gcps = result['gcps'] = match_placenames(placenames, db=r"C:\Users\kimok\Desktop\BIGDATA\gazetteer data\optim\gazetteers.db")
    except Exception as err:
        warnings.warn(err)
        return result

    # estimate
    if 'gcps' in result:
        order = 1
        coeff_x, coeff_y = polynomial(order, *zip(*gcps))[-2:]
        result['transform'] = {'type':'polynomial', 'order':order, 'xcof':list(coeff_x), 'ycof':list(coeff_y)}

    # image bbox
    if 'gcps' in result:
        pixels = []
        for row in range(img.size[1]):
            for col in range(img.size[0]):
                pixels.append((col,row))
        print 'calculating coordinate bounds'
        pred = predict(order, pixels, coeff_x, coeff_y)
        xmin,ymin,xmax,ymax = pred[:,0].min(), pred[:,1].min(), pred[:,0].max(), pred[:,1].max()
        result['bbox'] = [xmin,ymin,xmax,ymax]

    return result


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

    def process(self, link, img):
        print 'processing img'
        result = process_image(img)
        
        print 'inserting info'
        
        # map info
        if 'bbox' in result:
            xmin,ymin,xmax,ymax = result['bbox']
        else:
            xmin=ymin=xmax=ymax = None
        transform = result.get('transform', None)
        vals = (link, result['width'], result['height'], json.dumps(transform), xmin,ymin,xmax,ymax, json.dumps(result['mapregion']) )
        self.cur.execute('insert into maps (link, width, height, transform, xmin, ymin, xmax, ymax, mapregion) values (?, ?, ?, ?, ?, ?, ?, ?, ?)', vals)
        mapID = self.cur.lastrowid

        # text info
        for text in result['text']:
            vals = [mapID] + [text[k] for k in 'text_clean, conf, fontheight, top, left, width, height, function'.split(', ')]
            self.cur.execute('insert into maptext (map, text, conf, fontheight, top, left, width, height, function) values (?, ?, ?, ?, ?, ?, ?, ?, ?)', vals)

        # gcps info
        if 'gcps' in result:
            gcps = result['gcps']
            for (col,row),(x,y) in gcps:
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
        url,width,height,mapregion = self.get('select link,width,height,mapregion from maps where oid=?', (mapID,) )

        # init renderer
        render = pg.renderer.Map(width,height)
        render._create_drawer()
        render.drawer.pixel_space()

        # load image
        print 'loading', url
        fobj = io.BytesIO(urllib.urlopen(url).read())
        img = PIL.Image.open(fobj)
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
        url,width,height,transform = self.get('select link,width,height,transform from maps where oid=?', (mapID,) )
        transform = json.loads(transform) if transform else None

        if transform:
            # init renderer
            render = pg.renderer.Map()

            # load image
            print 'loading', url
            fobj = io.BytesIO(urllib.urlopen(url).read())
            img = PIL.Image.open(fobj)

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












            

    
