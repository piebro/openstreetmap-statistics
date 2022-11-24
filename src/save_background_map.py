import sys
import os

from PIL import Image, ImageDraw
import numpy as np
import geopandas as gpd

def save_map(geometry, scale, save_path, background_color, fill_color):
    img = Image.new(mode="RGB", size=(360*scale, 180*scale), color=background_color)
    draw = ImageDraw.Draw(img)

    for poly in geometry:
        x, y = poly.exterior.coords.xy
        x = (np.array(x) + 180)*scale
        y = (-np.array(y) + 90)*scale
        xy = [(xx, yy) for xx, yy in zip(x, y)]
        draw.polygon(xy, fill=fill_color, outline=None, width=0)
    img.save(save_path)

if __name__ == "__main__":
    shp_file_path = sys.argv[1]
    geometry = gpd.read_file(shp_file_path)["geometry"]
    scale = 1
    save_map(geometry, scale, save_path=f"background_map_scale_{scale}.png", background_color=(0,0,0), fill_color=(30,30,30))
        
