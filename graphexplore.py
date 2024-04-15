#!/usr/bin/env python3

import sys
from types import SimpleNamespace

import pandas as pd
import geopandas as gpd
import networkx as nx
import momepy
import shapely

import constants


def subgraphs(g, maxedges=0):
    for x in nx.connected_components(g):
        sg: nx.MultiGraph = g.subgraph(x)
        if maxedges and sg.number_of_edges() > maxedges:
            continue
        yield sg


def dfline(gdf):
    """
    Splits all linestrings into 2-seg strings

    May want to try to interpolate intersecting vertices

    :param gdf:
    :return:
    """
    df = gpd.GeoDataFrame()
    df.crs = 4326
    for _, f in gdf.iterrows():
        t = f.to_dict()
        g = f.geometry
        assert g.geom_type == 'LineString'
        prev = None
        for i, c in enumerate(g.coords):
            if prev is not None:
                t['si'] = i
                t['geometry'] = shapely.LineString([prev, c])
                df = pd.concat([df, gpd.GeoDataFrame([t])])
            prev = c
    return df


def subgraph_analyze(g):
    for x in subgraphs(g, maxedges=10):
        print(f'Graph: {x.number_of_nodes()}')
        edgedata = list(x.edges(data=True))
        for e in edgedata:
            sn = SimpleNamespace(**e[2])
            c = e[2]['class']
            gdf2 = gpd.GeoDataFrame([e[2]])
            gdf2.crs = constants.CHICAGO_DATUM
            #gdf2.to_crs(constants.CHICAGO_DATUM)
            gdflen = gdf2.length[0]
            print(f' {sn.trans_id}: {sn.street_nam} {gdflen} {c} {sn.suitability}')


if __name__ == "__main__":
    gdf = gpd.read_file(sys.argv[1])
    filt = gdf[gdf.geometry.type == 'LineString']
    proj = filt.to_crs(constants.CHICAGO_DATUM)
    G = momepy.gdf_to_nx(proj, approach='primal', oneway_column='bike_ow', directed=True)
    # https://stackoverflow.com/questions/21739569/finding-separate-graphs-within-a-graph-object-in-networkx
    #subgraph_analyze(G)

