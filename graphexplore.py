#!/usr/bin/env python3

import sys
from types import SimpleNamespace
from typing import List, Tuple

import pandas as pd
import geopandas as gpd
import networkx as nx
import momepy
import shapely
import tqdm

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


class NxFinder2:
    # big weight meaning don't use this edge
    MAX = 1000000000

    def __init__(self, network_gdf, points_gdf, silent=False, sample=None):
        self.gdf = network_gdf
        self.points_df = points_gdf
        self.gdf_alt = self.gdf.to_crs(constants.CHICAGO_DATUM)
        if sample and sample < len(self.points_df):
            print(f'Sampling original size {len(self.points_df)} to {sample}')
            self.points_df = self.points_df.sample(sample)
        self.points_alt = self.points_df.to_crs(constants.CHICAGO_DATUM)
        self.silent = silent
        self.pointrow_cache = {}
        filt = self.gdf_alt[self.gdf_alt.geometry.geom_type == 'LineString']
        self.graph = momepy.gdf_to_nx(filt, approach='primal', oneway_column='bike_ow', directed=True, length='actual')
        self.graph_index = {}
        for e in self.graph.edges(data=True):
            k = e[2]['trans_id']
            v = (e[0], e[1])
            self.graph_index[k] = v

    def closest_point2(self, pointrow):
        p = pointrow.iloc[0].geometry
        cached = self.pointrow_cache.get(p)
        if cached is not None:
            return cached
        alt = self.gdf_alt.clip(pointrow.geometry.buffer(20))
        if alt.empty:
            self.pointrow_cache[p] = gpd.GeoDataFrame()
            return gpd.GeoDataFrame()
        alt = alt.sample(1)
        rv = self.gdf[self.gdf.trans_id == alt.trans_id.iloc[0]].iloc[0]
        self.pointrow_cache[p] = rv
        return rv

    def edge_datas(self, path):
        previous = None
        for p1 in path:
            if previous:
                yield self.graph.get_edge_data(previous, p1)
            previous = p1

    @staticmethod
    def path_weight(e0, e1, raw):
        attrs = list(raw.values())[0]
        return attrs['weight']
        # suitability_multipliers = {
        #     2: 5.0,
        #     3: 2.0,
        #     4: 0.9,
        #     5: 1,
        #     6: 0.75,
        #     7: 0.6,
        #     8: 0.5,
        # }
        # weight = attrs['actual']
        # mult = suitability_multipliers.get(attrs['suitability'], 0)
        # #print(f'Path weight: {weight} m{mult} E{e0} {e1} {raw}')
        # if mult == 0:
        #     return NxFinder.MAX
        # return weight * mult

    def route_edges(self, colname, tups: List[Tuple[str, str]], full=False):
        points = []
        count = 0
        for start, end in tqdm.tqdm(tups):
            count += 1
            startpoint = self.closest_point2(self.points_alt[self.points_alt[colname] == start])
            endpoint = self.closest_point2(self.points_alt[self.points_alt[colname] == end])
            if startpoint.empty or endpoint.empty:
                continue
            startnode = self.graph_index[startpoint['trans_id']][0]
            endnode = self.graph_index[endpoint['trans_id']][0]
            # needs weighting function
            try:
                path = nx.shortest_path(self.graph, startnode, endnode, weight=self.path_weight)
                if not path:
                    print(f'Fail {startnode} {endnode}')
                    continue
                #print(path)
                if full:
                    yield [list(x.values())[0] for x in self.edge_datas(path)], count
                else:
                    yield [list(x.values())[0]['trans_id'] for x in self.edge_datas(path)], count
            except nx.exception.NetworkXNoPath:
                print(f'No path: fail {startnode} {endnode}')


class NxFinder(NxFinder2):
    # big weight meaning don't use this edge
    MAX = 1000000000

    def __init__(self, network_filename, points_filename, silent=False, sample=None):
        super().__init__(gpd.read_file(network_filename), gpd.read_file(points_filename), silent, sample)


def schooltest():
    schools_filename = '/Users/hailey/datasets/chicago/Chicago Public Schools - School Locations SY1819.geojson'
    filtered_file = open('/tmp/filterfile.txt').read().strip()
    f = NxFinder(filtered_file, schools_filename)
    #rj = f.router('school_nm', 'LAKE VIEW HS', 'LASALLE')
    #rj = f.router('school_nm', 'LASALLE', 'LAKE VIEW HS')

    for r, _ in f.route_edges('school_nm', [('LASALLE', 'LAKE VIEW HS'), ('PRESCOTT', 'NEWBERRY')], full=True):
        print(f'Route')
        for rr in r:
            print(f' Rt {rr}')

def graphtest():
    gdf = gpd.read_file(sys.argv[1])
    filt = gdf[gdf.geometry.type == 'LineString']
    proj = filt.to_crs(constants.CHICAGO_DATUM)
    G = momepy.gdf_to_nx(proj, approach='primal', oneway_column='bike_ow', directed=True, length='actual')
    e0 = list(G.edges(data=True))[0]
    e1 = list(G.edges(data=True))[-1]
    n0 = list(G.nodes(data=True))[0]
    n1 = list(G.nodes(data=True))[-1]
    sp = nx.shortest_path(G, n0[0], n1[0])
    prev = None
    for p in sp:
        if p:
            ed = G.get_edge_data(prev, p)
            print(ed)
        prev = p
    #G2 = momepy.gdf_to_nx(proj, approach='dual', oneway_column='bike_ow', directed=True, length='actual')
    # https://stackoverflow.com/questions/21739569/finding-separate-graphs-within-a-graph-object-in-networkx
    #subgraph_analyze(G)


if __name__ == "__main__":
    schooltest()
