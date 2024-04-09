#!/usr/bin/env python3

import csv
import json
from dataclasses import dataclass

@dataclass
class Item:
    url: str
    username: str
    password: str
    totp: str
    extra: str
    name: str
    grouping: str
    fav: str


class LastpassReader:
    def __init__(self, filename):
        self.pd = {}
        alts = set([])
        with open(filename) as fh:
            reader = csv.DictReader(fh)
            rowcount = 0
            for row in reader:
                rowcount += 1
                #key = (row['name'], row['grouping'])
                altkey = (row['name'], row['username'])
                if altkey in alts:
                    print(f'Alt dup {altkey}')
                alts.add(altkey)
                key = row['name']
                if key == '1Password import lock':
                    continue
                key = altkey
                #assert not key in self.pd
                if key in self.pd:
                    print(f'duplicate: {key}')
                assert type(row) is dict
                self.pd[key] = row
            print(f'Alts: {len(alts)}, rows {rowcount}')

    def get(self, name):
        row = self.pd[name]
        assert type(row) is dict
        item = Item(**row)
        if item.url == 'http://sn':
            item.url = ''
        return item


class OnePasswordReader:
    def __init__(self, filename):
        self.raw_pd = json.load(open(filename))
        self.pd = {}

    def count(self):
        for a in self.raw_pd['accounts']:
            v = a['vaults']
            for vault in v:
                vname = vault['attrs']['name']
                i = vault['items']
                print(f'In vault {vname} there are {len(i)} items')
                for item in i:
                    name = item['overview']['title']
                    username, _ = self.get_username_and_password(item)
                    #self.pd[name] = item
                    key = (name, username)
                    self.pd[key] = item

    @staticmethod
    def get_username_and_password(r):
        username = ''
        password = ''
        for lf in r['details']['loginFields']:
            if lf['name'] == 'username':
                username = lf['value']
            if lf['name'] == 'password':
                password = lf['value']
        return username, password

    def get(self, name):
        r = self.pd[name]
        username, password = self.get_username_and_password(r)
        tags = r['overview']['tags']
        nt = []
        for t in tags:
            if not t.startswith('LastPass') and not t.startswith('!-repair-items'):
                nt.append(t)
        return Item(
            url=r['overview']['url'],
            username=username,
            password=password,
            totp='',
            extra=r['details'].get('notesPlain', ''),
            name=name[0],
            grouping='/'.join(nt),
            fav='0'
        )


class Compare:
    def __init__(self, lp: LastpassReader, op: OnePasswordReader):
        self.lp = lp
        self.op = op

    def compare(self, key):
        #print(f'Compare {name}')
        l = self.lp.get(key)
        o = self.op.get(key)
        if l != o:
            print(f'Mismatch in {key}:')
            print(f'  lp: {l}')
            print(f'  op: {o}')

if __name__ == "__main__":
    r = LastpassReader('/Users/hailey/tmp/pw/lastpass_export.csv')
    op = OnePasswordReader('/Users/hailey/tmp/pw/export.data')
    op.count()
    print(f'Found {len(r.pd)} entries')
    c = Compare(r, op)
    #c.compare('onyen')
    for k in r.pd.keys():
        c.compare(k)
