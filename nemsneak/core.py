# -*- coding: utf-8 -*-

import json
from contextlib import closing
from urllib import request
from codecs import getreader
from datetime import datetime, timezone
import time
from queue import PriorityQueue
from threading import Thread
import collections

import pytz

from . import util


nem_epoch = datetime(2015, 3, 29, 0, 6, 25, 0, timezone.utc)


class Error(Exception):
    pass


class UnknownTypeError(Error):
    """Exception raised for unknown transaction type.

    :param tx_type: type of tx
    :param tx: original transaction data
    """
    def __init__(self, tx_type, tx):
        super(UnknownTypeError, self).__init__()
        self.tx_type = tx_type
        self.tx = tx


class MosaicNotFound(Error):
    """Exception raised for unknown mosaic

    :param namespace: namespace id
    :param mosaic: mosaic id
    """
    def __init__(self, namespace, mosaic):
        super(MosaicNotFound, self).__init__()
        self.namespace = namespace
        self.mosaic = mosaic


class Connection(object):
    """Connection to NIS

    :param tz: your timezone (default: ``timezone.utc``)
    :param base_url: base url for the NIS \
    (default: ``'http://localhost:7890'``)
    """
    def __init__(self, tz=None, base_url=None):
        super(Connection, self).__init__()
        self.tz = tz if tz is not None else timezone.utc
        self.base_url = base_url if base_url is not None else \
            'http://localhost:7890'

    def dt2ts(self, dt):
        """convert datetime to NEM timeStamp

        :param dt: datetime
        """
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return int((
                self.tz.localize(dt).astimezone(timezone.utc) - nem_epoch
            ).total_seconds())
        else:
            return int((
                dt.astimezone(timezone.utc) - nem_epoch
            ).total_seconds())

    def ts2dt(self, ts):
        """convert NEM timeStamp to tz aware datetime

        :param ts: NEM timeStamp
        """
        return pytz.utc.localize(
            datetime.fromtimestamp(ts + time.mktime(nem_epoch.timetuple()))
        ).astimezone(self.tz)

    def num2xem(self, num):
        return num / 1000000

    def pubkey2addr(self, pubkey):
        if not hasattr(self, 'pubkey_cache'):
            self.pubkey_cache = {}
        if pubkey not in self.pubkey_cache:
            time.sleep(0.1)
            self.pubkey_cache[pubkey] = self.get(
                '/account/get/from-public-key',
                {'publicKey': pubkey}
            )['account']['address']
        return self.pubkey_cache[pubkey]

    def get_mosaic_definitions(self, namespace, id_=None):
        """Get mosaic definitions in namespace

        :param namespace: namespace string
        :param id_: The topmost mosaic definition database id up to which\
        root mosaic definitions are returned. The parameter is optional.\
        If not supplied the most recent mosaic definitiona are returned.
        """
        param = {'namespace': namespace}
        if id_ is not None:
            param['id'] = id_
        return self.get('/namespace/mosaic/definition/page', param)

    def get_mosaic_definition(self, namespace, mosaic):
        """Get the mosaic definition

        :param namespace: namespace  string
        :param mosaic: mosaic id string
        """
        if namespace == 'nem' and mosaic == 'xem':
            return {
                'meta': {'id': -1},
                'mosaic': {
                    'id': {'namespaceId': 'nem', 'name': 'xem'},
                    'properties': [
                        {'name': 'initialSupply', 'value': 8999999999},
                        {'name': 'divisibility', 'value': 6},
                        {'name': 'supplyMutable', 'value': False},
                        {'name': 'transferable', 'value': True},
                    ],
                    'levy': {}
                }
            }
        id_ = None
        while True:
            res = self.get_mosaic_definitions(namespace, id_)
            if 'data' not in res:
                raise Exception(res)
            if len(res['data']) == 0:
                raise MosaicNotFound(namespace, mosaic)
            for d in res['data']:
                if d['mosaic']['id']['name'] == mosaic:
                    return d
                if id_ is None or id_ > d['meta']['id']:
                    id_ = d['meta']['id']

    def tx_balance(self, data):
        tx = data['transaction']
        time.sleep(0.1)
        signer = self.pubkey2addr(tx['signer'])
        if tx['type'] in set((2049, 4097, 16386)):
            return dict(filter(lambda x: x[1] != 0, [
                ('fee', tx['fee']), (signer, -tx['fee'])
            ]))
        if tx['type'] == 8193:
            return dict(filter(lambda x: x[1] != 0, [
                ('fee', tx['fee']), (signer, -tx['fee'] - tx['rentalFee']),
                (tx['rentalFeeSink'], tx['rentalFee'])
            ]))
        if tx['type'] == 16385:
            return dict(filter(lambda x: x[1] != 0, [
                ('fee', tx['fee']), (signer, -tx['fee'] - tx['creationFee']),
                (tx['creationFeeSink'], tx['creationFee'])
            ]))
        if tx['type'] == 257:
            recipient = tx['recipient']
            if 'mosaics' in tx and len(tx['mosaics']) > 0:
                res = {
                    'fee': tx['fee'],
                    signer: -tx['fee']
                }
                if recipient not in res:
                    res[recipient] = 0
                mosaic_unit = tx['amount'] / 1000000
                for m in tx['mosaics']:
                    quantity = mosaic_unit * m['quantity']
                    if m['mosaicId']['namespaceId'] == 'nem' and \
                            m['mosaicId']['name'] == 'xem':
                        res[signer] -= quantity
                        res[recipient] += quantity
                    else:
                        time.sleep(0.1)
                        md = self.get_mosaic_definition(
                            m['mosaicId']['namespaceId'],
                            m['mosaicId']['name']
                        )['mosaic']
                        if len(md['levy']) == 0:
                            continue
                        levy = md['levy']
                        if levy['mosaicId']['namespaceId'] == 'nem' and\
                                levy['mosaicId']['name'] == 'xem':
                            if levy['type'] == 1:
                                amount = levy['fee']
                            else:
                                amount = (levy['fee'] / 100) * quantity
                            if amount != 0:
                                if levy['recipient'] not in res:
                                    res[levy['recipient']] = 0
                                res[levy['recipient']] += amount
                                res[signer] -= amount
                return dict(filter(lambda x: x[1] != 0, res.items()))
            else:
                res = {
                    'fee': tx['fee'],
                    signer: -tx['fee'] - tx['amount']
                }
                if recipient not in res:
                    res[recipient] = 0
                res[recipient] += tx['amount']
                return dict(filter(lambda x: x[1] != 0, res.items()))
        if tx['type'] == 4100:
            res = self.tx_balance({'transaction': tx['otherTrans']})
            res['fee'] += tx['fee']
            if signer not in res:
                res[signer] = 0
            res[signer] -= tx['fee']
            return dict(filter(lambda x: x[1] != 0, res.items()))
        raise UnknownTypeError(tx['type'], tx)

    def xem_transfer(self, data):
        tx = data['transaction']
        if tx['type'] in set((2049, 4097, 16386, 8193, 16385)):
            return None
        if tx['type'] == 257:
            signer = self.pubkey2addr(tx['signer'])
            recipient = tx['recipient']
            amount = 0
            if signer == recipient:
                return None
            if 'mosaics' in tx and len(tx['mosaics']) > 0:
                mosaic_unit = tx['amount'] / 1000000
                for m in tx['mosaics']:
                    quantity = mosaic_unit * m['quantity']
                    if m['mosaicId']['namespaceId'] == 'nem' and \
                            m['mosaicId']['name'] == 'xem':
                        amount += quantity
            else:
                amount = tx['amount']
            return ((signer, recipient), amount)
        if tx['type'] == 4100:
            return self.xem_transfer({'transaction': tx['otherTrans']})
        raise UnknownTypeError(tx['type'], tx)

    def get(self, route, param=None):
        """GET request

        :param route: API route
        :param param: get parameters (dict)
        """
        url = self.base_url.strip('/') + '/' +\
            route.strip('/').strip('?') + ((
                '?' + '&'.join((k + '=' + str(v) for k, v in param.items()))
            ) if param is not None else '')
        with closing(request.urlopen(url)) as conn:
            return json.load(getreader('utf-8')(conn))

    def post(self, route, param=None):
        """POST request

        :param route: API route
        :param param: POST parameters (dict)
        """
        req = request.Request(
            self.base_url.strip('/') + '/' + route.strip('/')
        )
        req.add_header('Content-Type', 'application/json')
        query = bytes(json.dumps(param if param is not None else {}), 'utf-8')
        with closing(request.urlopen(req, query)) as conn:
            return json.load(getreader('utf-8')(conn))

    def get_account_info(self, account_address):
        """get account info from /account/get route

        :param account_address: the address of the account
        """
        return self.get(
            route='account/get',
            param={'address': account_address}
        )

    def get_tx_single(self, type_, account_address, id_=None, hash_=None):
        """get maximum of 25 transaction data.

        :param type_: transaction type. one of 'all', 'incoming', 'outgoing'
        :param account_address: the address of the account
        :param id_: The transaction id up to which transactions are returned.
        :param hash_: The 256 bit sha3 hash of the transaction up to which \
            transactions are returned.
        """
        param = {'address': account_address}
        if id_ is not None:
            param['id'] = id_
        if hash_ is not None:
            param['hash'] = hash_
        return self.get(
            route='account/transfers/' + type_,
            param=param
        )

    def get_outgoing_tx_single(self, account_address, id_=None, hash_=None):
        """get maximum of 25 outgoing transaction data.

        :param account_address: the address of the account
        :param id_: The transaction id up to which transactions are returned.
        :param hash_: The 256 bit sha3 hash of the transaction up to which \
            transactions are returned.
        """
        return self.get_tx_single('outgoing', account_address, id_, hash_)

    def get_incoming_tx_single(self, account_address, id_=None, hash_=None):
        """get maximum of 25 incoming transaction data.

        :param account_address: the address of the account
        :param id_: The transaction id up to which transactions are returned.
        :param hash_: The 256 bit sha3 hash of the transaction up to which \
            transactions are returned.
        """
        return self.get_tx_single('incoming', account_address, id_, hash_)

    def get_all_tx_single(self, account_address, id_=None, hash_=None):
        """get maximum of 25 transaction data.

        :param account_address: the address of the account
        :param id_: The transaction id up to which transactions are returned.
        :param hash_: The 256 bit sha3 hash of the transaction up to which \
            transactions are returned.
        """
        return self.get_tx_single('all', account_address, id_, hash_)

    def get_tx_loop(self, type_, account_address, dt_from=None, dt_to=None,
                    buffer_sec=600):
        """get the transaction data after ``dt_from``

        :param type_: transaction type. one of 'all', 'incoming', 'outgoing'
        :param account_address: the address of the account
        :param dt_from: native datetime
        :param buffer_sec: time buffer
        """
        ts_from = self.dt2ts(dt_from if dt_from is not None else nem_epoch)
        ts_to = self.dt2ts(dt_to) if dt_to is not None else None
        res = []
        id_ = None
        last_ts = None
        while True:
            tmp = self.get_tx_single(
                type_, account_address, id_=id_
            )
            if len(tmp['data']) == 0:
                break
            for d in tmp['data']:
                _t = d['transaction']['timeStamp']
                if _t >= ts_from and (
                            ts_to is None or
                            (ts_to is not None and _t <= ts_to)
                        ):
                    res.append(d)
                if id_ is None or id_ > d['meta']['id']:
                    id_ = d['meta']['id']
                if last_ts is None or last_ts > _t:
                    last_ts = _t
            if (last_ts + buffer_sec) < ts_from:
                break
            else:
                time.sleep(0.1)
        return res

    def get_outgoing_tx(self, account_address, dt_from=None, dt_to=None):
        """get the outgoing transaction data after ``dt_from``

        :param account_address: the address of the account
        :param dt_from: native datetime
        """
        return self.get_tx_loop('outgoing', account_address, dt_from, dt_to)

    def get_incoming_tx(self, account_address, dt_from=None, dt_to=None):
        """get the incoming transaction data after ``dt_from``

        :param account_address: the address of the account
        :param dt_from: native datetime
        """
        return self.get_tx_loop('incoming', account_address, dt_from, dt_to)

    def get_all_tx(self, account_address, dt_from=None, dt_to=None):
        """get the transaction data after ``dt_from``

        :param account_address: the address of the account
        :param dt_from: native datetime
        """
        return self.get_tx_loop('all', account_address, dt_from, dt_to)


class Chaser(Thread):
    """Enumerate all addresses and related transactions from the ``target``

    :param target: the root address
    :param conn: Connection class instance
    :param hook: a callable which is called as \
    ``hook(from_address, transaction)`` when a transaction is found.
    :param dt_from: time range start
    :param dt_to: time range end (default: ``None``; means now)
    :param chase_filter: ``None`` or a callable which is called as\
    ``chase_filter(tx, dt, to_addr, from_addr, self)`` and returns a bool\
    which indicates ``to_addr`` should be queued.\
    (default: None; means "always True")
    :param hook_filter: ``None`` or a callable which is called as\
    ``hook_filter(tx, dt, from_addr, self)`` and returns a bool\
    which indicates ``hook`` should be called.\
    (default: ``None`` means "always True")
    :param thread_name: The name of the thread. default: None
    :param deamon: If not None, daemon explicitly sets whether the thread is \
    daemonic. If None (the default), the daemonic property is inherited from \
    the current thread.
    """
    def __init__(self, target, conn, hook, dt_from, dt_to=None,
                 chase_filter=None, hook_filter=None,
                 thread_name=None, daemon=None):
        super(Chaser, self).__init__(name=thread_name, daemon=daemon)
        self.target = target
        self.hook = hook
        self.dt_from = dt_from
        self.dt_to = dt_to
        self.chase_filter = chase_filter
        self.hook_filter = hook_filter
        self.conn = conn

    @classmethod
    def get_recipient(cls, tx):
        if tx['transaction']['type'] == 4100:
            return cls.get_recipient({
                'meta': tx['meta'],
                'transaction': tx['transaction']['otherTrans']
            })
        if tx['transaction']['type'] == 257:
            return tx['transaction']['recipient']
        return None

    def run(self):
        queue = PriorityQueue()
        queue.put((self.dt_from, self.target))
        known = {}
        while not queue.empty() != 0:
            t = queue.get()
            to_dt = datetime.now(self.conn.tz)
            if t[1] in known:
                if known[t[1]] <= t[0]:
                    continue
                to_dt = known[t[1]]
            transactions = self.conn.get_outgoing_tx(t[1], t[0], self.dt_to)
            for tx in transactions:
                dt = self.conn.ts2dt(tx['transaction']['timeStamp'])
                if dt < to_dt and (
                            self.hook_filter is None or
                            self.hook_filter(tx, dt, t[1], self)
                        ):
                    self.hook(t[1], tx)
                to_addr = self.get_recipient(tx)
                if to_addr is not None and (
                            self.chase_filter is None or
                            self.chase_filter(
                                tx, dt, to_addr, t[1], self
                            )
                        ):
                    queue.put((dt, to_addr))
            known[t[1]] = t[0]
            time.sleep(0.1)


class Gazer(Thread):
    """Monitoring ``targets`` addresses. call ``hook`` when one of ``targets``
    makes transactions.

    :param targets: an address or a list of addresses
    :param conn: ``Connection`` object.
    :param hook: a callable object. this is called when one of the target
    makes transactions. ``hook(target_address, transactions[])``
    :param interval: checking interval
    :param thread_name: thread name
    :param daemon: whether if the thread is daemon or not
    """
    def __init__(self, targets, conn, hook, interval=5.0, thread_name=None,
                 daemon=None):
        super(Gazer, self).__init__(name=thread_name, daemon=daemon)
        if isinstance(targets, (str, bytes)):
            self.targets = [targets]
        elif isinstance(targets, collections.Iterable):
            self.targets = targets
        if len(self.targets) == 0:
            raise Exception('invalid targets')
        self.interval = interval / len(self.targets)
        self.hook = hook
        self.conn = conn
        self.stopping = False

    def stop(self):
        self.stopping = True

    def run(self):
        last_ids = {}
        for t in self.targets:
            tmp = self.conn.get_all_tx_single(t)
            if 'data' in tmp:
                tmp = tmp['data']
            if len(tmp) > 0:
                last_ids[t] = tmp[0]['meta']['id']
            else:
                last_ids[t] = None
            time.sleep(0.1)
        while True:
            for t in self.targets:
                tmp = self.conn.get_all_tx_single(t)
                if 'data' in tmp:
                    tmp = tmp['data']
                new_tx = []
                for tx in tmp:
                    if last_ids[t] < tx['meta']['id']:
                        new_tx.append(tx)
                if len(new_tx) > 0:
                    self.hook(t, new_tx)
                    last_ids[t] = tmp[0]['meta']['id']
                if self.stopping:
                    break
                time.sleep(self.interval)
            if self.stopping:
                break
