# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import os
import csv
from datetime import datetime
import time

import pytz
import jsonlines

import nemsneak
from nemsneak import util


if __name__ == '__main__':
    parser = ArgumentParser(
        'enumerate all the "infected" accounts.' +
        '"infected" means the accounts which recieved XEM from the root or ' +
        ' an infected account.'
    )
    parser.add_argument('target', help='the root account')
    parser.add_argument(
        '--dt_from',
        help='search after this datetime. specify if of the form ' +
        '%%Y%%m%%d%%H%%M%%S (ex: 20180126000200). (default: 20180126000200)',
        default='20180126000200'
    )
    parser.add_argument(
        '--dt_to', default=None
    )
    parser.add_argument(
        '--timezone', help='timezone (default: Asia/Tokyo)',
        default='Asia/Tokyo'
    )
    parser.add_argument(
        '--api_host', help='API Host (default: http://localhost:7890)',
        default='http://localhost:7890'
    )
    args = parser.parse_args()
    tz = pytz.timezone(args.timezone)
    conn = nemsneak.Connection(tz, args.api_host)

    start_time = datetime.now()

    target = args.target
    from_dt = datetime.strptime(args.dt_from, '%Y%m%d%H%M%S').replace(
        tzinfo=tz
    )
    to_dt = datetime.strptime(args.dt_to, '%Y%m%d%H%M%S').replace(
        tzinfo=tz
    ) if args.dt_to is not None else None

    marked_mosaics = ({
        'namespaceId': 'ts',
        'name': 'warning_dont_accept_stolen_funds'
    }, {
        'namespaceId': 'mizunashi.coincheck_stolen_funds_do_not_accept_trades',
        'name': 'owner_of_this_account_is_hacker',
    })

    marked_mosaic_slug = tuple(
        ':'.join((d['namespaceId'], d['name'])) for d in marked_mosaics
    )

    def is_marked(addr):
        tmp = conn.get('/account/mosaic/owned', {'address': addr})['data']
        time.sleep(0.1)
        mosaic_set = set(
            ':'.join((
                d['mosaicId']['namespaceId'],
                d['mosaicId']['name']
            )) for d in tmp
        )
        return tuple([
            s in mosaic_set for s in marked_mosaic_slug
        ])

    queue = [(target, from_dt)]
    known = {}

    log_fp = jsonlines.open(os.path.join(
        'results',
        'log_{}.jsonline'.format(start_time.strftime('%Y%m%d_%H%M%S'))
    ), 'w')
    res = []

    def hook_func(sender, tx):
        res.append(tuple(util.pp_transaction([
            'datetime', 'amount', 'from_address', 'to_address', 'fee',
            'message'
        ], util.tidy_transaction(
            tx, conn, sender
        ))))
        log_fp.write(tx)
        print(res[-1])

    def chase_filter(tx, dt, to_addr, from_addr, chaser):
        xem_trans = conn.xem_transfer(tx)
        return xem_trans is not None and xem_trans[1] > 0

    def hook_filter(tx, dt, from_addr, chaser):
        if tx['transaction']['type'] == 4100:
            return hook_filter({
                'meta': tx['meta'],
                'transaction': tx['transaction']['otherTrans']
            }, dt, from_addr, chaser)
        if 'amount' not in tx['transaction']:
            return False
        return tx['transaction']['amount'] > 0

    ch = nemsneak.Chaser(
        target, conn, hook_func, from_dt, to_dt, chase_filter, hook_filter,
        daemon=True
    )

    ch.start()

    ch.join()

    addrs = set(
        [d[2] for d in res if d[2] is not None] +
        [d[3] for d in res if d[3] is not None]
    )

    info = {}

    for addr in addrs:
        tmp = conn.get_account_info(addr)
        time.sleep(0.1)
        info[addr] = (
            tmp['account']['balance'],
            tmp['account']['vestedBalance']
        ) + is_marked(addr)

    res.sort(key=lambda x: x[0])

    if not os.path.exists('results'):
        os.makedirs('results')

    with open(os.path.join(
                'results',
                'info_{}.csv'.format(start_time.strftime('%Y%m%d_%H%M%S'))
            ), 'w') as fout:
        wr = csv.writer(fout, lineterminator='\n')
        wr.writerow((
            'address', 'balance', 'vestedBalance'
        ) + marked_mosaic_slug)
        for k, v in info.items():
            wr.writerow((k, ) + v)

    with open(os.path.join(
                'results',
                'tx_{}.csv'.format(start_time.strftime('%Y%m%d_%H%M%S'))
            ), 'w') as fout:
        wr = csv.writer(fout, lineterminator='\n')
        wr.writerow([
            'datetime', 'amount', 'from_address', 'to_address', 'fee',
            'message', 'is_sender_marked', 'is_recipient_marked'
        ])
        for d in res:
            wr.writerow(
                d + (
                    any(t for t in info[d[2]][2:]) if d[2] is not None else '',
                    any(t for t in info[d[3]][2:]) if d[3] is not None else ''
                )
            )
