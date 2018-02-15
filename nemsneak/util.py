# -*- coding: utf-8 -*-

import codecs


def tidy_transaction(tr, conn, sender=None):
    s_ = sender

    if tr['transaction']['type'] == 16385:
        return {
            'from_address': s_,
            'to_address': None,
            'amount': 'MosaicDefinitionCreationTransaction',
            'fee': tr['transaction']['fee'],
            'message': decode_message(tr),
            'datetime': conn.ts2dt(tr['transaction']['timeStamp'])
        }
    elif tr['transaction']['type'] == 16386:
        return {
            'from_address': s_,
            'to_address': None,
            'amount': 'MosaicModificationTransaction',
            'fee': tr['transaction']['fee'],
            'message': decode_message(tr),
            'datetime': conn.ts2dt(tr['transaction']['timeStamp'])
        }
    elif tr['transaction']['type'] == 8193:
        return {
            'from_address': s_,
            'to_address': None,
            'amount': 'ProvisionNamespaceTransaction',
            'fee': tr['transaction']['fee'],
            'message': decode_message(tr),
            'datetime': conn.ts2dt(tr['transaction']['timeStamp'])
        }
    elif tr['transaction']['type'] == 2049:
        return {
            'from_address': s_,
            'to_address': None,
            'amount': 'ImportanceTransferTransaction',
            'fee': tr['transaction']['fee'],
            'message': decode_message(tr),
            'datetime': conn.ts2dt(tr['transaction']['timeStamp'])
        }
    elif tr['transaction']['type'] == 4097:
        return {
            'from_address': s_,
            'to_address': None,
            'amount': 'ConvertingAnAccountToMultisigAccount',
            'fee': tr['transaction']['fee'],
            'message': decode_message(tr),
            'datetime': conn.ts2dt(tr['transaction']['timeStamp'])
        }
    elif tr['transaction']['type'] == 4100:
        return tidy_transaction({
            'meta': tr['meta'],
            'transaction': tr['transaction']['otherTrans']
        }, conn, sender)
    if sender is None:
        s_ = conn.pubkey2addr(tr['transaction']['signer'])
    try:
        return {
            'from_address': s_,
            'to_address': tr['transaction']['recipient'],
            'amount': tr['transaction']['amount'],
            'fee': tr['transaction']['fee'],
            'message': decode_message(tr),
            'datetime': conn.ts2dt(tr['transaction']['timeStamp'])
        }
    except Exception as e:
        print(e)
        print(tr)


def pp_transaction(keys, ttr):
    return [
        '' if k not in ttr else (
            ttr[k] if k != 'datetime' else
            ttr[k].strftime('%Y-%m-%d %H:%M:%S')
        ) for k in keys
    ]


def decode_message(tr):
    if 'transaction' not in tr:
        return '<<NO TRANSACTION>>'
    if 'message' not in tr['transaction']:
        return '<<NO MESSAGE FIELD>>'
    msg = tr['transaction']['message']
    if len(msg) == 0:
        return ''
    if 'payload' not in msg or 'type' not in msg:
        print(msg)
        return '<<INVALID MESSAGE>>'
    if msg['type'] != 1:
        return msg['payload'] + ' <<ENCRIPTED>>'
    try:
        return codecs.decode(
            bytes(msg['payload'], 'utf-8'),
            'hex_codec'
        ).decode('utf-8')
    except:
        return msg['payload'] + ' <<UTF-8 DECODE ERROR>>'
