# -*- coding: utf-8 -*-

from collections import Iterable, Mapping
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


def is_container(x):
    return isinstance(x, Iterable) and not isinstance(x, (str, bytes))


def rec_key_type(obj):
    if isinstance(obj, Mapping):
        res = []
        for k, v in obj.items():
            data = rec_key_type(v)
            if is_container(data):
                for d in data:
                    res.append(('.'.join((k, d[0])), d[1]))
            else:
                res.append((k, data))
        return tuple(sorted(res))
    else:
        return type(obj).__name__


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


def get_tx_type(data):
    if 'transaction' not in data or 'type' not in data['transaction']:
        raise TypeError(data)
    tx = data['transaction']
    tx_type = tx['type']
    if tx_type == 257:
        if 'modifications' in tx and 'minCosignatories' in tx:
            return 'MultisigAggregateModificationTransaction'
        if 'otherHash' in tx and 'otherAccount' in tx:
            return 'MultisigSignatureTransaction'
        if 'otherTrans' in tx and 'signatures' in tx:
            return 'MultisigTransaction'
        return 'TransferTransaction'
    if tx_type == 4097:
        return 'ConvertingAnAccountToAMultisigAccount'
    if tx_type == 4100:
        if 'otherTrans' not in tx['transaction']:
            raise TypeError(tx)
        return get_tx_type({'transaction': tx['transaction']['otherTrans']})
    if tx_type == 4098:
        return 'CosigningMultisigTransaction'
    if tx_type == 4097:
        return 'AddingAndRemovingCosignatories'
    if tx_type == 8193:
        return 'ProvisionNamespaceTransaction'
    if tx_type == 16385:
        return 'MosaicDefinitionCreationTransaction'
    if tx_type == 16386:
        return 'MosaicSupplyChangeTransaction'
    if tx_type == 2049:
        return 'ImportanceTransferTransaction'
    raise TypeError(data)


def get_raw_transaction(data):
    if 'transaction' not in data or 'type' not in data['transaction']:
        raise TypeError(data)
    if 'otherTrans' in data['transaction']:
        return data['transaction']['otherTrans']
    return data['transaction']


def is_transfer_transaction(data):
    tx_type = get_tx_type(data)
    return tx_type == 'TransferTransaction'
