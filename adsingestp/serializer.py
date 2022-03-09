from datetime import datetime

TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

# TODO will need to add boolean keys here if we want to keep falsy values - check these
required_keys = ['createdTime',
                 'parsedTime',
                 'loadType',
                 'loadFormat',
                 'loadLocation',
                 'recordOrigin']

def clean_empty(input_to_clean, keys_to_keep=required_keys):
    """

    :param input_to_clean: dictionary that contains empty key/value pairs to remove
    :param keys_to_keep: list of keys to keep, even if they're empty
    :return: copy of input dict with all keys that contain empty values removed
    """

    if isinstance(input_to_clean, dict):
        return {
            k: v
            for k, v in ((k, clean_empty(v)) for k,v in input_to_clean.items())
            if v or (k in keys_to_keep)
        }

    if isinstance(input_to_clean, list):
        return [v for v in map(clean_empty, input_to_clean) if v]

    return input_to_clean

def serialize(input_dict, format):
    """

    :param input_dict: parsed metadata dictionary to serialize
    :param format: JATS, OtherXML, HTML, Text
    :return: serialized JSON that follows our internal data model
    """

    if format not in ['JATS', 'OtherXML', 'HTML', 'Text']:
        # TODO give this a real exception
        raise Exception

    output = {}

    output['recordData'] = {
        'createdTime': '',
        'parsedTime': datetime.utcnow().strftime(TIMESTAMP_FMT),
        'loadType': 'fromURL' if format=='HTML' else 'fromFile',
        'loadFormat': format,
        'loadLocation': '',
        'recordOrigin': ''
    }
    # TODO this should be an array of objects - talk to MT
    # TODO once we can parse more than just errata, need to expand this
    if 'erratum' in input_dict:
        output['relatedTo'] = {
            'relationship': 'errata',
            'relatedDocID': input_dict['erratum']
        }

    output['editorialHistory'] = {
        'receivedDates': input_dict['edhist_rec'] if 'edhist_rec' in input_dict else '',
        'revisedDates': input_dict['edhist_rev'] if 'edhist_rev' in input_dict else '',
        'acceptedDate': input_dict['edhist_acc'] if 'edhist_acc' in input_dict else ''
    }

    output['pubDate'] = {
        'electrDate': input_dict['pubdate_electronic'] if 'pubdate_electronic' in input_dict else '',
        'printDate': input_dict['pubdate_print'] if 'pubdate_print' in input_dict else '',
        # 'otherPubDate': {
        #     'otherDateType': 'XXX', # TODO
        #     'otherDate': 'XXX' # TODO
        # }
    }

    output['publication'] = {
        #'docType': 'XXX',
        'pubName': input_dict['publication'] if 'publication' in input_dict else '',
        #'confName': 'XXX',
        #'confLocation': 'XXX',
        #'confDates': 'XXX',
        #'confEditors': ['XXX'],
        #'confPID': 'XXX',
        'publisher': input_dict['publisher'] if 'publisher' in input_dict else '',
        'issueNum': input_dict['issue'] if 'issue' in input_dict else '',
        'volumeNum': input_dict['volume'] if 'volume' in input_dict else '',
        'pubYear': input_dict['pubdate_print'][0:4] if 'pubdate_print' in input_dict
        else (input_dict['pubdate_electronic'][0:4] if 'pubdate_electronic' in input_dict else ''),
        'ISSN': [{'pubtype': pubtype, 'issnString': issn} for (pubtype, issn) in input_dict['issn']] if 'issn' in input_dict else '',
        #'isRefereed': True or False
    }

    output['persistentIDs'] = [
        {
            #'Crossref': 'XXX',
            #'ISBN': 'XXX',
            'DOI': input_dict['ids']['doi'] if 'ids' in input_dict and 'doi' in input_dict['ids'] else '',
            'preprint': {
                'source': input_dict['ids']['preprint']['source'] if 'ids' in input_dict
                                                                     and 'preprint' in input_dict['ids']
                                                                     and 'source' in input_dict['ids']['preprint'] else '',
                'identifier': input_dict['ids']['preprint']['id'] if 'ids' in input_dict
                                                                     and 'preprint' in input_dict['ids']
                                                                     and 'id' in input_dict['ids']['preprint'] else ''
            }
        }
    ]

    output['publisherIDs'] = [
        {
            'attribute': i['attribute'] if 'attribute' in i else '',
            'Identifier': i['Identifier'] if 'Identifier' in i else ''
        }
    for i in input_dict['ids']['pub-id'] if 'ids' in input_dict and 'pub-id' in input_dict['ids']]

    output['pagination'] = {
        'firstPage': input_dict['page_first'] if 'page_first' in input_dict else '',
        'lastPage': input_dict['page_last'] if 'page_last' in input_dict else '',
        'pageCount': input_dict['numpages'] if 'numpages' in input_dict else '',
        'pageRange': input_dict['page_range'] if 'page_range' in input_dict else '',
        'electronicID': input_dict['electronic_id'] if 'electronic_id' in input_dict else ''
    }

    output['authors'] = [
        {
            'name': {
                'surname': i['surname'] if 'surname' in i else '',
                'given-name': i['given'] if 'given' in i else '',
                'middle-name': i['middle'] if 'middle' in i else '',
                'prefix': i['prefix'] if 'prefix' in i else '',
                'suffix': i['suffix'] if 'suffix' in i else '',
                'pubraw': i['nameraw'] if 'nameraw' in i else '',
                #'native-lang': 'XXX',
                #'collab': 'XXX' # TODO need a collab example
            },
            'affiliation': [
                {
                    'affPubRaw': j,
                    'affPubID': i['xaff'][idx],
                    #'affPubIDType': 'XXX' # TODO ask MT
                }
            for idx, j in enumerate(i['aff']) if 'aff' in i],
            'attrib': {
                #'collab': True or False, # TODO need a collab example
                #'deceased': True or False, # TODO need an example
                #'coauthor': True or False, # TODO need an example
                'email': i['email'] if 'email' in i else '',
                #'funding': 'XXX', # TODO need an example
                'orcid': i['orcid'] if 'orcid' in i else '',
            }
        }
    for i in input_dict['authors'] if 'authors' in input_dict]

    # output['otherContributor'] = [
    #     {
    #         'role': 'XXX',
    #         'contrib': {
    #             'name': {
    #                 'surname': 'XXX',
    #                 'given-name': 'XXX',
    #                 'middle-name': 'XXX',
    #                 'prefix': 'XXX',
    #                 'suffix': 'XXX',
    #                 'pubraw': 'XXX',
    #                 'native-lang': 'XXX',
    #                 'collab': 'XXX'
    #             },
    #             'affiliation': [
    #                 {
    #                     'affPubRaw': 'XXX',
    #                     'affPubID': 'XXX',
    #                     'affPubIDType': 'XXX'
    #                 }
    #             ],
    #             'attrib': {
    #                 'collab': True or False,
    #                 'deceased': True or False,
    #                 'coauthor': True or False,
    #                 'email': 'XXX',
    #                 'funding': 'XXX',
    #                 'orcid': 'XXX'
    #             }
    #         }
    #     }
    # ] # TODO need an example

    output['title'] = {
        'textEnglish': input_dict['title'] if 'title' in input_dict else '', # TODO need to tweak for case of foreign language title
        # 'textNative': 'XXX', # TODO
        # 'langNative': 'XXX' # TODO # TODO need an example
    }

    #output['subtitle'] = 'XXX' # TODO need an example

    output['abstract'] = {
        'textEnglish': input_dict['abstract'] if 'abstract' in input_dict else '', # TODO need to tweak for case of foreign language abstract
        #'textNative': 'XXX', # TODO
        #'langNative': 'XXX' # TODO
    }

    # output['comments'] = [
    #     {
    #         'commentOrigin': 'XXX',
    #         'commentText': 'XXX'
    #     }
    # ] # TODO need an example

    # output['fulltext'] = {
    #     'language': 'XXX',
    #     'body': 'XXX'
    # } # TODO this is from fulltext

    #output['acknowledgements'] = 'XXX' # TODO this is from fulltext

    output['references'] = (input_dict['references'] if type(input_dict['references']) == list else [input_dict['references']]) or ''

    # output['backmatter'] = [
    #     {
    #         'backType': 'XXX',
    #         'language': 'XXX',
    #         'body': 'XXX'
    #     }
    # ] # TODO need an example

    # output['astronomicalObjects'] = [
    #     'XXX'
    # ] # TODO need an example

    # output['esources'] = [
    #     {
    #         'source': 'XXX',
    #         'location': 'XXX'
    #     }
    # ] # TODO need an example

    # output['dataLinks'] = [
    #     {
    #         'title': 'XXX',
    #         'identifier': 'XXX',
    #         'location': 'XXX',
    #         'dataType': 'XXX',
    #         'comment': 'XXX'
    #     }
    # ] # TODO need an example

    #output['doctype'] = 'XXX' # TODO ask MT about this

    output['keywords'] = [
        {
            'keyString': i['string'] if 'string' in i else '',
            'keySystem': i['system'] if 'system' in i else '',
            # 'keyIdent': {
            #     'system': 'XXX',
            #     'keyID': 'XXX'
            # }
        }
    for i in input_dict['keywords'] if 'keywords' in input_dict]

    output['copyright'] = {
        'status': True if 'copyright' in input_dict else False, # TODO ask MT about this
        'statement': input_dict['copyright'] if 'copyright' in input_dict else ''
    }

    output['openAccess'] = {
        'open': input_dict['openAccess']['open'] if 'openAccess' in input_dict
                                                    and 'open' in input_dict['openAccess'] else False,
        # 'license': 'XXX',
        # 'licenseURL': 'XXX',
        # 'preprint': 'XXX',
        # 'startDate': 'XXX',
        # 'endDate': 'XXX',
        # 'embargoLength': 'XXX'
    } # TODO need an example

    # output['pubnote'] = 'XXX' # TODO need an example
    #
    # output['funding'] = 'XXX' # TODO need an example
    #
    # output['version'] = 'XXX' # TODO need an example

    output_clean = clean_empty(output)

    return output_clean