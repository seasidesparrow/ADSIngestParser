from datetime import datetime

TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

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
        'createdTime': None,
        'parsedTime': datetime.utcnow().strftime(TIMESTAMP_FMT),
        'loadType': 'fromURL' if format=='HTML' else 'fromFile',
        'loadFormat': format,
        'loadLocation': None,
        'recordOrigin': None
    }
    # TODO this should be an array of objects - talk to MT
    # TODO once we can parse more than just errata, need to expand this
    output['relatedTo'] = {
        'relationship': 'errata' if 'erratum' in input_dict else None,
        'relatedDocID': input_dict['erratum'] if 'errata' in input_dict else None
    }

    output['editorialHistory'] = {
        'receivedDates': input_dict['edhist_rec'] if 'edhist_rec' in input_dict else None,
        'revisedDates': input_dict['edhist_rev'] if 'edhist_rev' in input_dict else None,
        'acceptedDate': input_dict['edhist_acc'] if 'edhist_acc' in input_dict else None
    }

    output['pubDate'] = {
        'electrDate': input_dict['pubdate_electronic'] if 'pubdate_electronic' in input_dict else None,
        'printDate': input_dict['pubdate_print'] if 'pubdate_print' in input_dict else None,
        'otherPubDate': {
            'otherDateType': 'XXX', # TODO
            'otherDate': 'XXX' # TODO
        }
    }

    output['publication'] = {
        'docType': 'XXX',
        'pubName': input_dict['publication'] if 'publication' in input_dict else None,
        'confName': 'XXX',
        'confLocation': 'XXX',
        'confDates': 'XXX',
        'confEditors': ['XXX'],
        'confPID': 'XXX',
        'publisher': input_dict['publisher'] if 'publisher' in input_dict else None,
        'issueNum': input_dict['issue'] if 'issue' in input_dict else None,
        'volumeNum': input_dict['volume'] if 'volume' in input_dict else None,
        'pubYear': input_dict['pubdate_print'][0:4] if 'pubdate_print' in input_dict
        else (input_dict['pubdate_electronic'][0:4] if 'pubdate_electronic' in input_dict else None),
        'ISSN': [{'pubtype': pubtype, 'issnString': issn} for (pubtype, issn) in input_dict['issn']] if 'issn' in input_dict else None,
        'isRefereed': True or False
    }

    output['persistentIDs'] = [
        {
            'Crossref': 'XXX',
            'ISBN': 'XXX',
            'DOI': input_dict['ids']['doi'] if 'ids' in input_dict and 'doi' in input_dict['ids'] else None,
            'preprint': {
                'source': input_dict['ids']['preprint']['source'] if 'ids' in input_dict
                                                                     and 'preprint' in input_dict['ids']
                                                                     and 'source' in input_dict['ids']['preprint'] else None,
                'identifier': input_dict['ids']['preprint']['id'] if 'ids' in input_dict
                                                                     and 'preprint' in input_dict['ids']
                                                                     and 'id' in input_dict['ids']['preprint'] else None
            }
        }
    ]

    output['publisherIDs'] = [
        {
            'attribute': i['attribute'] if 'attribute' in i else None,
            'Identifier': i['Identifier'] if 'Identifier' in i else None
        }
    for i in input_dict['ids']['pub-id'] if 'ids' in input_dict and 'pub-id' in input_dict['ids']]

    output['pagination'] = {
        'firstPage': input_dict['page_first'] if 'page_first' in input_dict else None,
        'lastPage': input_dict['page_last'] if 'page_last' in input_dict else None,
        'pageCount': input_dict['numpages'] if 'numpages' in input_dict else None,
        'pageRange': input_dict['page_range'] if 'page_range' in input_dict else None,
        'electronicID': input_dict['electronic_id'] if 'electronic_id' in input_dict else None
    }

    output['authors'] = [
        {
            'name': {
                'surname': i['surname'] if 'surname' in i else None,
                'given-name': i['given'] if 'given' in i else None,
                'middle-name': i['middle'] if 'middle' in i else None,
                'prefix': i['prefix'] if 'prefix' in i else None,
                'suffix': i['suffix'] if 'suffix' in i else None,
                'pubraw': i['nameraw'] if 'nameraw' in i else None,
                'native-lang': 'XXX',
                'collab': 'XXX' # TODO need a collab example
            },
            'affiliation': [
                {
                    'affPubRaw': j,
                    'affPubID': i['xaff'][idx],
                    'affPubIDType': 'XXX' # TODO ask MT
                }
            for idx, j in enumerate(i['aff']) if 'aff' in i],
            'attrib': {
                'collab': True or False, # TODO need a collab example
                'deceased': True or False, # TODO need an example
                'coauthor': True or False, # TODO need an example
                'email': i['email'] if 'email' in i else None, # TODO this is a list - should it be (for collab)?
                'funding': 'XXX', # TODO need an example
                'orcid': i['orcid'] if 'orcid' in i else None # TODO this is a list - should it be (for collab)?
            }
        }
    for i in input_dict['authors'] if 'authors' in input_dict]

    output['otherContributor'] = [
        {
            'role': 'XXX',
            'contrib': {
                'name': {
                    'surname': 'XXX',
                    'given-name': 'XXX',
                    'middle-name': 'XXX',
                    'prefix': 'XXX',
                    'suffix': 'XXX',
                    'pubraw': 'XXX',
                    'native-lang': 'XXX',
                    'collab': 'XXX'
                },
                'affiliation': [
                    {
                        'affPubRaw': 'XXX',
                        'affPubID': 'XXX',
                        'affPubIDType': 'XXX'
                    }
                ],
                'attrib': {
                    'collab': True or False,
                    'deceased': True or False,
                    'coauthor': True or False,
                    'email': 'XXX',
                    'funding': 'XXX',
                    'orcid': 'XXX'
                }
            }
        }
    ] # TODO need an example

    output['title'] = {
        'textEnglish': input_dict['title'] if 'title' in input_dict else None, # TODO need to tweak for case of foreign language title
        'textNative': 'XXX', # TODO
        'langNative': 'XXX' # TODO # TODO need an example
    }

    output['subtitle'] = 'XXX' # TODO need an example

    output['abstract'] = {
        'textEnglish': input_dict['abstract'] if 'abstract' in input_dict else None, # TODO need to tweak for case of foreign language abstract
        'textNative': 'XXX', # TODO
        'langNative': 'XXX' # TODO
    }

    output['comments'] = [
        {
            'commentOrigin': 'XXX',
            'commentText': 'XXX'
        }
    ] # TODO need an example

    output['fulltext'] = {
        'language': 'XXX',
        'body': 'XXX'
    } # TODO this is from fulltext

    output['acknowledgements'] = 'XXX' # TODO this is from fulltext

    output['references'] = (input_dict['references'] if type(input_dict['references']) == list else [input_dict['references']]) or None

    output['backmatter'] = [
        {
            'backType': 'XXX',
            'language': 'XXX',
            'body': 'XXX'
        }
    ] # TODO need an example

    output['astronomicalObjects'] = [
        'XXX'
    ] # TODO need an example

    output['esources'] = [
        {
            'source': 'XXX',
            'location': 'XXX'
        }
    ] # TODO need an example

    output['dataLinks'] = [
        {
            'title': 'XXX',
            'identifier': 'XXX',
            'location': 'XXX',
            'dataType': 'XXX',
            'comment': 'XXX'
        }
    ] # TODO need an example

    output['doctype'] = 'XXX' # TODO ask MT about this

    output['keywords'] = [
        {
            'keyString': i['string'] if 'string' in i else None,
            'keySystem': i['system'] if 'system' in i else None,
            'keyIdent': {
                'system': 'XXX',
                'keyID': 'XXX'
            }
        }
    for i in input_dict['keywords'] if 'keywords' in input_dict]

    output['copyright'] = {
        'status': True if 'copyright' in input_dict else False, # TODO ask MT about this
        'statement': input_dict['copyright'] if 'copyright' in input_dict else None
    }

    output['openAccess'] = {
        'open': input_dict['openAccess']['open'] if 'openAccess' in input_dict
                                                    and 'open' in input_dict['openAccess'] else None,
        'license': 'XXX',
        'licenseURL': 'XXX',
        'preprint': 'XXX',
        'startDate': 'XXX',
        'endDate': 'XXX',
        'embargoLength': 'XXX'
    } # TODO need an example

    output['pubnote'] = 'XXX' # TODO need an example

    output['funding'] = 'XXX' # TODO need an example

    output['version'] = 'XXX' # TODO need an example

    return output