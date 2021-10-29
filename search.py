import csv
import requests
from sys import argv
from os import getcwd, path
from re import sub
from urllib.parse import quote
from json.decoder import JSONDecodeError
from time import sleep
from thefuzz import fuzz


def clean_lccn(lccn):
    lccn = lccn.split('|')[1]
    lccn = ''.join(lccn.split())

    return lccn


def compare_auth_names(auth_name, matches):
    best_match = []
    best_match_ratio = 0
    for match in matches:
        match_name = match[0]
        # When a record with only a VIAF name (no LC) is passed
        # to the function.
        if match_name == '':
            match_name = match[2]
        match_ratio = fuzz.ratio(auth_name, match_name)
        if match_ratio > best_match_ratio:
            match.append(match_ratio)
            best_match = match
            best_match_ratio = match_ratio

    return best_match

# VIAF API only returns 10 records at a time, so where number
# of records is >10, we need to generate index for each page
# of records. 500 is an arbitray limit derived from testing.
# Should be enough to find a match without also needlessly
# checking records for names with a large number of records
# in the cluster.
def generate_indexes(num_records):
    index = 1
    indexes = [1]
    if num_records > 500:
        num_records = 500
    if num_records > 10:
        index_count = int(num_records/10)
        if num_records % 10 == 0:
            index_count -= 1
        index_count = int(num_records/10)
        for x in range(index_count):
            index += 10
            indexes.append(index)
        return [str(i) for i in indexes]
    else:
        return ['1']


def lc_auth_search(auth_name, auth_type):
    # Authors have to be distinguished between their two types
    # with corresponding values in the query in order to return
    # accurate results.
    if auth_type == 'PN':
        auth_type_flag = 'personal'
    if auth_type == 'CB':
        auth_type_flag = 'corporate'
    matched_records = []
    lc_name, lccn = None, None
    search_url = 'https://viaf.org/viaf/search/viaf?query=local.mainHeadingEl = "' + auth_name + '" and local.' + auth_type_flag + 'Names = "' + auth_name + \
        '" and local.sources = "lc"&recordSchema=http://viaf.org/VIAFCluster&maximumRecords=100&startRecord=1&resultSetTTL=300&httpAccept=application/json'
    print('Searching', auth_type_flag, '-', auth_name, '...', '\n')
    print(search_url, '\n')
    r = requests.get(search_url)
    try:
        num_records = int(
            r.json()['searchRetrieveResponse']['numberOfRecords'])
    # The VIAF API occasionally returns malformed JSON, so all we can do is
    # catch the exception and return a null/errant result.
    except JSONDecodeError:
        return ['JSON decode error', '', '', '']
    # Fall back to a VIAF only search (remove local.sources = "lc" from query)
    # if no records are returned.
    if num_records == 0:
        matched_records = viaf_only_search(auth_name, auth_type)
        return matched_records
    print("Evaluating", num_records, 'records...')
    indexes = generate_indexes(num_records)
    try:
        for i, index in enumerate(indexes):
            sleep(5)
            print('Evaluating records set at index', index, '...')
            search_url = 'https://viaf.org/viaf/search/viaf?query=local.mainHeadingEl = "' + auth_name + '" and local.' + auth_type_flag + 'Names = "' + auth_name + \
                '" and local.sources = "lc"&recordSchema=http://viaf.org/VIAFCluster&maximumRecords=100&startRecord=' + \
                index + '&resultSetTTL=300&httpAccept=application/json'
            r = requests.get(search_url)
            records = r.json()['searchRetrieveResponse']['records']
            num_records = r.json()['searchRetrieveResponse']['numberOfRecords']
            for record in records:
                record_data = record['record']['recordData']
                viaf_id = record_data['viafID']
                sources = record_data['sources']['source']
                # For records with multiple sources, identify the source containing
                # the LCCN by the use of "LC" in the source text field. Otherwise,
                # where there is only a single source (has to be LC because of
                # the query structure), pull the value from the @nsid field.
                for source in sources:
                    try:
                        if 'LC|' in source['#text']:
                            lccn = clean_lccn(source['#text'])
                    except TypeError:
                        lccn = sources['@nsid']
                headings = record_data['mainHeadings']['data']
                # Pull VIAF name depending on whether there are single or multiple
                # headings. API returns inconsistent key/value pair for this field,
                # depending on whether or not there are multiple headings.
                try:
                    viaf_name = headings[0]['text']
                except KeyError:
                    viaf_name = headings['text']
                if isinstance(headings, list):
                    for heading in headings:
                        # Sources have the same kind variable representation as is
                        # described in the previous comment.
                        source_data = heading['sources']['s']
                        if isinstance(source_data, list) and 'LC' in source_data:
                            lc_name = heading['text']
                        elif isinstance(source_data, str) and source_data == 'LC':
                            lc_name = heading['text']

                        matched_records.append(
                            [lc_name, lccn, viaf_name, viaf_id, num_records])
                else:
                    source_data = headings['sources']['s']
                    if isinstance(source_data, list) and 'LC' in source_data:
                        lc_name = headings['text']
                    elif isinstance(source_data, str) and source_data == 'LC':
                        lc_name = headings['text']

                    matched_records.append(
                        [lc_name, lccn, viaf_name, viaf_id, num_records])

        # Compare author names from all matches to author name input and return
        # the most similar (Levenshtein distance) result
        matched_records = compare_auth_names(auth_name, matched_records)
        if matched_records == [] or matched_records[0] == None or matched_records[1] == None:
            matched_records = viaf_only_search(auth_name, auth_type)
            return matched_records
        else:
            print('\n\n', 'Matched:', matched_records, '\n\n')
            return matched_records

    except KeyError:
        matched_records = viaf_only_search(auth_name, auth_type)
        return matched_records
    except JSONDecodeError:
        print('JSON decode error')
        return ['JSON decode error', '', '', '']

# Same as above function, without the LC as source qualifier. Fallback
# for when no LC authority record exists or other errors.
def viaf_only_search(auth_name, auth_type):
    if auth_type == 'PN':
        auth_type_flag = 'personal'
    if auth_type == 'CB':
        auth_type_flag = 'corporate'
    matched_records = []
    search_url = 'https://viaf.org/viaf/search/viaf?query=local.mainHeadingEl = "' + auth_name + '" and local.' + auth_type_flag + 'Names = "' + \
        auth_name + '"&recordSchema=http://viaf.org/VIAFCluster&maximumRecords=100&startRecord=1&resultSetTTL=300&httpAccept=application/json'
    print("Searching viaf only", '...', '\n', search_url, '\n')
    r = requests.get(search_url)
    try:
        num_records = int(
            r.json()['searchRetrieveResponse']['numberOfRecords'])
    except JSONDecodeError:
        matched_records.append(['JSON decode error', '', '', ''])
        return matched_records
    if num_records == 0:
        print('No match found')
        return ['No match found', '', '', '', ]
    print("Evaluating", num_records, 'records...')
    indexes = generate_indexes(num_records)
    try:
        for index in indexes:
            sleep(5)
            print('Evaluating records set at index', index, '...')
            search_url = 'https://viaf.org/viaf/search/viaf?query=local.mainHeadingEl = "' + auth_name + '" and local.' + auth_type_flag + 'Names = "' + \
                auth_name + '"&recordSchema=http://viaf.org/VIAFCluster&maximumRecords=100&startRecord=' + \
                index + '&resultSetTTL=300&httpAccept=application/json'
            r = requests.get(search_url)
            records = r.json()['searchRetrieveResponse']['records']
            num_records = r.json()['searchRetrieveResponse']['numberOfRecords']
            for record in records:
                record_data = record['record']['recordData']
                viaf_id = record_data['viafID']
                headings = record_data['mainHeadings']['data']
                try:
                    viaf_name = headings[0]['text']
                except KeyError:
                    viaf_name = headings['text']
                matched_records.append(
                    ['No LC name found', 'No lccn', viaf_name, viaf_id, num_records])

            matched_records = compare_auth_names(auth_name, matched_records)
            print('\n\n', 'Matched viaf only:', matched_records)
            return matched_records
    except KeyError:
        print('No match found')
        return ['No match found', '', '', '', ]
    except JSONDecodeError:
        print('JSON decode error')
        return ['JSON decode error', '', '', '']


def search_and_write(f):
    outfile = getcwd() + '/reconciled_results.csv'
    if not path.exists(outfile):
        header = ['local_id', 'type', 'local_name', 'lc_name', 'lccn',
                  'viaf_name', 'viaf_id', 'num_records', 'match_ratio']
        with open(outfile, 'w') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
    with open(f) as f_in:
        reader = csv.reader(f_in)
        # Remove or comment out if CSV input does not have a header.
        next(reader)
        for row in reader:
            # Assumes a local identifier, author type, and author name
            # as the CSV input. Change as needed.
            auth_name, auth_type = row[2], row[1]
            auth_type = auth_type.strip()
            # URL safe formatting for auth_name
            auth_name = sub('"', '', auth_name)
            quoted_auth_name = quote(auth_name)
            try:
                api_result = lc_auth_search(auth_name, auth_type)
            # Retry query after three minutes if we get a time out. Three
            # minutes is again an arbitrary, safe amount derived from testing.
            except requests.exceptions.RequestException:
                sleep(180)
                api_result = lc_auth_search(auth_name, auth_type)
            with open(outfile, 'a') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(row + api_result)


if __name__ == '__main__':
    search_and_write(argv[1])
