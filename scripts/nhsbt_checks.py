import re
import csv
from datetime import datetime


def find_duplicates(items):
    seen = set()
    duplicate_items = list()

    for item in items:
        if item in seen:
            duplicate_items.append(item)
        else:
            seen.add(item)

    return duplicate_items


def split_name(name):
    name = name.upper()
    name = re.sub('[^A-Z ]', '', name)
    parts = name.split(' ')
    return parts


def match_names(a, b, n):
    a = set(x[:n] for x in split_name(a))
    b = set(x[:n] for x in split_name(b))
    c = a & b
    return len(c) > 0


def match_forenames(a, b):
    return match_names(a, b, 1)


def match_surnames(a, b):
    return match_names(a, b, 3)


def is_good_match(item):
    # No match
    if item[15] == '':
        return True

    ukt_surname = item[9]
    rr_surname = item[16]
    ukt_forename = item[10]
    rr_forename = item[17]

    # Forename and surname don't match
    # Also check if they have been swapped
    if (
        (
            not match_surnames(ukt_surname, rr_surname) and
            not match_forenames(ukt_forename, rr_forename)
        ) and
        (
            not match_surnames(ukt_surname, rr_forename) and
            not match_forenames(ukt_forename, rr_surname)
        )
    ):
        return False

    ukt_nhs_no = item[14]
    rr_nhs_no = item[21]

    # NHS numbers match
    if ukt_nhs_no == rr_nhs_no:
        return True

    ukt_dob = item[11]
    rr_dob = item[18]

    if ukt_dob != '' and rr_dob != '':
        ukt_dob = datetime.strptime(ukt_dob, '%d%b%Y')
        rr_dob = datetime.strptime(rr_dob, '%d/%m/%Y')

        year_match = ukt_dob.year == rr_dob.year
        month_match = ukt_dob.month == rr_dob.month
        day_match = ukt_dob.day == rr_dob.day

        # Month and day swapped
        if not month_match and ukt_dob.month == rr_dob.day and ukt_dob.day == rr_dob.month:
            month_match = True
            day_match = True

        # Two parts match
        dob_match = sum([year_match, month_match, day_match]) >= 2
    else:
        dob_match = True

    ukt_postcode = item[13].replace(' ', '').upper()
    rr_postcode = item[20].replace(' ', '').upper()

    # DOBs and postcodes don't match
    if not dob_match and ukt_postcode != rr_postcode:
        return False

    return True


def check(filename):
    with open(filename, 'rb') as f:
        data = [list(x) for x in csv.reader(f)][1:]

    rr_nos = [x[15] for x in data if x[15] != '']
    duplicate_rr_nos = find_duplicates(rr_nos)

    for duplicate_rr_no in duplicate_rr_nos:
        print 'duplicate RR number:', duplicate_rr_no

    nhs_nos = [x[21] for x in data if x[21] != '']
    duplicate_nhs_nos = find_duplicates(nhs_nos)

    for duplicate_nhs_no in duplicate_nhs_nos:
        print 'duplicate NHS number:', duplicate_nhs_no

    changed_matches = [(x[0], x[15]) for x in data if x[0] != x[15] and x[0] != '' and not x[0].startswith('9999')]

    for old_rr_no, new_rr_no in changed_matches:
        print 'changed match: {0} => {1}'.format(old_rr_no, new_rr_no or '(None)')

    bad_matches = [x for x in data if not is_good_match(x)]

    for bad_match in bad_matches:
        print 'bad match: {}'.format(bad_match)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    args = parser.parse_args()

    check(args.filename)
