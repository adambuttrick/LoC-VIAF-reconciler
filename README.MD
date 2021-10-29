# Library of Congress and VIAF Reconciliation Service 

A lightweight, Python alternative to Conciliator and Open Refine services for reconciling names with the Library of Congress and VIAF record clusters.

# Setup

pip install -r requirements.txt 

# Usage:

Assumes as input a CSV file (with a header) containing a local identifier, authority type ("PN" for personal name or "CB" for corporate body), and the name to be reconciled, in that order. Authority type is mandatory for constructing the VIAF API query. Local identifier is not, but you'll have to change the positions of the other inputs accordingly in the script.

search.py (names_to_be_reconciled).csv

# Notes

Reconciliation is somewhat slow. In order to avoid VIAF's rate limiting, there has to be a five second delay between requests. For clusters with large numbers of records, parsing to find the correct Library of Congress authority record can add up.