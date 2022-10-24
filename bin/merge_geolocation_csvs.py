''' merge_geolocation_csvs.py
    Merge geolocation Country/Region/City CSVa into a single
    Country CSV.
'''

import argparse
import glob
import re
import sys
import colorlog
import pandas


def process_files():
    ''' Process files to produce the CSV
        Keyword arguments:
          None
        Returns:
          None
    '''
    files = glob.glob(ARG.PREFIX + "_*.csv")
    if not files:
        LOGGER.critical("No files found for prefix %s", ARG.PREFIX)
        sys.exit(0)
    files.sort()
    country = {}
    years = []
    allcountry = pandas.DataFrame()
    for file in files:
        year = re.search(r"_(\d+).csv", file)[1]
        years.append(year)
        frame = pandas.read_csv(file)
        country[year] = frame.groupby(['Country']).sum("Count")
        if allcountry.empty:
            allcountry["Country"] = frame["Country"].drop_duplicates()
        else:
            allcountry = pandas.merge(left=allcountry["Country"], right=frame["Country"],
                                      how="outer", on='Country', sort=True).drop_duplicates()
    new_order = ["Country"]
    for year in years:
        new_order.append(year)
        frame = pandas.merge(left=allcountry["Country"], right=country[year],
                             how="outer", on='Country', sort=True).fillna(0)
        frame.columns = ["Country", year]
        if year == years[0]:
            final = frame
            final[year] = final[year].astype(int)
            final["Total"] = 0
        else:
            final[year] = frame[year].astype(int)
        final["Total"] = final["Total"] + final[year]
    new_order.append("Total")
    final = final[new_order]
    final.set_index("Country", inplace=True)
    print(final)
    with pandas.ExcelWriter(ARG.PREFIX + "_comparison.xlsx") as writer:
        final.to_excel(writer)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Create a combined yearly country count spreadsheet')
    PARSER.add_argument('--prefix', dest='PREFIX', action='store', required=True,
                        help='File prefix')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    ARG = PARSER.parse_args()
    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    process_files()
    sys.exit(0)
