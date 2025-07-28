"""
Script that downloads raw financial statement data from SEC Financial Statements into the data/raw
folder.
"""

import requests
import zipfile
from io import BytesIO
from datetime import datetime
import os
from requests.exceptions import HTTPError

def download_and_unzip(url: str, extract_to: str):
    """
    Downloads a ZIP file from a URL and extracts its contents.

    Parameters
    ----------
    url: str
        URL pointing to ZIP file.
    extract_to: str
        Directory path where the contents will be extracted.
    
    Raises
    ------
    HTTPError
        If something goes wrong with accessing the URL.

    Returns
    -------
    None.
    """

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like "
                      "Gecko) Chrome/58.0.3029.110 Safari/537.36 (Contact: ericleonen@gmail.com)"
    }

    res = requests.get(url, headers=headers)
    res.raise_for_status()

    zip_file = zipfile.ZipFile(BytesIO(res.content))
    zip_file.extractall(path=extract_to)
    zip_file.close()

def download_raw_financial_statement_data(
        download_to: str,
        start_year: int = 2009,
        end_year: int | None = None
    ):
    """
    Downloads all SEC financial statement data from the given date range. Each quarter's files are
    placed in a folder titled 'YYYY_QQ'. If data for any quarter is unavailable for download
    or already downloaded, the quarter is skipped with a warning.

    Parameters
    ----------
    download_to: str
        Directory path where the data will be placed after downloaded.
    start_year: int, optional
        The first year from which to download data. Default is 2009.
    end_year: int | None, optional
        The last year from which to download data. None means the current year. Default in None.
    
    Raises
    ------
    HTTPError
        If something goes wrong with accessing the data.
    ValueError
        If the given date range is impossible to satisfy (i.e. start_year > end_year).

    Returns
    -------
    None
    """

    if end_year is None:
        end_year = datetime.now().year

    for year in range(start_year, end_year + 1):
        for quarter in range(1, 4 + 1):
            dirname = f"{year}_Q{quarter}"

            if not os.path.isdir(f"{download_to}/{dirname}"):
                print(f" - Downloading {dirname}.")

                try:
                    download_and_unzip(
                        url=f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}"
                            f"q{quarter}.zip",
                        extract_to=f"{download_to}/{dirname}"
                    )
                except HTTPError as err:
                    print(f"   > Error finding {year}q{quarter}.zip")
                    continue
            else:
                print(f" - {dirname} already in directory.")

if __name__ == "__main__":
    download_raw_financial_statement_data(
        download_to="data/raw",
        start_year=2009
    )