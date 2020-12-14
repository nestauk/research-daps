"""Helper functions for NSPL flow."""
import re
from io import BytesIO
from zipfile import ZipFile

import requests
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options


def find_download_url(driver: WebDriver, geo_portal_url: str) -> str:
    """Find download button and extract download URL."""
    driver.implicitly_wait(10)  # Poll DOM for up to 10 seconds when finding elements
    driver.get(geo_portal_url)
    return driver.find_element_by_id("simple-download-button").get_attribute("href")


def chrome_driver() -> WebDriver:
    """Headless Selenium Chrome Driver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(10)
    driver.set_script_timeout(10)
    return driver


def download_zip(url: str) -> ZipFile:
    """Download a URL and load into `ZipFile`."""
    response = requests.get(url)
    response.raise_for_status()
    return ZipFile(BytesIO(response.content), "r")


def parse_geoportal_url_to_nspl_csv(geoportal_url: str) -> str:
    """."""
    month, year = re.findall(r"lookup-(.*)-(.*)", geoportal_url)[0]
    month_abbrev = month.upper()[:3]
    return f"NSPL_{month_abbrev}_{year}_UK.csv"
