"""
TODO:
- DONE find twitter links
- DONE find twitter handles within HTML
- filter twitter links for endpoints like `intent`/`cards`
"""
import logging
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from itertools import chain
from typing import List, Iterable, Dict, Optional
from urllib3.util.url import parse_url

import toolz.curried as t
from bs4 import BeautifulSoup, SoupStrainer
from bs4.element import ResultSet
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from tenacity import (
    RetryError,
    retry,
    wait_fixed,
    retry_if_exception_type,
    stop_after_attempt,
)

TWITTER_HANDLE_FORMAT = r"[a-zA-Z0-9_]{1,15}"


def twitter_handle_regex() -> str:
    """."""

    pre = r"\B"
    post = r"[\s.,<]"  # Allow whitespace and some punctuation after handle
    # 1. Hard word boundary (incl. HTML closing tag)
    # 2. Capture `handle_format`
    # 3. Either: `post` or $
    return r"{}@({}){}|{}@({})$".format(
        pre, TWITTER_HANDLE_FORMAT, post, pre, TWITTER_HANDLE_FORMAT
    )


def twitter_link_regex() -> str:
    """."""
    return r"twitter.com/({})".format(TWITTER_HANDLE_FORMAT)


_TWITTER_HANDLE_REGEX = twitter_handle_regex()
_TWITTER_LINK_REGEX = twitter_link_regex()
_TWITTER_REGEX = re.compile(r"{}|{}".format(_TWITTER_HANDLE_REGEX, _TWITTER_LINK_REGEX))


def twitter_regex_matches(text: str) -> List[str]:
    """Runs a regex on `text` to extract twitter handle candidates."""

    matches = re.findall(_TWITTER_REGEX, text)
    return t.thread_last(
        matches, chain.from_iterable, t.filter(lambda x: x != ""), list
    )


class PossibleSchemeError(Exception):
    """Indicates Selenium scraping failed, likely due to scheme error."""

    pass


@retry(
    wait=wait_fixed(2),
    retry=retry_if_exception_type(TimeoutException),
    stop=stop_after_attempt(3),
)
def get(driver: WebDriver, url: str) -> Optional[str]:
    """GET `url` returning raw content response if successful."""
    driver.get("data:;")  # Avoid polluting state

    try:
        driver.get(url)
        # Wait up to 10 seconds for document readstate to be "complete"
        WebDriverWait(driver, 10).until(
            lambda driver: driver.execute_script("return document.readyState")
            == "complete"
        )
        return driver.page_source
    except WebDriverException as e:
        handle_webdriver_exception(e)
        logging.warning(f"{url} got error {e.msg}")
    except TimeoutException:
        pass
    except RetryError:
        logging.warning(f"Retry attempts failed for {url}")

    return None


def discover_anchor_tags(text: str) -> ResultSet:
    """Discover anchor tags within a souped webpage."""
    return BeautifulSoup(text, parse_only=SoupStrainer("a"), features="lxml").find_all(
        "a"
    )


def soup_for_regex(text: str) -> BeautifulSoup:
    """Soup a webpage, ignoring style and script."""
    soup = BeautifulSoup(text, features="lxml")
    [script_tag.extract() for script_tag in soup.find_all("script")]
    [style_tag.extract() for style_tag in soup.find_all("style")]
    return soup


def extract_links_from_anchor_results(anchors: ResultSet) -> Iterable[str]:
    return t.pipe(
        anchors, t.map(lambda x: x.get("href")), t.filter(lambda x: x is not None)
    )


@t.curry
def is_internal_link(full_url: str, link: str) -> bool:
    """Checks if `link` is a link to another page in the same location as `full_url`."""
    return link.startswith("/") or (
        get_network_location(full_url) == get_network_location(link) in link
    )


def get_network_location(link: str) -> str:
    """Parses `url` and extracts network location."""
    return parse_url(link).netloc


def find_links(content: str) -> Iterable[str]:
    return t.thread_last(
        content,
        discover_anchor_tags,
        extract_links_from_anchor_results,
    )


@t.curry
def take_best_link_candidates(n: int, links: Iterable[str]):
    """Take `n` best links from `links` according to simple heuristic."""
    links = list(links)
    n_head = n // 2
    n_tail = n - n_head
    return links[:n_head] + links[-n_tail:]


def default_to_https(url: str) -> str:
    """If `link` lacks `scheme` prepend 'https://'."""
    parsed_url = parse_url(url)
    if parsed_url.scheme is None:
        return f"https://{url}"
    else:
        return url

def https_to_http(url: str) -> str:
    """Swap use of https to http."""
    return url.replace("https://", "http://")

@t.curry
def resolve_link(full_url: str, link: str) -> str:
    """If `link` lacks `netloc` or `scheme` get them from `full_url`."""
    parsed_base = parse_url(full_url)
    parsed_link = parse_url(link)
    if parsed_link.netloc is None:
        link = parsed_base.netloc + link

    if parsed_link.scheme is None:
        link = f"{parsed_base.scheme}://{link}"

    return link


def chrome_driver() -> WebDriver:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")  # Docker /dev/shm too small
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(20)
    driver.set_script_timeout(20)
    return driver


def link_finder(driver: WebDriver, url: str, n: int = 4) -> Optional[List[str]]:
    """Discover links for `url`."""

    url = default_to_https(url)
    try:
        content = get(driver, url)
    except PossibleSchemeError:
        logging.warning(url)
        url = https_to_http(url)
        logging.warning(url)
        content = get(driver, url)

    if content is None:
        return []
    else:
        return t.pipe(
            content,
            find_links,
            t.filter(is_internal_link(url)),
            take_best_link_candidates(n),
            t.map(t.compose(resolve_link(url))),
            list,
        )


def handle_webdriver_exception(exception: WebDriverException) -> None:
    """Handle WebDriverException cases."""

    if "net::ERR_NAME_NOT_RESOLVED" in exception.msg:
        return
    elif "net::ERR_SSL_PROTOCOL_ERR" in exception.msg:
        raise PossibleSchemeError(exception.msg)
    elif "net::ERR_CONNECTION_REFUSED" in exception.msg:
        raise PossibleSchemeError(exception.msg)
    elif "net::ERR_CONNECTION_CLOSED" in exception.msg:
        raise PossibleSchemeError(exception.msg)
    elif "net::ERR_SSL_VERSION_OR_CIPHER_MISMATCH" in exception.msg:
        raise PossibleSchemeError(exception.msg)
    elif "Timed out receiving message from renderer" in exception.msg:
        raise PossibleSchemeError(exception.msg)
    else:
        return


def handle_finder(driver: WebDriver, url: str) -> Dict[str, Dict[str, int]]:
    """."""
    netloc = parse_url(url).netloc

    try:
        content = get(driver, url)
    except PossibleSchemeError:
        url = https_to_http(url)
        content = get(driver, url)

    if content is None:
        return {netloc: {}}

    try:
        handles = twitter_regex_matches(str(soup_for_regex(content)))
        return {netloc: dict(Counter(handles))}
    except RecursionError:
        logging.error(f"Recursion error for {url}")
        return {netloc: {}}


if __name__ == "__main__":
    urls: List[str] = ["https://nesta.org.uk/", "https://twitter.com"] * 2

