"""
TODO:
- DONE find twitter links
- DONE find twitter handles within HTML
- filter twitter links for endpoints like `intent`/`cards`
"""
import logging
import re
from collections import defaultdict, Counter
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


@t.curry
def get(driver, url: str) -> str:
    """GET `url` returning raw content response if successful."""
    driver.get(url)

    # Wait up to 10 seconds for document readstate to be "complete"
    WebDriverWait(driver, 10).until(
        lambda driver: driver.execute_script("return document.readyState") == "complete"
    )

    return driver.page_source


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
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(10)
    driver.set_script_timeout(10)
    return driver


def link_finder(driver: WebDriver, url: str) -> Optional[List[str]]:
    """Discover links for `url`."""

    n = 4
    try:
        content = get(driver, default_to_https(url))
    except WebDriverException as e:
        if "net::ERR_NAME_NOT_RESOLVED" in e.msg:
            logging.warning(f"Couldn't resolve URL: {url}")
            return []
        else:
            logging.warning(f"{url} got error {e.msg}")
            return []
    except TimeoutException:
        logging.warning(f"Timeout on: {url}")
        return []

    return t.pipe(
        content,
        find_links,
        t.filter(is_internal_link(url)),
        take_best_link_candidates(n),
        t.map(t.compose(resolve_link(url))),
        list,
    )


def handle_finder(driver: WebDriver, url: str) -> Dict[str, Dict[str, int]]:
    """."""
    netloc = parse_url(url).netloc
    try:
        content = get(driver, url)
    except WebDriverException as e:
        if "net::ERR_NAME_NOT_RESOLVED" in e.msg:
            logging.warning(f"Couldn't resolve URL: {url}")
            return {netloc: {}}
        else:
            logging.warning(f"{url} got error {e.msg}")
            return {netloc: {}}
    except TimeoutException:
        logging.warning(f"Timeout on: {url}")
        return {netloc: {}}

    handles = twitter_regex_matches(str(soup_for_regex(content)))
    return {netloc: dict(Counter(handles))}


if __name__ == "__main__":
    assert 0
    handle_lookup: Dict[str, Counter] = defaultdict(Counter)

    urls: List[str] = ["https://nesta.org.uk/", "https://twitter.com"] * 2

    def chunk_link_finder(urls):
        with chrome_driver() as driver:
            return list(map(lambda x: link_finder(driver, x), urls))

    n_processes = 8
    chunksize = max(1, len(urls) // n_processes)
    print(chunksize)

    with ThreadPoolExecutor(n_processes) as executor:
        futures = [
            executor.submit(chunk_link_finder, url_chunk)
            for url_chunk in t.partition_all(chunksize, urls)
        ]
    wait(futures)

    links = t.pipe(futures, t.map(lambda x: x.result()), chain.from_iterable, list)
    print(links)
    print(len(list(filter(lambda x: len(x) == 0, links))))

    def chunk_handle_finder(urls):
        with chrome_driver() as driver:
            return list(map(lambda x: handle_finder(driver, x), urls))

    with ThreadPoolExecutor(n_processes) as executor:
        handles = list(executor.map(chunk_handle_finder, links))
    print(handles)
