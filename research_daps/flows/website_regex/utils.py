"""
TODO:
- DONE find twitter links
- DONE find twitter handles within HTML
- filter twitter links for endpoints like `intent`/`cards`
"""
import logging
import re
from collections import Counter
from itertools import chain
from typing import List, Iterable, Dict, Optional, Callable
from urllib3.util.url import parse_url as parse_url_
from urllib3.exceptions import LocationParseError

import toolz.curried as t
from bs4 import BeautifulSoup, SoupStrainer
from bs4.element import ResultSet
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common.exceptions import (
    WebDriverException,
    TimeoutException,
)
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
    """Regex to find twitter handles."""

    pre = r"\B"
    post = r"[\s.,<]"  # Allow whitespace and some punctuation after handle
    # 1. Hard word boundary (incl. HTML closing tag)
    # 2. Capture `handle_format`
    # 3. Either: `post` or $
    return r"{}@({}){}|{}@({})$".format(
        pre, TWITTER_HANDLE_FORMAT, post, pre, TWITTER_HANDLE_FORMAT
    )


def twitter_link_regex() -> str:
    """Regex to find links to 'twitter.com'."""
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


class BrowserCrashException(Exception):
    """Indicates Selenium scraping failed due to browser crash."""

    pass


class Driver(object):
    """Callable wrapper for Selenium driver that returns a new driver after crash."""

    def __init__(self):
        """Initialise selenium driver."""
        self.driver = chrome_driver()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.driver.quit()

    def __call__(self):
        """Returns selenium driver on blank page with valid session."""
        from urllib3.exceptions import MaxRetryError

        try:
            self.driver.get("data:;")  # Reset state for next get
        except MaxRetryError as e:
            logging.error(f"Error wiping driver state: {e}")
            self.restart_driver()
        except WebDriverException as e:
            if ("invalid session id" in e.msg) or ("session deleted" in e.msg):
                self.restart_driver()
            else:
                logging.error(f"Driver error: {e.msg}")
                raise e
        return self.driver

    def restart_driver(self):
        """Restarts selenium driver."""
        logging.debug("Restarting driver...")
        try:
            self.driver.quit()
        except Exception as e:
            logging.error(f"Failed to close old driver: {e}")
        try:
            self.driver = chrome_driver()
        except Exception as e:
            logging.error(f"Failed to restart: {e}")


@retry(
    wait=wait_fixed(2),
    retry=(
        retry_if_exception_type(TimeoutException)
        | retry_if_exception_type(BrowserCrashException)
    ),
    stop=stop_after_attempt(2),
)
def get(driver: Callable[[], WebDriver], url: str) -> Optional[str]:
    """GET `url` returning raw content response if successful."""

    def get_(driver, url):
        try:
            logging.debug(f"Getting {url}")
            driver_ = driver()
            driver_.get(url)
            # Wait up to 10 seconds for document readstate to be "complete"
            WebDriverWait(driver_, 10).until(
                lambda driver: driver.execute_script("return document.readyState")
                == "complete"
            )
            return driver_.page_source
        except WebDriverException as e:
            possible_scheme_error_msgs = [
                "net::ERR_SSL_PROTOCOL_ERR",
                "net::ERR_CONNECTION_RESET",
                "net::ERR_CONNECTION_REFUSED",
                "net::ERR_CONNECTION_CLOSED",
                "net::ERR_SSL_VERSION_OR_CIPHER_MISMATCH",
            ]
            if (
                any([msg in e.msg for msg in possible_scheme_error_msgs])
                and parse_url(url).scheme != "http"
            ):
                raise PossibleSchemeError(e.msg)
            elif "Timed out receiving message from renderer" in e.msg:
                raise TimeoutException(e.msg)  # Captured by retry
            elif ("invalid session" in e.msg) or (
                "session deleted because of page crash" in e.msg
            ):
                logging.error(f"{url} got error {e.msg}")
                raise BrowserCrashException(e.msg)  # Captured by retry
            else:  # Includes "net::ERR_NAME_NOT_RESOLVED"
                return None
            logging.warning(f"{url} got error {e.msg}")
        except TimeoutException:
            raise  # Captured by retry

    try:
        return get_(driver, url)
    except PossibleSchemeError:  # Retry with modified argument
        logging.info(f"Trying {url} with 'http://' scheme")
        url = https_to_http(url)
        return get_(driver, url)


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
    try:
        url = parse_url(link)
        return (not url.netloc and url.path) or (
            get_network_location(full_url) == get_network_location(link)
        )
    except LocationParseError:
        logging.warning(f"Failed to parse one of: ['{full_url}', '{link}']")
        return False


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


def parse_url(url: str):
    """Safer parse url by stripping weird characters first."""
    return parse_url_(url.strip("‘'\""))


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

    chrome_options.add_experimental_option(  # Don't download images:
        "prefs", {"profile.managed_default_content_settings.images": 2}
    )
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")  # Docker /dev/shm too small
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(20)
    driver.set_script_timeout(20)
    return driver


def link_finder(
    driver: Callable[[], WebDriver], url: str, n: int = 4
) -> Optional[List[str]]:
    """Discover links for `url`."""

    url = default_to_https(url)
    try:
        content = get(driver, url)
    except RetryError:
        logging.warning(f"Retry attempts failed for {url}")
        return []
    # except WebDriverException as e:
    #     logging.warning(f"{url}: {e.msg}")
    #     if "invalid session" in e.msg:
    #         raise
    #     return []
    # except TimeoutException as e:
    #     logging.warning(f"{url}: {e.msg}")
    #     return []

    if content is None:
        return []
    else:
        try:
            return t.pipe(
                content,
                find_links,
                t.filter(is_internal_link(url)),
                take_best_link_candidates(n),
                t.map(resolve_link(url)),
                list,
            )
        except Exception as e:
            logging.error(e)
            return []


def handle_finder(
    driver: Callable[[], WebDriver], url: str
) -> Dict[str, Dict[str, int]]:
    """."""
    try:
        netloc = parse_url(url).netloc
    except LocationParseError as e:  # TODO : this shouldn't happen
        logging.error(e)
        return {f"__BADPARSE__{url}": {}}  # XXX : BAD!

    try:
        content = get(driver, url)
    except RetryError:
        logging.warning(f"Retry attempts failed for {url}")
        return {netloc: {}}
    # except WebDriverException as e:
    #     logging.warning(f"{url}: {e.msg}")
    #     return {netloc: {}}
    # except TimeoutException as e:
    #     logging.warning(f"{url}: {e.msg}")
    #     return {netloc: {}}

    if content is None:
        return {netloc: {}}

    try:
        handles = twitter_regex_matches(str(soup_for_regex(content)))
        return {netloc: dict(Counter(handles))}
    except RecursionError:
        logging.error(f"Recursion error for {url}")
        return {netloc: {}}
