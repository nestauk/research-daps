import pytest

from utils import (
    discover_anchor_tags,
    extract_links_from_anchor_results,
    get_network_location,
    is_internal_link,
)


@pytest.fixture
def html_sample():
    return b"""<a href='/internal_link'>foobar text</a>
    intermediate stuff
    <a>lonely</a>
    <b>don't parse this</b>
    <a><a href='google.com/link'>nested</a></a>"""


def test_discover_anchor_tags(html_sample):
    anchors = discover_anchor_tags(html_sample)

    assert len(anchors) == 4

    assert str(anchors[0]) == '<a href="/internal_link">foobar text</a>'


def test_extract_links_from_anchor_results(html_sample):

    anchors = discover_anchor_tags(html_sample)
    links = extract_links_from_anchor_results(anchors)
    assert len(list(links)) == 2


def test_get_network_location():
    assert (
        get_network_location("https://nesta.org.uk/subfolder/subpage.html")
        == "nesta.org.uk"
    )
    assert get_network_location("random://nesta.org.uk:8080") == "nesta.org.uk:8080"
    assert get_network_location("nesta.org.uk") == "nesta.org.uk"
    assert (
        get_network_location("https://subdomain.nesta.org.uk")
        == "subdomain.nesta.org.uk"
    )


def test_is_internal_link():
    full_url = "https://google.com/link"

    assert is_internal_link(full_url, "/foo/bar/baz.html")
    assert not is_internal_link(full_url, "#within-page-link")
    assert is_internal_link(full_url, "google.com/subpage.html")
    assert is_internal_link(full_url, "https://google.com/subpage.html")
    assert not is_internal_link(full_url, "nesta.org.uk/subpage.html")

    assert not is_internal_link(full_url, f"https://facebook.com/feed?link={full_url}")

    assert not is_internal_link(full_url, "//+1418125232″")

    # Failing cases:
    assert is_internal_link(full_url, "[~~]")


def test_find_links():
    pass


def test_take_best_link_candidates():
    pass
