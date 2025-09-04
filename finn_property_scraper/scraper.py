import asyncio
import json
import re
import traceback
import typing
from urllib.parse import urlencode
import zendriver as zd
from pydantic import BaseModel
from pydantic.v1.json import pydantic_encoder
from zendriver.core.tab import Tab

from finn_property_scraper.parsers.csv_exporter import properties_to_csv
from finn_property_scraper.parsers.property_page_parser import parse_property_page
from finn_property_scraper.schemas.property import Property, PropertyList
from finn_property_scraper.schemas.realestate_metadata import RealestateMetadata

FINN_BASE_URL = "https://www.finn.no/realestate/homes/search.html?filters"

REALESTATE_PATTERN = re.compile(r"^(?:https:\/\/www\.finn\.no)?/realestate/(?P<cat>[a-zA-Z]+)/ad\.html\?finnkode=(?P<finncode>[0-9]+)$")

def _generate_base_url(filters: dict[str, str]) -> str:
    """
    Generates a base scraping URL for Finn real estate.

    :param filters: filters to apply.
                    For example {"location": "0.20061"} is Oslo
    :return: URL with applied filters
    """
    if not filters:
        return FINN_BASE_URL

    # Encode filters into query string
    query_string = urlencode(filters)

    # Append encoded filters to base url
    return f"{FINN_BASE_URL}&{query_string}"






async def _find_realestate_meta(tab: Tab) -> list[RealestateMetadata]:
    a_element = await tab.select_all("a[href]")

    meta: list[RealestateMetadata] = []

    for element in a_element:
        url = element.attrs["href"]

        # try regex match
        match = REALESTATE_PATTERN.match(url)
        if match:
            meta.append(RealestateMetadata(
                url=f"https://www.finn.no{url}",
                category=match.group("cat"),
                finn_id=match.group("finncode"),
            ))
        else:
            continue

    return meta


async def _has_more_pages(tab: Tab) -> bool:
    # current page is given by aria-current='page' a class
    current_page_element = await tab.select('a[aria-current="page"]')
    current_page = int(current_page_element.text)

    all_page_elements = await tab.select_all('a[aria-label^="Side "]')
    all_pages = [int(el.text) for el in all_page_elements]

    max_page = max(all_pages)

    print(f"Current page: {current_page}, max page: {max_page}")

    return max_page >= current_page


def _load_neighbourhoods(geojson_path) -> dict[str, typing.Any]:
    with open(geojson_path) as f:
        return json.load(f)


async def scrape(filters: dict[str, str], max_pages: int | None = None, geojson_file_name: str = "../neighbourhoods.geojson") -> None:
    browser = await zd.start(sandbox=True, browser_executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", headless=True)


    page = 1
    property_meta: list[RealestateMetadata] = []

    neighbourhoods = _load_neighbourhoods(geojson_file_name)
    print(f"Loaded geojson for neighbourhoods: {geojson_file_name}")


    try:
        while True:
            filters_v2 = filters.copy()
            filters_v2["page"] = page

            url = _generate_base_url(filters_v2)
            print(f"Scraping page {page} with url {url}")

            tab = await browser.get(url)

            meta_for_page = await _find_realestate_meta(tab)

            print(f"Found mega in page {page}: {meta_for_page}")
            property_meta.extend(meta_for_page)

            if not await _has_more_pages(tab):
                break

            if max_pages and page >= max_pages:
                break
            page += 1
    except Exception:
        print(f"Error scraping page {page}: {traceback.format_exc()}")
        print(f"Stopping here...")


    # TODO - clean up duplicates
    print(f"Before filtering, have {len(property_meta)} items")
    properties = list(set(property_meta))
    print(f"After filtering, have {len(properties)} items")

    parsed_properties: list[Property] = []

    for property in properties:
        try:
            parsed = await parse_property_page(tab, property, neighbourhoods)
            print(f"Parsed property: {parsed}")
            parsed_properties.append(parsed)
        except Exception:
            print(f"Error parsing property {property}: {traceback.format_exc()}")

    properties_to_csv(parsed_properties, "output.csv")
    # json dump properties

    as_list = PropertyList(properties=parsed_properties)
    as_json = as_list.model_dump_json()
    with open("../output.json", "w") as f:
        f.write(as_json)

def main():
    asyncio.run(scrape({"location": "0.20061"}))
main()