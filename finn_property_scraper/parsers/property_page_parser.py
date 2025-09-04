import asyncio
import re
import typing
from typing import Optional

import zendriver
from zendriver.core.browser import Browser
from zendriver.core.tab import Tab

from finn_property_scraper.schemas.property import Property, Address
from finn_property_scraper.schemas.realestate_metadata import RealestateMetadata

_NUM = re.compile(r"[^\d]")
_FLOAT = re.compile(r"[^\d,.\-]")

def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    return s or None

def _to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    digits = _NUM.sub("", s)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None

def _to_float_m2(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    cleaned = _FLOAT.sub("", s).replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None

def _to_int_safe(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None

async def _text(tab: Tab, selector: str) -> Optional[str]:
    try:
        el = await tab.select(selector, timeout=0.1)
    except asyncio.TimeoutError:
        return None
    if not el:
        return None
    try:
        return _clean(el.text)
    except Exception:
        try:
            t = await el.text_content()
            return _clean(t)
        except Exception:
            return None

async def _all_texts(tab: Tab, selector: str) -> list[str]:
    try:
        els = await tab.select_all(selector, timeout=0.1)
    except asyncio.TimeoutError:
        return []
    except zendriver.core.connection.ProtocolException:
        return []
    out: list[str] = []
    for el in els or []:
        t = None
        try:
            t = el.text
        except Exception:
            try:
                t = await el.text_content()
            except Exception:
                t = None
        t = _clean(t)
        if t:
            out.append(t)
    return out

def _split_address(addr: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not addr:
        return None, None, None
    parts = [p.strip() for p in addr.split(",")]
    if len(parts) == 1:
        # try inline postal+city
        m = re.search(r"(\d{4})\s+(.+)", parts[0])
        if m:
            return None, m.group(1), m.group(2).strip()
        return parts[0], None, None
    m = re.search(r"(\d{4})\s+(.+)", parts[1])
    postal, city = (m.group(1), m.group(2).strip()) if m else (None, None)
    return parts[0], postal, city

async def _status(tab: Tab) -> Optional[str]:
    # badge area lives inside object-details; if it contains "Solgt" -> sold
    texts = await _all_texts(tab, '[data-testid="object-details"] *')
    for t in texts:
        if t and t.strip().lower() == "solgt":
            return "sold"
    return "active"


# -------------------- main parse --------------------
async def parse_property_page(browser: Browser, meta: RealestateMetadata, neighbourhoods_geojson: dict[str, typing.Any] | None) -> Property:
    """
    Opens meta.url and parses the property page into a Property model.
    """
    tab: Tab = await browser.get(meta.url)

    # title + area (subtitle)
    title = await _text(tab, '[data-testid="object-title"] h1')
    subtitle = await _text(tab, '[data-testid="object-title"] [data-testid="local-area-name"]')

    # status
    status_val = await _status(tab)

    # address
    address_line = await _text(tab, '[data-testid="map-link"] [data-testid="object-address"]')
    line, postal_code, city = _split_address(address_line)


    address = Address(line=line, postal_code=postal_code, city=city)
    await address.resolve_lat_long()
    if neighbourhoods_geojson:
        address.find_neighbourhood(neighbourhoods_geojson)

    # description
    description_raw = await _text(tab, '[data-testid="om boligen"] .description-area')

    # pricing
    asking_price = _to_int(await _text(tab, '[data-testid="pricing-incicative-price"] .text-28, [data-testid="pricing-incicative-price"] .font-bold'))
    total_price = _to_int(await _text(tab, '[data-testid="pricing-total-price"] dd'))
    transaction_costs = _to_int(await _text(tab, '[data-testid="pricing-registration-charge"] dd'))
    shared_debt = _to_int(await _text(tab, '[data-testid="pricing-joint-debt"] dd'))
    communal_fees = _to_int(await _text(tab, '[data-testid="pricing-common-monthly-cost"] dd'))
    shared_equity = _to_int(await _text(tab, '[data-testid="pricing-collective-assets"] dd'))
    assessed_wealth_value = _to_int(await _text(tab, '[data-testid="pricing-tax-value"] dd'))

    # specs
    property_type = await _text(tab, '[data-testid="info-property-type"] dd')
    ownership_type = await _text(tab, '[data-testid="info-ownership-type"] dd')
    bedrooms = _to_int_safe(await _text(tab, '[data-testid="info-bedrooms"] dd'))
    rooms = _to_int_safe(await _text(tab, '[data-testid="info-rooms"] dd'))
    floor = _to_int_safe(await _text(tab, '[data-testid="info-floor"] dd'))
    year_built = _to_int_safe(await _text(tab, '[data-testid="info-construction-year"] dd'))
    energy_label = await _text(tab, '[data-testid="energy-label"] [data-testid="energy-label-info"]')

    area_bra_i = _to_float_m2(await _text(tab, '[data-testid="info-usable-i-area"] dd'))
    area_bra = _to_float_m2(await _text(tab, '[data-testid="info-usable-area"] dd'))
    plot_area = _to_float_m2(await _text(tab, '[data-testid="info-plot-area"] dd'))

    # facilities
    facilities = await _all_texts(tab, '[data-testid="object-facilities"]') or None

    # build Property
    prop = Property(
        title=_clean(title),
        subtitle=_clean(subtitle),
        description_raw=description_raw,
        category=_clean(meta.category),
        address=address,
        asking_price=asking_price,
        total_price=total_price,
        transaction_costs=transaction_costs,
        communal_fees=communal_fees,
        assessed_wealth_value=assessed_wealth_value,
        shared_debt=shared_debt,
        shared_equity=shared_equity,
        property_type=_clean(property_type),
        ownership_type=_clean(ownership_type),
        bedrooms=bedrooms,
        rooms=rooms,
        floor=floor,
        year_built=year_built,
        energy_label=_clean(energy_label),
        area_bra_i=area_bra_i,
        area_bra=area_bra,
        plot_area=plot_area,
        facilities=facilities,
        finn_code=_clean(meta.finn_id),
        url=_clean(meta.url),
        status=status_val,
    )
    return prop