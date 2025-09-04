

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field
import aiohttp
from urllib.parse import quote

from shapely import Point
from shapely.geometry.geo import shape


class Address(BaseModel):
    line: str | None 
    """Adresselinje (f.eks. 'Jerikoveien 91B')"""

    postal_code: str | None 
    """Postnummer"""

    city: str | None 
    """Poststed/by"""

    lat: float | None = None
    """Breddegrad (WGS84)"""

    lon: float | None = None
    """Lengdegrad (WGS84)"""

    neighbourhood: str | None = None
    """Nabolag"""

    async def resolve_lat_long(self) -> tuple[float, float] | None:
        """Resolve latitude and longitude using Geonorge API."""

        search_parts = []
        if self.line:
            search_parts.append(self.line)
        if self.postal_code:
            search_parts.append(self.postal_code)
        if self.city:
            search_parts.append(self.city)

        if not search_parts:
            return None

        search_string = " ".join(search_parts)
        encoded_search = quote(search_string)
        url = f"https://ws.geonorge.no/adresser/v1/sok?sok={encoded_search}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("adresser") and len(data["adresser"]) > 0:
                            rep_punkt = data["adresser"][0].get("representasjonspunkt")
                            print(f"Resolved lat/lon for '{search_string}': {rep_punkt.get("lat")}, {rep_punkt.get("lon")}")
                            if rep_punkt and rep_punkt.get("lat") is not None and rep_punkt.get("lon") is not None:
                                self.lat = rep_punkt.get("lat")
                                self.lon = rep_punkt.get("lon")
                                return self.lat, self.lon
                    return None
        except Exception:
            return None

    def find_neighbourhood(self, geojson_data: dict[str, str]) -> str | None:
        """
        Find the neighbourhood for given coordinates using GeoJSON data.

        Args:
            lat: Latitude (WGS84)
            lon: Longitude (WGS84)
            geojson_data: Loaded GeoJSON data as dictionary

        Returns:
            Neighbourhood name or None if not found
        """
        if not self.lat or not self.lon:
            return None

        point = Point(self.lon, self.lat)  # Note: Point takes (lon, lat) not (lat, lon)

        for feature in geojson_data.get("features", []):
            try:
                polygon = shape(feature["geometry"])
                if polygon.contains(point):
                    neigh = feature["properties"].get("neighbourhood")
                    print(f"Resolved neighbourhood for lat: {self.lat}, lon {self.lon}: {neigh}")
                    self.neighbourhood = neigh
            except Exception:
                continue

        return None

class Property(BaseModel):
    # -----------------------
    # Identitet og tekst
    # -----------------------
    title: str | None 
    """Tittel på annonsen"""

    subtitle: str | None 
    """Undertittel eller teaser"""

    description_raw: str | None 
    """Lang beskrivelse (råtekst)"""

    category: str | None 
    # what kind (home, future project, etc)

    # -----------------------
    # Adresse
    # -----------------------
    address: Address | None 
    """Strukturert adresse"""

    # -----------------------
    # Pris/økonomi
    # -----------------------
    asking_price: int | None 
    """Prisantydning (kr)"""

    total_price: int | None 
    """Totalpris (kr)"""

    transaction_costs: int | None 
    """Omkostninger (kr)"""

    communal_fees: int | None 
    """Felleskostnader eller kommunale avgifter (kr)"""

    assessed_wealth_value: int | None 
    """Formuesverdi (kr)"""

    shared_debt: int | None 
    """Fellesgjeld (kr)"""

    shared_equity: int | None 
    """Fellesformue (kr)"""

    # -----------------------
    # Spesifikasjoner
    # -----------------------
    property_type: str | None 
    """Boligtype"""

    ownership_type: str | None 
    """Eieform"""

    bedrooms: int | None 
    """Antall soverom"""

    rooms: int | None 
    """Antall rom"""

    floor: int | None 
    """Etasje"""

    year_built: int | None 
    """Byggeår"""

    energy_label: str | None 
    """Energimerking"""

    area_bra_i: float | None 
    """Internt bruksareal (BRA-i)"""

    area_bra: float | None 
    """Bruksareal (BRA)"""

    plot_area: float | None 
    """Tomteareal (m²)"""

    facilities: list[str] | None 
    """Fasiliteter"""

    # -----------------------
    # Proveniens
    # -----------------------
    finn_code: str | None 
    """FINN-kode"""

    url: str | None 
    """Annonsens URL"""

    status: str | None 
    """Status (active/sold/ended)"""

    scraped_at: datetime | None = Field(default_factory=datetime.utcnow)
    """Tidspunkt for innhenting"""

    raw_meta: dict | None 
    """Eventuelle ekstra felt som ikke er strukturert"""


class PropertyList(BaseModel):
    properties: list[Property]