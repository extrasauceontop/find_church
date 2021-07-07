#  The address section for this site is inconsistent, with a ton of locations missing everything except for latitude and longitude.

from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
from sgzip.dynamic import DynamicGeoSearch, SearchableCountries
from sglogging import sglog

log = sglog.SgLogSetup().get_logger(logger_name="findchurch")

search = DynamicGeoSearch(country_codes=[SearchableCountries.BRITAIN])
session = SgRequests(retry_behavior=False)

# There are two functions that get data (get_urls, and get_data). 1# means the data point is grabbed in the first function, 2#'s in the second function
locator_domains = []
page_urls = []
location_names = []
street_addresses = []
citys = []
states = []
zips = []
country_codes = []
store_numbers = []
phones = []
location_types = []
latitudes = []
longitudes = []
hours_of_operations = []

headers_list = [
    {
        "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"
    },
    {"User-Agent": "PostmanRuntime/7.19.0"},
]


# Here use SgZip to get a list of all location URLs, and other data points
def get_urls():
    x = 0
    search = DynamicGeoSearch(country_codes=[SearchableCountries.BRITAIN])
    session = SgRequests(retry_behavior=False)
    for search_lat, search_lon in search:
        x = x + 1
        url = (
            "https://www.findachurch.co.uk/ajax/Nearby.ashx?CenterLat="
            + str(search_lat)
            + "&CenterLon="
            + str(search_lon)
        )
        y = 0
        while True:
            y = y + 1
            if y == 10:
                raise Exception
            try:
                response = session.get(url).text
                break

            except Exception:
                session = SgRequests()
                continue

        soup = bs(response, "html.parser")

        locations = soup.find_all("row")

        for location in locations:
            locator_domains.append("findachurch.co.uk")
            store_number = location["id"]
            location_name = location["title"]
            city = location["town"]
            latitude = location["latlon"].split(",")[0]
            longitude = location["latlon"].split(",")[1]
            url_city = re.sub(r"[^A-Za-z0-9 ]+", "", city).lower().replace(" ", "-")
            page_url = (
                "https://www.findachurch.co.uk/church/"
                + url_city
                + "/"
                + store_number
                + ".htm"
            )
            search.found_location_at(latitude, longitude)
            store_numbers.append(store_number)
            location_names.append(location_name)
            citys.append(city)
            latitudes.append(latitude)
            longitudes.append(longitude)
            page_urls.append(page_url)
            country_codes.append("UK")

    df = pd.DataFrame(
        {
            "store_number": store_numbers,
            "location_name": location_names,
            "city": citys,
            "latitude": latitudes,
            "longitude": longitudes,
            "page_url": page_urls,
        }
    )

    return df


# Here you iterate through the location URLs to grab the missing data fields for each location
def get_data(df):
    # Some data cleaning to remove duplicates and get a list of the page urls
    df = df.drop_duplicates()
    page_url_list = df["page_url"].to_list()
    x = 0
    session = SgRequests(retry_behavior=False)

    # Iterate through the URLs
    for url in page_url_list:
        log.info(url)
        x = x + 1

        try:

            response = session.get(url, headers=headers_list[1], timeout=5).text

        except Exception:
            session = SgRequests(retry_behavior=False)

        if (
            "awaiting verification" in response
            and "The contact data we hold" in response
        ):
            street_addresses.append("<MISSING>")
            states.append("<MISSING>")
            zips.append("<MISSING>")
            phones.append("<MISSING>")
            location_types.append("<MISSING>")
            hours_of_operations.append("<MISSING>")

            continue

        soup = bs(response, "html.parser")

        try:
            address_parts = (
                soup.find("div", attrs={"class": "contact_section"})
                .find("span")
                .text.strip()
                .split("\n")
            )

        except Exception:
            session = SgRequests(retry_behavior=False)
            response = session.get(url, headers=headers_list[0]).text
            if (
                "awaiting verification" in response
                and "The contact data we hold" in response
            ):
                street_addresses.append("<MISSING>")
                states.append("<MISSING>")
                zips.append("<MISSING>")
                phones.append("<MISSING>")
                location_types.append("<MISSING>")
                hours_of_operations.append("<MISSING>")

                continue

            soup = bs(response, "html.parser")
            address_parts = (
                soup.find("div", attrs={"class": "contact_section"})
                .find("span")
                .text.strip()
                .split("\n")
            )

        found_address = "No"
        found_zip = "No"
        for part in address_parts:
            try:
                if part[0].isdigit() is True and found_address == "No":
                    address = part
                    found_address = "Yes"

                elif bool(re.search(r"\d", part)) is True and found_zip == "No":
                    zipp = part
                    found_zip = "Yes"

                    index = address_parts.index(part)
                    state = address_parts[index - 1]

            except Exception:
                pass

        if found_address == "No":
            address = address_parts[0]

        if found_zip == "No":

            if address_parts[-1] == "(This is not necessarily the venue address.)":
                try:
                    zipp = address_parts[-3]
                    state = address_parts[-4]
                except:
                    zipp = "<MISSING>"
                    state = "<MISSING>"

            else:
                zipp = address_parts[-2]
                state = address_parts[-3]

        phone = (
            soup.find("span", attrs={"class": "contact_phone"})
            .text.strip()
            .replace(" ", "")
        )

        if "e" in phone:
            phone = "<MISSING>"

        try:
            location_type = soup.find("div", attrs={"class": "tag"}).text.strip()

        except Exception:
            location_type = "<MISSING>"

        try:
            hours_parts = soup.find(
                "section", attrs={"id": "profile_worship"}
            ).find_all("div", {"class": "service_time si_summary"})
            hours = ""
            for part in hours_parts:
                hours = hours + part.text.strip() + ", "
            hours = hours[:-2]

        except Exception:
            hours = "<MISSING>"

        street_addresses.append(address)
        states.append(state)
        zips.append(zipp)
        phones.append(phone)
        location_types.append(location_type)
        hours_of_operations.append(hours)


df = get_urls()

data = get_data(df)

df = pd.DataFrame(
    {
        "locator_domain": locator_domains[: len(street_addresses)],
        "page_url": page_urls[: len(street_addresses)],
        "location_name": location_names[: len(street_addresses)],
        "street_address": street_addresses,
        "city": citys[: len(street_addresses)],
        "state": states[: len(street_addresses)],
        "zip": zips[: len(street_addresses)],
        "store_number": store_numbers[: len(street_addresses)],
        "phone": phones[: len(street_addresses)],
        "latitude": latitudes[: len(street_addresses)],
        "longitude": longitudes[: len(street_addresses)],
        "hours_of_operation": hours_of_operations[: len(street_addresses)],
        "country_code": country_codes[: len(street_addresses)],
        "location_type": location_types[: len(street_addresses)],
    }
)

df = df.fillna("<MISSING>")
df = df.replace(r"^\s*$", "<MISSING>", regex=True)

df["dupecheck"] = (
    df["location_name"]
    + df["street_address"]
    + df["city"]
    + df["state"]
    + df["location_type"]
)

df = df.drop_duplicates(subset=["dupecheck"])
df = df.drop(columns=["dupecheck"])
df = df.replace(r"^\s*$", "<MISSING>", regex=True)
df = df.fillna("<MISSING>")

df.to_csv("data.csv", index=False)
