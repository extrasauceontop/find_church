import pandas as pd
from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
import re

locator_domains = [] #
page_urls = [] #
location_names = [] #
street_addresses = [] #
citys = [] #
states = [] #
zips = [] #
country_codes = [] #
store_numbers = [] #
phones = [] #
location_types = []
latitudes = [] #
longitudes = [] #
hours_of_operations = []

headers = {"user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"}
session = SgRequests(retry_behavior=False)
df = pd.read_csv("list.csv")

urls = df["page_url"].to_list()[:1000]

x = 0
for url in urls:
    print(url)
    session = SgRequests()
    x = x+1

    response = session.get(url, headers=headers).text
    if "awaiting verification" in response and "The contact data we hold" in response:
        street_addresses.append("<MISSING>")
        states.append("<MISSING>")
        zips.append("<MISSING>")
        phones.append("<MISSING>")
        location_types.append("<MISSING>")
        hours_of_operations.append("<MISSING>")
    
    
        continue

    soup = bs(response, "html.parser")

    try:
        address_parts = soup.find("div",  attrs={"class": "contact_section"}).find("span").text.strip().split("\n")
    
    except Exception:
        session = SgRequests()
        response = session.get(url, headers=headers).text
        if "awaiting verification" in response and "The contact data we hold" in response:
            street_addresses.append("<MISSING>")
            states.append("<MISSING>")
            zips.append("<MISSING>")
            phones.append("<MISSING>")
            location_types.append("<MISSING>")
            hours_of_operations.append("<MISSING>")
        
        
            continue

        soup = bs(response, "html.parser")
        address_parts = soup.find("div",  attrs={"class": "contact_section"}).find("span").text.strip().split("\n")



    found_address = "No"
    found_zip = "No"
    for part in address_parts:
        if part[0].isdigit() is True and found_address == "No":
            address = part
            found_address = "Yes"

        elif bool(re.search(r'\d', part)) is True and found_zip == "No":
            zipp = part
            found_zip = "Yes"

            index = address_parts.index(part)
            state = address_parts[index-1]
    
    if found_address == "No":
        address = address_parts[0]
    
    if found_zip == "No":
        
        if address_parts[-1] == "(This is not necessarily the venue address.)":
            zipp = address_parts[-3]
            state = address_parts[-4]
        
        else:
            zipp = address_parts[-2]
            state = address_parts[-3]
    
    phone = soup.find("span", attrs={"class": "contact_phone"}).text.strip().replace(" ", "")
    print(phone)
    if re.search('[a-zA-Z]', phone) is True:
        phone = "<MISSING>"

    try:
        location_type = soup.find("div", attrs={"class": "tag"}).text.strip()

    except Exception:
        location_type = "<MISSING>"
    
    try:
        hours_parts = soup.find("section", attrs={"id": "profile_worship"}).find_all("div", {"class": "service_time si_summary"})
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
