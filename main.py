import re
from datetime import datetime

import certifi
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

# Add Certificate for connection
ca = certifi.where()

client = MongoClient("mongodb+srv://dmodzelewski:Start1234@hurtowniedanych.oqbfe.mongodb.net", tlsCAFile=ca)
db = client['search_laptops']
collection_3 = db.media_expert


def scrapper():
    job_sites = ['https://www.mediaexpert.pl/komputery-i-tablety/laptopy-i-ultrabooki/laptopy?limit=50',
                 'https://www.morele.net/kategoria/laptopy-31/',
                 'https://www.komputronik.pl/category/5022/laptopy.html?showBuyActiveOnly=1'
                 ]

    print("Łączenie do stron")

    r_me = requests.get(job_sites[0])
    r_mo = requests.get(job_sites[1])
    r_ko = requests.get(job_sites[2])

    first_page = BeautifulSoup(r_me.text, 'html.parser')
    second_page = BeautifulSoup(r_mo.text, 'html.parser')
    third_page = BeautifulSoup(r_ko.text, 'html.parser')

    # Get number of pages

    page_number_me = int(first_page.find_all("span", attrs={'class': 'from'})[0].text.split(" ")[1])
    page_number_mo = int(second_page.find_all("div", attrs={'class': 'pagination-btn-nolink-anchor'})[0].text)
    page_number_ko = int(third_page.find("div", {'class': 'product-list-top-pagination'}).findChildren("a")[-2].text)

    print("Zbieranie danych")

    get_all_data(page_number_mo, job_sites[1], "mo")
    get_all_data(page_number_me, job_sites[0], "me")
    get_all_data(page_number_ko, job_sites[2], "ko")

    print("Zakończono zbierać danych")


def import_data_to_mongo(data, collection):
    collection.insert_many(data.to_dict("record"))


def get_all_data(number_of_pages, site, flag):
    links = []
    names = []
    prices = []
    screens = []
    processors = []
    rams = []
    disks = []
    graphics = []
    specifications = {"links": [], "names": [], "prices": [], "screens": [], "processors": [], "rams": [],
                      "disks": [], "graphics": [], "data": []}
    if flag == "me":
        print("Trwa Pobieranie informacji ze strony Media Expert")
        for number in range(1, number_of_pages + 1):
            r = requests.get(site + f"&page={number}")
            page = BeautifulSoup(r.text, 'html.parser')
            laptops_containers = page.find_all('div', class_='offer-box')
            data = datetime.today().strftime("%d-%m-%Y")
            specifications = {"links": [], "names": [], "prices": [], "screens": [], "processors": [], "rams": [],
                              "disks": [], "graphics": [], "data": []}
            for container in laptops_containers:
                if container.find('div', class_='main-price is-big') is not None:
                    specifications["links"].append(f"https://www.mediaexpert.pl{container.h2.a.get('href')}")
                    specifications["names"].append(container.h2.a.text.strip())
                    specifications["prices"].append(
                        int(container.find('div', class_='main-price is-big').span.text.encode('ascii', 'ignore')))
                    parameters = []
                    for param in container.find('table', class_='list attributes'):
                        for elem in param.find_all("span", class_='attribute-values is-regular'):
                            parameters.append(elem.text.strip())
                        specifications["screens"].append(parameters[0])
                        specifications["processors"].append(parameters[1])
                        specifications["rams"].append(parameters[2])
                        specifications["disks"].append(parameters[3])
                        specifications["graphics"].append(parameters[4])
            laptop_information = pd.DataFrame({
                'links': specifications["links"],
                'names': specifications["names"],
                'prices': specifications["prices"],
                'screens': specifications["screens"],
                'rams': specifications["rams"],
                'disks': specifications["disks"],
                'graphics': specifications["graphics"],
                "processors": specifications["processors"]
            })

            if laptop_information.empty:
                pass
            else:
                import_data_to_mongo(laptop_information, db.media_expert)

            print(f"Pobrano - {number} z {number_of_pages}\n")

        print("Zakończono")
    elif flag == "mo":
        print("Trwa pobieranie informacji ze strony Morele")
        for number in range(1, number_of_pages + 1):
            r = requests.get(site + f",,,,,,,,0,,,,/{number}/")
            page = BeautifulSoup(r.text, 'html.parser')
            laptops_containers = page.find_all('div', class_='cat-product card')
            specifications = {"links": [], "names": [], "prices": [], "screens": [], "processors": [], "rams": [],
                              "graphics": [], "data": []}
            for container in laptops_containers:
                if (container.find('div', class_='cat-product-price price-box') is not None) and (
                        container.div.p.a is not None):

                    specifications["links"].append(
                        f"https://www.morele.net{container.find('a', class_='cat-product-image productLink').get('href')}")
                    specifications["names"].append(
                        container.find('a', class_='cat-product-image productLink').get('title'))
                    specifications["prices"].append(
                        int(''.join(container.find('div', class_='price-new').text.split(" ")[:2]).split(',')[0]))
                    parameters = []
                    for param in container.find_all('div', class_='cat-product-feature'):
                        parameters.append(param.b.text)

                    try:
                        specifications["screens"].append(parameters[3])
                    except IndexError:
                        specifications["screens"].append("Brak")
                    try:
                        specifications["processors"].append(parameters[2])
                    except IndexError:
                        specifications["processors"].append("Brak")
                    try:
                        specifications["rams"].append(parameters[1])
                    except IndexError:
                        specifications["rams"].append("Brak")
                    try:
                        specifications["graphics"].append(parameters[0])
                    except IndexError:
                        specifications["graphics"].append("Brak")
            laptop_information = pd.DataFrame({
                'links': specifications["links"],
                'names': specifications["names"],
                'prices': specifications["prices"],
                'screens': specifications["screens"],
                'rams': specifications["rams"],
                'graphics': specifications["graphics"],
                "processors": specifications["processors"]
            })
            import_data_to_mongo(laptop_information, db.morele)
            print(f"Pobrano - {number} z {number_of_pages}\n")

        print("Zakończono")

    elif flag == "ko":
        print("Trwa pobieranie informacji ze strony Komputronik")
        for number in range(1, number_of_pages + 1):
            r = requests.get(site + f"&p{number}")
            page = BeautifulSoup(r.text, 'html.parser')
            laptops_containers = page.find_all('li', class_='product-entry2')[:-1]
            specifications = {"links": [], "names": [], "prices": [], "screens": [], "processors": [], "rams": [],
                              "graphics": [], "data": []}
            for container in laptops_containers:
                if (container.find('div', class_='prices').span.span is not None):
                    specifications["links"].append(container.find('div', class_='pe2-head').a.get('href'))
                    specifications["names"].append(container.find('div', class_='pe2-head').a.text.strip())
                    specifications["prices"].append(''.join(re.findall(r'(\d+)', container.find('div', class_='prices').span.span.text)))
                    parameters = container.find('div', class_="inline-features").text.strip().split("|")
                    specifications["screens"].append(parameters[1])
                    specifications["processors"].append(parameters[0])
                    specifications["rams"].append(parameters[2])
                    specifications["disks"].append(parameters[3])
            laptop_information = pd.DataFrame({
                'links': specifications["links"],
                'names': specifications["names"],
                'prices': specifications["prices"],
                'screens': specifications["screens"],
                'rams': specifications["rams"],
                "processors": specifications["processors"]
            })
            import_data_to_mongo(laptop_information, db.komputronik)
            print(f"Pobrano - {number} z {number_of_pages}\n")



        print("Zakończono")
    else:
        print("Błąd")


if __name__ == '__main__':
    scrapper()
