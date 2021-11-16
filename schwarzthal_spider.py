import scrapy
from scrapy import signals
from pydispatch import dispatcher
import json
import time
import scrapy.downloadermiddlewares.robotstxt
import re


class SchwarzthalSpider(scrapy.Spider):
    name = "schwarzthal"
    start_urls = [
        "https://datacvr.virk.dk/data/visninger?soeg=&oprettet=null&ophoert=null&branche=&language=en-gb&type=virksomhed&sortering=default"
    ]
    results = {}
    counter = 0
    NUM_COMPANY = 1100

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        # pass

    def parse(self, response):
        #looping through each company (entry) within a page
        companies = response.css("div.item > .row > .col-sm-12 > .name > a::attr(href)")
        for company in companies:
            if self.counter < self.NUM_COMPANY:
                yield scrapy.Request(company.get(), self.parseInnerPage)
                time.sleep(1)

        nextPage = response.css("li.next > a::attr(href)").get()
        #looping through pages
        if nextPage is not None and self.counter < self.NUM_COMPANY:
            yield response.follow(nextPage, self.parse)
            time.sleep(1)

    def parseInnerPage(self, response):
        # regex to remove new line and white space characters from the start and end of a string
        regex = re.compile(r"^(\n*)(\s*)|(\n*)(\s*)$")
        company_data = {}
        company_name = response.css("h1.enhedsnavn::text").get()
        basic_info = response.css(".table.stamdata")
        for row in basic_info.css(".row"):
            key, value = row.css("[class^=col]")
            try:
                key = regex.sub("", key.css("strong::text").get())
            except Exception:
                #at the end of basic info there's a row where key is none and value is an irrelevant link, so bypass
                continue
            value = regex.sub("", "".join(value.css("::text").getall()))
            company_data[key] = value

        tables = response.css(".accordion-inner")
        table_names = list(
            filter(
                lambda x: not "\n" in x,
                response.css(".accordion-heading").css("*::text").getall(),
            )
        )[: len(tables)] #the last table is not needed, so chop it out

        for table_name, table in zip(table_names, tables):
            table_data = {}
            if table_name in [
                "Expanded business information",
                "Power to bind, key individuals and auditor",
                "Ownership",
            ]:
                for row in table.css(".row.dataraekker"):
                    self.record_parser(row, table_data, regex)
            elif table_name in ["Production units"]:
                rows = table.css(".row.dataraekker")
                for row in rows:
                    for sub_row in row.css(".row"):
                        self.record_parser(sub_row, table_data, regex)
            elif table_name in ["Registration history (in Danish)"]:
                rows = table.css(".row.dataraekker")
                for row in rows:
                    raw_text = "".join(row.css("::text").getall())
                    raw_text = re.sub(
                        "^\n|\n$",
                        "",
                        re.sub(
                            "\n+",
                            "\n",
                            raw_text.replace("Navn og adresse", "\nNavn og adresse") #some human typo that merges two separated lines
                            .replace("Første", "\nFørste")
                            .replace("Regnskabsår", "\nRegnskabsår")
                            .replace(":\n", ":"),
                        ),
                    )
                    lines = raw_text.split("\n")
                    for i in range(len(lines)):
                        if i == 0:
                            record_date, record_type = lines[i].split(" ", maxsplit=1)
                            table_data[record_date] = {}
                            table_data[record_date]["record_type"] = record_type
                        else:
                            splited_line = lines[i].split(":")
                            if len(splited_line) == 2:
                                table_data[record_date][splited_line[0]] = splited_line[
                                    1
                                ]
            elif table_name in ["Historical basic data"]:
                raw_text = "".join(table.css("::text").getall())
                # clean \n and space characters
                raw_text = re.sub("\n+", "\n", raw_text)
                raw_text = re.sub("^\n| \n $", "", re.sub("\n+", "\n", raw_text))
                sub_tables = re.split("\n(.+\nFrom.+)", raw_text)
                #reconstruct clean version of original sub tables. After split some part is separated too
                for i in range(1, len(sub_tables), 2):
                    sub_tables[i] = "".join([sub_tables[i], sub_tables[i + 1]])
                first_table = sub_tables[0]
                sub_tables = [sub_tables[i] for i in range(1, len(sub_tables), 2)]
                sub_tables.append(first_table)
                # here we successfully splitted all sub tables
                for sub_table in sub_tables:
                    splited_rows = sub_table.split("\n")
                    splited_rows = list(
                        filter(lambda x: x not in ["", "\n"], splited_rows)
                    )
                    sub_table_name = splited_rows[0]
                    first_col = splited_rows[1]
                    second_col = splited_rows[2]
                    third_col = "value"
                    sub_table_data = {}
                    try:
                        for i in range(3, len(splited_rows), 3):
                            sub_table_data[first_col] = splited_rows[i]
                            sub_table_data[second_col] = splited_rows[i + 1]
                            sub_table_data[third_col] = splited_rows[i + 2]
                        table_data[sub_table_name] = sub_table_data
                    except Exception:
                        pass

            company_data[table_name] = table_data

        company_data["name"] = company_name
        if self.counter < self.NUM_COMPANY:
            print("Current counter:", self.counter)
            self.counter += 1
        self.results[self.counter] = company_data

    def record_parser(self, row, table_data, regex): # a parser first few tables in a page share
        record = row.css("[class^=col]")
        if len(record) >= 2:
            key, values = record[0:2]
            key = regex.sub("", key.css("strong::text").get())
            values = [regex.sub("", value) for value in values.css("::text").getall()]
            if len(values) == 1:
                values = values[0]
            table_data[key] = values

    def spider_closed(self):
        with open("results.json", "w", encoding="utf8") as f:
            json.dump(self.results, f, ensure_ascii=False)
