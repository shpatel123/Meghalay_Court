import scrapy
import json
import os
import html
from scrapy.http import FormRequest, Request
from bs4 import BeautifulSoup
from urllib.parse import unquote


class MeghalayaCourtSpider(scrapy.Spider):
    name = "meghalaya_court"
    allowed_domains = ["meghalayahighcourt.nic.in"]
    start_urls = ["https://meghalayahighcourt.nic.in/orders"]

    def __init__(self, fdate="01-01-2024", tdate="20-01-2024", status="pending", download_pdfs=True, *args, **kwargs):
        super(MeghalayaCourtSpider, self).__init__(*args, **kwargs)

        self.fdate = fdate
        self.tdate = tdate
        self.status = status
        self.download_pdfs = download_pdfs
        self.cases_list = []

        # Define table structures for each selector
        self.table_structures = {
            "case_detials": {
                "name": "Case Details",
                "columns": ["Case Type/CNR", "Filing No: Date", "Reg No: Date"],
                "output_type": "dict"
            },
            "cat_detials": {
                "name": "Category Details",
                "columns": ["Category", "Sub Category"],
                "output_type": "dict"
            },
            "cs_status": {
                "name": "Case Status",
                "columns": ["Decision Date/Status", "Coram", "Branch/Bench/Causelist"],
                "output_type": "dict"
            },
            "pet_dtl": {
                "name": "Petitioner Details",
                "columns": ["Petitioner", "Advocate"],
                "output_type": "list"
            },
            "res_dtl": {
                "name": "Respondent Details",
                "columns": ["Respondent", "Advocate"],
                "output_type": "list"
            },
            "orders": {
                "name": "Order Details",
                "columns": ["Order No", "Bench", "Order Date", "Order Details"],
                "output_type": "list",
                "link_column": 3
            }
        }

    def parse(self, response):
        form_build_id = response.css(
            'input[name="form_build_id"]::attr(value)').get()

        if not form_build_id:
            print("Failed to fetch form_build_id.")
            return

        payload = {
            "qry": "odate",
            "form_build_id": form_build_id,
            "form_id": "case_order_form1",
            "fdate": self.fdate,
            "tdate": self.tdate,
            "status": self.status
        }

        yield FormRequest(
            url="https://meghalayahighcourt.nic.in/orders?ajax_form=1&_wrapper_format=drupal_ajax",
            formdata=payload,
            callback=self.parse_cases
        )

    def parse_cases(self, response):
        textarea_content = response.xpath("//textarea/text()").get()

        if not textarea_content:
            print("No JSON data found inside <textarea>.")
            return

        try:
            response_json = json.loads(textarea_content)
        except json.JSONDecodeError:
            print("Failed to decode JSON response.")
            return

        html_content = next((item.get("data") for item in response_json
                             if item.get("command") == "insert" and item.get("selector") == ".cstable"), None)

        if not html_content:
            print("No case data found in the response.")
            return

        soup = BeautifulSoup(html_content, "html.parser")
        table_body = soup.find("tbody")

        if not table_body:
            print("No case data found in the response.")
            return

        rows = table_body.find_all("tr")
        print(f"Found {len(rows)} rows in the table.")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                case_link_element = cells[0].find("a", href=True)
                case_number = case_link_element.text.strip(
                ) if case_link_element else cells[0].text.strip()
                case_link = case_link_element["href"] if case_link_element else None
                if case_link and not case_link.startswith("http"):
                    case_link = f"https://meghalayahighcourt.nic.in{case_link}"

                pdf_element = cells[3].find("a", href=True)
                pdf_link = pdf_element["href"] if pdf_element else None
                if pdf_link and not pdf_link.startswith("http"):
                    pdf_link = f"https://meghalayahighcourt.nic.in{pdf_link}"

                citation_number = None
                if pdf_element:
                    citation_div = pdf_element.find("div", class_="nc")
                    citation_number = citation_div.text.strip(
                    ) if citation_div else cells[3].text.strip()

                case_data = {
                    "Case Number": case_number,
                    "Case Link": case_link,
                    "Judge Name": cells[1].text.strip(),
                    "Order Date": cells[2].text.strip(),
                    "Citation Number": citation_number,
                    "PDF Link": pdf_link
                }

                self.cases_list.append(case_data)

                if case_link:
                    yield Request(
                        url=case_link,
                        callback=self.parse_case_details,
                        meta={
                            'case_index': len(self.cases_list) - 1,
                            'pdf_link': pdf_link,
                            'case_number': case_number,
                            'order_date': cells[2].text.strip()
                        }
                    )
                elif pdf_link and self.download_pdfs:
                    yield self.download_pdf(pdf_link, case_number, cells[2].text.strip(), is_main=True)

        self.save_to_json()

    def parse_case_details(self, response):
        print(f"Processing Case Link: {response.url}")
        case_index = response.meta['case_index']
        pdf_link = response.meta['pdf_link']
        case_number = response.meta['case_number']
        order_date = response.meta['order_date']

        try:
            textarea_content = response.xpath("//textarea/text()").get()

            if not textarea_content:
                print("No JSON data found in case details response.")
                return

            response_json = json.loads(textarea_content)
            case_details = {}

            for item in response_json:
                if item.get("command") == "insert" and "data" in item:
                    selector = item.get("selector", "").strip(".")
                    if selector in self.table_structures:
                        html_content = item.get("data")
                        if html_content:
                            clean_html = html.unescape(html_content)
                            soup = BeautifulSoup(clean_html, "html.parser")
                            table_data = self.extract_table_data(
                                soup, selector)
                            if table_data:
                                case_details[self.table_structures[selector]
                                             ["name"]] = table_data
                                # Queue order detail PDFs for download
                                if selector == "orders" and self.download_pdfs:
                                    for order in table_data:
                                        if isinstance(order.get("Order Details"), str) and order["Order Details"].endswith('.pdf'):
                                            yield self.download_pdf(
                                                order["Order Details"],
                                                case_number,
                                                order["Order Date"],
                                                is_main=False,
                                                order_no=order["Order No"]
                                            )

            self.cases_list[case_index]["Details"] = case_details

            # Download main PDF if exists
            if pdf_link and self.download_pdfs:
                yield self.download_pdf(pdf_link, case_number, order_date, is_main=True)

            self.save_to_json()

        except json.JSONDecodeError:
            print("Failed to decode JSON response.")
        except Exception as e:
            print(f"Error processing case details: {e}")

    def download_pdf(self, url, case_number, order_date, is_main=True, order_no=None):
        # Create a unique filename
        safe_case_number = "".join(
            c for c in case_number if c.isalnum() or c in ("_", "-"))
        if is_main:
            filename = f"Main_Order_{order_date.replace('-', '_')}_{safe_case_number}.pdf"
        else:
            filename = f"Order_{order_no}_{order_date.replace('-', '_')}_{safe_case_number}.pdf"

        # Create directory structure
        output_dir = os.path.join("pdf_downloads", safe_case_number)
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)

        return Request(
            url=url,
            callback=self.save_pdf,
            meta={'filepath': filepath},
            dont_filter=True
        )

    def save_pdf(self, response):
        filepath = response.meta['filepath']
        try:
            with open(filepath, 'wb') as f:
                f.write(response.body)
            print(f"Successfully downloaded PDF to {filepath}")
        except Exception as e:
            print(f"Failed to download PDF from {response.url}: {e}")

    def extract_table_data(self, soup, selector):
        table_config = self.table_structures[selector]
        table = soup.find("table")

        if not table:
            return None if table_config["output_type"] == "dict" else []

        rows = table.find_all("tr")
        result = [] if table_config["output_type"] == "list" else {}

        if selector == "orders" and rows:
            relevant_rows = []
            if rows:
                relevant_rows.append(rows[1])
            if len(rows) > 1:
                relevant_rows.append(rows[-1])

            for row in relevant_rows:
                cols = row.find_all("td")
                if len(cols) >= len(table_config["columns"]):
                    row_data = {}
                    for i, col_name in enumerate(table_config["columns"]):
                        text = cols[i].get_text(strip=True)

                        if i == table_config.get("link_column"):
                            link = cols[i].find("a")
                            if link and link.get("href"):
                                link_url = link["href"]
                                if not link_url.startswith("http"):
                                    link_url = f"https://meghalayahighcourt.nic.in{link_url}"
                                row_data[col_name] = link_url
                                continue

                        row_data[col_name] = text

                    if table_config["output_type"] == "list":
                        result.append(row_data)
                    else:
                        result.update({k: v for k, v in row_data.items() if v})
        else:
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= len(table_config["columns"]):
                    row_data = {}
                    for i, col_name in enumerate(table_config["columns"]):
                        text = cols[i].get_text(strip=True)
                        row_data[col_name] = text
                    if table_config["output_type"] == "list":
                        result.append(row_data)
                    else:
                        result.update({k: v for k, v in row_data.items() if v})
        return result

    def save_to_json(self):
        output_filename = f"cases_{self.fdate}_to_{self.tdate}.json"
        output_filepath = os.path.join(os.getcwd(), output_filename)

        try:
            with open(output_filepath, "w", encoding="utf-8") as json_file:
                json.dump(self.cases_list, json_file,
                          indent=4, ensure_ascii=False)
            print(f"Data successfully saved to {output_filepath}")
        except Exception as e:
            print(f"Failed to save JSON file: {e}")