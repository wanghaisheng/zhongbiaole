import json
import os
import io
import threading
import requests
import time
import datetime
from dotenv import load_dotenv
from DataRecorder import Recorder
from getbrowser import setup_chrome
from markitdown import MarkItDown

# Initialize global variables
date_today = datetime.date.today().strftime("%Y-%m-%d")
os.makedirs("./result", exist_ok=True)
outfile = Recorder(f'./result/{date_today}.csv')
load_dotenv()

markitdown = MarkItDown()
concurrency = 5
browser = setup_chrome()

# File to save URLs
URLS_FILE = 'urls.txt'
# File to save processed URLs
PROCESSED_URLS_FILE = 'processed_urls.txt'


def save_urls(urls):
    with open(URLS_FILE, 'w') as f:
        for url in urls:
            f.write(url + '\n')


def load_urls():
    if not os.path.exists(URLS_FILE):
        return []
    with open(URLS_FILE, 'r') as f:
        return [line.strip() for line in f]


def save_processed_url(url):
    with open(PROCESSED_URLS_FILE, 'a') as f:
        f.write(url + '\n')


def load_processed_urls(file):
    if not os.path.exists(file):
        return set()
    with open(file, 'r') as f:
        return set(line.strip() for line in f)


def openai_api_call(api_key, prompt, model="gpt-4o-mini", retries=3, delay=5):
    urls =[
    "https://heisenberg-duckduckgo-66.deno.dev/v1/chat/completions",
    "https://heisenberg-duckduckgo-12.deno.dev/v1/chat/completions",

    "https://heisenberg-duckduckgo-38.deno.dev/v1/chat/completions"
]
    import random
    url=random.choice(urls)
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    data = {"model": model, "messages": [{"role": "user", "content": prompt}]}

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Request failed with status code {response.status_code}: {response.text}")
        except requests.RequestException as e:
            print(f"Request failed: {e}")

        if attempt < retries - 1:
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    
    return None


def md2json(md, api_key):
    prompt = (
        "extract structure csv,include project_name,purchasing_unit,administrative_area,"
        "announcement_time,review_experts list,total_bid_amount,采购标的,品目名称,品牌,规格型号,"
        "单价,数量,contact_phone,contact_person list,purchasing_unit_address,purchasing_unit_contact,"
        "pdf attchmentlink list,return as flat csv only\n" + md
    )
    response = openai_api_call(api_key, prompt)

    if  response is None:
        return None
    else:
        return response["choices"][0]["message"]["content"].strip()


def get_page_count():
    print("browser", browser)
    tab = browser.new_tab()
    domain = (
        "https://search.ccgp.gov.cn/bxsearch?searchtype=2&page_index=1&bidSort=0&buyerName=&projectId=&pinMu=0&"
        "bidType=7&dbselect=bidx&kw=CT&start_time=2024%3A06%3A25&end_time=2024%3A12%3A24&timeType=5&displayZone=&"
        "zoneId=&pppStatus=0&agentName="
    )

    tab.get(domain)
    print("Loading page count...")
    counts = tab.ele(".pager").eles("tag:a")[-2].text
    print(f"Found {counts} pages")
    tab.close()
    
    return int(counts)


def get_urls(counts):
    urls = []

    for page in range(1, counts + 1):
        print(f'Detecting page {page}')
        tab = browser.new_tab()
        domain = (
            f"https://search.ccgp.gov.cn/bxsearch?searchtype=2&page_index={page}&bidSort=0&buyerName=&projectId=&pinMu=0&"
            "bidType=7&dbselect=bidx&kw=CT&start_time=2024%3A06%3A25&end_time=2024%3A12%3A24&timeType=5&displayZone=&"
            "zoneId=&pppStatus=0&agentName="
        )
        tab.get(domain,timeout=30,retry=3)
        results = tab.eles("@href^http://www.ccgp.gov.cn/cggg/dfgg/")
        for i in results:
            url = i.attr("href")
            urls.append(url)
        # time.sleep(3)
        tab.close()
    return urls


def process_url(url, api_key):
    filename = url.split("/")[-1].replace(".htm", ".txt")
    md=None
    if os.path.exists(f'./result/{filename}'):
        md=' '.join(open(f'./result/{filename}',encoding='utf-8').readlines())
    else:
        tab = browser.new_tab()
    # tab.get(url)
        print('processing',url)
        tab.get(url,timeout=30,retry=3)

        html = tab.html
    
        if html is None or '中标公告' not in html:
            tab.close()
            return 

        md = markitdown.convert_stream(io.StringIO(html)).text_content
    

        outfile1 = Recorder(f'./result/{filename}')
        outfile1.add_data(md)
        outfile1.record()

    data = md2json(md, api_key)

    if data is None:
        if tab:
            tab.close()
        return

    if '```':
        data = data.replace('\n```', '').replace('```csv\n', '')
        print('===', data)
        
        for line in data.split('\n'):
            outfile.add_data(line.strip().split(','))
        save_processed_url(url)  # Save processed URL
    if tab:
        tab.close()

    time.sleep(5)

def main():
    api_key = os.getenv("OPENAI_API_KEY")
    api_key = '123456'
    
    if not api_key:
        raise SystemExit("Missing OPENAI_API_KEY environment variable.")
    urls=None
    if not os.path.exists(URLS_FILE):
        counts = get_page_count()
    
        if counts is None:
            print('No pages found')
            return

        urls = get_urls(counts)
        save_urls(urls)  # Save URLs to file
    else:


        urls = load_processed_urls(URLS_FILE)

    processed_urls = load_processed_urls(PROCESSED_URLS_FILE)
    tasks = []
    print(urls)
    
    for url in urls:
        url = url.replace('http://', 'https://')
        if url in processed_urls:
            continue  # Skip already processed URLs

        task = threading.Thread(target=process_url, args=(url, api_key))
        tasks.append(task)
        task.start()
        
        if len(tasks) >= concurrency:
            for task in tasks:
                task.join()
            tasks = []

    # Ensure remaining tasks are completed
    for task in tasks:
        task.join()
    
    outfile.record()

if __name__ == "__main__":
    main()
