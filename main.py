import scraping_kalibrr_re
import scraping_jobstreet_re
import scraping_glints_re

import time

def main():
    start = time.time()

    scraping_kalibrr_re.scrapeData('public.scrape_items')
    scraping_jobstreet_re.scrapeData('public.scrape_items')
    scraping_glints_re.scrapeData('public.scrape_items')
    
    end = time.time()
    print('Waktu total scraping data: ', end-start)
    
    
if __name__ == '__main__':
    main()
