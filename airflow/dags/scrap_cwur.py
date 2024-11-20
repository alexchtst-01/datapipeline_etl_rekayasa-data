import scrapy

class scrapCwur(scrapy.Spider):
    name = "myspider"
    start_urls = ["https://cwur.org/2024.php"]
    
    def parse(self, response):
        rows = response.css('table#cwurTable tbody tr')
        
        for row in rows:
            yield {
                'world_rank': row.css('td:nth-child(1)::text').get(),
                'institution': row.css('td:nth-child(2) a::text').get(),
                'location': row.css('td:nth-child(3)::text').get(),
                'national_rank': row.css('td:nth-child(4)::text').get(),
                'education_rank': row.css('td:nth-child(5)::text').get(),
                'employability_rank': row.css('td:nth-child(6)::text').get(),
                'faculty_rank': row.css('td:nth-child(7)::text').get(),
                'research_rank': row.css('td:nth-child(8)::text').get(),
                'score': row.css('td:nth-child(9)::text').get()
            }

        # Pagination (if there is a next page)
        next_page = response.css('a.next::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

