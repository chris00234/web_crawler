import logging
import re
from urllib.parse import urlparse

# external libraries
import os
import pickle
from bs4 import BeautifulSoup
import nltk
from lxml import html
from urllib.parse import urljoin
import pdb

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """
    # File names to be used when loading and saving the crawler state
    # CRAWLER_DIR_NAME = "crawler_state"
    # MOST_LINKS_FILE_NAME = os.path.join(".", CRAWLER_DIR_NAME, "most_links.pkl")
    # TRAPS_FILE_NAME = os.path.join(".", CRAWLER_DIR_NAME, "traps.pkl")
    # LINKS_FILE_NAME = os.path.join(".", CRAWLER_DIR_NAME, "links.pkl")
    # DYNAMIC_URLS_FILE_NAME = os.path.join(".", CRAWLER_DIR_NAME, "dynamic.pkl")
    # SUBDOMAINS_FILE_NAME = os.path.join(".", CRAWLER_DIR_NAME, "subdomains.pkl")
    # WORDS_FILE_NAME = os.path.join(".", CRAWLER_DIR_NAME, "words.pkl")

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.most_links = [0 for i in range(2)] # In this list, 0 index will be the url with most pages, and 1 index will be how many valid links there are.
        self.most_words = [0 for i in range(2)] # In this list, 0 index will be the url with most words, and 1 index will be how many words there are.
        self.identified_traps = set() # set to keep all identified traps.
        self.links = set() # set to keep all valid links.
        self.dynamic_urls = {} # dictionary to keep track of dynamic url counts; used in is_trap().
        self.subdomains = {} # dictionary to keep each subdomain and their counts. Keys will be urls.
        self.words = {} # dictionary to keep track of each non-stopword and how many times they appear in the file.

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
        
        self.output_files('output.txt')

    # def save_crawler(self):
    #     """
    #     saves the current state of the crawler in 6 files using pickle
    #     """
    #     if not os.path.exists(self.CRAWLER_DIR_NAME):
    #         os.makedirs(self.CRAWLER_DIR_NAME)

    #     most_links_file = open(self.MOST_LINKS_FILE_NAME, "wb")
    #     traps_file = open(self.TRAPS_FILE_NAME, "wb")
    #     links_file = open(self.LINKS_FILE_NAME, "wb")
    #     dynamic_file = open(self.DYNAMIC_URLS_FILE_NAME, "wb")
    #     subdomain_file = open(self.SUBDOMAINS_FILE_NAME, "wb")
    #     words_file = open(self.WORDS_FILE_NAME, "wb")

    #     pickle.dump(self.most_links, most_links_file)
    #     pickle.dump(self.identified_traps, traps_file)
    #     pickle.dump(self.links, links_file)
    #     pickle.dump(self.dynamic_urls, dynamic_file)
    #     pickle.dump(self.subdomains, subdomain_file)
    #     pickle.dump(self.words, words_file)

    # def load_crawler(self):
    #     """
    #     loads the previous state of the crawler into memory, if exists
    #     """
    #     if os.path.isfile(self.MOST_LINKS_FILE_NAME) and os.path.isfile(self.TRAPS_FILE_NAME) and\
    #             os.path.isfile(self.LINKS_FILE_NAME) and os.path.isfile(self.DYNAMIC_URLS_FILE_NAME)\
    #             and os.path.isfile(self.SUBDOMAINS_FILE_NAME) and os.path.isfile(self.WORDS_FILE_NAME):
    #         try:
    #             self.most_links = pickle.load(open(self.MOST_LINKS_FILE_NAME, 'rb'))
    #             self.identified_traps = pickle.load(open(self.TRAPS_FILE_NAME, 'rb'))
    #             self.links = pickle.load(open(self.LINKS_FILE_NAME, 'rb'))
    #             self.dynamic_urls = pickle.load(open(self.DYNAMIC_URLS_FILE_NAME, 'rb'))
    #             self.subdomains = pickle.load(open(self.SUBDOMAINS_FILE_NAME, 'rb'))
    #             self.words = pickle.load(open(self.WORDS_FILE_NAME, 'rb'))
    #             logger.info(f"Crawler variables loaded successfully!\n{self.most_links}")
                
    #         except:
    #             pass
    #     else:
    #         logger.info("No previous crawler state found. No changes in variables are required")

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Used library: Beautiful Soup, lxml (Beautiful Soup was used over lxml because the code is easier to understand with this library)

        """
        if url_data['content'] == None or url_data['size'] == 0:
            return []
        if url_data['http_code'] == 400:
            return []
        
        outputLinks = []
        url_content = url_data['content']
        # if isinstance(url_content, bytes):
        #     url_content = url_content.decode('utf-8')
        original_url = url_data['final_url'] if url_data.get('is_redirected', False) and url_data['final_url'] else url_data['url']
        soup = BeautifulSoup(url_content, 'html.parser') # creates a soup object with the html file

        self.filter_words(soup, original_url) # this function will filter and add words from content of website to words' dictionary

        for link in soup('a'): # traverse through all '<a>' tags to check for href.
            link = link.get('href')
            if link: # checks if there is an href first
                absolute_link = urljoin(original_url, link) # transforms the link into an absolute link.
                outputLinks.append(absolute_link)
            # print(absolute_link)

        if len(outputLinks) > self.most_links[1]: # keeps track of page with most valid links
            self.most_links[0] = original_url
            self.most_links[1] = len(outputLinks) 

        return outputLinks
    
        # FOLLOWING CODE IS THE SAME AS THE ONE ABOVE BUT WITH LXML. I OPTED TO USE BEAUTIFUL SOUP SINCE IT IS EASIER TO READ.
        # --------------------------------------------------------------------------------------------------------------------
        # content_page = url_data['content'].decode('utf-8') # decodes content into a string.
        # content_page = ''.join(content_page) # transforms content_page into a single string with html code.
        # html_tree = html.fromstring(content_page)
        # links = html_tree.xpath('//a/@href')  # extracts all href attributes of <a> tags, into links.
        # outputLinks = [urljoin(url_data['url'], link) for link in links] # transforms each link into an absolute link
        # return outputLinksee
        # --------------------------------------------------------------------------------------------------------------------


    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method

        Sources I found interesting to assist me with handling traps:
            1. https://www.iquanti.com/insights/blog/guide-seo-spider-traps-causes-solutions/#:~:text=A%20never%2Dending%20spider%20traps,server%2Dside%20URL%20rewrite%20rules.&text=1234%2F1234%2Fsize11php-,http%3A%2F%2F%20abcwebsite.com%2F1234%2F1234%2F1234%2F,%2F1234%2F1234%2Fsize11php%E2%80%A6
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            if ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):
                if self.is_trap(url):
                    self.identified_traps.add(url)
                    return False
                
                subdomain = self.get_subdomain(parsed)
                if subdomain is not None:
                    if subdomain not in self.subdomains:
                        self.subdomains[subdomain] = 1
                    else:
                        self.subdomains[subdomain] += 1

                self.links.add(url)
                return True
            
            else: return False

        except TypeError:
            print("TypeError for ", parsed)
            return False
        
    def is_trap(self, url):
        '''
        Checks for all kinds of traps, including calendar traps, infinite traps with directories, long traps, etc.
        Nothing was hard-coded.

        Input: url path.
        Output: True if link is a trap, False otherwise.
        '''
        # The following code catches long url traps
        if len(url) > 750: # URLs that grow too big are long url traps.
            return True
        
        # The following code identifies dynamic urls and calendar traps.
        # Since not all dynamic urls are traps, we chose to put them in a dictionary to catch only the ones that keep changing the parameters infinitely, such as a calendar.
        dynamic_page = url.split('?') # will split dynamic url into 2 sections.
        if dynamic_page[0] not in self.dynamic_urls:
            self.dynamic_urls[dynamic_page[0]] = 0
        self.dynamic_urls[dynamic_page[0]] += 1
        if self.dynamic_urls[dynamic_page[0]] > 25: # We chose an arbritrary number
            return True # identified a dynamic url trap

        # The following code identifies directory traps.
        directories = urlparse(url).path.split('/') # returns a list that contains tokens
        dir_count = {} # dictionary to count how many directories of each will be
        for dir in directories:
            if dir not in dir_count:
                dir_count[dir] = 0
            dir_count[dir] += 1
            if dir_count[dir] >= 10: # if a directory occurs at least 10 times, we will consider this to be a directory trap.
                return True

        return False
    
    def get_subdomain(self, parsed_url):
        '''
        Scraps url to take off subdomain to be added to file later.

        Input: parsed url.
        Output: None if there is no subdomain in url, or subdomain of the given parsed_url.
        '''
        url_structure = parsed_url.netloc.split('.') # splits string given by url structure based on separator '.'
        if len(url_structure) < 3: # if structure < 3 strings, no subdomain.
            return None
        return url_structure[0] # returns subdomain

    
    def tokenize(self, text) -> list:
        '''
        Tokenizes a text and returns a list with the tokens.

        From assignment 1, we still change the non-alphanumeric characters for just a ' ', assuming that words \
        containing hyphen will be split into 2 words.
        '''
        # since we will only check the 50 most common words, we will exclude any token that includes numbers, so we will use isalpha.
        processed_text = ''.join([char if char.isalpha() else ' ' for char in text]) # changes any non-alphabethic character to a space.
        tokens = processed_text.split() # splits the content into tokens
        new_tokens = []
        for word in tokens:
            if len(word) >= 3:
                new_tokens.append(word) # considering a word to be a sequence of at least 3 alpha characters.
        
        return new_tokens
    
    def filter_words(self, soup, url):
        '''
        Filter words on page's content by using beautiful soup and tokenize function.

        Input: soup object.
        Output: None, updates words dictionary.
        '''
        txt = soup.get_text() # text contains all text content from webpage.
        list_tokens = self.tokenize(txt.lower()) # Convert text to lowercase before tokenizing
        
        if self.most_words[1] < len(list_tokens): # this page has the most words.
            self.most_words[0] = url
            self.most_words[1] = len(list_tokens)

        for word in list_tokens: # counts occurrences of each word.
            if word not in self.words:
                self.words[word] = 0
            self.words[word] += 1
        # Removed return 0
    
    def output_files(self, path: str):
        '''
        Creates an output file based on what type of file is.

        Library: nlkt (will be used to identify English stop words, so we do not have to create a set with all the English
        stop words from the website provided.)

        Input: file path to be written to, variables to keep track of, and type of file.
        Output: None, besides written file.
        '''
        # creates the file
        try:
            with open(path, 'w') as file:
                # Writes most links and url.
                file.write('URL with most links:\n')
                file.write(self.most_links[0] + ' with a total of ' + str(self.most_links[1]) + ' valid links.\n')

                # writes most words and url.
                file.write('\n---------------------------------------------------------------------------------\n')
                file.write('URL with most words:\n')
                file.write(self.most_words[0] + ' with a total of ' + str(self.most_words[1]) + ' valid words.\n')

                # writes the 50 most common words, not including stop words.
                file.write('\n---------------------------------------------------------------------------------\n')
                file.write('The 50 most common words in all pages content:\n')
                stop_words = { # the stopwords might not be 100% right but most will work.
                                "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "arent", "as", "at",
                                "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
                                "couldnt", "did", "didn't", "do", "does", "doesnt", "doing", "dont", "down", "during", "each", "few", "for",
                                "from", "further", "had", "hadnt", "has", "hasn't", "have", "havent", "having", "he", "hell", "hes",
                                "her", "here", "heres", "hers", "herself", "him", "himself", "his", "how", "hows", "i", "id", "ill", "im",
                                "ive", "if", "in", "into", "is", "isn", "it", "its", "itself", "lets", "me", "more", "most", "mustnt",
                                "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
                                "ourselves", "out", "over", "own", "same", "shant", "she", "shed", "shell", "shes", "should", "shouldnt", "so",
                                "some", "such", "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there",
                                "theres", "these", "they", "ll", "theyre", "this", "those", "through", "to", "too",
                                "under", "until", "up", "very", "was", "wasnt", "we", "were", "we've", "were", "weren", "what", "when", "whens",
                                "where", "wheres", "which", "while", "who", "whos", "whom", "why", "whys", "with", "would", "you", "ll", "re", "ve", "r", "s", "t", 
                                "will", "wont"
                            }
                filtered_words = {k: v for k, v in self.words.items() if k.lower() not in stop_words} # filter out stop words
                sorted_words = sorted(filtered_words.items(), key=lambda x: x[1], reverse=True) # sort dictionary in reverse order.
                sorted_words = dict(sorted_words)
                index = 1
                for key, value in sorted_words.items():
                    if index > 50:
                        break
                    file.write(key + '\t' + str(value) + '\n')
                    index += 1

                # Writes all subdomains and each time they appear.
                file.write('\n---------------------------------------------------------------------------------\n')
                file.write('Subdomains and amount of processed urls:\n')
                for key, value in self.subdomains.items():
                    if key != 'www': # while printing out the subdomains, we might have to exclude www from the printing list, based on how we chose to implement the subdomain function.
                        file.write(key + '\t' + str(value) + '\n')

                # Writes all traps that were filtered.
                file.write('\n---------------------------------------------------------------------------------\n')
                file.write('URLs that were considered a trap:\n')
                for link in self.identified_traps:
                    file.write(link + '\n')

                # Writes all urls identified.
                file.write('\n---------------------------------------------------------------------------------\n')
                file.write('URLs that were identified:\n')
                for link in self.links:
                    file.write(link + '\n')

        except FileNotFoundError:
            raise FileNotFoundError(f"The file {path} was not found.")
        except PermissionError:
            raise PermissionError(f"The file {path} cannot be written.")
