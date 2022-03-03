import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os.path
import csv
import threading
from queue import Queue

# Proxies for BURP - update requests if you want to use this proxy
proxies = {"http": "http://127.0.0.1:8080", "https": "http://127.0.0.1:8080"}
playersFile = 'sample_corrected.txt'
ids = Queue()
# Please be nice to the PDGA site :)
THREADS = 1


class Player:
    def __init__(self, pdga):
        self.store = []
        self.failure = False
        self.pdga = pdga

        r = requests.get(f'https://www.pdga.com/player/{pdga}')
        soup = BeautifulSoup(r.text, 'html.parser')
        pi = soup.find('ul', class_='player-info info-list')

        self.today = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        # if access denied|page not found go to next player
        self.check_failures(soup)
        if self.failure:
            return

        player = soup.h1.get_text()

        # Fields that will always exist for all members
        self.name = player.split(' #')[0].replace(',', ' ')
        self.status = pi.find('li', class_='membership-status').text.split('Status:  ')[1].split(' ')[0]

        # The remaining fields may not be on the profile so I had to check to see if they exist before parsing
        expiration = pi.find('li', class_='membership-status').text.split('Status:  ')[1]
        if 'until' in expiration:
            self.expiration = expiration.split('until ')[1].replace(')', '')
        else:
            self.expiration = expiration.split('as of ')[1].replace(')', '')

        self.joindate = pi.find('li', class_='join-date')
        if self.joindate:
            self.joindate = self.joindate.text.split('Member Since: ')[1].split(' ')[0]
        else:
            self.joindate = ''

        try:
            location = pi.find('li', class_='location').text.split('Classification:')[0].split('Location: ')[1].split(
                ',')
        except:
            location = ''

        if location:
            # City, State, Country
            if len(location) >= 3:
                self.city = location[0].lstrip()
                self.state = location[1].lstrip()
                self.country = location[2].split('Member Since: ')[0].lstrip()
            # Only State/Prov, Country
            if len(location) == 2:
                self.city = 'N/A'
                self.state = location[0].lstrip()
                self.country = location[1].split('Member Since: ')[0].lstrip()
            # Country Only
            if len(location) == 1:
                self.city = 'N/A'
                self.state = 'N/A'
                self.country = location[0].split('Member Since: ')[0].lstrip()
            self.loclink = pi.find('li', class_='location').find('a')['href']
        else:
            self.city = ''
            self.state = ''
            self.country = ''
            self.loclink = ''

        self.rating = pi.find('li', class_='current-rating')
        if self.rating:
            self.rating = self.rating.text.split('Current Rating: ')[1].split(' ')[0]
        else:
            self.rating = ''

        self.classification = pi.find('li', class_='classification')
        if self.classification:
            self.classification = self.classification.text.split('Classification:  ')[1]
        else:
            self.classification = ''

        self.events = pi.find('li', class_='career-events')
        if self.events:
            self.events = self.events.text.split('Career Events: ')[1].replace(',', '')
        else:
            self.events = ''

        self.earnings = pi.find('li', class_='career-earnings')
        if self.earnings:
            self.earnings = self.earnings.text.split('Career Earnings: ')[1].replace(',', '').strip('$')
        else:
            self.earnings = '0'

        self.wins = pi.find('li', class_='career-wins disclaimer')
        if self.wins:
            self.wins = self.wins.text.split('Career Wins: ')[1]
        else:
            self.wins = '0'

        self.store_vals()
        self.write_data()

    # Set values to store in file
    def store_vals(self):
        self.store = [self.pdga, self.name, self.city, self.state, self.country, self.loclink, self.classification,
                      self.joindate, self.status, self.expiration, self.rating, self.events, self.wins, self.earnings,
                      self.today]
        print(self.store)

    # Append player to file
    def write_data(self):
        with open(playersFile, 'a+', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(self.store)

    # Display detailed data on each a player
    def verbose(self):
        print(f'Scrape Date: {self.today}')
        print(f"ID: {self.pdga}")
        print(f"Name: {self.name}")
        print(f"Status: {self.status}")
        print(f"Expiration: {self.expiration}")
        print(f"City: {self.city}")
        print(f"State: {self.state}")
        print(f"Location Link: {self.loclink}")
        print(f"Country: {self.country}")
        print(f"Rating: {self.rating}")
        print(f"Classification: {self.classification}")
        print(f"Events: {self.events}")
        print(f"Wins: {self.wins}")
        print(f"Earnings: {self.earnings}")

    # Check if player page exists before trying to scrape profile
    def check_failures(self, soup):
        fail = ['Page not found', 'Access denied']
        if any(x in soup.h1.get_text() for x in fail):
            print(f'Not a valid player: {self.pdga}')
            self.name = ''
            self.status = ''
            self.start = ''
            self.expiration = ''
            self.city = ''
            self.state = ''
            self.country = ''
            self.loclink = ''
            self.rating = ''
            self.classification = ''
            self.earnings = ''
            self.events = ''
            self.wins = ''
            self.joindate = ''
            self.store_vals()
            self.failure = True


# Create player file if it doesn't exist. If it does exist return the next user to scrape.
def check_file():
    header = ['id', 'name', 'city', 'state', 'country', 'loclink', 'classification', 'joindate', 'status', 'expiration',
              'rating', 'events', 'wins', 'earnings', 'scrape date']
    if os.path.exists(playersFile):
        print(f'Appending to already created file - {playersFile}')
        return get_recent_scrape()
    else:
        print(f'File created - {playersFile}')
        with open(playersFile, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
        return 0


# Get total number of PDGA players to set limit of scraping
def find_last_player():
    print('\nFinding number of registered PDGA members...')
    pl = requests.get(
        'https://www.pdga.com/players?FirstName=&LastName=&PDGANum=&Status=All&Gender=All&Class=All&MemberType=All'
        '&City=&StateProv=All&Country=All&Country_1=All&UpdateDate=&order=PDGANum&sort=desc')
    psoup = BeautifulSoup(pl.text, 'html.parser')
    last_player = psoup.find('table', class_='views-table cols-8').find('td', class_='views-field views-field-PDGANum '
                                                                                     'active pdga-number').get_text(

    ).rstrip()
    print(f'There are {int(last_player)} registered PDGA members!!!')
    return int(last_player)


# Get the last PDGA member scraped and saved
def get_recent_scrape():
    # Since threading can cause the last saved player to be out of order check the last THREADS number of lines
    # and find the max PDGA number of the last saved
    with open(playersFile, "r", encoding="utf-8", errors="ignore") as scraped:
        # final_line = (scraped.readlines()[-1].split(',')[0])
        print(f'Cecking last {THREADS} lines to find last saved player')
        last_lines = []
        scrape = scraped.readlines()
        for line in range(1, int(THREADS) + 1):
            # print(f"Line: {line} - {scrape[-line].split(',')[0]}")
            last_lines.append(scrape[-line].split(',')[0])

        nextScrape = int(max(last_lines)) + 1
        print(f'Last lines: {last_lines}')
        print(f"\nThe last player scrapted was PDGA #{max(last_lines)}")
        print(f"Continuing scraping on PDGA #{nextScrape}...")
        return nextScrape


# Return [next player to scrape, most recent registered member]
def get_range():
    return range(check_file(), find_last_player())


# Scrape function for threading
def scrape_player():
    global ids
    while True:
        pdga = ids.get()
        Player(pdga)
        ids.task_done()


# Fill queue with remaining players
def fill_queue():
    id_range = get_range()
    for id in id_range:
        ids.put(id)
    print(f'\nAdding PDGA members from {id_range[0]} to {id_range[1]}')
    print(f'Queue of IDs full with {ids.qsize()} members to go!')


if __name__ == '__main__':
    fill_queue()
    print('Starting scraping of members...')

    for i in range(THREADS):
        print(f'Starting thread #{i}')
        t = threading.Thread(target=scrape_player)
        t.start()
