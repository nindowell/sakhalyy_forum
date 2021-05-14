from multiprocessing import Pool
import random

import numpy as np
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import re
import time


def random_agents(num: int):
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=300)
    agents_list = []
    for agent in range(num):
        agent = user_agent_rotator.get_random_user_agent()
        agents_list.append(agent)
    return agents_list


def fill_urls(num: int):
    links = []
    http = 'https://forum.ykt.ru/viewforum.jsp?id=149&page={}'.format(num)
    try:
        page = requests.get(http, headers={'User-Agent': random.choice(agents_list)})
        soup = bs(page.text, 'html.parser')
        evens = soup.find_all('div', class_='f-topics_item f-topics_item--even')
        odds = soup.find_all('div', class_='f-topics_item f-topics_item--odd')
        all_topics = evens+odds
        for topic in all_topics:
            link = topic.find('a', class_='topic-expand')['href']
            links.append(link)
    except requests.exceptions.ConnectionError:
        print('error while taking URLS on:', http)
    return links


def parse_topic(link):
    dataframe = []
    while True:
        try:
            page = requests.get('https://forum.ykt.ru'+link, headers={'User-Agent': random.choice(agents_list)})
            if page.status_code == 200:
                soup = bs(page.text, 'html.parser')
                if soup is not None:
                    if soup.find('div', class_='f-view_topic-text emojify') is not None:
                        content = soup.find('div', class_='f-view_topic-text emojify').text.strip()
                        title = soup.find('div', class_='f-view_title emojify').text.strip()
                        n_likes = soup.find('div', class_='f-view_like_count f-comment_like_count f-js_like_count').text
                        date = soup.find('time', class_='f-view_createdate')['datetime'][:19]
                        views = soup.find('span', class_='post-views').text
                        n_comments = soup.find('span', class_='f-comments_count').text
                        dataframe.append((date, link[-7:], views, n_likes, n_comments, title, content))
                        return dataframe
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
            print('trying to download data again:', 'https://forum.ykt.ru'+link)
            continue
        break


def parse_comments(link):
    dataframe = []
    while True:
        try:
            page = requests.get('https://forum.ykt.ru'+link, headers={'User-Agent': random.choice(agents_list)})
            if page.status_code == 200:
                soup = bs(page.text, 'html.parser')
                if soup is not None:
                    if soup.find('div', class_='f-comments_content topic-comments yui-block alone') is not None:
                        comments = soup.find_all('li', class_='f-comments_item')
                        for comm in comments:
                            content = comm.find('div', class_='f-comment_text').text.replace('\n', ' ').strip()
                            content = re.sub(r'\s{2,}', ' ', content)
                            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(comm.find('div', class_='f-comment')['data-date']) / 1000))
                            n_likes = comm.find('span', class_='f-comment_like_count f-js_like_count').text.strip()
                            dataframe.append((date, link[-7:], n_likes, content))
                        return dataframe
                    else:
                        print('down')
                        continue
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
            print('trying to download data again:', 'https://forum.ykt.ru'+link)
            continue
        break


def topic_n_comments(link):
    topic = []
    comment = []
    while True:
        try:
            page = requests.get('https://forum.ykt.ru'+link, headers={'User-Agent': random.choice(agents_list)})
            if page.status_code == 200:
                soup = bs(page.text, 'html.parser')
                if soup is not None:
                    if soup.find('div', class_='f-comments_content topic-comments yui-block alone') is not None:
                        if soup.find('div', class_='f-view_topic-text emojify') is not None:
                            topic_content = soup.find('div', class_='f-view_topic-text emojify').text.strip()
                        else:
                            topic_content = np.nan
                        if soup.find('div', class_='f-view_title emojify') is not None:
                            title = soup.find('div', class_='f-view_title emojify').text.strip()
                        elif soup.find('div', class_='f-view_title emojify f-view_title--archive') is not None:
                            title = soup.find('div', class_='f-view_title emojify f-view_title--archive').text.strip()
                        else:
                            title = np.nan
                        topic_n_likes = soup.find('div', class_='f-view_like_count f-comment_like_count f-js_like_count').text.strip()
                        topic_date = soup.find('time', class_='f-view_createdate')['datetime'][:19]
                        views = soup.find('span', class_='post-views').text.strip()
                        n_comments = soup.find('span', class_='f-comments_count').text
                        topic.append((topic_date, link[-7:], views, topic_n_likes, n_comments, title, topic_content))
                        comments = soup.find_all('li', class_='f-comments_item')
                        for comm in comments:
                            if comm.find('div', class_='f-comment_text') is not None:
                                comm_content = comm.find('div', class_='f-comment_text').text.replace('\n', ' ').strip()
                                comm_content = re.sub(r'\s{2,}', ' ', comm_content)
                            else:
                                comm_content = np.nan
                            comm_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(comm.find('div', class_='f-comment')['data-date']) / 1000))
                            if comm.find('span', class_='f-comment_like_count f-js_like_count') is not None:
                                comm_n_likes = comm.find('span', class_='f-comment_like_count f-js_like_count').text.strip()
                            else:
                                comm_n_likes = 0
                            comment.append((comm_date, link[-7:], comm_n_likes, comm_content))
                        return topic, comment
                    else:
                        print('down')
                        continue
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
            print('trying to download data again:', 'https://forum.ykt.ru'+link)
            continue
        break


agents_list = random_agents(100)

if __name__ == '__main__':
    range_n = [i for i in range(1, 200)]
    list_links = []
    p = Pool(12)
    for links in p.map(fill_urls, range_n):
        list_links.extend(links)
    print('links:', len(list_links))


    topic_info = []
    comment_info = []
    for topic, comment in p.map(topic_n_comments, list_links):
        if topic:
            topic_info.extend(topic)
            comment_info.extend(comment)
        else:
            continue
    print('topics:', len(topic_info))
    print('comments:', len(comment_info))
    topic_df = pd.DataFrame(topic_info,
                            columns=['date', 'topic_id', 'views', 'n_likes', 'n_comments', 'title', 'content'])
    topic_df.to_csv('topics.csv', index=False)

    comments_df = pd.DataFrame(comment_info, columns=['date', 'topic_id', 'n_likes', 'content'])
    comments_df.to_csv('comments.csv', index=False)
