from bs4 import BeautifulSoup
import urllib.request
import pandas as pd

tail_url = [
    "2004/01/",
    "2004/02/",
    "2004/03/",
    "2004/04/",
    "2004/05/",
    "2004/06/",
    "2004/07/",
    "2004/08/",
    "2004/09/",
    "2004/10/",
    "2004/12/",
    "2005/01/",
    "2005/02/",
    "2005/03/",
    "2005/04/",
    "2005/05/"
]

row_dates = [
    200401,
    200402,
    200403,
    200404,
    200405,
    200406,
    200407,
    200408,
    200409,
    200410,
    200412,
    200501,
    200502,
    200503,
    200504,
    200505,
]

def load_data_month(pos):
    with urllib.request.urlopen(f'http://fifthnail.blogspot.com/{tail_url[pos]}') as f:
        html = f.read()
        soup = BeautifulSoup(html, 'html.parser')
        all_divs = soup.find_all("div")
        fifth_date = []
        for div in all_divs:
            try:
                if div.get("class")[0] == "DateHeader":
                    temp_date = div.get_text()
                    print(temp_date)
                elif div.get("class")[0] == "Post":
                    fifth_date.append(temp_date)
            except:
                pass
        fifth_posts = soup.find_all("div", {"class": "Post"})
        fifth_post_titles = []
        fifth_post_hours = []
        fifth_post_text = []
        for div in fifth_posts:
            post_title = div.find("span", {"class": "PostTitle"})
            post_hour = div.find("a", {"title": "permanent link"})
            post_text = div.get_text()
            with open("temp.txt", "w") as temp_file:
                temp_file.write(post_text)
            try:
                fifth_post_titles.append(post_title.string[2:])
            except:
                fifth_post_titles.append("")
            try:
                fifth_post_hours.append(post_hour.string)
            except:
                fifth_post_hours.append("")

            to_use_text = ""
            with open("temp.txt", "r") as temp_file:
                if fifth_post_titles[-1] != "":
                    temp_file.readline()
                    temp_file.readline()
                    temp_file.readline()
                to_use_text += temp_file.readline()
                if fifth_post_hours[-1] != "":
                    temp_file.readline()
                    temp_file.readline()
                temp_list = []
                line = temp_file.readline()
                while line:
                    temp_list.append(line)
                    line = temp_file.readline()
                to_use_text += "".join(temp_list[:-5])
                fifth_post_text.append(to_use_text)
        ##### Checking Correctness
        print(len(fifth_post_titles), len(fifth_date), len(fifth_post_hours), len(fifth_post_text))
        ##### Building DF
        month_df = pd.DataFrame()
        month_df["post_title"] = fifth_post_titles
        month_df["post_date"] = fifth_date
        month_df["post_hour"] = fifth_post_hours
        month_df["post_text"] = fifth_post_text

        return month_df

print(len(tail_url), len(row_dates))

df_list = []

for it_date in range(len(tail_url)):
    month_df =load_data_month(it_date)
    df_list.append(month_df)

full_df = pd.concat(df_list)
full_df.to_parquet("SK_Blog_Dataset.parquet", index=False)
