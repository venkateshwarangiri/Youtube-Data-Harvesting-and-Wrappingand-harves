import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
from pymongo import MongoClient
import mysql.connector as sql
from googleapiclient.discovery import build
from PIL import Image
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# SETTING THE TITLE ELEMENT FOR THE PAGE
icon = Image.open("C:/Users/venka/Downloads/you_tube.png")
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded",
                   )

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home", "Extract and Transform", "View Analytics"],
                           icons=["house-door-fill", "tools", "card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin-top": "20px",
                                                "--hover-color": "#266c81"},
                                   "icon": {"font-size": "20px"},
                                   "container": {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#266c81"}})



# CONNECTING TO THE MONGODB ATLAS DATA BASE AND CREATING A DATABASE


client = MongoClient('mongodb+srv://venkatnew:KgOi0yjCHaX2Yi96@cluster0.wy041xr.mongodb.net/?retryWrites=true&w=majority')

db = client['you_tube_data']


# MAKING CONNECTION TO MYSQL

my_db = sql.connect(host="localhost",
                  user="root",
                  password="Venkat@123",
                  database="my_db"
                  )



mycursor = my_db.cursor(buffered=True)


# GOOGLE API_KEY FOR FETCHING THE DATA
api_key = "AIzaSyDsIHpo4uKc0SRmHyxXb8KXE7_43x-0L70"
#channel_id=("UCduIoIMfD8tT3KoU0-zBRgQ")
# BUILDING CONNECTION WITH GOOGLE YOUTUBE
youtube = build('youtube', 'v3', developerKey=api_key)




# GETTING CHANNEL DETAILS FROM YOUTUBE CONNECTION.






























def get_channel_details(channel_id):
    ch_data = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    )
    response = request.execute()

    for i in range(len(response['items'])):
        data = dict(
            Channel_id=response['items'][i]['id'],
            Channel_name=response['items'][i]['snippet']['title'],
            Upload_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
            Subscribers=response['items'][i]['statistics']['subscriberCount'],
            Views=response['items'][i]['statistics']['viewCount'],
            Total_videos=response['items'][i]['statistics']['videoCount'],
            Description=response['items'][i]['snippet']['description'][:30],
            Country=response['items'][i]['snippet'].get('country'),
            Thumbnail=response['items'][i]['snippet']["thumbnails"]["default"]["url"]
        )

        ch_data.append(data)

    return ch_data

# GETTING VIDEOS IDS FOR THE RESPECTIVE CHANNELS


def get_channel_videos_details(channel_id):
    video_ids = []

    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )

    response = request.execute()
    Upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    more_pages = True

    request = youtube.playlistItems().list(playlistId=Upload_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token)
    response = request.execute()

    for i in range(len(response['items'])):
        video_ids.append(
            response['items'][i]['snippet']['resourceId']['videoId']
        )

    next_page_token = response.get('nextPageToken')

    if next_page_token is None:
        more_pages = False

    else:

        while more_pages:

            request = youtube.playlistItems().list(playlistId=Upload_id,
                                                   part='snippet',
                                                   maxResults=50,
                                                   pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]
                                 ['snippet']['resourceId']['videoId'])
            if response.get('nextPageToken') is None:
                break
            else:
                next_page_token = response.get('nextPageToken')

    return video_ids



# GETTING VIDEOS DETAILS FOR THE RESPECTIVE CHANNELS


def get_video_details(vd_ids):
    video_stats = []

    for i in range(0, len(vd_ids), 50):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=",".join(vd_ids[i:i+50])
        )

        response = request.execute()

        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        for video in response['items']:
            video = dict(Channel_name=video['snippet']['channelTitle'],
                         Channel_id=video['snippet']['channelId'],
                         Video_id=video['id'],
                         Title=video['snippet']['title'],
                         Published_date=video['snippet']['publishedAt'],
                         Duration=time_duration(
                             video['contentDetails']['duration']),
                         Views=video['statistics'].get('viewCount'),
                         Likes=video['statistics'].get('likeCount'),
                         Comments=video['statistics'].get('commentCount')
                         )
            video_stats.append(video)
    return video_stats



# GETTING COMMENTS FOR THE RESPECTIVE VIDEO_IDS


def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        more_comments = True
        while more_comments:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=30,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
            next_page_token = None
            if next_page_token is None:
                more_comments = False
                break
    except:
        pass
    return comment_data

  # TEMPERORY DATA BASE FOR CHANNEL LIST
def temp_channel_list(channel_name, ch_id):
    data = dict(
        Channel_name=channel_name,
        Channel_id=ch_id
    )

    return data



# FUNCTION FOR GETTING CHANNEL LIST


def channel_list():
    channel_list = []
    for i in db.temp_channel.find():
        channel_list.append(i['Channel_name'])

    if channel_list != []:
        return channel_list
    else:
        channel_list = ["NO COLLECTION TO DISPLAY PLEASE EXTRACT !!!"]
        return channel_list


# FUNCTION TO COLLECT ALL THE COMMENTS FOR RESPECTIVE CHANNEL AND THEIR VIDEO_IDS


def get_comments(v_ids):
    com_d = []
    for i in v_ids:
        com_d = com_d + get_comments_details(v_id=i)
    return com_d
# CREAING A TABLE SCHEMA FOR MYSQL TABLES

def create_mysql_tables():
    mycursor.execute("""CREATE TABLE IF NOT EXISTS Channel_table (
    CHANNEL_ID VARCHAR(40) PRIMARY KEY,
    CHANNEL_NAME VARCHAR(40),
    PLAYLIST_ID VARCHAR(40),
    SUBSCRIBERS BIGINT,
    VIEWS BIGINT,
    TOTAL_VIDEOS INT,
    DESCRIPTION VARCHAR(100),
    COUNTRY VARCHAR(10))""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS Videos_table (
    CHANNEL_NAME VARCHAR(30),
    CHANNEL_ID VARCHAR(30),
    VIDEO_ID VARCHAR(30) PRIMARY KEY,
    VIDEO_TITLE VARCHAR(200),
    UPLOADED_DATE VARCHAR(30),
    DURATION VARCHAR(30),
    TOTAL_VIEWS BIGINT,
    TOTAL_LIKES BIGINT,
    TOTAL_COMMENTS BIGINT
    )""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS Comments_table (
    COMMENT_ID VARCHAR(30) PRIMARY KEY,
    VIDEO_ID VARCHAR(30),
    COMMENT_TEXT MEDIUMTEXT,
    COMMENT_AUTHOR MEDIUMTEXT,
    COMMENT_POSTED_DATE VARCHAR(30),
    LIKE_COUNT INT,
    REPLY_COUNT INT
    )""")

# CALLING THIS FUNCTION CREATES AN MYSQL TABLES


create_mysql_tables()

# HOME PAGE
if selected == "Home":
    # Title Image

    st.markdown("##  :red[OBJECTIVE OF THE PROJECT :]")
    st.markdown("#### :black[This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and get some Insights and Bussiness value in the Streamlit app]")
    st.markdown("### :red[Technologies Used in this Application :]")
    st.markdown(
        "##### :black[API Intergration Fetching data from the server (google api client)]")
    st.markdown(
        "##### :black[Making connection to MongoDB Atlas (using Python,pymongo MongoClient)]")
    st.markdown(
        "##### :black[Creating a database and storing the data for Migration(Mongodb Atlas)]")
    st.markdown(
        "##### :black[Migrating Big data as Normalized data with multiple Tables in local server (MYSQL)]")
    st.markdown(
        "##### :black[Using pandas powerfull data frames for data manipulation and multiple operations(Pandas)]")
    st.markdown(
        "##### :black[Visualization using beautiful plotly charts(Plotly)]")
    st.markdown(
        "#####  :black[Using streamlit rapidly and customized building web Application(Streamlit)]")
    st.markdown("### :red[Application flow:]")
    st.markdown("##### :black[ 1.Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count,video ID, likes,comments of each video) using Google API.]")
    st.markdown(
        "##### :black[ 2.Option to store the data in a MongoDB database as a data lake.]")
    st.markdown(
        "##### :black[3.Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.]")
    st.markdown(
        "##### :black[4.Option to select a channel name and migrate its data from the data lake to a SQL database as tables.]")
    st.markdown(
        "##### :black[5.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details]")
    st.markdown(
        "##### :black[6.Visualizing Some Imporant Parameters for Each Channel using Plotly]")
    st.markdown("##### :black[7.Getting some Bussiness Value from the data]")

    

# EXTRACT and TRANSFORM PAGE
if selected == "Extract and Transform":
    tab1, tab2 = st.tabs(
        ["$\huge EXTRACT $",      "$\huge TRANSFORM $"])

    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input(
            "Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
        if ch_id and st.button("Extract Data"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                st.image(f'{ch_details[0]["Thumbnail"]}',
                         # Manually Adjust the width of the image as per requirement
                         width=150, caption=f'{ch_details[0]["Channel_name"]}'
                         )
                st.write(
                    f'##### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
                st.table(ch_details)
                st.success("Data Successfully extracted !!")

        if st.button("Extract to MongoDB"):
            with st.spinner('Please Wait...'):
                flag = 0
                ch_details = get_channel_details(ch_id)
                Channel_name = ch_details[0]["Channel_name"]
                Channel_id = ch_details[0]["Channel_id"]
                temp_list = temp_channel_list(Channel_name, Channel_id)
                for i in db.channel_details.find():
                    if i["Channel_name"] == Channel_name:
                        flag = 1
                        st.warning('Channel Already Extracted', icon="⚠️")
                        break
                if flag == 0:
                    v_ids = get_channel_videos_details(ch_id)
                    vid_details = get_video_details(v_ids)
                    comm_details = get_comments(v_ids)
                    st.table(ch_details)
                    st.write("Sample Videos Data")
                    st.write(vid_details[:5])
                    st.write("Sample comments Data")
                    st.write(comm_details[:5])
                    collections1 = db.channel_details
                    collections1.insert_many(ch_details)
                    collections2 = db.video_details
                    collections2.insert_many(vid_details)
                    collections3 = db.comments_details
                    collections3.insert_many(comm_details)
                    collections4 = db.temp_channel
                    collections4.insert_one(temp_list)
                    st.success("Upload to MogoDB successful !!")

    with tab2:
        st.markdown("#")

        st.markdown("### Select a channel to begin Transformation to SQL")

        ch_names = channel_list()

        user_inp = st.selectbox("Select channel", options=ch_names)

        st.markdown("#")

        def table_for_added_channel_to_sql():
            query = ('select * from channel_table')
            mycursor.execute(query)
            tabel = mycursor.fetchall()

            i = [i for i in range(1, len(tabel)+1)]
            tabel = pd.DataFrame(tabel, columns=mycursor.column_names, index=i)
            tabel = tabel[["CHANNEL_NAME", "SUBSCRIBERS", "VIEWS"]]
            st.markdown("### channels migrated to mysql")
            st.dataframe(tabel)

        table_for_added_channel_to_sql()




# FUNCTION FOR MIGRATION OF VIDEOS FROM MONGODB TO MYSQL TABLE


        def insert_into_videos(user):
            collections1 = db.video_details
            query1 = """INSERT INTO my_db.videos_table VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            for i in collections1.find({"Channel_name": user}, {"_id": 0}):
                t = tuple(i.values())
                mycursor.execute(query1, t)
                my_db.commit()

# FUNCTION FOR MIGRATING OF COMMENTS FROM MONGODB TO MYSQL TABLE

        def insert_into_comments(user):
            collection1 = db.video_details
            collection2 = db.comments_details
            query = """INSERT INTO my_db.comments_table VALUES(%s,%s,%s,%s,%s,%s,%s)"""
            for vid in collection1.find({'Channel_name': user}, {'_id': 0}):
                for i in collection2.find({'Video_id': vid["Video_id"]}, {'_id': 0}):
                    mycursor.execute(query, tuple(i.values()))
                    my_db.commit()

# FUNCTION FOR MIGRATING OF CHANNEL DETAILS FROM MONGODB TO MYSQL TABLE

        def insert_into_channels(user):
            collections = db.channel_details
            query = """INSERT INTO my_db.channel_table VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
            for i in collections.find({"Channel_name": user}, {'_id': 0, 'Thumbnail': 0}):
                a = i["Description"]
                i["Description"] = a[:30]
                t = tuple(i.values())
                mycursor.execute(query, t)
                my_db.commit()

# FUNCTION TO REMOVE COLLECTION IN MONGODB AFTER THE DATA MIGRATED TO MYSQL

        def remove_collection(user):
            coll_chann = db.temp_channel
            coll_chann.delete_one({"Channel_name": user})

        st.markdown("#")
        if st.button("Migrate to Mysql"):
            with st.spinner('Please Wait...'):
                try:
                    insert_into_videos(user_inp)
                    insert_into_channels(user_inp)
                    insert_into_comments(user_inp)
                    remove_collection(user_inp)
                    st.success("Migration to Mysql Success")
                except:
                    st.error("Channel details already Migrated!!")


if selected == "View Analytics":

    with st.container():
        st.write("### :orange[Channel Details :]")

        def sql_get_channel_details():
            mycursor.execute(
                "select CHANNEL_NAME,SUBSCRIBERS,VIEWS,TOTAL_VIDEOS from channel_table order by SUBSCRIBERS desc")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            sql_channel = pd.DataFrame(
                data, columns=['CHANNEL_NAME', 'SUBSCRIBERS', 'VIEWS', 'TOTAL_VIDEOS'], index=i)
            return sql_channel

        ch = sql_get_channel_details()

        st.dataframe(ch)

        st.markdown("#")

        ch_corr = ch[['SUBSCRIBERS', 'VIEWS', 'TOTAL_VIDEOS']].corr()
        fig = px.imshow(ch_corr, text_auto=True)
        st.write("##### :orange[Heat map :]")
        st.write(fig)
        st.write("##### :orange[channel Visuals :]")
        option = st.selectbox(
            'Select what would you like to See?',
            ('SUBSCRIBERS', 'VIEWS', 'TOTAL_VIDEOS'))
        ch_chan = ch[['CHANNEL_NAME', 'SUBSCRIBERS', 'VIEWS', 'TOTAL_VIDEOS']]
        fig = px.bar(ch_chan, x='CHANNEL_NAME', y=option,
                     hover_data=['SUBSCRIBERS', 'VIEWS', 'TOTAL_VIDEOS'], color='CHANNEL_NAME',
                     labels={'CHANNEL_NAME': 'CHANNEL'}, height=400,
                     title='Channel vs Subscribers or Views or Total_videos')
        st.write(fig)

        st.write("##### :orange[,Select The Channel and Respective Year To Analyse :]")

        def get_sql_channel_list():
            mycursor.execute("""select channel_name from channel_table""")
            data = pd.DataFrame(mycursor.fetchall(), columns=["channel_name"])
            return data
        sql_ch_list = get_sql_channel_list()
        sql_ch_list = list(sql_ch_list["channel_name"])

        def sql_get_video_details():
            mycursor.execute(
                """select channel_name,video_id,uploaded_date,total_views,total_likes from videos_table""")
            data = pd.DataFrame(mycursor.fetchall(),
                                columns=mycursor.column_names)
            return data
        
        view = sql_get_video_details()
        view["Published_date"] = view["uploaded_date"].apply(lambda x: x[0:10])
        view.drop("uploaded_date", axis=1, inplace=True)
        view["Published_date"] = pd.to_datetime(view["Published_date"])
        view["year"] = view["Published_date"].apply(lambda x: x.year)
        view["month"] = view["Published_date"].apply(lambda x: x.month)
        data_view = view.groupby(["channel_name", "year", "month"])[
            "total_views"].sum().reset_index()
        data_view["month_cat"] = data_view["month"].apply(lambda x: str(x)).map({"1": "jan", "2": "feb", "3": "march", "4": "april", "5": "may", "6": "june", "7": "july",
                                                                                 "8": "aug", "9": "sep", "10": "oct", "11": "nov", "12": "dec"})
        sql_year_list = list(data_view["year"].unique())
        col1, col2 = st.columns(2)
        with col1:
            ch_list = st.selectbox(
                "Select the channel name", options=sql_ch_list)

        with col2:
            years = st.selectbox(
                "select the most  recent  years?",
                options=sql_year_list

            )

        

        # CHANNEL VIEWS PER MONTH

      #  selected_ch_year = data_view[(data_view["channel_name"] == str(
      #      ch_list)) & (data_view["year"] == int(years))]
      #   fig = px.line(selected_ch_year, x="month_cat", y="total_views",
      #                color="channel_name", labels={"month_cat": str(years)},
      #                title="Views per month")

      #  st.write(
      #      "##### :orange[Total Views Per Month In a Particular Year :]")
      #  st.write(fig)

 

        
        # LIKES PER MONTH

        likes_data = sql_get_video_details()
        likes_data["Published_date"] = likes_data["uploaded_date"].apply(
                lambda x: x[0:10])
        likes_data["total_likes"].fillna(0, inplace=True)
        likes_data.drop("uploaded_date", axis=1, inplace=True)
        likes_data["total_likes"].fillna(0, inplace=True)
        likes_data["Published_date"] = pd.to_datetime(
                likes_data["Published_date"])
        likes_data["year"] = likes_data["Published_date"].apply(
                lambda x: x.year)
        likes_data["month"] = likes_data["Published_date"].apply(
                lambda x: x.month)
        likes_data.drop(columns="total_views", inplace=True)
        likes_data = likes_data.groupby(["channel_name", "year", "month"])[
                "total_likes"].sum().reset_index()
        likes_data["month_cat"] = likes_data["month"].apply(lambda x: str(x)).map({"1": "jan", "2": "feb", "3": "march", "4": "april", "5": "may", "6": "june", "7": "july",
                                                                                    "8": "aug", "9": "sep", "10": "oct", "11": "nov", "12": "dec"})

   #     selected_df = likes_data[(likes_data["channel_name"] == str(
   #             ch_list)) & (likes_data["year"] == int(years))]
   #     fig = px.bar(selected_df, x='month_cat', y='total_likes',
   #                     color="channel_name", labels={"month_cat": str(years)}, title="Likes per month")
        st.write(
                "##### :orange[Channel Total Likes Per Month In a Given Year :]")
        st.write(fig)

        # VIDEOS UPLOADS PER MONTH

        uploads = sql_get_video_details()
        uploads["Published_date"] = uploads["uploaded_date"].apply(
            lambda x: x[0:10])
        uploads.drop("uploaded_date", axis=1, inplace=True)
        uploads["Published_date"] = pd.to_datetime(uploads["Published_date"])
        uploads["year"] = uploads["Published_date"].apply(lambda x: x.year)
        uploads["month"] = uploads["Published_date"].apply(lambda x: x.month)
        uploads.drop("Published_date", axis=1, inplace=True)
        uploads = uploads.groupby(["channel_name", "year", "month"])[
            "video_id"].count().reset_index()
        uploads["month_cat"] = uploads["month"].apply(lambda x: str(x)).map({"1": "jan", "2": "feb", "3": "march", "4": "april", "5": "may", "6": "june", "7": "july",
                                                                             "8": "aug", "9": "sep", "10": "oct", "11": "nov", "12": "dec"})
        uploads.rename(columns={"video_id": "video_uploads"}, inplace=True)

   #     selected_uploads = uploads[(uploads["channel_name"] == str(
   #         ch_list)) & (uploads["year"] == int(years))]
   #     fig = px.bar(selected_uploads, x='month_cat', y='video_uploads',
   #                  color="channel_name", labels={"month_cat": str(years)}, title="Video uploads per month")
        st.write(
            "##### :orange[Channel Videos Uploads Per Month In a Given Year :]")
        st.write(fig)

        # BOX PLOTS FOR CUMMULATIVE LIKES AND VIEWS IN AN YEAR

        box_plot = sql_get_video_details()
        box_plot["Published_date"] = box_plot["uploaded_date"].apply(
            lambda x: x[0:10])
        box_plot.drop("uploaded_date", axis=1, inplace=True)
        box_plot["Published_date"] = pd.to_datetime(box_plot["Published_date"])
        box_plot["year"] = box_plot["Published_date"].apply(lambda x: x.year)
        box_plot["month"] = box_plot["Published_date"].apply(lambda x: x.month)
        box_plot["total_likes"].fillna(0, inplace=True)
        box_plot_like = box_plot.groupby(["channel_name", "year"])[
            "total_likes"].sum().reset_index()
        box_plot_view = box_plot.groupby(["channel_name", "year"])[
            "total_views"].sum().reset_index()

        fig = px.box(box_plot_like, x="channel_name", y="total_likes", labels={
                     "total_likes": "Cummualtive Likes per year Insights"}, title="MAXIMUM MINIMUM MEDIAN CUMMULATIVE LIKES PER YEARS")
        st.write(
            "##### :orange[Box plots :]")
        st.write(fig)

        fig = px.box(box_plot_view, x="channel_name", y="total_views", labels={
                     "total_views": "Cummualtive view per year  Insights"},
                     title="MAXIMUM MINIMUM MEDIAN CUMMULATIVE VIEWS PER YEARS")
        st.write(fig)

        # AVERAGE VIEWS AND LIKES PER YEAR USING BUBBLE PLOTS

        avg_bubble_plot_view = box_plot.groupby(["channel_name", "year"])[
            "total_views"].mean().reset_index()
        avg_bubble_plot_like = box_plot.groupby(["channel_name", "year"])[
            "total_likes"].mean().reset_index()

        bubble_plot = pd.merge(avg_bubble_plot_like,
                               avg_bubble_plot_view, on=None, how="outer")
        bubble_plot.rename(columns={
                           'total_likes': "Average_likes", "total_views": "Average_views"}, inplace=True)
        box_val = st.selectbox(
            "Select the respective year :",
            options=(2023, 2022, 2021, 2019))

        df = bubble_plot[bubble_plot["year"] == box_val]
        fig = px.scatter(df, x="year", y="Average_likes",
                         size="Average_views", color="channel_name",
                         hover_name="channel_name", log_x=True, size_max=60, title="Bigger the bubble higher the Performance")
        st.write(
            "##### :orange[Bubble plots :]")
        st.write(fig)
        st.text("the size of the bubble denotes the performance of the channel for the respective year and here we can compare")
        st.text("with other channels")

        # COMPARITIVE PERCENTAGE OF SUBSCRIBERS

        def get_sql_channel_list():
            mycursor.execute(
                """select channel_name,views from channel_table""")
            data = pd.DataFrame(mycursor.fetchall(), columns=[
                                "CHANNEL_NAME", "VIEWS"])
            return data
        fig = px.pie(ch, values="SUBSCRIBERS", names="CHANNEL_NAME",
                     hole=.3, title="Comparitive Percentage Of Subscribers")
        st.write(fig)

        # PERCENTAGE OF SUBSCRIBERS VIEWS PER UPLOADS

        mycursor.execute(
            """SELECT CHANNEL_NAME,VIDEO_ID,UPLOADED_DATE FROM MY_DB.VIDEOS_TABLE;""")
        data = mycursor.fetchall()
        i = [i for i in range(1, len(data)+1)]
        table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
        table["UPLOADED_DATE"] = pd.to_datetime(table["UPLOADED_DATE"])
        table.drop(columns=["UPLOADED_DATE"], inplace=True)
        table = table.groupby("CHANNEL_NAME")["VIDEO_ID"].size().reset_index()
        table.rename(columns={"VIDEO_ID": "UPLOADS"}, inplace=True)
        ch = get_sql_channel_list()
        df = ch.merge(table, how="inner")


        def get_views_sub_ratio(df):
            doc = []
            for i in range(len(df)):
                data = df.iloc[i]["VIEWS"]/df.iloc[i]["UPLOADS"]
                doc.append(data)
            return doc
        val = get_views_sub_ratio(df)
        ch_sub = sql_get_channel_details()
        ch_sub = ch_sub[["CHANNEL_NAME", "SUBSCRIBERS"]]
        df = df.merge(ch_sub, how="inner")

        def avg_view_per_sub(df, val):
            list = []
            for i in range(len(df)):
                data = (val[i]/df.iloc[i]["SUBSCRIBERS"])
                list.append(data*100)
            return list
        value = avg_view_per_sub(df, val)
        fig = go.Figure(go.Bar(
            x=value,
            y=df["CHANNEL_NAME"],
            orientation='h'))
        st.markdown("#### :orange[Percentage Of Subscibers views per upload]")
        st.write(fig)
        st.write(
            "This chart gives percentage of active subscribers viewing the channel uploads greater than 100")
        st.write(
            "signifies there are significant non subscribers viewers reaching more Audience")
        

          # VIEWS PER UPLOADS
        mycursor.execute(
            """SELECT CHANNEL_NAME,VIDEO_ID,UPLOADED_DATE FROM MY_DB.VIDEOS_TABLE;""")
        data = mycursor.fetchall()
        i = [i for i in range(1, len(data)+1)]
        table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
        table["UPLOADED_DATE"] = pd.to_datetime(table["UPLOADED_DATE"])
        table.drop(columns=["UPLOADED_DATE"], inplace=True)
        table = table.groupby("CHANNEL_NAME")["VIDEO_ID"].size().reset_index()
        table.rename(columns={"VIDEO_ID": "UPLOADS"}, inplace=True)
        

        def get_sql_channel_list():
            mycursor.execute(
                """select channel_name,views from channel_table""")
            data = pd.DataFrame(mycursor.fetchall(), columns=[
                                "CHANNEL_NAME", "VIEWS"])
            return data
        ch = get_sql_channel_list()
        df = ch.merge(table, how="inner")

        def get_views_sub_ratio(df):
            doc = []
            for i in range(len(df)):
                data = df.iloc[i]["VIEWS"]/df.iloc[i]["UPLOADS"]
                doc.append(data)
            return doc
        val = get_views_sub_ratio(df)
        name = list(df["CHANNEL_NAME"])
        fig = go.Figure(go.Bar(
            x=val,
            y=name,
            orientation='h'))

        st.markdown("#### :orange[Expected Average Views Per Uploads]")
        st.write(fig)
        st.text(
            "Average views per uploads from this we can infer that what could be the expected views")
        st.text(
            "per uploads higher the score,then there are chances getting more views")

    with st.container():

   
        st.write("## :orange[Select Here To View Some Pre-Defined Queries]")
        questions = st.selectbox('Questions',
                                 ['Click the question that you would like to query',
                                  '1. What are the names of all the videos and their corresponding channels?',
                                  '2. Which channels have the most number of videos, and how many videos do they have?',
                                  '3. What are the top 10 most viewed videos and their respective channels?',
                                  '4. How many comments were made on each video, and what are their corresponding video names?',
                                  '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                  '6. What is the total number of likes for each video, and what are their corresponding video names?',
                                  '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                  '8. What are the names of all the channels that have published videos in the year 2022?',
                                  '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                  '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

        if questions == '1. What are the names of all the videos and their corresponding channels?':
            mycursor.execute(
                """SELECT CHANNEL_NAME,VIDEO_TITLE AS TITLE,TOTAL_COMMENTS AS COMMENTS,TOTAL_LIKES AS LIKES FROM my_db.videos_table ORDER BY CHANNEL_NAME;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            data = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            st.write(data)

        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("""SELECT CHANNEL_NAME,COUNT(VIDEO_ID)  AS TOTAL_VIDEOS FROM my_db.videos_table GROUP BY CHANNEL_NAME ORDER BY TOTAL_VIDEOS DESC;
""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)

            fig = go.Figure(go.Bar(
                x=table['TOTAL_VIDEOS'],
                y=table['CHANNEL_NAME'],
                orientation='h'))

            st.write(table)
            st.write("#### :orange[Channels with total Videos uploads]")
            st.write(fig)

        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            mycursor.execute(
                """SELECT CHANNEL_NAME,VIDEO_TITLE AS TITLE,TOTAL_VIEWS AS VIEWS FROM my_db.videos_table ORDER BY VIEWS DESC LIMIT 10;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            st.write(table)

        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            mycursor.execute(
                """SELECT CHANNEL_TABLE.CHANNEL_NAME,VIDEOS_TABLE.VIDEO_TITLE,VIDEOS_TABLE.TOTAL_COMMENTS
FROM CHANNEL_TABLE INNER JOIN VIDEOS_TABLE ON VIDEOS_TABLE.CHANNEL_NAME=CHANNEL_TABLE.CHANNEL_NAME ORDER BY CHANNEL_NAME ASC,TOTAL_COMMENTS DESC;;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            st.write(table)

        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            mycursor.execute(
                """SELECT CHANNEL_NAME,VIDEO_TITLE AS TITLE,TOTAL_LIKES AS LIKES FROM my_db.videos_table ORDER BY LIKES DESC LIMIT 10;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            st.write(table)

        elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
            mycursor.execute(
                """SELECT CHANNEL_NAME,VIDEO_TITLE AS TITLE,TOTAL_LIKES AS LIKES,UPLOADED_DATE AS PUBLISHED_DATE FROM my_db.videos_table ORDER BY CHANNEL_NAME,LIKES DESC;;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            st.write(table)
        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            mycursor.execute(
                """SELECT CHANNEL_TABLE.CHANNEL_NAME,SUM(VIDEOS_TABLE.TOTAL_VIEWS) AS VIEWS
FROM CHANNEL_TABLE INNER JOIN VIDEOS_TABLE ON VIDEOS_TABLE.CHANNEL_NAME=CHANNEL_TABLE.CHANNEL_NAME 
GROUP BY CHANNEL_NAME;;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            st.table(table)
            st.write("####  :orange[Channel and the respective total views]")
            fig = go.Figure(go.Bar(
                x=table['VIEWS'],
                y=table['CHANNEL_NAME'],
                orientation='h'))
            st.write(fig)

        elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            mycursor.execute(
                """SELECT CHANNEL_TABLE.CHANNEL_NAME,COUNT(VIDEOS_TABLE.VIDEO_ID) AS TOTAL_UPLOADS
FROM CHANNEL_TABLE INNER JOIN VIDEOS_TABLE ON VIDEOS_TABLE.CHANNEL_NAME=CHANNEL_TABLE.CHANNEL_NAME 
WHERE VIDEOS_TABLE.UPLOADED_DATE LIKE "2022%" GROUP BY CHANNEL_NAME;;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            st.table(table)

            year = st.selectbox("Please Select the years :",
                                options=sql_year_list)
            mycursor.execute(
                """SELECT CHANNEL_NAME,VIDEO_ID,UPLOADED_DATE FROM MY_DB.VIDEOS_TABLE;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=mycursor.column_names, index=i)
            table["UPLOADED_DATE"] = pd.to_datetime(table["UPLOADED_DATE"])
            table["year"] = table["UPLOADED_DATE"].apply(lambda x: x.year)
            table.drop(columns=["UPLOADED_DATE"], inplace=True)
            table = table.groupby(["CHANNEL_NAME", "year"])[
                "VIDEO_ID"].size().reset_index()
            table.rename(
                columns={"year": "YEAR", "VIDEO_ID": "UPLOADS"}, inplace=True)
            table = table[table["YEAR"] == year]
            table = table[["CHANNEL_NAME", "UPLOADS"]]
            i = [i for i in range(1, len(table)+1)]
            table_df = pd.DataFrame(table.values.tolist(), columns=[
                                    "CHANNEL_NAME", "UPLOADS"], index=i)
            st.table(table_df)

        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            def get_duration():
                mycursor.execute(
                    """SELECT CHANNEL_NAME,VIDEO_ID,DURATION FROM my_db.videos_table;""")
                data = mycursor.fetchall()
                i = [i for i in range(1, len(data)+1)]
                table = pd.DataFrame(
                    data, columns=mycursor.column_names, index=i)
                return table
            table = get_duration()
            table["DURATION"] = pd.to_datetime(
                table["DURATION"], format="%H:%M:%S")
            uploads = table.groupby("CHANNEL_NAME")["VIDEO_ID"].size()
            uploads = pd.DataFrame(uploads)
            uploads = uploads.reset_index()
            table["hour"] = table["DURATION"].apply(lambda x: x.hour)
            table["minute"] = table["DURATION"].apply(lambda x: x.minute)
            table["second"] = table["DURATION"].apply(lambda x: x.second)
            table.drop(columns=["VIDEO_ID", "DURATION"], inplace=True)
            hour = table.groupby("CHANNEL_NAME")["hour"].sum().reset_index()
            minute = table.groupby("CHANNEL_NAME")[
                "minute"].sum().reset_index()
            second = table.groupby("CHANNEL_NAME")[
                "second"].sum().reset_index()
            hour["hour"] = hour["hour"].apply(lambda x: x*60*60)
            hour.rename(columns={"hour": "hour_sec"}, inplace=True)
            minute["minute"] = minute["minute"].apply(lambda x: x*60)
            minute.rename(columns={"minute": "minute_sec"}, inplace=True)
            df = hour.merge(minute, how="inner")
            df = second.merge(df, how="inner")


            def total_time(df):
                time_total = []
                for i in range(len(df)):
                    data = df.iloc[i]["second"] + \
                        df.iloc[i]["hour_sec"]+df.iloc[i]["minute_sec"]
                    time_total.append(data)
                return time_total

            time = total_time(df)
            time = pd.DataFrame(time)
            df1 = pd.concat([uploads, time], axis=1)


            def avg_dur(df1):
                avg_time = []
                for i in range(len(df1)):
                    data = (df1.iloc[i][0]/df1.iloc[i]["VIDEO_ID"])
                    avg_time.append(data)
                return avg_time
            avg = avg_dur(df1)
            av = pd.DataFrame(avg, columns=["time"])
            av.rename(columns={"time": "Average_Duration"}, inplace=True)
            av['Average_Duration'] = pd.to_datetime(
                av['Average_Duration'], unit='s')
            av['Average_Duration'] = av['Average_Duration'].dt.strftime(
                '%H:%M:%S')
            final = pd.concat([df1, av], axis=1, join="inner")
            final = final[["CHANNEL_NAME", "Average_Duration"]]
            i = [i for i in range(1, len(final)+1)]
            final_df = pd.DataFrame(
                final.values.tolist(), columns=["CHANNEL_NAME", "Average_Duration"], index=i)

            st.table(final_df)

        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            mycursor.execute(
                """SELECT CHANNEL_NAME,VIDEO_TITLE,TOTAL_COMMENTS FROM MY_DB.VIDEOS_TABLE ORDER BY TOTAL_COMMENTS DESC;""")
            data = mycursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            data = pd.DataFrame(data, columns=mycursor.column_names, index=i)

            st.write(data) 





