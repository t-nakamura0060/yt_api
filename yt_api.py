# -*- coding: utf-8 -*-
import datetime
import time
import csv
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.tools import argparser
#以下firestore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
#   https://cloud.google.com/console
# Please ensure that you have enabled the YouTube Data API for your project.

#APIにアクセスするとQuotasが蓄積される（コスト）．通常は1dayで10,000が上限となるため注意．
#DEVELOPER_KEY = "AIzaSyBPBrtub4pGcQUNmBnqRU7--FVEKLYWJkc"
DEVELOPER_KEY = "AIzaSyDIjpFsr34Aq2Xbc2mAuDKyL2k92mozJMk"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
MAX_RESULTS = 50    #一回のクエリで取得する件数．検索コストには依存しない．
NUM = 1            #何回クエリを実行するか．MAX_RESULTS * NUMが1ループにおける総表示件数．
TIME_LAG = 0       #現在時刻をクエリに指定すると表示件数にバグが生じる．タイムラグの猶予を設けるが，時刻を指定しないほうが良いと思われる．

def get_nowtime():
    now_date = datetime.datetime.utcnow()
    now_date = now_date + datetime.timedelta(days = 3)
    search_date = now_date.isoformat("T") + "Z"
    return search_date

#条件にあった動画（ライブ）の検索　1回につきコスト100
def live_broadcast_search(now_date,nextpage_token,youtube_data):
    # Call the search.list method to retrieve results matching the specified
    # query term.
    live_id = []
    search_response = youtube_data.search().list(
        part="id",
        eventType = "live",
        maxResults = MAX_RESULTS,
        pageToken = nextpage_token,
        order='viewCount',
        #publishedAfter = now_date,
        relevanceLanguage = "ja",
        regionCode = "jp",
        type = "video"
    ).execute()
  
    # Add each result to the appropriate list, and then display the lists of
    # matching videos, channels, and playlists.
    nextpage_token = search_response["nextPageToken"]
    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video" :
            live_id.append("%s" % (search_result["id"]["videoId"]))
    return live_id,nextpage_token

#video_idから動画の情報を取得　1回につきコスト1
#video_id，チャンネル名，配信開始時間，タイトル，サムネイル，同接数，高評価数，低評価数，（登録者数）
def liveStreamingDetails(live_id,youtube_data):
    livestream_array = []

    youtube_video = youtube_data.videos().list(
        part = "liveStreamingDetails,snippet,statistics",
        id = live_id,
        #regionCode = "jp"
    ).execute()

    live_dict_key = ["video_id", 
                     "channel", "published_time", "title", "thumbnails",
                     "current_viewers", "good_num", "bad_num"]
    live_dict_values = []
    for search_result in youtube_video.get("items", []):
        try:
            video_id = search_result["id"] 
        
            channel = search_result["snippet"]["channelTitle"]
            published_time = search_result["snippet"]["publishedAt"]
            title = search_result["snippet"]["title"]
            thumbnails = search_result["snippet"]["thumbnails"]["default"]["url"]
        
            current_viewers = search_result["liveStreamingDetails"]["concurrentViewers"]
            good_num = search_result["statistics"]["likeCount"]
            bad_num = search_result["statistics"]["dislikeCount"]
            
            livestream_array = [video_id,channel, published_time,title,thumbnails,current_viewers,good_num,bad_num]
            live_dict_values.append(livestream_array)
        except KeyError:
            print("KEYERROR : " ,video_id)
            pass
        
    return live_dict_key,live_dict_values

#条件にあった動画（放送予定）の検索　1回につきコスト100
def upcoming_broadcast_search(now_date,nextpage_token,youtube_data):
    # Call the search.list method to retrieve results matching the specified
    # query term.
    upcoming_id = []
    search_response = youtube_data.search().list(
        part="id",
        eventType = "upcoming",
        maxResults = MAX_RESULTS,
        pageToken = nextpage_token,
        order='viewCount',
        publishedBefore = now_date,
        relevanceLanguage = "ja",
        regionCode = "jp",
        type = "video"
    ).execute()

    # Add each result to the appropriate list, and then display the lists of
    # matching videos, channels, and playlists.
    nextpage_token = search_response["nextPageToken"]
    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video" :
            upcoming_id.append("%s" % (search_result["id"]["videoId"]))        
    return upcoming_id,nextpage_token

#video_idから動画の情報を取得　1回につきコスト1
#video_id，チャンネル名，配信開始時間，タイトル，サムネイル，（登録者数）
def upcomingStreamingDetails(upcoming_id,youtube_data):
    upcomingstream_array = []

    youtube_video = youtube_data.videos().list(
        part = "snippet,statistics",
        id = upcoming_id,
        regionCode = "jp"
    ).execute()

    upcoming_dict_key = ["video_id", 
                         "channel", "published_time", "title", "thumbnails"]
    upcoming_dict_values = []
    for search_result in youtube_video.get("items", []):
        try:
            video_id = search_result["id"] 
            
            channel = search_result["snippet"]["channelTitle"]
            published_time = search_result["snippet"]["publishedAt"]
            title = search_result["snippet"]["title"]
            thumbnails = search_result["snippet"]["thumbnails"]["default"]["url"]

            upcomingstream_array = [video_id,channel,title,thumbnails]
            upcoming_dict_values.append(upcomingstream_array)

        except KeyError:
            print("KEYERROR : " ,video_id)
            pass
    return upcoming_dict_key,upcoming_dict_values

 #一括でfirecloudへ書き込み   
def batch_insert(db,collection,key_array,values_array):
    #データの追加
    #db.collection(u'users')...データベース内のどのコレクションを指定するか
    #.document(u'alovelace')...前で指定したコレクション内の どのドキュメントを指定するか
    #u'first': u'Ada',..."first"フィールドに"Ada"という値を追加する
    stream_dict = {}    
    batch = db.batch()
    for i,fact in enumerate(values_array):
        db_document = fact[0]
        stream_dict.update(zip(key_array, fact))
        doc_ref = db.collection('%s'%collection).document('%s'%db_document)
        doc_ref.set(stream_dict)
        #print(fact,"\n\n\n")
    batch.commit

if __name__ == "__main__":
    #必須のパラメータはpartだけとされていますが、クエリを指定するqパラメータを指定しないと、レスポンスのitemsパラメータが空のリストとして返されることがあります。
    #argparser.add_argument("--q", help="Search term", default="Google")
    #argparser.add_argument("--max-results", help="Max results", default=MAX_RESULTS)
    #args = argparser.parse_args()
    
    #youtube api
    youtube_data = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)

    #データベース初期化　APIキー（秘密鍵）を用いて行う
    cred = credentials.Certificate("C:/Users/nakamura/Desktop/yt-api.app/yt-api-290713-firebase-adminsdk-jc2um-88843dc1be.json") # ダウンロードした秘密鍵
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    
    try:               
        while(True):
            live_dict = {}
            t = 0
            live_nexttoken = ""
            upcoming_nexttoken = ""
            
            #現在時刻の取得　api取得にラグがあるため，数十分程減算を行う．→しないほうがいい．
            now_date = get_nowtime()
            print("Search time : " , now_date)

            for t in range(NUM):
                #live_id,live_nexttoken = live_broadcast_search(now_date,live_nexttoken,youtube_data)
                upcoming_id, upcoming_nexttoken = upcoming_broadcast_search(now_date,upcoming_nexttoken,youtube_data)
                #サンプルデータ
                live_id = ["5qap5aO4i9A" ,"36YnV9STBqc"]
                #upcoming_id = ["ZWIdxb_s1-I"]

                print ("total live = ",len(live_id) ,"\ntotal upcoming =" ,len(upcoming_id))
                
                live_dict_key,live_dict_values = liveStreamingDetails(live_id ,youtube_data)
                upcoming_dict_key,upcomming_dict_values = upcomingStreamingDetails(upcoming_id,youtube_data)

                batch_insert(db,"live",live_dict_key, live_dict_values)
                batch_insert(db,"upcoming", upcoming_dict_key, upcomming_dict_values)                

                #確認用csv書き込み
                with open("ytapi11.csv","a",encoding="utf-8-sig") as f:
                    writer = csv.writer(f,lineterminator = '\n')
                    writer.writerows(live_dict_values)
                    writer.writerows(upcomming_dict_values)               
            
            time.sleep(5)
            break
            

    except HttpError as e:
        print ("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))