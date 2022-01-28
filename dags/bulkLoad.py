
import mysql.connector
import os
import requests

ENDPOINT="db"
PORT="3306"
REGION="us-east-2"
DBNAME="itunes"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
def bulkLoad(data : list ):
    query="""INSERT INTO itunes.Tracks (trackId, wrapperType, kind, artistId, collectionId, artistName, collectionName, trackName, collectionCensoredName,
    trackCensoredName, artistViewUrl, collectionViewUrl, trackViewUrl, previewUrl, artworkUrl30, artworkUrl60, artworkUrl100, collectionPrice,
    trackPrice, releaseDate, collectionExplicitness, trackExplicitness, discCount, discNumber, trackCount, trackNumber, trackTimeMillis, 
    country, currency, primaryGenreName, isStreamable )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) ON DUPLICATE KEY UPDATE trackPrice=VALUES(trackPrice) """

    try:
        conn =  mysql.connector.connect(host=ENDPOINT, user='root', passwd='root', port=PORT, database=DBNAME)
        cur = conn.cursor()
        cur.executemany(query,data)
        conn.commit() 
        print('Inserted {rows} rows in Itunes DB'.format(rows=len(data)))
    except Exception as e:
        print("Database insertion failed due to {}".format(e)) 
    finally:
        cur.close()
        conn.close()

    return 

                                    
def getItuneResponse(serchTerm:str,limit=10):
    my_keys=['trackId', 'wrapperType', 'kind', 'artistId', 'collectionId', 'artistName', 'collectionName', 'trackName', 'collectionCensoredName',
     'trackCensoredName', 'artistViewUrl', 'collectionViewUrl', 'trackViewUrl', 'previewUrl', 'artworkUrl30', 'artworkUrl60', 'artworkUrl100', 'collectionPrice',
      'trackPrice', 'releaseDate', 'collectionExplicitness', 'trackExplicitness', 'discCount', 'discNumber', 'trackCount', 'trackNumber', 'trackTimeMillis', 
      'country', 'currency', 'primaryGenreName', 'isStreamable']
    endpoint='https://itunes.apple.com/search?term={serchTerm}&limit={limit}&media=music'.format(serchTerm=serchTerm, limit=limit)
    data=[]
    try: 
        response=requests.get(endpoint).json()  
        for res in response['results']:
            tmpTrack=[]
            for sub_k in my_keys:
                try:
                    tmpTrack.append(res[sub_k])
                except:
                    tmpTrack.append('no data')
            data.append(tuple(tmpTrack))
    except Exception as e:
        print("ITunes API failed due to {}".format(e))  
    print('Fetched {rows} tracks from Itunes related to {search}'.format(rows=len(data), search=serchTerm))
    return data

def loadDataFromAPI(serchTerm:str):
    traks= getItuneResponse(serchTerm, limit=80)   
    bulkLoad(traks) 

def getRandomArtist():
    artist=''
    try:
        conn =  mysql.connector.connect(host=ENDPOINT, user='root', passwd='root', port=PORT, database=DBNAME)
        cur = conn.cursor()
        cur.execute("""SELECT distinct artistName FROM itunes.Tracks  ORDER BY RAND() LIMIT 1""")
        artist = cur.fetchone()[0]

    except Exception as e:
        print("Database insertion failed due to {}".format(e)) 
    finally:
        cur.close()
        conn.close()
    return artist

