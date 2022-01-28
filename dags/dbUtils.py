import snowflake.connector
import mysql.connector
from scripts.scripts import *

PASSWORD = "Parzival5&"
USER = 'NIKMEND56'
ACCOUNT = 'kn12851.us-east-2.aws'
WAREHOUSE = 'COMPUTE_WH'

DATABASE = 'ITUNES_DB'
ENDPOINT="db"
PORT="3306"
DBNAME="itunes"

def getDIMDict(tableName,):
  con = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
  database=DATABASE
  )
  dictTmp={}
  try:
      result = con.cursor().execute("SELECT * FROM DIM."+tableName)
      result_list = result.fetchall()
      for a , b in result_list:
        dictTmp[b]=a
  finally:
      con.cursor().close()
  con.cursor().close()
  return dictTmp
# -- (> ------------------- SECTION=Collections --------------------

def cleanCollections(colls,DIM_DICTS):
  newColls=[]
  for collection in colls:
    try:
      tmp=list(collection)
      tmp[0]=int(collection[0])
      tmp[1]=collection[1].replace('\"','').replace('\'','')
      tmp[2]=DIM_DICTS['DIM_EXPLICITNESS'].get(collection[2],1)
      tmp[4]=float(collection[4])
      tmp[5]=DIM_DICTS['DIM_EXPLICITNESS'].get(collection[5],1)
      tmp[6]=int(collection[6])
      tmp[7]=DIM_DICTS['DIM_GENRE'].get(collection[7],1)
      tmp[8]=int(collection[8])
      newColls.append(tuple(tmp))
    except:
      pass
  return newColls

def getCollectionsFromRaw(DIM_DICTS):
  collections=[]
  try:
      conn =  mysql.connector.connect(host=ENDPOINT, user='root', passwd='root', port=PORT, database=DBNAME)
      cur = conn.cursor()
      cur.execute(dim_collections)
      colls = cur.fetchall()
      collections=cleanCollections(colls,DIM_DICTS)
  except Exception as e:
      print("Database insertion failed due to {}".format(e)) 
  finally:
      cur.close()
      conn.close()
  
  return collections


def updateCollections(DIM_DICTS ):
  colls=getCollectionsFromRaw(DIM_DICTS)
  con = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
  database=DATABASE
  )
  result=''
  try:
      sql=dim_collections_insert.format(rows=str(colls)[1:-1]) #remove []
      print(sql)
      result = con.cursor().execute(sql)
      result = result.fetchall()
  except:
    pass
  finally:
      con.cursor().close()
  con.cursor().close()
  return result
  
# -- (> ------------------- SECTION= Artists --------------------



def cleanArtists(colls):
  newColls=[]
  for artists in colls:
    try:
      tmp=list(artists)
      tmp[0]=int(artists[0])
      tmp[1]=artists[1].replace('\"','').replace('\'','')
      newColls.append(tuple(tmp))
    except:
      pass
  return newColls

def getArtistsFromRaw():
  Artists=[]
  try:
      conn =  mysql.connector.connect(host=ENDPOINT, user='root', passwd='root', port=PORT, database=DBNAME)
      cur = conn.cursor()
      cur.execute(dim_artists)
      colls = cur.fetchall()
      Artists=cleanArtists(colls)
  except Exception as e:
      print("Database insertion failed due to {}".format(e)) 
  finally:
      cur.close()
      conn.close()
  
  return Artists


def updateArtists( ):
  colls=getArtistsFromRaw()
  con = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
  database=DATABASE
  )
  result=''
  try:
      sql=dim_artists_insert.format(rows=str(colls)[1:-1]) #remove []
      print(sql)
      result = con.cursor().execute(sql)
      result = result.fetchall()
  except Exception as e:
      print("Database insertion failed due to {}".format(e)) 
  finally:
      con.cursor().close()
  con.cursor().close()
  return result
  
# -- (> ------------------- SECTION=Tracks --------------------


def cleanTracks(colls,DIM_DICTS):
  newColls=[]
  for track in colls:
    try:
      tmp=list(track)
      tmp[0]=int(track[0])
      tmp[1]=track[1].replace('\"','').replace('\'','')
      tmp[2]=track[2].replace('\"','').replace('\'','')
      tmp[4]=float(track[4])
      tmp[6]=DIM_DICTS['DIM_EXPLICITNESS'].get(track[6],1)
      tmp[7]=int(track[7])
      tmp[8]=DIM_DICTS['DIM_COUNTRY'].get(track[8],1)
      tmp[9]=DIM_DICTS['DIM_CURRENCY'].get(track[9],1)
      tmp[10]=int(track[10])
      tmp[11]=int(track[11])
      tmp[12]=DIM_DICTS['DIM_KIND'].get(track[12],1)
      newColls.append(tuple(tmp))
    except:
      pass
  return newColls

def getTracksFromRaw(DIM_DICTS):

  tracks=[]
  try:
      conn =  mysql.connector.connect(host=ENDPOINT, user='root', passwd='root', port=PORT, database=DBNAME)
      cur = conn.cursor()
      cur.execute(fact_tracks)
      colls = cur.fetchall()
      tracks=cleanTracks(colls,DIM_DICTS)
  except Exception as e:
      print("Database insertion failed due to {}".format(e)) 
  finally:
      cur.close()
      conn.close()
  
  return tracks


def updateTracks(DIM_DICTS ):
  colls=getTracksFromRaw(DIM_DICTS)
  con = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
  database=DATABASE
  )
  result=''
  try:
      sql=fact_track_insert.format(rows=str(colls)[1:-1]) #remove []
      print(sql)
      result = con.cursor().execute(sql)
      result = result.fetchall()
  except:
    pass
  finally:
      con.cursor().close()
  con.cursor().close()
  return result
  


DIM_DICTS={'DIM_COUNTRY': {1: 'USA'},
 'DIM_CURRENCY': {1: 'USD'},
 'DIM_EXPLICITNESS': {1: 'cleaned', 2: 'explicit', 3: 'notExplicit'},
 'DIM_GENRE': {1: 'Adult Contemporary', 2: 'Afro House', 3: 'Afrobeats', 4: 'Alternative', 5: 'Alternative Folk', 6: 'Ambient', 7: 'Americana', 8: 'Anime', 9: 'Bass', 10: 'Big Band', 11: 'Blues', 12: 'Breakbeat', 13: 'CCM', 14: 'Chicago Blues', 15: "Children's Music", 16: 'Christian', 17: 'Christian & Gospel', 18: 'Christian Pop', 19: 'Christmas', 20: 'Christmas: Classical', 21: 'Christmas: R&B', 22: 'Christmas: Religious', 23: 'Classical', 24: 'Classical Crossover', 25: 'Comedy', 26: 'Contemporary Country', 27: 'Country', 28: 'Country Blues', 29: 'Dance', 30: 'Disco', 31: 'Dubstep', 32: 'Easy Listening', 33: 'Electronic', 34: 'Electronica', 35: 'Fitness & Workout', 36: 'Folk', 37: 'Folk-Rock', 38: 'French Pop', 39: 'Funk', 40: 'Gangsta Rap', 41: 'German Pop', 42: 'Gospel', 43: 'Hard Rock', 44: 'Hardcore Rap', 45: 'Hip-Hop', 46: 'Hip-Hop/Rap', 47: 'Holiday', 48: 'House', 49: 'Instrumental', 50: 'Jazz', 51: 'K-Pop', 52: 'Karaoke', 53: 'Latin', 54: 'Latin Rap', 55: 'Lounge', 56: 'Metal', 57: 'Modern Dancehall', 58: 'Motown', 59: 'Música tropical', 60: 'Musicals', 61: 'New Acoustic', 62: 'New Age', 63: 'Old School Rap', 64: 'Pop', 65: 'Pop Latino', 66: 'Praise & Worship', 67: 'Punk', 68: 'R&B/Soul', 69: 'Rap', 70: 'Reggae', 71: 'Relaxation', 72: 'Rock', 73: 'Sing-Along', 74: 'Singer/Songwriter', 75: 'Ska', 76: 'Soul', 77: 'Soundtrack', 78: 'South America', 79: 'Spoken Word', 80: 'Techno', 81: 'Traditional Folk', 82: 'Trance', 83: 'Tribute', 84: 'TV Soundtrack', 85: 'Underground Rap', 86: 'Urbano latino', 87: 'Vocal', 88: 'Vocal Jazz', 89: 'Vocal Pop', 90: 'West Coast Rap', 91: 'Worldwide'},
 'DIM_KIND': {1: 'music-video', 2: 'song'}}
 
#print(updateCollections(DIM_DICTS))
#print(updateTracks(DIM_DICTS))
#print(updateArtists())
#print(getDIMDict('DIM_GENRE'))