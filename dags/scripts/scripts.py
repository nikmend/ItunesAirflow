dim_collections="""
SELECT  t.collectionId, max(t.collectionName) AS collectionName , max(t.collectionCensoredName) AS collectionCensoredName, max(t.collectionViewUrl) AS collectionViewUrl,
max(t.collectionPrice) AS collectionPrice, max(t.trackExplicitness) AS trackExplicitness, max(t.discCount) AS discCount, max(t.primaryGenreName) AS primaryGenreName,
max(t.trackCount) AS trackCount
 from itunes.Tracks t
where t.creationDate >= NOW() - INTERVAL 2 HOUR
group by t.collectionId"""
dim_collections_insert="""
insert into DIM.DIM_COLLECTION (COLLECTIONID, COLLECTIONNAME, COLLECTIONCENSOREDNAME, COLLECTIONVIEWURL, COLLECTIONPRICE,
                            EXPLICITNESSID, DISCCOUNT, GENREID, TRACKCOUNT)
values {rows};
"""

dim_artists="""SELECT t.artistId, min(t.artistName)AS artistName,  max(t.artistViewUrl) as artistViewUrl, max(t.previewUrl) as previewUrl
from Tracks t
where t.creationDate >= NOW() - INTERVAL 2 HOUR
group by t.artistId"""

dim_artists_insert="""insert into DIM.DIM_ARTIST (ARTISTID, ARTISTNAME, ARTISTVIEWURL, PREVIEWURL) values {rows};"""

fact_tracks="""SELECT t.trackId, t.trackName, t.trackCensoredName,t.trackViewUrl, t.trackPrice, t.releaseDate,
       t.trackExplicitness, t.trackTimeMillis,t.country, t.currency, t.artistId,t.collectionId, t.kind
from Tracks t
where t.creationDate >= NOW() - INTERVAL 2 HOUR;
"""
fact_track_insert="""
insert into FACT.FAC_TRACKS (TRACKID, TRACKNAME, TRACKCENSOREDNAME, TRACKVIEWURL, TRACKPRICE, RELEASEDATE, EXPLICITNESSID,
                        TRACKTIMEMILLIS, COUNTRYID, CURRENCYID, ARTISTID, COLLECTIONID, KINDID)
values {rows};"""

fact_top="""
insert into FACT.FACT_TOP_ARTIST_PER_GENRES (GENREID, ARTISTID, "collectionCount", "trackCount", "avgPrice", POSITION)
 (select *
from (select a.GENREID, a.artistId,a.collectionCount,a.avgPrice,a.trackCount,
       rank() over (partition by GENREID
           order by a.collectionCount desc,
               a.avgPrice desc,
               a.trackCount desc) as position
FROM(
SELECT g.GENREID,t.artistId,count(t.collectionId) as collectionCount,sum(c.trackCount) as trackCount, avg(t.trackPrice) as avgPrice
FROM FACT.FAC_TRACKS t
INNER JOIN DIM.DIM_GENRE g
INNER JOIN DIM.DIM_ARTIST a
INNER JOIN DIM.DIM_COLLECTION c
group by t.artistId, g.GENREID) a) x where x.position <=5);"""