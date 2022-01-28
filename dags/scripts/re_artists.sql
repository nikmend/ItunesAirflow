INSERT INTO itunes.Artists (artistId, artistName, artistViewUrl) 
SELECT artistId, max(artistName), max(artistViewUrl)
from itunes.Tracks
GROUP BY artistId
ON DUPLICATE KEY UPDATE artistViewUrl=VALUES(artistViewUrl)
