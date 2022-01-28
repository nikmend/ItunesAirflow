USE itunes;
DROP TABLE IF EXISTS Artists;
CREATE TABLE Artists (
  artistId varchar(40) NOT NULL,
  artistName text DEFAULT NULL,
  artistViewUrl text DEFAULT NULL,
  PRIMARY KEY (artistId)
)
