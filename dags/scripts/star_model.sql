--SNOWFLAKE
create_db=
"""
create schema DIM;
create schema FACT;create table DIM.DIM_ARTIST
(
    ARTISTID      NUMBER not null
        primary key,
    ARTISTNAME    VARCHAR(255),
    ARTISTVIEWURL VARCHAR,
    PREVIEWURL    VARCHAR
);

create table DIM.DIM_COUNTRY
(
    COUNTRYID   NUMBER not null
        primary key,
    COUNTRYNAME VARCHAR(40)
);

create table DIM.DIM_CURRENCY
(
    CURRENCYID   NUMBER not null
        primary key,
    CURRENCYCODE VARCHAR(40)
);

create table DIM.DIM_EXPLICITNESS
(
    EXPLICITNESSID   NUMBER not null
        primary key,
    EXPLICITNESSNAME VARCHAR(40)
);

create table DIM.DIM_GENRE
(
    GENREID   NUMBER not null
        primary key,
    GENRENAME VARCHAR(40)
);

create table DIM.DIM_COLLECTION
(
    COLLECTIONID           NUMBER not null
        primary key,
    COLLECTIONNAME         VARCHAR(255),
    COLLECTIONCENSOREDNAME VARCHAR(255),
    COLLECTIONVIEWURL      VARCHAR,
    COLLECTIONPRICE        DOUBLE,
    EXPLICITNESSID         NUMBER
        constraint DIM_COLLECTION_DIM_EXPLICITNESS_EXPLICITNESSID_FK
            references DIM.DIM_EXPLICITNESS,
    DISCCOUNT              NUMBER,
    GENREID                NUMBER
        constraint DIM_COLLECTION_DIM_GENRE_GENREID_FK
            references DIM.DIM_GENRE,
    TRACKCOUNT             NUMBER
);

create table DIM.DIM_KIND
(
    KINDID NUMBER not null
        constraint DIM_KIND_PK
            primary key,
    KIND   VARCHAR(255)
);
create table FAC_TRACKS
(
    TRACKID           NUMBER not null
        constraint FAC_TRACKS_PK
            primary key,
    TRACKNAME         VARCHAR(255),
    TRACKCENSOREDNAME VARCHAR(255),
    TRACKVIEWURL      VARCHAR,
    TRACKPRICE        NUMBER,
    RELEASEDATE       TIMESTAMPNTZ,
    EXPLICITNESSID    NUMBER
        constraint FAC_TRACKS_DIM_EXPLICITNESS_EXPLICITNESSID_FK
            references DIM.DIM_EXPLICITNESS,
    TRACKTIMEMILLIS   NUMBER,
    COUNTRYID         NUMBER
        constraint FAC_TRACKS_DIM_COUNTRY_COUNTRYID_FK
            references DIM.DIM_COUNTRY,
    CURRENCYID        NUMBER
        constraint FAC_TRACKS_DIM_CURRENCY_CURRENCYID_FK
            references DIM.DIM_CURRENCY,
    ARTISTID          NUMBER
        constraint FAC_TRACKS_DIM_ARTIST_ARTISTID_FK
            references DIM.DIM_ARTIST,
    COLLECTIONID      NUMBER
        constraint FAC_TRACKS_DIM_COLLECTION_COLLECTIONID_FK
            references DIM.DIM_COLLECTION,
    KINDID            NUMBER
        constraint FAC_TRACKS_DIM_KIND_KINDID_FK
            references DIM.DIM_KIND
);
"""


