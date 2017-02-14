CREATE KEYSPACE IF NOT EXISTS test WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE test;

DROP TABLE IF EXISTS test.eav;

CREATE TABLE test.eav (
   ent text,
   attr text,
   value text,
   PRIMARY KEY (ent, attr)
   ) WITH comment = 'This table models the (lamentably) common Entity-Attribute-Value (EAV) pattern.'
   ;

DROP TABLE IF EXISTS test.rel;

CREATE TABLE test.rel (
    key int,
    updated bigint,
    created bigint,
    movie_title text,
    facenumber_in_poster int,
    actor_2_facebook_likes int,
    director_name text,
    duration int,
    plot_keywords text,
    content_rating text,
    imdb_score float,
    title_year int,
    movie_facebook_likes int,
    cast_total_facebook_likes int,
    gross bigint,
    country text,
    color text,
    num_critic_for_reviews int,
    actor_1_facebook_likes int,
    actor_3_facebook_likes int,
    actor_1_name text,
    actor_2_name text,
    language text,
    num_user_for_reviews int,
    actor_3_name text,
    genres text,
    budget bigint,
    movie_imdb_link text,
    num_voted_users int,
    director_facebook_likes int,
    aspect_ratio float,
    PRIMARY KEY (key)
    ) WITH comment = 'This table models a regular relational-style table.'
    ;
