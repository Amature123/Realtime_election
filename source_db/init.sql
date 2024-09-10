CREATE TABLE IF NOT EXISTS
candidate(
    candidate_id VARCHAR(255) PRIMARY KEY ,
    candidate_name VARCHAR(255) ,
    party_affiliation VARCHAR(255) ,
    biography TEXT ,
    campaign_platform TEXT ,
    photo_url TEXT 
);

CREATE TABLE IF NOT EXISTS
voter(
    voter_id VARCHAR(255) PRIMARY KEY ,
    voter_name VARCHAR(255) ,
    age VARCHAR(255) ,
    gender VARCHAR(255) ,
    nationality VARCHAR(255) ,
    registion_number VARCHAR(255),
    address_street VARCHAR(255) ,
    address_city VARCHAR(255) ,
    address_state VARCHAR(255) ,
    address_country VARCHAR(255) ,
    address_postal_code VARCHAR(255) ,
    email VARCHAR(255) ,
    phone_number VARCHAR(255) ,
    picture_url TEXT 
);

CREATE TABLE IF NOT EXISTS
vote(
    voter_id VARCHAR(255) UNIQUE ,
    candidate_id VARCHAR(255) ,
    vote_date TIMESTAMP DEFAULT ,
    vote int DEFAULT 0,
    PRIMARY KEY(voter_id, candidate_id)
);