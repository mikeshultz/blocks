CREATE TABLE block (
    block_number serial PRIMARY KEY,
    block_timestamp timestamp without time zone NOT NULL,
    hash varchar(66) NOT NULL,
    miner varchar(42) NOT NULL,
    nonce numeric NOT NULL,
    difficulty numeric NOT NULL,
    gas_used numeric NOT NULL,
    gas_limit integer NOT NULL,
    size integer NOT NULL
);
CREATE INDEX block__block_timestamp ON block (block_timestamp);
CREATE INDEX block__hash ON block (hash);
CREATE INDEX block__miner ON block (miner);
CREATE INDEX block__hash_lower ON block ((lower(hash)));

CREATE TABLE transaction (
    hash varchar(66) PRIMARY KEY,
    dirty boolean DEFAULT true NOT NULL,
    block_number integer REFERENCES block(block_number),
    from_address varchar(42),
    to_address varchar(42),
    value numeric,
    gas_price numeric,
    gas_limit integer,
    nonce integer,
    input varchar
);
CREATE INDEX transaction__block_number ON transaction (block_number);
CREATE INDEX transaction__from_address ON transaction (from_address);
CREATE INDEX transaction__to_address ON transaction (to_address);
CREATE INDEX transaction__from_address_lower ON transaction ((lower(from_address)));
CREATE INDEX transaction__to_address_lower ON transaction ((lower(to_address)));

CREATE TABLE lock (
    lock_id serial PRIMARY KEY,
    updated timestamp without time zone NOT NULL DEFAULT now(),
    pid integer NOT NULL DEFAULT (random()*1000)::integer,
    name varchar
);
CREATE INDEX lock__updated ON lock (updated);
CREATE INDEX lock__pid ON lock (updated);
CREATE INDEX lock__name ON lock (name);
CREATE INDEX lock__name__updated ON lock (name,updated);