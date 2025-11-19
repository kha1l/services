CREATE TABLE special.franchisee (
    uuid        VARCHAR PRIMARY KEY,
    name        VARCHAR(200),
    group_uuid  VARCHAR(200),
    country     VARCHAR(10)
);

CREATE TABLE special.units (
    uuid        VARCHAR PRIMARY KEY,
    name        VARCHAR(200),
    timezone    INTEGER,
    latitude    VARCHAR(50),
    longitude   VARCHAR(50),
    country     VARCHAR(10),
    CONSTRAINT fk_units_franchisee
        FOREIGN KEY (uuid) REFERENCES special.franchisee(uuid)
);

CREATE TABLE special.users (
    id          SERIAL PRIMARY KEY,
    platform_id BIGINT,
    username    VARCHAR,
    name        VARCHAR,
    platform    varchar(20)
);

CREATE TABLE special.accounts (
    id          SERIAL PRIMARY KEY,
    sub         VARCHAR(100),
    name        VARCHAR(100),
    properties  VARCHAR(100),
    active      BOOLEAN,
    email       VARCHAR(150)
);

CREATE TABLE special.user_account (
    user_id     INTEGER NOT NULL,
    account_id  INTEGER NOT NULL,
    PRIMARY KEY (user_id, account_id),

    CONSTRAINT fk_user_account_user
        FOREIGN KEY (user_id) REFERENCES special.users(id) ON DELETE CASCADE,
    CONSTRAINT fk_user_account_account
        FOREIGN KEY (account_id) REFERENCES special.accounts(id) ON DELETE CASCADE
);

CREATE TABLE special.applications (
    id           SERIAL PRIMARY KEY,
    name         VARCHAR(100),
    client_id    VARCHAR(200),
    secret       VARCHAR(200),
    redirect_io  VARCHAR(200),
    redirect_com VARCHAR(200),
    challenge    VARCHAR(200),
    verifier     VARCHAR(200),
    scope        VARCHAR
);

CREATE TABLE special.tokens (
    id            SERIAL PRIMARY KEY,
    access        VARCHAR,
    refresh       VARCHAR,
    last_update   TIMESTAMP,
    account_id    INTEGER,
    application_id INTEGER,

    CONSTRAINT fk_tokens_account
        FOREIGN KEY (account_id) REFERENCES special.accounts(id),

    CONSTRAINT fk_tokens_application
        FOREIGN KEY (application_id) REFERENCES special.applications(id)
);

CREATE TABLE special.subscribes (
    uuid        VARCHAR NOT NULL,
    token_id    INTEGER NOT NULL,
    expires     TIMESTAMP,
    alias       VARCHAR(50),

    CONSTRAINT pk_subscribes PRIMARY KEY (uuid, token_id),

    CONSTRAINT fk_subscribes_token
        FOREIGN KEY (token_id) REFERENCES special.tokens(id) ON DELETE CASCADE,

    CONSTRAINT fk_subscribes_unit
        FOREIGN KEY (uuid) REFERENCES special.units(uuid) ON DELETE cascade
);